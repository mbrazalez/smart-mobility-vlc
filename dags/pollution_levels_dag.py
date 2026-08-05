from datetime import datetime, timezone
import os
import json
import requests
import boto3
import pyodbc
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.sns import SnsHook


#####################################################
# Environment Variables
#####################################################
API_URL = os.getenv("API_URL")
BUCKET = os.getenv("AWS_S3_BUCKET_NAME")
AWS_SNS_TOPIC = os.getenv("AWS_SNS_TOPIC")
s3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_DEFAULT_REGION")
)
CITY = "Valencia"
WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY")
SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=sqlserver,1433;"
    "DATABASE=smart_moblity_vlc;"
    "UID=sa;"
    "PWD=Password1234%;"
    "TrustServerCertificate=yes;"
)

#####################################################
# SNS notifications
#####################################################
def notify_failure(context):
    """Envia una alerta SNS cuando falla cualquier tarea."""

    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    execution_date = context["logical_date"]
    exception = context["exception"]

    message = f"""
Pipeline failed

DAG: {dag_id}
Task: {task_id}
Execution: {execution_date}

Error:
{exception}
"""

    hook = SnsHook(aws_conn_id="aws_default")

    hook.publish_to_target(
        target_arn=AWS_SNS_TOPIC,
        subject=f"[ALERT] {task_id} failed",
        message=message,
    )


def notify_success():

    hook = SnsHook(aws_conn_id="aws_default")

    hook.publish_to_target(
        target_arn=AWS_SNS_TOPIC,
        subject="Pollution Levels Pipeline",
        message="""
        Pipeline for pollution_levels_app finished successfully.
        """,
    )


#####################################################
# API requests
#####################################################
def fetch_and_store_pollution_levels():

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    payload = response.json()

    now = datetime.now(timezone.utc)

    key = (
        "delta/pollution-levels/landing-zone/"
        f"year={now:%Y}/"
        f"month={now:%m}/"
        f"day={now:%d}/"
        f"pollution_levels_{now:%Y%m%dT%H%M%S}.json"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False),
        ContentType="application/json"
    )

    print(f"Uploaded snapshot to s3://{BUCKET}/{key}")

    return key 

def fetch_and_store_weather(**context):
    
    url = "http://api.weatherapi.com/v1/current.json"
    params = {
        "key": WEATHER_API_KEY,
        "q": CITY,
        "aqi": "yes"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    pm2_5 = data["current"]["air_quality"]["pm2_5"]
    pm10 = data["current"]["air_quality"]["pm10"]
    so2 = data["current"]["air_quality"]["so2"]
    no2 = data["current"]["air_quality"]["no2"]
    o3 = data["current"]["air_quality"]["o3"]
    wind_kph = data["current"]["wind_kph"]
    observed_at = data["current"]["last_updated"]

    conn = pyodbc.connect(SQLSERVER_CONN_STR, autocommit=True)
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO dbo.weather_raw (
            city,
            pm2_5,
            pm10,
            so2,
            no2,
            o3,
            wind_kph,
            observed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            CITY,
            pm2_5,
            pm10,
            so2,
            no2,
            o3,
            wind_kph,
            observed_at
        )
    )

    cursor.close()
    conn.close()

#####################################################
# DAG
#####################################################
default_args = {
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="pollution_levels_dag",
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["pollution", "api", "spark", "s3"],
) as dag:

    #################################################
    # Download info from APIs
    #################################################
    get_weatherAPI_info = PythonOperator(
        task_id="get_weatherAPI_info",
        python_callable=fetch_and_store_weather,
    )

    get_pollution_levels = PythonOperator(
        task_id="get_pollution_levels",
        python_callable=fetch_and_store_pollution_levels,
    )
    
    #################################################
    # Launch Spark Pipeline
    #################################################
    start_pollution_levels_pipeline = SparkSubmitOperator(
        task_id="pollution_levels_pipeline",
        application="/opt/spark-apps/pollution_levels_app.py",
        conn_id="spark_default",
        name="pollution_levels_pipeline",
        total_executor_cores=2,
        executor_cores=2,
        verbose=True,
        application_args=[
            "{{ ti.xcom_pull(task_ids='get_pollution_levels') }}"
        ]
    )

    #################################################
    # Success notification
    #################################################
    success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    get_weatherAPI_info >> get_pollution_levels >> start_pollution_levels_pipeline >> success