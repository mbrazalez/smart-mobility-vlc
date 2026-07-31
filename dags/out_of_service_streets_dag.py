from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


#####################################################
# Configuración
#####################################################
BUCKET_NAME = "smart-mobility-vlc"
UPLOAD_PREFIX = "delta/out-of-services/landing-zone/"

#####################################################
# Comprobar si existen nuevos CSV
#####################################################
def check_new_csv():

    hook = S3Hook(aws_conn_id="aws_default")

    s3 = hook.get_conn()

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=UPLOAD_PREFIX
    )

    objects = response.get("Contents", [])

    current_files = sorted(
        obj["Key"]
        for obj in objects
        if obj["Key"].lower().endswith(".csv")
    )

    previous_files = Variable.get(
        "out_of_services_csvs",
        default_var="[]",
        deserialize_json=True
    )

    new_files = set(current_files) - set(previous_files)

    if new_files:
        Variable.set(
            "out_of_services_csvs",
            current_files,
            serialize_json=True
        )

        return "out_of_services_pipeline"

    return "end"


#####################################################
# DAG
#####################################################
with DAG(
    dag_id="out_of_service_streets_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "s3", "batch", "delta"],
) as dag:

    #################################################
    # Esperar a que exista algún CSV
    #################################################
    wait_for_csv = S3KeySensor(
        task_id="wait_for_csv",
        bucket_name=BUCKET_NAME,
        bucket_key=f"{UPLOAD_PREFIX}*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    branch = BranchPythonOperator(
        task_id="check_new_csv",
        python_callable=check_new_csv,
    )

    out_of_services_pipeline = SparkSubmitOperator(
        task_id="out_of_services_pipeline",
        application="/opt/spark-apps/out_of_service_streets_app.py",
        conn_id="spark_default",
        name="out_of_services_pipeline",
        verbose=True,
        total_executor_cores=2,
        executor_cores=2,
        conf={
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
        )

    #################################################
    # Fin
    #################################################
    end = EmptyOperator(
        task_id="end"
    )

    #################################################
    # Dependencias
    #################################################
    wait_for_csv >> branch

    branch >> out_of_services_pipeline >> end
    branch >> end