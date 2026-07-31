from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pyodbc
from airflow.providers.amazon.aws.hooks.sns import SnsHook
import os

AWS_SNS_TOPIC = os.getenv("AWS_SNS_TOPIC")

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
    Initialization of data storages failed.

    DAG: {dag_id}
    Task: {task_id}
    Execution: {execution_date}

    Error:
    {exception}
    """

    hook = SnsHook(aws_conn_id="aws_default")

    hook.publish_to_target(
        target_arn=AWS_SNS_TOPIC,
        subject="[ALERT] Data Storage Initialization Failed",
        message=message,
    )


def notify_success():
    """Envia una notificación cuando la inicialización ha finalizado correctamente."""

    hook = SnsHook(aws_conn_id="aws_default")

    hook.publish_to_target(
        target_arn=AWS_SNS_TOPIC,
        subject="Data Storage Initialization",
        message="""
The SQL Server database and Delta Lake tables were initialized successfully.
""",
    )



def start_up_sqlserver():
    conn_str_master = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=sqlserver,1433;"
        "DATABASE=master;"
        "UID=sa;"
        "PWD=Password1234%;"
        "TrustServerCertificate=yes;"
    )

    conn_master = pyodbc.connect(conn_str_master, autocommit=True)
    cur_master = conn_master.cursor()

    cur_master.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'smart_moblity_vlc')
        BEGIN
            CREATE DATABASE smart_moblity_vlc;
        END
    """)

    cur_master.close()
    conn_master.close()

    conn_str_db = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=sqlserver,1433;"
        "DATABASE=smart_moblity_vlc;"
        "UID=sa;"
        "PWD=Password1234%;"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str_db, autocommit=True)
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM sys.tables WHERE name = 'weather_raw'
        )
        BEGIN
            CREATE TABLE dbo.weather_raw (
                id INT IDENTITY(1,1) PRIMARY KEY,
                city NVARCHAR(50),
                pm2_5 FLOAT,
                pm10 FLOAT,
                so2 FLOAT,
                no2 FLOAT,
                o3 FLOAT,
                air_quality NVARCHAR(50),
                observed_at DATETIME2,
            );
        END
    """)

    cursor.close()
    conn.close()

default_args = {
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="init_datastorages_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    tags=["sqlserver", "bootstrap", "spark", "delta"],
) as dag:

    init_sql_tables = PythonOperator(
        task_id="start_up_sqlserver",
        python_callable=start_up_sqlserver,
    )

    #################################################
    # Create Delta Tables
    #################################################
    init_deltalake = SparkSubmitOperator(
        task_id="init_deltalake",
        application="/opt/spark-apps/create_delta_tables.py",
        conn_id="spark_default",
        name="init-deltalake",
        verbose=True,
    )

    #################################################
    # Success notification
    #################################################
    success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    trigger_check_infrastructure = TriggerDagRunOperator(
        task_id="trigger_check_infrastructure",
        trigger_dag_id="infrastructure_check_dag",
    )

    init_sql_tables >> init_deltalake >> success >> trigger_check_infrastructure  
