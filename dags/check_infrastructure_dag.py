from datetime import datetime
import os
import socket
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sns import SnsHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

#####################################################
# Environment Variables
#####################################################
BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_SNS_TOPIC = os.getenv("AWS_SNS_TOPIC")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")


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
        Infrastructure check failed

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
    """Envia una notificación cuando toda la infraestructura está disponible."""

    hook = SnsHook(aws_conn_id="aws_default")

    hook.publish_to_target(
        target_arn=AWS_SNS_TOPIC,
        subject="[OK] Infrastructure available",
        message="""
            Infrastructure check completed successfully.

            ✔ Spark
            ✔ Kafka
            ✔ AWS S3
            ✔ SQL Server
            """,
    )


#####################################################
# Spark check
#####################################################
def check_spark():

    try:
        socket.create_connection(("spark-master", 7077), timeout=5)
        print("✅ Spark Master disponible")

    except Exception as e:
        raise AirflowException(f"❌ Spark no disponible: {e}")


#####################################################
# Kafka check
#####################################################
def check_kafka():

    host, port = KAFKA_BROKER.split(":")
    port = int(port)

    try:
        socket.create_connection((host, port), timeout=5)
        print("✅ Kafka disponible")

    except Exception as e:
        raise AirflowException(f"❌ Kafka no disponible: {e}")


#####################################################
# AWS S3 check
#####################################################
def check_s3():

    hook = S3Hook(aws_conn_id="aws_default")

    if not hook.check_for_bucket(BUCKET_NAME):
        raise AirflowException(f"Bucket {BUCKET_NAME} no existe")

    print(f"✅ Bucket {BUCKET_NAME} encontrado")


#####################################################
# SQL Server check
#####################################################
def check_sqlserver():

    hook = MsSqlHook(mssql_conn_id="mssql_default")

    result = hook.get_first("SELECT 1")

    if result[0] != 1:
        raise AirflowException("No se pudo ejecutar SELECT 1")

    print("✅ SQL Server disponible")


#####################################################
# DAG
#####################################################
default_args = {
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="infrastructure_check_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["healthcheck"],
) as dag:

    spark = PythonOperator(
        task_id="check_spark",
        python_callable=check_spark,
    )

    kafka = PythonOperator(
        task_id="check_kafka",
        python_callable=check_kafka,
    )

    s3 = PythonOperator(
        task_id="check_s3",
        python_callable=check_s3,
    )

    sqlserver = PythonOperator(
        task_id="check_sqlserver",
        python_callable=check_sqlserver,
    )

    success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    trigger_traffic_dag = TriggerDagRunOperator(
        task_id="trigger_traffic_dag",
        trigger_dag_id="traffic_status_dag",
    )

    trigger_pollution_dag = TriggerDagRunOperator(
        task_id="trigger_pollution_dag",
        trigger_dag_id="pollution_levels_dag",
    )

    trigger_streets_dag = TriggerDagRunOperator(
        task_id="trigger_streets_dag",
        trigger_dag_id="out_of_service_streets_dag"
    )

    trigger_info_for_routes_dag = TriggerDagRunOperator(
        task_id="trigger_info_for_routes_dag",
        trigger_dag_id="info_for_routes_dag"
    )

    
    spark >> kafka >> s3 >> sqlserver >> success >> [
        trigger_traffic_dag,
        trigger_pollution_dag,
        trigger_streets_dag,
        trigger_info_for_routes_dag
    ]

