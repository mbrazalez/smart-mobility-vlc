from datetime import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.sns import SnsHook
from kafka.admin import KafkaAdminClient, NewTopic
import os


#####################################################
# Environment Variables
#####################################################
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRAFFIC")
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
        subject="Traffic Status Pipeline",
        message="""
            Pipeline for traffic_status_app was launched successfully.
            """,
    )

#####################################################
# Kafka Topic
#####################################################
def ensure_kafka_topic():

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            client_id="airflow"
        )

        topics = admin.list_topics()

        if KAFKA_TOPIC in topics:
            print(f"✅ Kafka topic '{KAFKA_TOPIC}' already exists.")
        else:
            print(f"⚠️ Kafka topic '{KAFKA_TOPIC}' does not exist. Creating...")

            admin.create_topics(
                [
                    NewTopic(
                        name=KAFKA_TOPIC,
                        num_partitions=1,
                        replication_factor=1,
                    )
                ]
            )

            print(f"✅ Kafka topic '{KAFKA_TOPIC}' created.")

        admin.close()

    except Exception as e:
        raise AirflowException(f"Error checking Kafka topic: {e}")


#####################################################
# DAG
#####################################################
default_args = {
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="traffic_status_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["traffic", "streaming", "kafka", "spark"],
) as dag:

    #################################################
    # Check/Create Kafka Topic
    #################################################
    check_kafka_topic = PythonOperator(
        task_id="check_kafka_topic",
        python_callable=ensure_kafka_topic,
    )

    #################################################
    # Launch Spark Pipeline
    #################################################
    start_traffic_status_pipeline = SparkSubmitOperator(
        task_id="start_traffic_status_pipeline",
        application="/opt/spark-apps/traffic_status_app.py",
        conn_id="spark_default",
        name="traffic_status_pipeline",
        total_executor_cores=2,
        executor_cores=2,
        verbose=True,
    )

    #################################################
    # Success notification
    #################################################
    success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    check_kafka_topic >> [start_traffic_status_pipeline, success]