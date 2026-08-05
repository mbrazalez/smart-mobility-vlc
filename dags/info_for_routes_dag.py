from datetime import datetime
import os
import boto3
import pickle
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sns import SnsHook


#####################################################
# Environment Variables
#####################################################
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_SNS_TOPIC = os.getenv("AWS_SNS_TOPIC")
INFO_FOR_ROUTE_SNAPSHOT_PATH = os.getenv("INFO_FOR_ROUTE_SNAPSHOT_PATH")+"*.parquet"
BUCKET_NAME = "smart-mobility-vlc"

s3 = boto3.client("s3")


#####################################################
# SNS notifications
#####################################################

def notify_failure(context):
    """Envía una alerta SNS cuando falla cualquier tarea."""

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
        subject="Route Calculations Pipeline",
        message="""
        Pipeline for route_calculation_app finished successfully.

        Snapshot generated correctly.
        NetworkX graph uploaded to S3.
        """,
    )


def apply_changes_in_graph():

    con = duckdb.connect()

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute(f"""
        SET s3_region='{AWS_DEFAULT_REGION}';
        SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';
        SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';
    """)

    latest = con.execute(f"""
        SELECT *
        FROM read_parquet('{INFO_FOR_ROUTE_SNAPSHOT_PATH}')
    """).fetch_df()

    con.close()

    if latest.empty:
        raise RuntimeError("Route snapshot is empty")

    response = s3.get_object(Bucket=BUCKET_NAME, Key="delta/route-calculations/graph.pkl")

    G = pickle.loads(response["Body"].read())

    for _,row in latest.iterrows():
        u = int(row["u"])
        v = int(row["v"])
        key = int(row["key"])

        if row["estado_via"] == "Cerrada":
            G[u][v][key]["weight"] = 1e12
        else:
            G[u][v][key]["weight"] = float(row["weight"])
            
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key="delta/route-calculations/graph.pkl",
        Body=pickle.dumps(G)
    )

#####################################################
# DAG
#####################################################
default_args = {
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="info_for_routes_dag",
    start_date=datetime(2026, 1, 1),
    schedule="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spark", "routes", "networkx"],
) as dag:

    update_data_for_routes = SparkSubmitOperator(
        task_id="update_data_for_routes",
        application="/opt/spark-apps/info_for_routes_app.py",
        conn_id="spark_default",
        name="route_calculation_pipeline",
        verbose=True,
        total_executor_cores=2,
        executor_cores=2,
        conf={
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )

    update_graph = PythonOperator(
        task_id="generate_graph",
        python_callable=apply_changes_in_graph,
    )

    success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    update_data_for_routes >> update_graph >> success