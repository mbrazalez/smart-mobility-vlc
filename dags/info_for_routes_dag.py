from datetime import datetime
import os
import boto3
import pickle
import duckdb
import pandas as pd
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

GRAPH_BASE_KEY = "delta/route-calculations/graph_base.pkl"
GRAPH_KEY = "delta/route-calculations/graph.pkl"

def apply_changes_in_graph():

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        CREATE OR REPLACE SECRET aws (TYPE S3, PROVIDER credential_chain);
    """)

    latest = con.execute(f"""
        SELECT * FROM read_parquet('{INFO_FOR_ROUTE_SNAPSHOT_PATH}')
    """).fetch_df()

    con.close()

    if latest.empty:
        raise RuntimeError("Route snapshot is empty")

    # Se parte SIEMPRE del grafo base, no del último operativo. Así una
    # ejecución fallida no deja pesos a medio actualizar, y las aristas
    # que hayan dejado de aparecer en el conjunto de datos no conservan
    # indefinidamente un peso obsoleto.
    response = s3.get_object(Bucket=BUCKET_NAME, Key=GRAPH_BASE_KEY)
    G = pickle.loads(response["Body"].read())

    aplicadas = 0
    ausentes = 0

    for _, row in latest.iterrows():

        if pd.isna(row["u"]) or pd.isna(row["v"]):
            ausentes += 1
            continue

        u, v, key = int(row["u"]), int(row["v"]), int(row["key"])

        if not G.has_edge(u, v, key):
            ausentes += 1
            continue

        if row["estado_via"] == "Cerrada":
            G[u][v][key]["weight"] = 1e12
        else:
            G[u][v][key]["weight"] = float(row["weight"])

        aplicadas += 1

    # Toda arista debe tener peso: las que no aparezcan en el conjunto de
    # datos reciben su propia longitud, es decir, el factor mínimo.
    sin_peso = 0
    for _, _, d in G.edges(data=True):
        if d.get("weight") is None:
            d["weight"] = d["length"]
            sin_peso += 1

    print(f"Pesos aplicados: {aplicadas} | filas descartadas: {ausentes} "
          f"| aristas sin dato: {sin_peso}")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=GRAPH_KEY,
        Body=pickle.dumps(G),
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