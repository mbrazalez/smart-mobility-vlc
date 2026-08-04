from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
import networkx as nx

def build_graph():
    G = nx.MultiDiGraph()

    for row in latest.itertuples(index=False):

        G.add_edge(
            row.u,
            row.v,
            key=row.key,

            # Coste utilizado por A*
            weight=row.weight,

            # Información adicional
            length=row.length,
            geometry=row.geometry_osm,
            tramo_osm_id=row.tramo_osm_id,
            tramo_id=row.tramo_id,
            station_id=row.station_id,
            estado_trafico=row.estado_trafico,
            environmental_risk=row.environmental_risk,
        )




with DAG(
    dag_id="route_calculations_dag",
    start_date=datetime(2026, 1, 1),
    schedule="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "routes", "networkx"],
) as dag:

    #################################################
    # Generación del snapshot (Spark)
    #################################################

    calculate_routes = SparkSubmitOperator(
        task_id="calculate_routes",
        application="/opt/spark-apps/route_calculation_app.py",
        conn_id="spark_default",
        name="route_calculation_pipeline",
        verbose=True,
        total_executor_cores=2,
        executor_cores=2,
        conf={
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )


    generate_graph = PythonOperator(
        task_id="generate_graph",
        python_callable=build_graph,
    )


    end = EmptyOperator(
        task_id="end"
    )


    calculate_routes >> generate_graph >> end