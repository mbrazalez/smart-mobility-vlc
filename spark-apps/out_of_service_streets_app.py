from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import sys
from datetime import datetime

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

#####################################################
# Spark Session
#####################################################
spark = (
    SparkSession.builder
    .appName("out-of-services-pipeline")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


UPLOADING_PATH = "s3a://smart-mobility-vlc/delta/out-of-services/landing-zone/"
SILVER_PATH = "s3a://smart-mobility-vlc/delta/out-of-services/2-silver/"
GOLD_PATH = "s3a://smart-mobility-vlc/delta/out-of-services/3-gold/"
CHECKPOINT_PATH = "s3a://smart-mobility-vlc/delta/out-of-services/_checkpoints/"

#####################################################
# Obtener archivos ya procesados
#####################################################
processed_files_df = spark.read.format("delta").load(CHECKPOINT_PATH)

processed_files = {
    row.archivo
    for row in processed_files_df.select("archivo").collect()
}

#####################################################
# Listar archivos del bucket
#####################################################
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jvm.java.net.URI.create(UPLOADING_PATH),
    spark._jsc.hadoopConfiguration()
)

path = spark._jvm.org.apache.hadoop.fs.Path(UPLOADING_PATH)

if not fs.exists(path):
    print("La carpeta de landing todavía no existe.")
    spark.stop()
    sys.exit(0)

status_list = fs.listStatus(path)

all_csv_files = [
    status.getPath().toString()
    for status in status_list
    if status.getPath().toString().lower().endswith(".csv")
]

#####################################################
# Obtener únicamente archivos nuevos
#####################################################
new_files = [
    file
    for file in all_csv_files
    if file not in processed_files
]

if not new_files:
    print("No hay archivos nuevos para procesar.")
    spark.stop()
    sys.exit(0)

#####################################################
# Ingesta en silver
#####################################################
schema_bronze = StructType([
    StructField("tramo_osm_id", IntegerType(), True),
    StructField("nombre_tramo", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("tipoincidencia", StringType(), True)
])

bronze_df = (
    spark.read
    .option("header", True)
    .option("delimiter", ";")
    .schema(schema_bronze)
    .csv(new_files)
)

silver_df = (
    bronze_df
    .withColumns({
        "nombre_tramo": F.from_json(
            F.regexp_replace(
                F.regexp_replace("nombre_tramo", '^"|"$', ""),
                '""',
                '"'
            ),
            ArrayType(StringType())
        ),
        "tipoincidencia": F.coalesce(
            F.col("tipoincidencia"),
            F.lit("No Especificado")
        ),
        "ingestion_ts": F.current_timestamp(),
        "source_file": F.input_file_name()
    })
    .filter(
        F.col("tramo_osm_id").isNotNull() &
        F.col("estado").isin("Abierta", "Cerrada")
    )
)


delta_curated = DeltaTable.forPath(spark,SILVER_PATH)

(
    delta_curated.alias("t")
    .merge(
        silver_df.alias("s"),
        """
            t.tramo_osm_id = s.tramo_osm_id
        """
    )
    .whenMatchedUpdate(
        condition="t.ingestion_ts < s.ingestion_ts",
        set={
            "nombre_tramo": "s.nombre_tramo",
            "estado": "s.estado",
            "tipoincidencia": "s.tipoincidencia",
            "ingestion_ts": "s.ingestion_ts",
            "source_file": "s.source_file"
        }
    )
    .whenNotMatchedInsert(
        values={
            "tramo_osm_id": "s.tramo_osm_id",
            "nombre_tramo": "s.nombre_tramo",
            "estado": "s.estado",
            "tipoincidencia": "s.tipoincidencia",
            "ingestion_ts": "s.ingestion_ts",
            "source_file": "s.source_file"
        }
    )
    .execute()
)

#####################################################
# Ingesta en Gold
#####################################################
schema_silver = StructType([
    StructField("tramo_osm_id", IntegerType(), False),
    StructField("nombre_tramo", ArrayType(StringType(), True)),
    StructField("estado", StringType(), True),
    StructField("tipoincidencia", StringType(), True),
    StructField("ingestion_ts", TimestampType(), True),
    StructField("source_file", StringType(), True)
])

silver_df = (
    spark.read
        .schema(schema_silver)
        .format("delta")
        .load(SILVER_PATH)
)

mapping_schema = StructType([
    StructField("tramo_osm_id", IntegerType(), True),
    StructField("nombre_tramo", StringType(), True),
    StructField("geometry_tramo", StringType(), True),
    StructField("barrio_id", IntegerType(), True),
    StructField("nombre_barrio", StringType(), True),
    StructField("distrito_id", IntegerType(), True),
    StructField("nombre_distrito", StringType(), True),
])

mappings_df = (
    spark.read
        .schema(mapping_schema)
        .option("header", True)
        .option("delimiter", ";")
        .csv("s3a://smart-mobility-vlc/delta/resources/callejero-con-mappings.csv")
)

gold_df = (
    silver_df.join(
        mappings_df,
        how="inner",
        on="tramo_osm_id"
    )
)

delta_curated = DeltaTable.forPath(spark,GOLD_PATH)

(
    delta_curated.alias("t")
    .merge(
        gold_df.alias("s"),
        """
            t.tramo_osm_id = s.tramo_osm_id AND
            t.barrio_id = s.barrio_id AND
            t.distrito_id = s.distrito_id AND
        """
    )
    .whenMatchedUpdate(
        condition="t.ingestion_ts < s.ingestion_ts",
        set={
            "nombre_tramo": "s.nombre_tramo",
            "estado": "s.estado",
            "tipoincidencia": "s.tipoincidencia",
            "barrio_id": "s.barrio_id",
            "distrito_id": "s.distrito_id",
            "geometry_tramo": "s.geometry_tramo",
            "ingestion_ts": "s.ingestion_ts",
            "source_file": "s.source_file"
        }
    )
    .whenNotMatchedInsert(
        values={
            "tramo_id": "s.tramo_id",
            "nombre_tramo": "s.nombre_tramo",
            "estado": "s.estado",
            "tipoincidencia": "s.tipoincidencia",
            "barrio_id": "s.barrio_id",
            "distrito_id": "s.distrito_id",
            "geometry_tramo": "s.geometry_tramo",
            "ingestion_ts": "s.ingestion_ts",
            "source_file": "s.source_file"
        }
    )
    .execute()
)

#####################################################
# Registrar archivos procesados
#####################################################
(
    spark.createDataFrame(
        [(f,) for f in new_files],
        ["archivo"]
    )
    .dropDuplicates(["archivo"])
    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
    .write
    .format("delta")
    .mode("append")
    .save(CHECKPOINT_PATH)
)

spark.stop()