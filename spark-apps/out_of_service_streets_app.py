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
CURATED_PATH = "s3a://smart-mobility-vlc/delta/out-of-services/curated-zone/"
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
# Curated Zone
#####################################################
schema_curated = StructType([
    StructField("tramo_id", IntegerType(), True),
    StructField("nombre_tramo", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("tipoincidencia", StringType(), True),
    StructField("barrio_id", IntegerType(), True),
    StructField("distrito_id", IntegerType(), True),
    StructField("geometry_tramo", StringType(), True),
])


curated_df = (
    spark.read
    .option("header", True)
    .option("delimiter", ";")
    .schema(schema_curated)
    .csv(new_files)
)

curated_df = (
    curated_df
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
        F.col("tramo_id").isNotNull() &
        F.col("estado").isin("Abierta", "Cerrada")
    )
)


delta_curated = DeltaTable.forPath(spark,CURATED_PATH)

(
    delta_curated.alias("t")
    .merge(
        curated_df.alias("s"),
        """
            t.tramo_id = s.tramo_id AND
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