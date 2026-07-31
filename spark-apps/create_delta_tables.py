from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

def create_delta_table(spark, path: str, schema: StructType):
    if not DeltaTable.isDeltaTable(spark, path):
        (
            spark.createDataFrame([], schema)
            .write
            .format("delta")
            .mode("overwrite")
            .save(path)
        )

######################################################
# Create Spark Session                               #
######################################################
spark = (
    SparkSession.builder
    .appName("init-deltalake")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

######################################################
# Generar delta table para bronze                    #
######################################################
BRONZE_PATH = "s3a://smart-mobility-vlc/delta/traffic-status/1-bronze/"

schema_bronze = StructType([
    StructField("raw_value", StringType(), True),
    StructField("event_ts", TimestampType(), True),
])

create_delta_table(spark, BRONZE_PATH, schema_bronze)

######################################################
# Generar delta table para silver                    #
######################################################
SILVER_PATH = "s3a://smart-mobility-vlc/delta/traffic-status/2-silver/"

schema_silver = StructType([
    StructField("tramo_id", IntegerType(), True),
    StructField("nombre_tramo", StringType(), True),
    StructField("estado", IntegerType(), True),
    StructField("event_ts", TimestampType(), True)
])

create_delta_table(spark, SILVER_PATH, schema_silver)

######################################################
# Generar delta table para gold                      #
######################################################
GOLD_PATH = "s3a://smart-mobility-vlc/delta/traffic-status/3-gold/"

schema_gold = StructType([
    StructField("tramo_id", IntegerType(), True),
    StructField("nombre_tramo", StringType(), True),
    StructField("estado", IntegerType(), True),
    StructField("geometry_tramo", StringType(), True),
    StructField("barrio_id", IntegerType(), True),
    StructField("distrito_id", IntegerType(), True),
    StructField("station_id", IntegerType(), True),
    StructField("event_ts", TimestampType(), True)
])

create_delta_table(spark, GOLD_PATH, schema_gold)

######################################################
# Generar delta table para CURATED DATA              #
######################################################
CURATED_PATH = "s3a://smart-mobility-vlc/delta/pollution-levels/curated-zone/"

schema_curated = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("station_name", StringType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("calidad_am", StringType(), True),
    StructField("geometry_station", StringType(), True),
    StructField("event_ts", TimestampType(), True)
])

create_delta_table(spark, CURATED_PATH, schema_curated)

#####################################################
# Creación de una tabla checkpoint
#####################################################
CHECKPOINT_PATH = "s3a://smart-mobility-vlc/delta/out-of-services/_checkpoints/"

schema_checkpoint = StructType([
    StructField("archivo", StringType(), True),
    StructField("processed_at", TimestampType(), True)
])

create_delta_table(spark, CHECKPOINT_PATH, schema_checkpoint)

######################################################
# Generar delta table para SILVER                    #
######################################################
SILVER_PATH = "s3a://smart-mobility-vlc/delta/out-of-services/2-silver/"

schema_silver = StructType([
    StructField("tramo_id", IntegerType(), False),
    StructField("nombre_tramo", ArrayType(StringType(), True)),
    StructField("estado", StringType(), True),
    StructField("tipoincidencia", StringType(), True),
    StructField("geometry_tramo", StringType(), True),
    StructField("ingestion_ts", TimestampType(), True),
    StructField("source_file", StringType(), True)
])

create_delta_table(spark, SILVER_PATH, schema_silver)

######################################################
# Generar delta table para GOLD                      #
######################################################
GOLD_PATH = "s3a://smart-mobility-vlc/delta/out-of-services/3-gold/"

schema_gold = StructType([
    StructField("tramo_id", IntegerType(), False),
    StructField("nombre_tramo", ArrayType(StringType(), True)),
    StructField("estado", StringType(), True),
    StructField("tipoincidencia", StringType(), True),
    StructField("barrio_id", IntegerType(), True),
    StructField("distrito_id", IntegerType(), True),
    StructField("geometry_tramo", StringType(), True),
    StructField("ingestion_ts", TimestampType(), True),
    StructField("source_file", StringType(), True)
])

create_delta_table(spark, GOLD_PATH, schema_gold)
