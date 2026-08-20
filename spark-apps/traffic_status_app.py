from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os

#####################################################
# Env Vars
#####################################################
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRAFFIC")

#####################################################
# Spark Session
#####################################################
spark = (
    SparkSession.builder
    .appName("traffic-status-pipeline")
    .config("spark.executor.cores", "2")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

#####################################################
# Read kafka stream
#####################################################
kafka_schema = StructType([
    StructField("source", StringType()),
    StructField(
        "attributes",
        StructType([
            StructField("gid", IntegerType()),
            StructField("denominacion", StringType()),
            StructField("estado", IntegerType()),
            StructField("idtramo", IntegerType()),
            StructField("fiwareid", StringType())
        ])
    ),
    StructField(
        "geometry",
        StructType([
            StructField(
                "paths",
                ArrayType(
                    ArrayType(
                        ArrayType(DoubleType())
                    )
                )
            )
        ])
    )
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

#####################################################
# Ingesta en Bronze
#####################################################
CHECKPOINT_PATH = "s3a://smart-mobility-vlc/delta/traffic-status/_checkpoints/"

BRONZE_PATH = "s3a://smart-mobility-vlc/delta/traffic-status/1-bronze/"

bronze_df = (
    kafka_df
    .select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("timestamp").alias("event_ts"),
    )
)

bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH + '1-bronze') \
    .start(BRONZE_PATH)

#####################################################
# Ingesta en Silver
#####################################################
SILVER_PATH = "s3a://smart-mobility-vlc/delta/traffic-status/2-silver/"

silver_df = (
    bronze_df
    .select(
        F.from_json("raw_value", kafka_schema).alias("json"),
        "event_ts"
    )
    .select(
        F.col("json.attributes.gid").alias("tramo_id"),
        F.col("json.attributes.estado").alias("estado"),
        F.col("json.attributes.denominacion").alias("nombre_tramo"),
        "event_ts"
    )
).filter(
    F.col("tramo_id").isNotNull() &
    F.col("nombre_tramo").isNotNull() &
    (F.col("estado") != 3) & # Cortado
    (F.col("estado") != 8) # Subterraneo cortado
)

silver_df = silver_df.withColumn("estado",
    F.when(F.col("estado").isin([1,2]), F.col("estado")) # Mantener Denso o Congestionado
    .when(F.col("estado").isin([0,4,9]), 0) # Considerar sin datos como Fluido
    .when(F.col("estado") == 5, 0) # Fluido subterraneo -> Fluido
    .when(F.col("estado") == 6, 1) # Denso subterraneo -> Denso
    .when(F.col("estado") == 7, 2) # Congestionado subterraneo -> Congestionado
    .otherwise(0)
)

silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH + '2-silver') \
    .start(SILVER_PATH)

#####################################################
# Ingesta en Gold 
#####################################################
GOLD_PATH = "s3a://smart-mobility-vlc/delta/traffic-status/3-gold/"

mapping_schema = StructType([
    StructField("tramo_id", IntegerType(), True),
    StructField("nombre_tramo", StringType(), True),
    StructField("geometry_tramo", StringType(), True),
    StructField("barrio_id", IntegerType(), True),
    StructField("distrito_id", IntegerType(), True),
    StructField("station_id", IntegerType(), True),
])


mappings_df = (
    spark.read
        .schema(mapping_schema)
        .option("header", True)
        .option("delimiter", ";")
        .csv("s3a://smart-mobility-vlc/delta/resources/tramos-con-mappings.csv")
).select("geometry_tramo", "barrio_id", "distrito_id", "station_id", "tramo_id")

gold_df = (
    silver_df.join(
        F.broadcast(mappings_df),
        how="inner",
        on="tramo_id",
    )
)

gold_query = (
    gold_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH + '3-gold')
    .start(GOLD_PATH)
)

gold_query.awaitTermination()

