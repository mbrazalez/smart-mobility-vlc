from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window

#####################################################
# Spark Session
#####################################################
spark = (
    SparkSession.builder
    .appName("info-for-routes-pipeline")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

out_of_service_silver_path = "s3a://smart-mobility-vlc/delta/out-of-services/2-silver/"
traffic_silver_path = "s3a://smart-mobility-vlc/delta/traffic-status/2-silver/"
pollution_levels_path = "s3a://smart-mobility-vlc/delta/pollution-levels/curated-zone/"
mapping_path = "s3a://smart-mobility-vlc/delta/resources/mappings-entre-tramos.csv"
route_path = "s3a://smart-mobility-vlc/delta/route-calculations/history/"
 
mapping_schema = StructType([
    StructField("tramo_osm_id", IntegerType(), True),
    StructField("tramo_id", IntegerType(), True),
    StructField("station_id", IntegerType(), True),
    StructField("osmid", StringType(), True),
    StructField("u", IntegerType(), True),
    StructField("v", IntegerType(), True),
    StructField("key", IntegerType(), True),
    StructField("length", FloatType(), True),
    StructField("geometry_osm", StringType(), True),
])

mappings_df = (
    spark.read
        .schema(mapping_schema)
        .option("header", True)
        .option("delimiter", ";")
        .csv("file:///opt/resources/mappings-entre-tramos.csv")
).select(
    "tramo_osm_id",
    "tramo_id",
    "station_id",
    "u",
    "v",
    "length",
    "geometry_osm"
)

w = Window.partitionBy("tramo_osm_id").orderBy(F.col("ingestion_ts").desc())

out_df = (
    spark.read
        .format("delta")
        .load(out_of_service_silver_path)
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
        .select("tramo_osm_id", F.col("estado").alias("estado_via"))
)

w = Window.partitionBy("station_id").orderBy(F.col("event_ts").desc())

pollution_df = (
    spark.read
        .format("delta")
        .load(pollution_levels_path)
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
        .select("station_id", "environmental_risk")
)

w = Window.partitionBy("tramo_id").orderBy(F.col("event_ts").desc())

traffic_df = (
    spark.read
        .format("delta")
        .load(traffic_silver_path)
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
        .select("tramo_id", F.col("estado").alias("estado_trafico"))
)

route_df = (
    mappings_df
        .join(traffic_df, on="tramo_id", how="left")
        .join(pollution_df, on="station_id", how="left")
        .join(out_df, on="tramo_osm_id", how="left")
)

route_df = (
    route_df
        .filter(
            (F.col("estado_via").isNull()) |
            (F.col("estado_via") != "Cerrada")
        )
)

route_df = route_df.withColumns({
    "environmental_risk": F.coalesce("environmental_risk", F.lit(10.0)),
    "estado_trafico": F.coalesce("estado_trafico", F.lit(1)),
    "event_ts": F.current_timestamp()
})

route_df = route_df.withColumn(
    "weight",
    F.col("length") * (
        1 *
        F.col("environmental_risk") / 10 *
        F.col("estado_trafico") * 0.5
    )
).select(
    "tramo_osm_id",
    "tramo_id",
    "station_id",
    "u",
    "v",
    "key",
    "length",
    "geometry_osm",
    "estado_trafico",
    "weight",
    "event_ts"
)

route_df.write.format("delta").mode("append").save(route_path)

latest_path = "s3a://smart-mobility-vlc/delta/route-calculations/latest/"

route_df.write.mode("overwrite").parquet("s3a://smart-mobility-vlc/route-calculations/latest/")