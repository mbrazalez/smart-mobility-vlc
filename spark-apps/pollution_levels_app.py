from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import sys

#####################################################
# Spark Session
#####################################################
spark = (
    SparkSession.builder
    .appName("pollution-levels-pipeline")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

#####################################################
# Read S3 info                                      #
#####################################################
landing_key = sys.argv[1]
LANDING_PATH = f"s3a://smart-mobility-vlc/{landing_key}"

df_raw = (
    spark.read
    .option("multiline", True)
    .json(LANDING_PATH)
)

#####################################################
# Curated Zone
#####################################################
CURATED_PATH = "s3a://smart-mobility-vlc/delta/pollution-levels/curated-zone/"

curated_df = (
    df_raw
    .select(F.explode("features").alias("feature"))
    .select(
        F.col("feature.attributes.nombre").alias("station_name"),
        F.col("feature.attributes.pm10").cast("double").alias("pm10"),
        F.col("feature.attributes.pm25").cast("double").alias("pm25"),
        F.col("feature.attributes.so2").cast("double").alias("so2"),
        F.col("feature.attributes.no2").cast("double").alias("no2"),
        F.col("feature.attributes.o3").cast("double").alias("o3"),
        F.col("feature.attributes.calidad_am").alias("calidad_am"),
        (
            F.col("feature.attributes.fecha_carg") / 1000
        ).cast("timestamp").alias("event_ts")
    )
)

# Join to get geometry and station_id
mapping_schema = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("station_name", StringType(), True),
    StructField("geometry_station", StringType(), True)
])

mappings_df = (
    spark.read
        .schema(mapping_schema)
        .option("header", True)
        .option("delimiter", ";")
        .csv("s3a://smart-mobility-vlc/delta/resources/estaciones-calidad-ambiental.csv")
).select("station_id", "station_name", "geometry_station")

# Fill missing values using WeatherAPI ingestion
jdbc_url = (
    "jdbc:sqlserver://sqlserver:1433;"
    "database=smart_moblity_vlc"
)
connection_properties = {
    "user": "sa",
    "password": "Password1234%",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "encrypt": "false",
    "trustServerCertificate": "true"
}

weather_df = (
    spark.read.jdbc(
        url=jdbc_url,
        table="""
        (
            SELECT TOP (1)
                pm2_5,
                pm10,
                so2,
                no2,
                o3,
                air_quality,
                observed_at
            FROM dbo.weather_raw
            ORDER BY observed_at DESC
        ) weather
        """,
        properties=connection_properties
    )
).first()

curated_df = curated_df.join(
    F.broadcast(mappings_df), 
    how="inner", 
    on="station_name"
)

cols = ["pm10", "pm25", "so2", "no2", "o3", "calidad_am"]

curated_df = curated_df.withColumns({
    "pm10": F.coalesce(F.col("pm10"), F.lit(weather_df.pm10)),
    "pm25": F.coalesce(F.col("pm25"), F.lit(weather_df.pm2_5)),
    "so2": F.coalesce(F.col("so2"), F.lit(weather_df.so2)),
    "no2": F.coalesce(F.col("no2"), F.lit(weather_df.no2)),
    "o3": F.coalesce(F.col("o3"), F.lit(weather_df.o3)),
    "calidad_am": F.coalesce(F.col("calidad_am"), F.lit(weather_df.air_quality)),
    "event_ts": F.coalesce(F.col("event_ts"), F.lit(weather_df.observed_at)),
})

delta_curated = DeltaTable.forPath(spark,CURATED_PATH)

delta_curated.alias("t").merge(
    curated_df.alias("s"),
    """
    t.station_id = s.station_id
    AND
    t.event_ts = s.event_ts
    """
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

spark.stop()