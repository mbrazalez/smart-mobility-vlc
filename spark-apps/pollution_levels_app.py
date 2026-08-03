from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import udf
import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl
import sys

#####################################################
# FIS Set-Up
#####################################################
pm10 = ctrl.Antecedent(np.arange(0, 151, 1), "pm10")
pm25 = ctrl.Antecedent(np.arange(0, 101, 1), "pm25")
wind = ctrl.Antecedent(np.arange(0, 41, 1), "wind")

risk = ctrl.Consequent(np.arange(0, 10.1, 0.1), "environmental_risk")

pm10["good"] = fuzz.zmf(pm10.universe, 18, 25)
pm10["fair"] = fuzz.pimf(pm10.universe, 19, 21, 33, 38)
pm10["moderate"] = fuzz.pimf(pm10.universe, 31, 36, 45, 55)
pm10["poor"] = fuzz.pimf(pm10.universe, 46, 51, 90, 110)
pm10["very_poor"] = fuzz.smf(pm10.universe, 91, 101)

pm25["good"] = fuzz.zmf(pm25.universe, 9, 12)
pm25["fair"] = fuzz.pimf(pm25.universe, 9, 11, 18, 22)
pm25["moderate"] = fuzz.pimf(pm25.universe, 18, 21, 24, 28)
pm25["poor"] = fuzz.pimf(pm25.universe, 23, 26, 45, 55)
pm25["very_poor"] = fuzz.smf(pm25.universe, 46, 55)

wind["low"] = fuzz.trapmf(wind.universe, [0, 0, 6, 12])
wind["medium"] = fuzz.trimf(wind.universe, [8, 18, 28])
wind["high"] = fuzz.trapmf(wind.universe, [22, 30, 40, 40])

risk["low"] = fuzz.trapmf(risk.universe, [0, 0, 2.5, 4])
risk["medium"] = fuzz.trimf(risk.universe, [3, 5, 7])
risk["high"] = fuzz.trapmf(risk.universe, [6, 8, 10, 10])

rules = [
    ctrl.Rule(pm10["good"] & pm25["good"], risk["low"]),
    ctrl.Rule(pm10["good"] & pm25["fair"] & ~wind["high"], risk["low"]),
    ctrl.Rule(pm10["good"] & pm25["moderate"] & ~wind["high"], risk["medium"]),
    ctrl.Rule(pm10["good"] & pm25["poor"] & ~wind["high"], risk["high"]),
    ctrl.Rule(pm10["good"] & pm25["very_poor"], risk["high"]),
    ctrl.Rule(pm10["fair"] & pm25["good"] & ~wind["high"], risk["low"]),
    ctrl.Rule(pm10["fair"] & pm25["fair"] & ~wind["high"], risk["low"]),
    ctrl.Rule(pm10["fair"] & pm25["moderate"] & ~wind["high"], risk["medium"]),
    ctrl.Rule(pm10["fair"] & pm25["poor"] & ~wind["high"], risk["high"]),
    ctrl.Rule(pm10["fair"] & pm25["very_poor"], risk["high"]),
    ctrl.Rule(pm10["moderate"] & pm25["good"] & ~wind["high"], risk["medium"]),
    ctrl.Rule(pm10["moderate"] & pm25["fair"] & ~wind["high"], risk["medium"]),
    ctrl.Rule(pm10["moderate"] & pm25["moderate"] & ~wind["high"], risk["medium"]),
    ctrl.Rule(pm10["moderate"] & pm25["poor"] & ~wind["high"], risk["high"]),
    ctrl.Rule(pm10["moderate"] & pm25["very_poor"], risk["high"]),
    ctrl.Rule(pm10["good"] & pm25["fair"] & wind["high"], risk["low"]),
    ctrl.Rule(pm10["good"] & pm25["moderate"] & wind["high"], risk["low"]),
    ctrl.Rule(pm10["good"] & pm25["poor"] & wind["high"], risk["medium"]),
    ctrl.Rule(pm10["fair"] & pm25["good"] & wind["high"], risk["low"]),
    ctrl.Rule(pm10["fair"] & pm25["fair"] & wind["high"], risk["low"]),
    ctrl.Rule(pm10["fair"] & pm25["moderate"] & wind["high"], risk["low"]),
    ctrl.Rule(pm10["fair"] & pm25["poor"] & wind["high"], risk["medium"]),
    ctrl.Rule(pm10["moderate"] & pm25["good"] & wind["high"], risk["low"]),
    ctrl.Rule(pm10["moderate"] & pm25["fair"] & wind["high"], risk["low"]),
    ctrl.Rule(pm10["moderate"] & pm25["moderate"] & wind["high"], risk["low"]),
    ctrl.Rule(pm10["moderate"] & pm25["poor"] & wind["high"], risk["medium"]),
    ctrl.Rule(pm10["poor"] & ~wind["high"], risk["high"]),
    ctrl.Rule(pm10["very_poor"], risk["high"]),
    ctrl.Rule(pm25["very_poor"], risk["high"])
]

fis = ctrl.ControlSystem(rules)

def compute_environmental_risk(pm10_value, pm25_value, wind_value):

    if (
        pm10_value is None or
        pm25_value is None or
        wind_value is None
    ):
        return None


    simulation = ctrl.ControlSystemSimulation(fis)

    wind_value = min(max(float(wind_value), 0), 40)
    pm10_value = min(max(float(pm10_value), 0), 150)
    pm25_value = min(max(float(pm25_value), 0), 100)

    simulation.input["wind"] = wind_value
    simulation.input["pm10"] = float(pm10_value)
    simulation.input["pm25"] = float(pm25_value)
    simulation.input["wind"] = float(wind_value)

    simulation.compute()

    return float(simulation.output["environmental_risk"])

risk_udf = udf(compute_environmental_risk, DoubleType())

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
                wind_kph,
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

curated_df = curated_df.withColumns({
    "pm10": F.coalesce(F.col("pm10"), F.lit(weather_df.pm10)),
    "pm25": F.coalesce(F.col("pm25"), F.lit(weather_df.pm2_5)),
    "so2": F.coalesce(F.col("so2"), F.lit(weather_df.so2)),
    "no2": F.coalesce(F.col("no2"), F.lit(weather_df.no2)),
    "o3": F.coalesce(F.col("o3"), F.lit(weather_df.o3)),
    "wind_kph": F.lit(weather_df.wind_kph or 0.0),
    "event_ts": F.coalesce(F.col("event_ts"), F.lit(weather_df.observed_at)),
    "fis_version": F.lit("v1")
})

curated_df = curated_df.withColumn(
    "environmental_risk",
    risk_udf(
        F.col("pm10"),
        F.col("pm25"),
        F.col("wind_kph"),
    )
)

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