import io
import random
import time
from datetime import datetime, timezone

import boto3
import pandas as pd

BUCKET_NAME = "smart-mobility-vlc"
PREFIX = "delta/out-of-services/1-bronze"

INTERVAL_SECONDS = 180

PCT_MAX_CERRADAS = 0.015 
MAX_CIERRES = 6
MAX_APERTURAS = 6

INCIDENCIAS = [
    "Obra",
    "Evento",
    "Accidente",
    "Manifestación",
    "Mantenimiento",
    "Avería",
    "Acto deportivo",
]

s3 = boto3.client("s3")

streets_df = pd.read_csv("callejero-cleaned.csv", sep=";")
streets_df["estado"] = "Abierta"
streets_df["tipoincidencia"] = None

MAX_CERRADAS = int(len(streets_df) * PCT_MAX_CERRADAS)
print(f"Tramos totales: {len(streets_df)} | tope de cortes simultáneos: {MAX_CERRADAS}")


while True:

    idx_cerradas = streets_df.index[streets_df["estado"] == "Cerrada"]
    idx_abiertas = streets_df.index[streets_df["estado"] == "Abierta"]

    n_aperturas = min(random.randint(0, MAX_APERTURAS), len(idx_cerradas))

    for row in random.sample(list(idx_cerradas), n_aperturas):
        streets_df.at[row, "estado"] = "Abierta"
        streets_df.at[row, "tipoincidencia"] = None

    cerradas_actuales = (streets_df["estado"] == "Cerrada").sum()
    margen = max(0, MAX_CERRADAS - cerradas_actuales)

    n_cierres = min(random.randint(0, MAX_CIERRES), margen, len(idx_abiertas))

    for row in random.sample(list(idx_abiertas), n_cierres):
        streets_df.at[row, "estado"] = "Cerrada"
        streets_df.at[row, "tipoincidencia"] = random.choice(INCIDENCIAS)

    total_cerradas = (streets_df["estado"] == "Cerrada").sum()

    snapshot = streets_df[
        ["tramo_osm_id", "nombre_tramo", "estado", "tipoincidencia"]
    ]

    csv_buffer = io.StringIO()
    snapshot.to_csv(csv_buffer, sep=";", index=False)

    now = datetime.now(timezone.utc)
    key = f"{PREFIX}/out_of_service_{now:%Y%m%d_%H%M%S}.csv"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    print(
        f"[{now:%H:%M:%S}] +{n_cierres} cortes, -{n_aperturas} resueltos "
        f"-> {total_cerradas} cerradas ({total_cerradas/len(streets_df):.2%})"
    )

    time.sleep(INTERVAL_SECONDS)