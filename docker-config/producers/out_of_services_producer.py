import io
import random
import time
from datetime import datetime
import boto3
import pandas as pd

BUCKET_NAME = "smart-mobility-vlc"
PREFIX = "delta/out-of-services/1-bronze"

INTERVAL_SECONDS = 180
MAX_CHANGES = 20

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

streets_df = pd.read_csv(
    "callejero-cleaned.csv",
    sep=";"
)
streets_df["estado"] = "Abierta"
streets_df["tipoincidencia"] = None

while True:

    snapshot = streets_df.copy()

    n_changes = random.randint(1, MAX_CHANGES)
    rows = random.sample(list(snapshot.index), n_changes)

    for row in rows:

        current = snapshot.at[row, "estado"]

        if current == "Abierta":
            snapshot.at[row, "estado"] = "Cerrada"
            snapshot.at[row, "tipoincidencia"] = random.choice(INCIDENCIAS)

        else:
            snapshot.at[row, "estado"] = "Abierta"
            snapshot.at[row, "tipoincidencia"] = None

    streets_df = snapshot.copy()

    snapshot = snapshot[
        [
            "tramo_osm_id",
            "nombre_tramo",
            "estado",
            "tipoincidencia",
        ]
    ]

    csv_buffer = io.StringIO()

    print(snapshot.columns)
    print(snapshot.head())
    snapshot.to_csv(
        csv_buffer,
        sep=";",
        index=False
    )

    now = datetime.utcnow()

    key = (
        f"{PREFIX}/"
        f"out_of_service_{now:%Y%m%d_%H%M%S}.csv"
    )

    snapshot.to_csv("test.csv",sep=";")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    #################################################
    time.sleep(INTERVAL_SECONDS)