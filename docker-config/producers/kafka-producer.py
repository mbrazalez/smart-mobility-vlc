import json
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer
import os

API_URL = os.getenv("API_URL")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
INTERVAL = int(os.getenv("INTERVAL"))


def load_traffic_data():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    return data.get("features", [])


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


if __name__ == "__main__":
    try:
        while True:

            features = load_traffic_data()

            ingestion_timestamp = datetime.now(timezone.utc).isoformat()

            for feature in features:

                event = {
                    "source": "valencia-open-data",
                    "attributes": feature.get("attributes", {}),
                    "geometry": feature.get("geometry"),
                }
                

                producer.send(
                    KAFKA_TOPIC,
                    key=str(
                        feature.get("attributes", {}).get("fiwareid", "")
                    ).encode("utf-8"),
                    value=event,
                )

                producer.flush()

            print(
                f"{len(features)} eventos enviados "
                f"({ingestion_timestamp})"
            )

            # La API se actualiza aproximadamente cada 3 minutos
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\nProducer detenido manualmente")

    finally:
        producer.close()