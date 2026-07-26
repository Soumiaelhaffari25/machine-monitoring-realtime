"""
Simulateur de capteurs industriels.
Génère des mesures réalistes (avec anomalies) et les publie sur Kafka en Avro.
"""

import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# --- Configuration ---
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "sensor-readings"

# Les machines simulees et leurs capteurs
MACHINES = ["machine-01", "machine-02", "machine-03"]

# Pour chaque capteur : valeur normale moyenne, ecart-type du bruit, unite
SENSORS = {
    "temperature": {"mean": 65.0, "noise": 2.0, "unit": "C"},
    "vibration":   {"mean": 2.5,  "noise": 0.3, "unit": "mm/s"},
    "pressure":    {"mean": 5.0,  "noise": 0.2, "unit": "bar"},
    "current":     {"mean": 30.0, "noise": 1.5, "unit": "A"},
}

ANOMALY_PROBABILITY = 0.04  # ~4% des mesures sont anormales


def load_schema() -> str:
    """Charge le schema Avro depuis le fichier .avsc."""
    schema_path = Path(__file__).parent / "schemas" / "reading.avsc"
    return schema_path.read_text(encoding="utf-8")


def make_reading(machine_id: str, sensor: str, cfg: dict) -> dict:
    """Genere une mesure : normale la plupart du temps, parfois anormale."""
    value = random.gauss(cfg["mean"], cfg["noise"])

    # Injection d'anomalie : un pic brutal vers le haut
    if random.random() < ANOMALY_PROBABILITY:
        value = cfg["mean"] * random.uniform(1.5, 2.5)

    return {
        "machine_id": machine_id,
        "sensor": sensor,
        "value": round(value, 2),
        "unit": cfg["unit"],
        "event_time": int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def delivery_report(err, msg):
    """Callback appele apres chaque tentative d'envoi."""
    if err is not None:
        print(f"Echec envoi: {err}")


def main():
    # Client Schema Registry
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # Serialiseur Avro : valide chaque message contre le schema
    avro_serializer = AvroSerializer(
        schema_registry,
        load_schema(),
    )

    # Producteur Kafka
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    print(f"Publication sur le topic '{TOPIC}'. Ctrl+C pour arreter.")
    count = 0

    try:
        while True:
            for machine_id in MACHINES:
                for sensor, cfg in SENSORS.items():
                    reading = make_reading(machine_id, sensor, cfg)

                    # Serialisation Avro (le message est valide ici)
                    serialized = avro_serializer(
                        reading,
                        SerializationContext(TOPIC, MessageField.VALUE),
                    )

                    # Publication, cle = machine_id -> partitionnement
                    producer.produce(
                        topic=TOPIC,
                        key=machine_id,
                        value=serialized,
                        on_delivery=delivery_report,
                    )
                    count += 1

            producer.poll(0)      # traite les callbacks en attente
            print(f"{count} mesures envoyees", end="\r")
            time.sleep(1)         # une salve par seconde

    except KeyboardInterrupt:
        print("\nArret demande.")
    finally:
        producer.flush()          # s'assure que tout est parti
        print("Producteur ferme proprement.")


if __name__ == "__main__":
    main()