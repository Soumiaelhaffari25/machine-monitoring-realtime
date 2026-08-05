import os
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import start_http_server, Counter, Gauge

# --- Configuration ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = "sensor-readings"
MACHINES = ["machine-01", "machine-02", "machine-03"]

# Charge la calibration reelle issue du dataset NASA
CALIB_PATH = Path(__file__).parent / "calibration.json"
CALIB = json.loads(CALIB_PATH.read_text(encoding="utf-8"))
VIB = CALIB["vibration"]

# Capteurs : vibration calibree sur donnees reelles, les autres sur ordres de grandeur industriels
SENSORS = {
    "vibration":   {"mean": VIB["normal_mean"], "noise": VIB["normal_std"],
                    "anomaly_mean": VIB["anomaly_mean"], "unit": "g"},
    "temperature": {"mean": 45.0, "noise": 1.5, "anomaly_mean": 85.0, "unit": "C"},
    "pressure":    {"mean": 5.0,  "noise": 0.2, "anomaly_mean": 8.0,  "unit": "bar"},
    "current":     {"mean": 30.0, "noise": 1.5, "anomaly_mean": 55.0, "unit": "A"},
}

# Etat de derive par (machine, capteur) : 0.0 = sain, 1.0 = pleinement degrade
drift_state = {(m, s): 0.0 for m in MACHINES for s in SENSORS}

ANOMALY_SPIKE_PROB = 0.02   # pic brutal ponctuel
DRIFT_START_PROB = 0.001    # demarrage d'une derive progressive
DRIFT_INCREMENT = 0.02      # vitesse de montee de la derive

# --- Metriques Prometheus ---
# Compteur : nombre total de messages envoyes (ne fait qu'augmenter)
MESSAGES_SENT = Counter(
    "producer_messages_sent_total",
    "Nombre total de mesures envoyees a Kafka",
    ["machine_id", "sensor"],   # on peut ventiler par machine et capteur
)
# Jauge : nombre d'anomalies injectees (peut monter et descendre)
ANOMALIES_INJECTED = Counter(
    "producer_anomalies_injected_total",
    "Nombre d'anomalies volontairement injectees",
    ["sensor"],
)


def load_schema() -> str:
    return (Path(__file__).parent / "schemas" / "reading.avsc").read_text(encoding="utf-8")


def make_reading(machine_id: str, sensor: str, cfg: dict) -> dict:
    key = (machine_id, sensor)

    # 1. Valeur de base = normale + bruit
    value = random.gauss(cfg["mean"], cfg["noise"])

    # 2. Derive progressive : parfois une usure demarre, puis monte lentement
    if drift_state[key] == 0.0 and random.random() < DRIFT_START_PROB:
        drift_state[key] = 0.01  # amorce la derive
    if drift_state[key] > 0.0:
        drift_state[key] = min(1.0, drift_state[key] + DRIFT_INCREMENT)
        # interpolation entre valeur normale et valeur degradee
        target = cfg["anomaly_mean"]
        value = cfg["mean"] + drift_state[key] * (target - cfg["mean"])
        value += random.gauss(0, cfg["noise"] * (1 + drift_state[key] * 5))  # bruit croissant
        if drift_state[key] >= 1.0 and random.random() < 0.1:
            drift_state[key] = 0.0  # la panne "reset" (maintenance simulee)

    # 3. Pic brutal ponctuel (independant de la derive)
    elif random.random() < ANOMALY_SPIKE_PROB:
        value = cfg["anomaly_mean"] * random.uniform(1.0, 1.5)

    return {
        "machine_id": machine_id,
        "sensor": sensor,
        "value": round(value, 4),
        "unit": cfg["unit"],
        "event_time": int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Echec envoi: {err}")


def main():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry, load_schema())
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    print(f"Publication sur '{TOPIC}'. Vibration calibree sur NASA Bearing Dataset. Ctrl+C pour arreter.")
    count = 0

    # Demarre le serveur de metriques Prometheus sur le port 8001
    start_http_server(8001)
    print("Metriques Prometheus exposees sur http://localhost:8001/metrics")  
    
    try:
        while True:
            for machine_id in MACHINES:
                for sensor, cfg in SENSORS.items():
                    reading = make_reading(machine_id, sensor, cfg)
                    serialized = avro_serializer(
                        reading, SerializationContext(TOPIC, MessageField.VALUE)
                    )
                    producer.produce(
                        topic=TOPIC, key=machine_id,
                        value=serialized, on_delivery=delivery_report,
                    )
                    count += 1
                    MESSAGES_SENT.labels(machine_id=machine_id, sensor=sensor).inc()
            producer.poll(0)
            print(f"{count} mesures envoyees", end="\r")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nArret demande.")
    finally:
        producer.flush()
        print("Producteur ferme proprement.")


if __name__ == "__main__":
    main()
