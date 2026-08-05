import os
from pymongo import MongoClient
from prometheus_client import Counter

# --- Configuration MongoDB ---
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "machine_monitoring"
ANOMALIES_COLLECTION = "anomalies"
READINGS_COLLECTION = "readings"

# --- Metriques Prometheus ---
SPARK_ANOMALIES = Counter(
    "spark_anomalies_total",
    "Nombre total d'anomalies detectees",
    ["machine_id", "sensor"],
)
SPARK_READINGS = Counter(
    "spark_readings_processed_total",
    "Nombre total de mesures traitees",
)


def write_anomalies_to_mongo(batch_df, batch_id):
    """
    Ecrit un lot d'anomalies dans MongoDB en mode UPSERT.
    Chaque anomalie a une cle unique (machine + capteur + fenetre).
    Si elle existe deja, on la remplace au lieu d'ajouter un doublon.
    """
    if batch_df.isEmpty():
        return

    rows = [row.asDict() for row in batch_df.collect()]

    client = MongoClient(MONGO_URI)
    try:
        collection = client[DB_NAME][ANOMALIES_COLLECTION]
        for r in rows:
            # Cle unique : une seule anomalie par (machine, capteur, fenetre)
            key = {
                "machine_id": r.get("machine_id"),
                "sensor": r.get("sensor"),
                "debut": r.get("debut"),
            }
            collection.replace_one(key, r, upsert=True)
            # Metrique : on compte chaque anomalie
            SPARK_ANOMALIES.labels(
                machine_id=r.get("machine_id", "?"),
                sensor=r.get("sensor", "?"),
            ).inc()
        print(f">>> Batch {batch_id} : {len(rows)} anomalie(s) upsert dans MongoDB")
    finally:
        client.close()


def write_readings_to_mongo(batch_df, batch_id):
    """
    Ecrit un lot de mesures brutes dans MongoDB (collection 'readings').
    On insere tout (historique complet des capteurs) pour tracer les courbes.
    """
    if batch_df.isEmpty():
        return

    rows = [row.asDict() for row in batch_df.collect()]

    client = MongoClient(MONGO_URI)
    try:
        collection = client[DB_NAME][READINGS_COLLECTION]
        collection.insert_many(rows)
        # Metrique : on compte les mesures traitees
        SPARK_READINGS.inc(len(rows))
        print(f">>> Batch {batch_id} : {len(rows)} mesure(s) ecrite(s) dans readings")
    finally:
        client.close()
