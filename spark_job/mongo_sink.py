"""
Pont entre Spark Streaming et MongoDB.
La fonction write_anomalies_to_mongo() est appelee par foreachBatch :
pour chaque lot (batch) d'anomalies, elle insere les lignes dans MongoDB.
"""
from pymongo import MongoClient

# --- Configuration MongoDB ---
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "machine_monitoring"
ANOMALIES_COLLECTION = "anomalies"
READINGS_COLLECTION = "readings"

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
            # replace_one avec upsert : remplace si existe, insere sinon
            collection.replace_one(key, r, upsert=True)
        print(f">>> Batch {batch_id} : {len(rows)} anomalie(s) upsert dans MongoDB")
    finally:
        client.close()
        
def write_readings_to_mongo(batch_df, batch_id):
    """
    Ecrit un lot de mesures brutes dans MongoDB (collection 'readings').
    Contrairement aux anomalies, on insere tout (historique complet des capteurs).
    Sert a tracer les courbes de capteurs dans le temps.
    """
    if batch_df.isEmpty():
        return

    rows = [row.asDict() for row in batch_df.collect()]

    client = MongoClient(MONGO_URI)
    try:
        collection = client[DB_NAME][READINGS_COLLECTION]
        collection.insert_many(rows)
        print(f">>> Batch {batch_id} : {len(rows)} mesure(s) ecrite(s) dans readings")
    finally:
        client.close()