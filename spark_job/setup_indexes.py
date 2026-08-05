from pymongo import MongoClient, ASCENDING, DESCENDING

client = MongoClient("mongodb://localhost:27017")
db = client["machine_monitoring"]

# --- Collection readings ---
readings = db["readings"]

# Index TTL : supprime les mesures de plus de 3600 secondes (1 heure)
# NB : fonctionne uniquement si event_time est de type Date
readings.create_index(
    [("event_time", ASCENDING)],
    expireAfterSeconds=3600,
    name="ttl_event_time",
)

# Index de performance : requetes par machine + temps
readings.create_index(
    [("machine_id", ASCENDING), ("event_time", DESCENDING)],
    name="idx_machine_time",
)

# --- Collection anomalies ---
anomalies = db["anomalies"]

# Index de performance : requetes par machine + date de fenetre
anomalies.create_index(
    [("machine_id", ASCENDING), ("debut", DESCENDING)],
    name="idx_machine_debut",
)

# Index sur le score, pour filtrer rapidement les anomalies critiques
anomalies.create_index([("anomaly_score", DESCENDING)], name="idx_score")

print("Index crees :")
for coll_name in ["readings", "anomalies"]:
    print(f"\n{coll_name} :")
    for idx in db[coll_name].list_indexes():
        print(f"  - {idx['name']}")

client.close()
