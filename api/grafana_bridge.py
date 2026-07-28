"""
Pont entre MongoDB et Grafana.
Expose les anomalies stockees dans MongoDB via une API JSON que le plugin
'JSON datasource' de Grafana sait consommer.

Le plugin Grafana JSON attend 4 endpoints :
  - GET  /            -> test de connexion (doit repondre 200)
  - POST /search      -> liste des "metriques" disponibles
  - POST /query       -> les donnees a afficher
  - POST /annotations -> (optionnel) evenements ponctuels
"""
from datetime import datetime
from fastapi import FastAPI, Request
from pymongo import MongoClient

app = FastAPI(title="Grafana Bridge - Machine Monitoring")

# Connexion MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["machine_monitoring"]
anomalies = db["anomalies"]


@app.get("/")
def health():
    """Test de connexion : Grafana verifie que le pont repond."""
    return {"status": "ok"}


@app.post("/search")
async def search(request: Request):
    """
    Grafana demande quelles 'metriques' sont disponibles.
    On propose une entree par type de donnee affichable.
    """
    return ["anomaly_count", "anomalies_by_machine", "anomalies_timeline"]


@app.post("/query")
async def query(request: Request):
    """
    Grafana demande les donnees a afficher.
    On renvoie les anomalies au format 'table' que Grafana comprend.
    """
    # On recupere les 100 anomalies les plus recentes
    docs = list(anomalies.find().sort("debut", -1).limit(100))

    # Format 'table' de Grafana : colonnes + lignes
    columns = [
        {"text": "debut", "type": "time"},
        {"text": "machine_id", "type": "string"},
        {"text": "sensor", "type": "string"},
        {"text": "value", "type": "number"},
        {"text": "zscore", "type": "number"},
        {"text": "anomaly_score", "type": "number"},
    ]

    rows = []
    for d in docs:
        # 'debut' est une date -> Grafana veut un timestamp en millisecondes
        debut_ms = int(d["debut"].timestamp() * 1000) if isinstance(d.get("debut"), datetime) else None
        rows.append([
            debut_ms,
            d.get("machine_id"),
            d.get("sensor"),
            d.get("value"),
            d.get("zscore"),
            d.get("anomaly_score"),
        ])

    return [{
        "type": "table",
        "columns": columns,
        "rows": rows,
    }]


@app.post("/annotations")
async def annotations(request: Request):
    """Non utilise pour l'instant, mais l'endpoint doit exister."""
    return []