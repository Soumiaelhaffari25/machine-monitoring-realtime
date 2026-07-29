"""
Pont entre MongoDB et Grafana.
Expose les anomalies stockees dans MongoDB via une API JSON.
"""
from datetime import datetime
from fastapi import FastAPI
from pymongo import MongoClient
from fastapi import Request 

app = FastAPI(title="Grafana Bridge - Machine Monitoring")

client = MongoClient("mongodb://localhost:27017")
db = client["machine_monitoring"]
anomalies = db["anomalies"]
readings = db["readings"]


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/query")
async def query():
    """Tableau detaille : les 100 anomalies les plus recentes."""
    docs = list(anomalies.find().sort("debut", -1).limit(100))
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
        debut_ms = int(d["debut"].timestamp() * 1000) if isinstance(d.get("debut"), datetime) else None
        rows.append([
            debut_ms, d.get("machine_id"), d.get("sensor"),
            d.get("value"), d.get("zscore"), d.get("anomaly_score"),
        ])
    return [{"type": "table", "columns": columns, "rows": rows}]


@app.post("/stats")
async def stats():
    """Chiffres cles pour les cartes du haut."""
    total = anomalies.count_documents({})
    critiques = anomalies.count_documents({"anomaly_score": {"$gte": 0.8}})

    pipeline = [
        {"$group": {"_id": "$machine_id", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 1},
    ]
    top = list(anomalies.aggregate(pipeline))
    top_machine = top[0]["_id"] if top else "aucune"

    zmax_doc = list(anomalies.find().sort("zscore", -1).limit(1))
    zmax = round(zmax_doc[0]["zscore"], 2) if zmax_doc else 0

    machines_risque = len(anomalies.distinct("machine_id", {"anomaly_score": {"$gte": 0.8}}))
    machines_total = len(anomalies.distinct("machine_id"))

    return [{
        "type": "table",
        "columns": [
            {"text": "total", "type": "number"},
            {"text": "critiques", "type": "number"},
            {"text": "top_machine", "type": "string"},
            {"text": "zscore_max", "type": "number"},
            {"text": "machines_risque", "type": "number"},
            {"text": "machines_total", "type": "number"},
        ],
        "rows": [[total, critiques, top_machine, zmax, machines_risque, machines_total]],
    }]


@app.post("/timeline")
async def timeline():
    """Anomalies groupees par minute, pour la courbe temporelle."""
    pipeline = [
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%dT%H:%M:00", "date": "$debut"}},
            "n": {"$sum": 1},
        }},
        {"$sort": {"_id": 1}},
    ]
    docs = list(anomalies.aggregate(pipeline))
    rows = []
    for d in docs:
        try:
            ts = int(datetime.strptime(d["_id"], "%Y-%m-%dT%H:%M:00").timestamp() * 1000)
            rows.append([ts, d["n"]])
        except Exception:
            continue
    return [{
        "type": "table",
        "columns": [
            {"text": "time", "type": "time"},
            {"text": "count", "type": "number"},
        ],
        "rows": rows,
    }]


@app.post("/by_machine")
async def by_machine():
    """Nombre d'anomalies par machine."""
    pipeline = [
        {"$group": {"_id": "$machine_id", "n": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    docs = list(anomalies.aggregate(pipeline))
    return [{
        "type": "table",
        "columns": [
            {"text": "machine_id", "type": "string"},
            {"text": "count", "type": "number"},
        ],
        "rows": [[d["_id"], d["n"]] for d in docs],
    }]


@app.post("/by_sensor")
async def by_sensor():
    """Nombre d'anomalies par type de capteur."""
    pipeline = [
        {"$group": {"_id": "$sensor", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
    ]
    docs = list(anomalies.aggregate(pipeline))
    return [{
        "type": "table",
        "columns": [
            {"text": "sensor", "type": "string"},
            {"text": "count", "type": "number"},
        ],
        "rows": [[d["_id"], d["n"]] for d in docs],
    }]


@app.post("/recent")
async def recent():
    """Anomalies recentes avec niveau de severite."""
    docs = list(anomalies.find().sort("debut", -1).limit(50))
    columns = [
        {"text": "debut", "type": "time"},
        {"text": "machine_id", "type": "string"},
        {"text": "sensor", "type": "string"},
        {"text": "value", "type": "number"},
        {"text": "zscore", "type": "number"},
        {"text": "severity", "type": "string"},
    ]
    rows = []
    for d in docs:
        debut_ms = int(d["debut"].timestamp() * 1000) if isinstance(d.get("debut"), datetime) else None
        score = d.get("anomaly_score", 0)
        if score >= 0.8:
            severity = "CRITICAL"
        elif score >= 0.4:
            severity = "WARNING"
        else:
            severity = "INFO"
        rows.append([
            debut_ms, d.get("machine_id"), d.get("sensor"),
            round(d.get("value", 0), 2), round(d.get("zscore", 0), 2), severity,
        ])
    return [{"type": "table", "columns": columns, "rows": rows}]

@app.post("/by_rule")
async def by_rule():
    """Repartition des anomalies par regle declenchee (pour le camembert)."""
    pipeline = [
        {"$group": {"_id": "$rule", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
    ]
    docs = list(anomalies.aggregate(pipeline))
    return [{
        "type": "table",
        "columns": [
            {"text": "rule", "type": "string"},
            {"text": "count", "type": "number"},
        ],
        "rows": [[d["_id"] if d["_id"] else "Inconnu", d["n"]] for d in docs],
    }]

@app.post("/sensor_readings")
async def sensor_readings(request: Request):
    """
    Evolution des capteurs dans le temps pour UNE machine.
    Renvoie une COLONNE PAR CAPTEUR -> 4 courbes distinctes dans Grafana.
    """
    machine = "machine-01"
    try:
        body = await request.json()
        if body and "machine" in body:
            machine = body["machine"]
    except Exception:
        pass

    pipeline = [
        {"$match": {"machine_id": machine}},
        {"$group": {
            "_id": {
                "minute": {"$dateToString": {"format": "%Y-%m-%dT%H:%M:00", "date": "$event_time"}},
                "sensor": "$sensor",
            },
            "avg_value": {"$avg": "$value"},
        }},
        {"$sort": {"_id.minute": 1}},
    ]
    docs = list(readings.aggregate(pipeline))

    # On regroupe par minute, avec une valeur par capteur
    by_minute = {}
    for d in docs:
        minute = d["_id"]["minute"]
        sensor = d["_id"]["sensor"]
        if minute not in by_minute:
            by_minute[minute] = {}
        by_minute[minute][sensor] = round(d["avg_value"], 3)

    rows = []
    for minute in sorted(by_minute.keys()):
        try:
            ts = int(datetime.strptime(minute, "%Y-%m-%dT%H:%M:00").timestamp() * 1000)
        except Exception:
            continue
        vals = by_minute[minute]
        rows.append([
            ts,
            vals.get("temperature"),
            vals.get("vibration"),
            vals.get("pressure"),
            vals.get("current"),
        ])

    return [{
        "type": "table",
        "columns": [
            {"text": "time", "type": "time"},
            {"text": "temperature", "type": "number"},
            {"text": "vibration", "type": "number"},
            {"text": "pressure", "type": "number"},
            {"text": "current", "type": "number"},
        ],
        "rows": rows,
    }]
    
@app.post("/machines_list")
async def machines_list():
    """Liste des machines disponibles, pour remplir le menu deroulant Grafana."""
    machines = sorted(readings.distinct("machine_id"))
    # Format attendu par la variable Grafana (plugin JSON) : liste de {text, value}
    return [{"text": m, "value": m} for m in machines]


@app.post("/machine_status")
async def machine_status():
    """Etat de chaque machine : critique / warning / normal."""
    machines = anomalies.distinct("machine_id")
    columns = [
        {"text": "machine_id", "type": "string"},
        {"text": "etat", "type": "string"},
        {"text": "nb_anomalies", "type": "number"},
    ]
    rows = []
    for m in sorted(machines):
        nb = anomalies.count_documents({"machine_id": m})
        nb_crit = anomalies.count_documents({"machine_id": m, "anomaly_score": {"$gte": 0.8}})
        if nb_crit > 0:
            etat = "CRITIQUE"
        elif nb > 0:
            etat = "WARNING"
        else:
            etat = "NORMAL"
        rows.append([m, etat, nb])
    return [{"type": "table", "columns": columns, "rows": rows}]