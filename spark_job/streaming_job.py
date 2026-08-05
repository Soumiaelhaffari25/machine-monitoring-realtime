import os
import sys
from pathlib import Path

os.environ["PYTHONUTF8"] = "1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, window, avg, stddev, last, count
from pyspark.sql.avro.functions import from_avro

from prometheus_client import start_http_server, Counter

SPARK_DLQ = Counter("spark_dlq_total", "Nombre total de messages corrompus (DLQ)")

sys.path.insert(0, str(Path(__file__).parent))
from rules import compute_anomaly_score
from mongo_sink import write_anomalies_to_mongo, write_readings_to_mongo
# --- Configuration ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "sensor-readings"
DLQ_TOPIC = "dead-letter"
CHECKPOINT_BASE = "checkpoints"   # dossier racine des checkpoints
PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.spark:spark-avro_2.12:3.5.1",
])

# Chemin du schema : variable d'env en priorite (K8s), sinon recherche locale
SCHEMA_PATH = os.environ.get("SCHEMA_PATH")
if not SCHEMA_PATH:
    # Recherche pour l'execution locale hors conteneur
    candidates = [
        Path(__file__).parent / "schemas" / "reading.avsc",
        Path(__file__).parent.parent / "producer" / "schemas" / "reading.avsc",
    ]
    SCHEMA_PATH = next((str(p) for p in candidates if p.exists()), str(candidates[0]))
SCHEMA_STR = Path(SCHEMA_PATH).read_text(encoding="utf-8")


def build_spark():
    return (SparkSession.builder
            .appName("machine-monitoring-streaming")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    # Serveur de metriques Prometheus pour le job Spark
    start_http_server(8002)
    print(">>> Metriques Spark exposees sur http://localhost:8002/metrics")
    
    print("\n>>> Job de streaming complet demarre...")

    # 1. Lecture Kafka
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    # 2. Decodage Avro permissif (on garde le brut pour la DLQ)
    attempted = raw.select(
        col("value").alias("raw_value"),
        expr("substring(value, 6, length(value)-5)").alias("avro_value"),
    )
    decoded = attempted.select(
        col("raw_value"),
        from_avro(col("avro_value"), SCHEMA_STR, {"mode": "PERMISSIVE"}).alias("d"),
    )

    # 3. Separation valides / invalides (DLQ)
    valid = decoded.filter(col("d.machine_id").isNotNull()).select("d.*")
    invalid = decoded.filter(col("d.machine_id").isNull()).select(
        col("raw_value").alias("value")
    )

    # 4. Agregations fenetrees sur les valides
    windowed = (valid
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("machine_id"), col("sensor"),
        )
        .agg(
            avg("value").alias("mu"),
            stddev("value").alias("sigma"),
            last("value").alias("value"),
            count("value").alias("n"),
        ))

    # 5. Moteur de regles
    scored = compute_anomaly_score(windowed)
    anomalies = scored.filter(col("is_anomaly")).select(
        col("window.start").alias("debut"),
        col("machine_id"), col("sensor"), col("value"),
        col("mu"), col("sigma"), col("zscore"),
        col("signal_physical"), col("signal_zscore"), col("signal_drift"),
        col("anomaly_score"), col("rule"),
    )

    # 6. Deux sorties, CHACUNE avec son propre checkpoint

    # Sortie A : anomalies -> MongoDB (via foreachBatch et le pont mongo_sink)
    q_anomalies = (anomalies.writeStream
                   .foreachBatch(write_anomalies_to_mongo)
                   .outputMode("update")
                   .option("checkpointLocation", f"{CHECKPOINT_BASE}/anomalies")
                   .start())

    # Sortie B : messages invalides -> topic dead-letter (checkpoint dedie)
    q_dlq = (invalid.writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("topic", DLQ_TOPIC)
             .option("checkpointLocation", f"{CHECKPOINT_BASE}/dlq")
             .outputMode("append")
             .start())
    
    # Sortie C : toutes les mesures -> MongoDB collection 'readings'
    q_readings = (valid.writeStream
                  .foreachBatch(write_readings_to_mongo)
                  .outputMode("append")
                  .option("checkpointLocation", f"{CHECKPOINT_BASE}/readings")
                  .start())

    print(">>> Pipeline actif : anomalies -> console | invalides -> dead-letter")
    print(">>> Checkpoints dans le dossier 'checkpoints/'. Ctrl+C pour arreter.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
