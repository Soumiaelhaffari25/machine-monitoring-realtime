"""
Job Spark Structured Streaming complet - Phase 2 assemblee.
Pipeline : Kafka -> decodage Avro -> DLQ -> fenetres -> moteur de regles -> anomalies.
Checkpoints actifs pour la garantie exactly-once (reprise apres crash).
"""
import os
import sys
from pathlib import Path

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYTHONUTF8"] = "1"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, window, avg, stddev, last, count
from pyspark.sql.avro.functions import from_avro

sys.path.insert(0, str(Path(__file__).parent))
from rules import compute_anomaly_score
from mongo_sink import write_anomalies_to_mongo, write_readings_to_mongo
# --- Configuration ---
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "sensor-readings"
DLQ_TOPIC = "dead-letter"
CHECKPOINT_BASE = "checkpoints"   # dossier racine des checkpoints
PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.spark:spark-avro_2.12:3.5.1",
])
SCHEMA_STR = (Path(__file__).parent.parent / "producer" / "schemas" / "reading.avsc").read_text(encoding="utf-8")


def build_spark():
    return (SparkSession.builder
            .appName("machine-monitoring-streaming")
            .master("local[*]")
            .config("spark.jars.packages", PACKAGES)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
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
        col("signal_physical"), col("signal_zscore"), col("anomaly_score"),
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