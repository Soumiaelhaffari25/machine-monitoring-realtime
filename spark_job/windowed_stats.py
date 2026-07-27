"""
Palier P5 : agregations fenetrees glissantes par machine et capteur, avec watermark.
Calcule moyenne (mu), ecart-type (sigma), min, max, count sur des fenetres temporelles.
Base statistique du moteur de regles (Z-score).
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
from pyspark.sql.functions import (
    col, expr, window, avg, stddev, min as smin, max as smax, count
)
from pyspark.sql.avro.functions import from_avro

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "sensor-readings"
PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.spark:spark-avro_2.12:3.5.1",
])
SCHEMA_STR = (Path(__file__).parent.parent / "producer" / "schemas" / "reading.avsc").read_text(encoding="utf-8")

spark = (SparkSession.builder
         .appName("windowed-stats")
         .master("local[*]")
         .config("spark.jars.packages", PACKAGES)
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("\n>>> Spark demarre, agregations fenetrees...")

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", TOPIC)
      .option("startingOffsets", "latest")
      .load())

# Decodage Avro permissif + on ne garde que les messages valides
decoded = (df.select(expr("substring(value, 6, length(value)-5)").alias("avro_value"))
           .select(from_avro(col("avro_value"), SCHEMA_STR, {"mode": "PERMISSIVE"}).alias("d"))
           .filter(col("d.machine_id").isNotNull())
           .select("d.*"))

# event_time est deja un TIMESTAMP (grace au logicalType timestamp-millis du schema Avro)
readings = decoded

# Agregation fenetree glissante : fenetre de 1 minute, glissant toutes les 20 secondes
windowed = (readings
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        window(col("event_time"), "1 minute", "20 seconds"),
        col("machine_id"),
        col("sensor"),
    )
    .agg(
        avg("value").alias("mu"),
        stddev("value").alias("sigma"),
        smin("value").alias("min_val"),
        smax("value").alias("max_val"),
        count("value").alias("n"),
    ))

display = windowed.select(
    col("window.start").alias("debut"),
    col("machine_id"), col("sensor"),
    col("mu"), col("sigma"), col("min_val"), col("max_val"), col("n"),
)

query = (display.writeStream
         .format("console")
         .outputMode("update")
         .option("truncate", "false")
         .start())

print(">>> En attente d'agregations... (laisse tourner ~1 min)")
query.awaitTermination()