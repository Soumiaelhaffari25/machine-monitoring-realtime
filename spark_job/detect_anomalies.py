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

# Import du moteur de regles (meme dossier)
sys.path.insert(0, str(Path(__file__).parent))
from rules import compute_anomaly_score

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "sensor-readings"
PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.spark:spark-avro_2.12:3.5.1",
])
SCHEMA_STR = (Path(__file__).parent.parent / "producer" / "schemas" / "reading.avsc").read_text(encoding="utf-8")

spark = (SparkSession.builder
         .appName("detect-anomalies")
         .master("local[*]")
         .config("spark.jars.packages", PACKAGES)
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("\n>>> Spark demarre, moteur de regles actif...")

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", TOPIC)
      .option("startingOffsets", "latest")
      .load())

decoded = (df.select(expr("substring(value, 6, length(value)-5)").alias("avro_value"))
           .select(from_avro(col("avro_value"), SCHEMA_STR, {"mode": "PERMISSIVE"}).alias("d"))
           .filter(col("d.machine_id").isNotNull())
           .select("d.*"))

# Agregation fenetree : on garde mu, sigma ET la derniere valeur de la fenetre
windowed = (decoded
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("machine_id"), col("sensor"),
    )
    .agg(
        avg("value").alias("mu"),
        stddev("value").alias("sigma"),
        last("value").alias("value"),   # derniere valeur, sur laquelle on teste les regles
        count("value").alias("n"),
    ))

# APPLICATION DU MOTEUR DE REGLES
scored = compute_anomaly_score(windowed)

# On n'affiche que les anomalies detectees
anomalies = scored.filter(col("is_anomaly")).select(
    col("window.start").alias("debut"),
    col("machine_id"), col("sensor"), col("value"),
    col("mu"), col("sigma"), col("zscore"),
    col("signal_physical"), col("signal_zscore"), col("anomaly_score"),
)

query = (anomalies.writeStream
         .format("console")
         .outputMode("update")
         .option("truncate", "false")
         .start())

print(">>> Detection en cours... (les anomalies s'affichent ici)")
query.awaitTermination()
