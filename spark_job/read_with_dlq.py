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
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "sensor-readings"
DLQ_TOPIC = "dead-letter"
PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.spark:spark-avro_2.12:3.5.1",
])
SCHEMA_STR = (Path(__file__).parent.parent / "producer" / "schemas" / "reading.avsc").read_text(encoding="utf-8")

spark = (SparkSession.builder
         .appName("read-with-dlq")
         .master("local[*]")
         .config("spark.jars.packages", PACKAGES)
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("\n>>> Spark demarre, lecture avec Dead Letter Queue...")

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .load())

# On garde la valeur brute (pour la DLQ) ET on tente le decodage
attempted = raw.select(
    col("key"),
    col("value").alias("raw_value"),   # message brut original, garde pour la DLQ
    expr("substring(value, 6, length(value)-5)").alias("avro_value"),
)

# DECODAGE PERMISSIF : un message corrompu -> data = null (pas de crash)
decoded = attempted.select(
    col("key"),
    col("raw_value"),
    from_avro(col("avro_value"), SCHEMA_STR, {"mode": "PERMISSIVE"}).alias("data"),
)

# --- Tas 1 : messages VALIDES (un champ interne est non null) ---
valid = decoded.filter(col("data.machine_id").isNotNull()).select("data.*")

# --- Tas 2 : messages INVALIDES (le decodage a echoue) -> vers la DLQ ---
invalid = decoded.filter(col("data.machine_id").isNull()).select(
    col("key"),
    col("raw_value").alias("value"),   # Kafka attend une colonne 'value'
)

# Flux 1 : afficher les valides dans la console
q_valid = (valid.writeStream
           .format("console")
           .outputMode("append")
           .option("truncate", "false")
           .queryName("valides")
           .start())

# Flux 2 : ecrire les invalides dans le topic Kafka 'dead-letter'
q_dlq = (invalid.writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("topic", DLQ_TOPIC)
         .option("checkpointLocation", "checkpoints/dlq")
         .outputMode("append")
         .start())

print(">>> Valides -> console | Invalides -> topic 'dead-letter'")
spark.streams.awaitAnyTermination()
