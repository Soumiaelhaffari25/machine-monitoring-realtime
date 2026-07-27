"""
Palier P3 : desérialiser l'Avro Confluent et afficher les vraies valeurs.
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

# CORRECTIF : forcer Spark a utiliser localhost pour ses communications internes
# (evite qu'il choisisse une fausse adresse 169.254.x.x d'une carte reseau virtuelle)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "sensor-readings"

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.spark:spark-avro_2.12:3.5.1",
])

SCHEMA_STR = (Path(__file__).parent.parent / "producer" / "schemas" / "reading.avsc").read_text(encoding="utf-8")

spark = (SparkSession.builder
         .appName("read-kafka-avro")
         .master("local[*]")
         .config("spark.jars.packages", PACKAGES)
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("\n>>> Spark demarre, lecture + desérialisation Avro...")

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", TOPIC)
      .option("startingOffsets", "latest")
      .load())

avro_bytes = df.select(
    col("key").cast("string").alias("machine_id_key"),
    expr("substring(value, 6, length(value)-5)").alias("avro_value"),
)

decoded = avro_bytes.select(
    col("machine_id_key"),
    from_avro(col("avro_value"), SCHEMA_STR).alias("data"),
).select("data.*")

query = (decoded.writeStream
         .format("console")
         .outputMode("append")
         .option("truncate", "false")
         .start())

print(">>> En attente de messages decodes...")
query.awaitTermination()