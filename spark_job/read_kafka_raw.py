import os
import sys

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYTHONUTF8"] = "1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "sensor-readings"

# Le connecteur Kafka pour Spark 3.5.1 (telecharge automatiquement au 1er lancement)
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

spark = (SparkSession.builder
         .appName("read-kafka-raw")
         .master("local[*]")
         .config("spark.jars.packages", KAFKA_PACKAGE)
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("\n>>> Spark demarre, connexion a Kafka...")

# Lecture en streaming depuis Kafka
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", TOPIC)
      .option("startingOffsets", "latest")
      .load())

# Un message Kafka a : key, value (binaire), topic, partition, offset, timestamp
# On affiche des infos lisibles : la cle (machine_id) et la taille du message
readable = df.select(
    col("key").cast("string").alias("machine_id"),
    length(col("value")).alias("taille_message_avro"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)

# Ecrit le flux dans la console
query = (readable.writeStream
         .format("console")
         .outputMode("append")
         .option("truncate", "false")
         .start())

print(">>> En attente de messages... (lance le producteur dans un autre terminal)")
query.awaitTermination()
