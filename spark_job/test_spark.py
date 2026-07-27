"""Mini-test : verifie que Spark demarre avec Java 17 et winutils."""
import os
import sys

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Correctif Windows + Python recent : evite le crash du worker Python
os.environ["PYTHONUTF8"] = "1"

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("test")
         .master("local[*]")
         .config("spark.python.worker.faulthandler.enabled", "true")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("\n>>> Spark demarre ! Version:", spark.version)

df = spark.createDataFrame([(1, "machine-01"), (2, "machine-02")], ["id", "name"])
df.show()

spark.stop()
print(">>> Test reussi, Spark s'est arrete proprement.")