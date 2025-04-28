from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV Count Job") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("hdfs://localhost:9000/datasets/mon_fichier.csv", header=True, inferSchema=True)

row_count = df.count()

print(f"Nombre de lignes dans le fichier CSV : {row_count}")

spark.stop()