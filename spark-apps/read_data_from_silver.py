from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Check_Silver") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đọc bảng Delta Silver
df_silver = spark.read.format("delta").load("hdfs://namenode:9000/lakehouse/silver/users/")
df_silver.show(truncate=False)