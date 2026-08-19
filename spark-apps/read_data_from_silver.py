from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Check_All_Silver_Tables") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

TABLES = ["users", "devices", "cam_bills"]

for tbl in TABLES:
    silver_path = f"hdfs://namenode:9000/lakehouse/silver/{tbl}/"
    print(f"\n=======================================================")
    print(f"📊 DỮ LIỆU TẦNG SILVER: {tbl}")
    print(f"=======================================================")
    if DeltaTable.isDeltaTable(spark, silver_path):
        df_silver = spark.read.format("delta").load(silver_path)
        df_silver.show(truncate=False)
    else:
        print(f"⚠️ Bảng '{tbl}' chưa được tạo trong Silver Delta Lake.")