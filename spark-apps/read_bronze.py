from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read_Bronze_Inspection") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

for t in ["users", "devices", "cam_bills"]:
    print(f"\n==========================================")
    print(f"📊 BẢNG BRONZE: {t}")
    print(f"==========================================")
    try:
        df = spark.read.format("delta").load(f"hdfs://namenode:9000/lakehouse/bronze/{t}/")
        print(f"Tổng số bản ghi trong Bronze '{t}': {df.count()}")
        df.select("id", "op", "ts_ms", "year", "month", "day").show(5, truncate=False)
    except Exception as e:
        print(f"Lỗi đọc bảng Bronze {t}: {e}")

spark.stop()
