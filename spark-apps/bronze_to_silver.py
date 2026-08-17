from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, expr, coalesce
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def process_bronze_to_silver(spark, bronze_path, silver_path, pk_col="id"):
    print(f"--- Đang đọc dữ liệu Bronze từ: {bronze_path} ---")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Kiểm tra xem bảng Delta Silver đã tồn tại chưa
    is_silver_exists = DeltaTable.isDeltaTable(spark, silver_path)

    if not is_silver_exists:
        # =========================================================================
        # 1. RUN LẦN ĐẦU: DÙNG KĨ THUẬT CRUD RECONSTRUCTION
        # =========================================================================
        print("⚡ [FIRST RUN] Khởi tạo bảng Delta Silver bằng kĩ thuật CRUD...")

        df_cr = df_bronze.filter((col("op") == "c") | (col("op") == "r"))
        df_u = df_bronze.filter(col("op") == "u")
        df_d = df_bronze.filter(col("op") == "d")

        # Lấy bản ghi UPDATE mới nhất theo pk_col (id)
        window_spec = Window.partitionBy(pk_col).orderBy(desc("ts_ms"))
        df_u_latest = df_u.withColumn("row_number", row_number().over(window_spec)) \
                          .filter(col("row_number") == 1) \
                          .drop("row_number")

        # Anti join
        df_cr_updated = df_cr.join(df_u_latest, pk_col, "left_anti")
        df_cru = df_cr_updated.unionByName(df_u_latest)
        df_crud = df_cru.join(df_d, pk_col, "left_anti")

        # Parse JSON từ cột 'after'
        sample_json = df_crud.filter(col("after").isNotNull()).select("after").first()[0]
        
        # CHỈ GIỮ CÁC CỘT BẢNG GỐC: Parse JSON thành struct 'payload' và bung ra 'payload.*'
        # Bỏ hẳn cột ts_ms và op
        silver_initial_df = df_crud.select(
            expr(f"from_json(after, schema_of_json('{sample_json}'))").alias("payload")
        ).select("payload.*")

        # Ghi khởi tạo bảng Delta Silver
        silver_initial_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_path)

        print("✅ [FIRST RUN] Khởi tạo bảng Delta Silver thành công (Chỉ chứa cột bản gốc)!")

    else:
        # =========================================================================
        # 2. RUN LẦN SAU: DELTA MERGE (UPSERT)
        # =========================================================================
        print("🔄 [INCREMENTAL RUN] Thực hiện Delta MERGE...")

        delta_target = DeltaTable.forPath(spark, silver_path)

        # Deduplicate batch CDC dựa trên ts_ms
        window_spec = Window.partitionBy(pk_col).orderBy(desc("ts_ms"))
        df_batch_latest = df_bronze.withColumn("row_num", row_number().over(window_spec)) \
                                   .filter(col("row_num") == 1) \
                                   .drop("row_num")

        sample_json = df_batch_latest.filter(col("after").isNotNull()).select("after").first()[0]

        # Giải mã JSON lấy các cột của DB gốc, GIỮ CỘT 'op' TẠM THỜI để làm điều kiện Merge
        updates_df = df_batch_latest.select(
            col("op"),
            expr(f"from_json(coalesce(after, before), schema_of_json('{sample_json}'))").alias("payload")
        ).select("op", "payload.*")

        # Danh sách CHỈ CHỨA các cột của bảng gốc trong DB (Đã loại bỏ cột 'op')
        target_db_columns = [c for c in updates_df.columns if c != "op"]

        # Cú pháp Delta MERGE: Chỉ update/insert các cột thuộc bảng gốc
        delta_target.alias("target") \
            .merge(
                source=updates_df.alias("source"),
                condition=f"target.{pk_col} = source.{pk_col}"
            ) \
            .whenMatchedDelete(
                condition="source.op = 'd'"
            ) \
            .whenMatchedUpdate(
                condition="source.op IN ('u', 'c', 'r')",
                set={c: f"source.{c}" for c in target_db_columns}
            ) \
            .whenNotMatchedInsert(
                condition="source.op IN ('c', 'r', 'u')",
                values={c: f"source.{c}" for c in target_db_columns}
            ) \
            .execute()

        print("✅ [INCREMENTAL RUN] Delta MERGE thành công!")

def main():
    spark = SparkSession.builder \
        .appName("Bronze_To_Silver_Clean_DB_Schema") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    BRONZE_PATH = "hdfs://namenode:9000/lakehouse/bronze/users/"
    SILVER_PATH = "hdfs://namenode:9000/lakehouse/silver/users/"

    process_bronze_to_silver(spark, BRONZE_PATH, SILVER_PATH, pk_col="id")

if __name__ == "__main__":
    main()