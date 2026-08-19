from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, expr, coalesce
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def process_bronze_to_silver(spark, table_name, pk_col="id"):
    bronze_path = f"hdfs://namenode:9000/lakehouse/bronze/{table_name}/"
    silver_path = f"hdfs://namenode:9000/lakehouse/silver/{table_name}/"

    print(f"\n=======================================================")
    print(f"📦 XỬ LÝ BẢNG: {table_name}")
    print(f"=======================================================")

    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
    except Exception as e:
        print(f"⚠️ Chưa có dữ liệu Bronze cho bảng '{table_name}'. Bỏ qua: {e}")
        return

    # Kiểm tra xem bảng Delta Silver đã tồn tại chưa
    is_silver_exists = DeltaTable.isDeltaTable(spark, silver_path)

    if not is_silver_exists:
        # =========================================================================
        # 1. RUN LẦN ĐẦU: DÙNG KĨ THUẬT CRUD RECONSTRUCTION
        # =========================================================================
        print(f"⚡ [FIRST RUN] Khởi tạo bảng Delta Silver cho '{table_name}' bằng CRUD...")

        df_cr = df_bronze.filter((col("op") == "c") | (col("op") == "r"))
        df_u = df_bronze.filter(col("op") == "u")
        df_d = df_bronze.filter(col("op") == "d")

        window_spec = Window.partitionBy(pk_col).orderBy(desc("ts_ms"))
        df_u_latest = df_u.withColumn("row_number", row_number().over(window_spec)) \
                          .filter(col("row_number") == 1) \
                          .drop("row_number")

        df_cr_updated = df_cr.join(df_u_latest, pk_col, "left_anti")
        df_cru = df_cr_updated.unionByName(df_u_latest)
        df_crud = df_cru.join(df_d, pk_col, "left_anti")

        sample_row = df_crud.filter(col("after").isNotNull()).select("after").first()
        if not sample_row:
            print(f"⚠️ Bảng '{table_name}' chưa có dữ liệu hợp lệ để khởi tạo.")
            return

        sample_json = sample_row[0]
        silver_initial_df = df_crud.select(
            expr(f"from_json(after, schema_of_json('{sample_json}'))").alias("payload")
        ).select("payload.*")

        silver_initial_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_path)

        print(f"✅ [FIRST RUN] Khởi tạo bảng Delta Silver cho '{table_name}' thành công!")

    else:
        # =========================================================================
        # 2. RUN LẦN SAU: DELTA MERGE (UPSERT)
        # =========================================================================
        print(f"🔄 [INCREMENTAL RUN] Thực hiện Delta MERGE cho '{table_name}'...")

        delta_target = DeltaTable.forPath(spark, silver_path)

        window_spec = Window.partitionBy(pk_col).orderBy(desc("ts_ms"))
        df_batch_latest = df_bronze.withColumn("row_num", row_number().over(window_spec)) \
                                   .filter(col("row_num") == 1) \
                                   .drop("row_num")

        sample_row = df_batch_latest.filter(col("after").isNotNull()).select("after").first()
        if not sample_row:
            sample_row = df_batch_latest.filter(col("before").isNotNull()).select("before").first()

        sample_json = sample_row[0]
        updates_df = df_batch_latest.select(
            col("op"),
            expr(f"from_json(coalesce(after, before), schema_of_json('{sample_json}'))").alias("payload")
        ).select("op", "payload.*")

        target_db_columns = [c for c in updates_df.columns if c != "op"]

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

        print(f"✅ [INCREMENTAL RUN] Delta MERGE cho '{table_name}' thành công!")

def main():
    spark = SparkSession.builder \
        .appName("Bronze_To_Silver_MultiTable_Merge") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    TABLES = ["users", "devices", "cam_bills"]
    for tbl in TABLES:
        process_bronze_to_silver(spark, tbl, pk_col="id")

if __name__ == "__main__":
    main()