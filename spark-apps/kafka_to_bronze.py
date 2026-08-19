import json
import time
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, year, month, day, to_json, coalesce

def get_latest_schema_from_registry(registry_url, topic_name):
    """
    Hàm gọi API Schema Registry để lấy JSON Schema mới nhất của Topic
    """
    subject = f"{topic_name}-value"
    url = f"{registry_url}/subjects/{subject}/versions/latest"
    
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode('utf-8'))
        return data['schema']

def start_stream_for_table(spark, table_name, schema_registry_url, bootstrap_servers):
    topic_name = f"cdc_data.public.{table_name}"
    print(f"[START STREAM] Khởi động stream cho bảng '{table_name}' (Topic: {topic_name})...")
    
    avro_schema_json = get_latest_schema_from_registry(schema_registry_url, topic_name)

    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    binary_df = raw_kafka_df.filter(col("value").isNotNull()).select(
        expr("substring(value, 6, length(value)-5)").alias("avro_payload"),
        col("timestamp")
    )

    parsed_df = binary_df.select(
        from_avro(col("avro_payload"), avro_schema_json, {"mode": "PERMISSIVE"}).alias("data"),
        col("timestamp")
    ).select(
        coalesce(expr("data.after.id"), expr("data.before.id")).cast("string").alias("id"),
        to_json(col("data.before")).alias("before"),
        to_json(col("data.after")).alias("after"),
        to_json(col("data.source")).alias("source"),
        col("data.op").alias("op"),
        col("data.ts_ms").alias("ts_ms"),
        year("timestamp").alias("year"),
        month("timestamp").alias("month"),
        day("timestamp").alias("day")
    )

    query = parsed_df.writeStream \
        .queryName(f"stream_{table_name}") \
        .format("delta") \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="10 minutes") \
        .option("checkpointLocation", f"hdfs://namenode:9000/lakehouse/checkpoints/bronze_{table_name}/") \
        .outputMode("append") \
        .start(f"hdfs://namenode:9000/lakehouse/bronze/{table_name}/")

    return query

def main():
    spark = SparkSession.builder \
        .appName("Bronze_MultiTable_Unified_Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
    BOOTSTRAP_SERVERS = "kafka:29092"
    TABLES = ["users", "devices", "cam_bills"]

    queries = []
    for tbl in TABLES:
        try:
            q = start_stream_for_table(spark, tbl, SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVERS)
            queries.append(q)
        except Exception as e:
            print(f"Bỏ qua bảng '{tbl}' do chưa có topic trên Kafka Connect: {e}")

    print(f"\n✅ Đang chạy đồng thời {len(queries)} stream queries trong 1 Spark App liên tục!")
    
    # Duy trì tiến trình Spark Streaming sống vĩnh viễn và giám sát ngoại lệ
    try:
        while True:
            for q in queries:
                if not q.isActive:
                    print(f"⚠️ Cảnh báo: Stream '{q.name}' không còn active!")
                    if q.exception():
                        print(f"❌ Chi tiết lỗi Stream '{q.name}': {q.exception()}")
                        raise q.exception()
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n⏹️ Nhận lệnh dừng từ người dùng. Đang dừng tất cả các streams...")
        for q in queries:
            q.stop()
        spark.stop()

if __name__ == "__main__":
    main()