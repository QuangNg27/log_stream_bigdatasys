import json
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
        return data['schema'] # Trả về chuỗi JSON Avro Schema

def main():
    spark = SparkSession.builder \
        .appName("Bronze_Debezium_Avro_SchemaRegistry") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
    TOPIC_NAME = "cdc_data.public.users"

    # 1. Tự động lấy Schema mới nhất từ Schema Registry API
    print("Fetching schema from Schema Registry...")
    avro_schema_json = get_latest_schema_from_registry(SCHEMA_REGISTRY_URL, TOPIC_NAME)
    print("Schema fetched successfully!")

    # 2. Đọc dữ liệu từ Kafka
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Lọc bỏ tombstone (value null) và Cắt 5 bytes Magic Header của Confluent Wire Format
    binary_df = raw_kafka_df.filter(col("value").isNotNull()).select(
        expr("substring(value, 6, length(value)-5)").alias("avro_payload"),
        col("timestamp")
    )

    # 4. Decode với from_avro(col, jsonFormatSchema, options)
    parsed_df = binary_df.select(
        from_avro(
            col("avro_payload"), 
            avro_schema_json,             # Tham số jsonFormatSchema bắt buộc
            {"mode": "PERMISSIVE"}        # Options
        ).alias("data"),
        col("timestamp")
    ).select(
        # Bóc tách dữ liệu
        coalesce(
            expr("data.after.id"), 
            expr("data.before.id")
        ).cast("string").alias("id"),
        to_json(col("data.before")).alias("before"),
        to_json(col("data.after")).alias("after"),
        to_json(col("data.source")).alias("source"),
        col("data.op").alias("op"),
        col("data.ts_ms").alias("ts_ms"),
        year("timestamp").alias("year"),
        month("timestamp").alias("month"),
        day("timestamp").alias("day")
    )

    # 5. Ghi xuống HDFS dưới định dạng Delta Lake
    query = parsed_df.writeStream \
        .format("delta") \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "hdfs://namenode:9000/lakehouse/checkpoints/bronze_users/") \
        .outputMode("append") \
        .start("hdfs://namenode:9000/lakehouse/bronze/users/")

    query.awaitTermination()

if __name__ == "__main__":
    main()