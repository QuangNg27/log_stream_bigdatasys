import json
import urllib.request
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, when, coalesce, lit, current_timestamp,
    from_unixtime, to_date, round as _round, avg
)
from delta.tables import DeltaTable

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_DB = "gold_db"
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASS = "Admin@123"

def execute_clickhouse_query(query):
    """
    Hàm thực thi câu lệnh DDL/SQL trên ClickHouse qua HTTP interface
    """
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?user={CLICKHOUSE_USER}&password={CLICKHOUSE_PASS}&database={CLICKHOUSE_DB}"
    req = urllib.request.Request(url, data=query.encode('utf-8'), method='POST')
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode('utf-8')

def push_df_to_clickhouse(df, table_name):
    """
    Hàm chuyển đổi DataFrame sang JSONEachRow và đẩy vào ClickHouse với tốc độ cao
    """
    row_count = df.count()
    if row_count == 0:
        print(f"⚠️ Bảng {table_name} không có dữ liệu để ghi.")
        return

    # Thu thập dữ liệu dạng JSON Lines
    json_lines = df.toJSON().collect()
    payload = "\n".join(json_lines) + "\n"

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?query=INSERT+INTO+{CLICKHOUSE_DB}.{table_name}+FORMAT+JSONEachRow&user={CLICKHOUSE_USER}&password={CLICKHOUSE_PASS}"
    req = urllib.request.Request(url, data=payload.encode('utf-8'), method='POST')
    
    with urllib.request.urlopen(req) as resp:
        print(f"✅ Đã ghi thành công {row_count} bản ghi vào ClickHouse table: {CLICKHOUSE_DB}.{table_name}")

def init_clickhouse_tables():
    """
    Tạo cấu trúc bảng trên ClickHouse nếu chưa tồn tại
    """
    print("🛠️ Khởi tạo schema các bảng Gold Mart trên ClickHouse...")

    # 1. Bảng User 360 & Doanh thu
    execute_clickhouse_query("""
    CREATE TABLE IF NOT EXISTS gold_db.user_360_gold (
        user_id UInt64,
        user_name String,
        email String,
        total_devices UInt32,
        online_devices UInt32,
        offline_devices UInt32,
        total_bills UInt32,
        total_spent Decimal(15, 2),
        vip_tier String,
        primary_plan String,
        updated_at DateTime
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY user_id;
    """)

    # 2. Bảng Hiệu năng Thiết bị & Gói cước
    execute_clickhouse_query("""
    CREATE TABLE IF NOT EXISTS gold_db.device_performance_gold (
        device_id UInt64,
        device_name String,
        device_type String,
        status String,
        owner_id UInt64,
        owner_name String,
        owner_email String,
        bill_amount Decimal(12, 2),
        plan_type String,
        bill_status String,
        updated_at DateTime
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (device_type, device_id);
    """)

    # 3. Bảng Báo cáo KPI Kinh doanh tổng hợp
    execute_clickhouse_query("""
    CREATE TABLE IF NOT EXISTS gold_db.daily_executive_kpi_gold (
        report_date Date,
        total_registered_users UInt32,
        total_active_devices UInt32,
        online_devices UInt32,
        device_online_ratio_pct Float32,
        total_revenue Decimal(15, 2),
        total_paid_transactions UInt32,
        avg_revenue_per_user Decimal(15, 2),
        updated_at DateTime
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY report_date;
    """)
    print("✅ Các bảng ClickHouse Gold Layer đã sẵn sàng!")

def main():
    spark = SparkSession.builder \
        .appName("Silver_To_Gold_ClickHouse_Pipeline") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Khởi tạo DDL bảng trên ClickHouse
    init_clickhouse_tables()

    # 2. Đọc dữ liệu từ tầng Silver Delta Lake trên HDFS
    print("\n📥 Đang đọc dữ liệu từ Silver Delta Lake...")
    USERS_PATH = "hdfs://namenode:9000/lakehouse/silver/users/"
    DEVICES_PATH = "hdfs://namenode:9000/lakehouse/silver/devices/"
    BILLS_PATH = "hdfs://namenode:9000/lakehouse/silver/cam_bills/"

    if not DeltaTable.isDeltaTable(spark, USERS_PATH):
        print(f"❌ Bảng Silver users chưa tồn tại tại {USERS_PATH}")
        return

    users_df = spark.read.format("delta").load(USERS_PATH)
    
    # Đọc devices nếu có hoặc tạo rỗng
    if DeltaTable.isDeltaTable(spark, DEVICES_PATH):
        devices_df = spark.read.format("delta").load(DEVICES_PATH)
    else:
        print("⚠️ Bảng devices chưa có trong Silver, khởi tạo DF rỗng...")
        devices_df = spark.createDataFrame([], "id long, name string, device_type string, status string, owner_id long, updated_at long")

    # Đọc cam_bills nếu có hoặc tạo rỗng
    if DeltaTable.isDeltaTable(spark, BILLS_PATH):
        bills_df = spark.read.format("delta").load(BILLS_PATH)
    else:
        print("⚠️ Bảng cam_bills chưa có trong Silver, khởi tạo DF rỗng...")
        bills_df = spark.createDataFrame([], "id long, user_id long, device_id long, amount double, plan_type string, status string, updated_at long")

    # Cast type chuẩn xác
    users_df = users_df.select(
        col("id").cast("long").alias("user_id"),
        col("name").alias("user_name"),
        col("email")
    )
    
    devices_df = devices_df.select(
        col("id").cast("long").alias("device_id"),
        col("name").alias("device_name"),
        col("device_type"),
        col("status").alias("device_status"),
        col("owner_id").cast("long")
    )

    bills_df = bills_df.select(
        col("id").cast("long").alias("bill_id"),
        col("user_id").cast("long").alias("bill_user_id"),
        col("device_id").cast("long").alias("bill_device_id"),
        col("amount").cast("double").alias("amount"),
        col("plan_type"),
        col("status").alias("bill_status")
    )

    # =========================================================================
    # 🎯 MART 1: USER 360 & REVENUE MART
    # =========================================================================
    print("\nĐang tính toán Mart 1: User 360 & Revenue Mart...")
    
    # Thống kê thiết bị theo User
    dev_agg = devices_df.groupBy("owner_id").agg(
        count("device_id").alias("total_devices"),
        count(when(col("device_status") == "online", 1)).alias("online_devices"),
        count(when(col("device_status") != "online", 1)).alias("offline_devices")
    )

    # Thống kê hóa đơn & gói cước theo User
    bills_agg = bills_df.groupBy("bill_user_id").agg(
        count("bill_id").alias("total_bills"),
        coalesce(_sum(when(col("bill_status") == "paid", col("amount")).otherwise(0.0)), lit(0.0)).alias("total_spent")
    )

    user_360_df = users_df \
        .join(dev_agg, users_df["user_id"] == dev_agg["owner_id"], "left") \
        .join(bills_agg, users_df["user_id"] == bills_agg["bill_user_id"], "left") \
        .select(
            users_df["user_id"].cast("long").alias("user_id"),
            users_df["user_name"].alias("user_name"),
            users_df["email"].alias("email"),
            coalesce(col("total_devices"), lit(0)).cast("int").alias("total_devices"),
            coalesce(col("online_devices"), lit(0)).cast("int").alias("online_devices"),
            coalesce(col("offline_devices"), lit(0)).cast("int").alias("offline_devices"),
            coalesce(col("total_bills"), lit(0)).cast("int").alias("total_bills"),
            _round(coalesce(col("total_spent"), lit(0.0)), 2).cast("double").alias("total_spent"),
            when(col("total_spent") >= 1000000, "DIAMOND")
                .when(col("total_spent") >= 500000, "GOLD")
                .when(col("total_spent") > 0, "SILVER")
                .otherwise("STANDARD").alias("vip_tier"),
            lit("monthly").alias("primary_plan"),
            from_unixtime(current_timestamp().cast("long")).alias("updated_at")
        )

    push_df_to_clickhouse(user_360_df, "user_360_gold")

    # =========================================================================
    # 🎯 MART 2: DEVICE PERFORMANCE & BILLING MART
    # =========================================================================
    print("\nĐang tính toán Mart 2: Device Performance Mart...")
    
    device_perf_df = devices_df \
        .join(users_df, devices_df["owner_id"] == users_df["user_id"], "left") \
        .join(bills_df, devices_df["device_id"] == bills_df["bill_device_id"], "left") \
        .select(
            devices_df["device_id"].cast("long").alias("device_id"),
            devices_df["device_name"].alias("device_name"),
            devices_df["device_type"].alias("device_type"),
            devices_df["device_status"].alias("status"),
            coalesce(devices_df["owner_id"], lit(0)).cast("long").alias("owner_id"),
            coalesce(users_df["user_name"], lit("Unassigned")).alias("owner_name"),
            coalesce(users_df["email"], lit("N/A")).alias("owner_email"),
            _round(coalesce(bills_df["amount"], lit(0.0)), 2).cast("double").alias("bill_amount"),
            coalesce(bills_df["plan_type"], lit("none")).alias("plan_type"),
            coalesce(bills_df["bill_status"], lit("unbilled")).alias("bill_status"),
            from_unixtime(current_timestamp().cast("long")).alias("updated_at")
        )

    push_df_to_clickhouse(device_perf_df, "device_performance_gold")

    # =========================================================================
    # 🎯 MART 3: DAILY EXECUTIVE KPI MART
    # =========================================================================
    print("\nĐang tính toán Mart 3: Daily Executive KPI Mart...")
    
    total_users_count = users_df.count()
    total_devices_count = devices_df.count()
    online_devices_count = devices_df.filter(col("device_status") == "online").count()
    
    online_ratio = round((online_devices_count / total_devices_count * 100), 2) if total_devices_count > 0 else 0.0
    
    revenue_agg = bills_df.filter(col("bill_status") == "paid").agg(
        coalesce(_sum("amount"), lit(0.0)).alias("total_revenue"),
        count("bill_id").alias("paid_count")
    ).collect()[0]

    tot_rev = float(revenue_agg["total_revenue"]) if revenue_agg["total_revenue"] else 0.0
    paid_cnt = int(revenue_agg["paid_count"]) if revenue_agg["paid_count"] else 0
    arpu = round(tot_rev / total_users_count, 2) if total_users_count > 0 else 0.0

    today_str = datetime.now().strftime("%Y-%m-%d")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    kpi_df = spark.createDataFrame([{
        "report_date": today_str,
        "total_registered_users": int(total_users_count),
        "total_active_devices": int(total_devices_count),
        "online_devices": int(online_devices_count),
        "device_online_ratio_pct": float(online_ratio),
        "total_revenue": float(tot_rev),
        "total_paid_transactions": int(paid_cnt),
        "avg_revenue_per_user": float(arpu),
        "updated_at": now_str
    }])

    push_df_to_clickhouse(kpi_df, "daily_executive_kpi_gold")

    print("\n🎉 Hoàn thành toàn bộ quy trình đẩy dữ liệu sang Gold Layer ClickHouse!")

if __name__ == "__main__":
    main()
