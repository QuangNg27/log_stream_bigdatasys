#!/bin/bash

# ==============================================================================
# Script điều khiển chạy nhanh các ứng dụng Spark
# ==============================================================================

SPARK_MASTER_CONTAINER="spark-master"
SPARK_MASTER_URL="spark://spark-master:7077"

# Gói thư viện
KAFKA_AVRO_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0"
DELTA_PACKAGES="io.delta:delta-spark_2.12:3.1.0"
ALL_PACKAGES="${KAFKA_AVRO_PACKAGES},${DELTA_PACKAGES}"
IVY_CONF="--conf spark.jars.ivy=/tmp/.ivy2"
RESOURCE_CONF="--total-executor-cores 1 --executor-memory 1G --driver-memory 512M"
DELTA_CONF="--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog $IVY_CONF $RESOURCE_CONF"

show_usage() {
    echo "Sử dụng: ./run-spark.sh [lệnh] [tùy chọn]"
    echo ""
    echo "Các lệnh hỗ trợ:"
    echo "  bronze [-d]    : Chạy job streaming Kafka -> Bronze Delta (Thêm -d để chạy ngầm)"
    echo "  silver         : Chạy job batch Bronze -> Silver Delta Merge"
    echo "  gold           : Chạy job tổng hợp Silver -> Gold Marts đẩy vào ClickHouse"
    echo "  check          : Đọc và hiển thị dữ liệu tầng Silver"
    echo "  help           : Hiển thị hướng dẫn này"
    echo ""
    echo "Ví dụ:"
    echo "  ./run-spark.sh bronze -d    # Chạy streaming ở chế độ ngầm"
    echo "  ./run-spark.sh silver       # Chạy merge silver"
    echo "  ./run-spark.sh gold         # Chạy tổng hợp đẩy sang ClickHouse Gold"
    echo "  ./run-spark.sh check        # Xem các bảng silver"
}

case "$1" in
    bronze)
        if [ "$2" == "-d" ]; then
            echo "🚀 Đang khởi chạy Kafka -> Bronze Delta ở chế độ chạy ngầm (Background)..."
            docker exec -d $SPARK_MASTER_CONTAINER /opt/spark/bin/spark-submit \
                --master $SPARK_MASTER_URL \
                --packages $ALL_PACKAGES \
                $DELTA_CONF \
                /opt/spark-apps/kafka_to_bronze.py
            echo "✅ Job đã được đưa vào chạy ngầm! Kiểm tra tiến trình tại: http://localhost:8085"
        else
            echo "🚀 Đang khởi chạy Kafka -> Bronze Delta Streaming (Nhấn Ctrl+C để dừng)..."
            docker exec -it $SPARK_MASTER_CONTAINER /opt/spark/bin/spark-submit \
                --master $SPARK_MASTER_URL \
                --packages $ALL_PACKAGES \
                $DELTA_CONF \
                /opt/spark-apps/kafka_to_bronze.py
        fi
        ;;

    silver)
        echo "🔄 Đang chạy Bronze -> Silver Delta Merge cho toàn bộ các bảng..."
        docker exec -it $SPARK_MASTER_CONTAINER /opt/spark/bin/spark-submit \
            --master $SPARK_MASTER_URL \
            --packages $DELTA_PACKAGES \
            $DELTA_CONF \
            /opt/spark-apps/bronze_to_silver.py
        ;;

    gold)
        echo "🌟 Đang tổng hợp dữ liệu Silver -> Gold Marts đẩy vào ClickHouse..."
        docker exec -it $SPARK_MASTER_CONTAINER /opt/spark/bin/spark-submit \
            --master $SPARK_MASTER_URL \
            --packages $DELTA_PACKAGES \
            $DELTA_CONF \
            /opt/spark-apps/silver_to_gold_clickhouse.py
        ;;

    check)
        echo "📊 Đang truy vấn dữ liệu tầng Silver..."
        docker exec -it $SPARK_MASTER_CONTAINER /opt/spark/bin/spark-submit \
            --master $SPARK_MASTER_URL \
            --packages $DELTA_PACKAGES \
            $DELTA_CONF \
            /opt/spark-apps/read_data_from_silver.py
        ;;

    *)
        show_usage
        ;;
esac
