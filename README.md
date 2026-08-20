# HỆ THỐNG ENTERPRISE BIG DATA & DATA LAKEHOUSE PLATFORM

Dự án triển khai mô hình **Modern Enterprise Lakehouse & Data Governance Platform** đa nguồn dữ liệu, kết hợp xử lý Streaming CDC, Batch Data Lakehouse (HDFS/Delta Lake), Fast-Path Data Ingestion (NiFi), OLAP Engine (ClickHouse), Trực quan hóa (Superset) và Quản trị dữ liệu toàn diện (OpenMetadata).

---

## 🏛️ 1. Cấu trúc Thư mục Dự án

```text
bigdata-sys/
├── docker-compose.yaml        # File cấu hình toàn bộ cụm dịch vụ Docker
├── start-stack.sh             # Script khởi động hệ thống theo Option 1 hoặc Option 2
├── seeder.py                  # Script sinh dữ liệu đa nguồn (Postgres, IoT Logs, CSV đối soát)
├── seed.sh                    # Wrapper script chạy seeder.py
├── run-spark.sh               # Script điều khiển chạy các ứng dụng Spark Lakehouse
├── register-debezium.sh       # Script đăng ký Debezium CDC Source Connector
├── sink-connector.sh          # Script đăng ký JDBC Sink Connector
├── spark-apps/                # Mã nguồn các Spark jobs (Medallion Architecture)
│   ├── kafka_to_bronze.py     # Spark Streaming: Kafka -> HDFS Bronze Delta
│   ├── bronze_to_silver.py    # Spark Batch Merge: Bronze -> HDFS Silver Delta
│   ├── silver_to_gold_clickhouse.py # Spark Aggregation: Silver -> ClickHouse Gold
│   ├── read_bronze.py         # Script kiểm tra đọc dữ liệu Bronze
│   └── read_data_from_silver.py # Script kiểm tra đọc dữ liệu Silver
└── data_lake_source/          # Nơi lưu trữ file sinh ra (IoT Logs & Payment CSV)
    ├── logs/                  # Thư mục chứa JSON logs phân vùng theo ngày/tháng
    └── files/                 # Thư mục chứa các file CSV đối soát cước
```

---

## ⚡ 2. Hướng dẫn Khởi chạy Hệ thống (`start-stack.sh`)

Script [`start-stack.sh`](start-stack.sh) cung cấp 2 chế độ chạy độc lập:

### 🔹 [Option 1]: Cụm Big Data CDC & Lakehouse + Airflow + OpenMetadata
> **Thành phần:** PostgreSQL Source, Kafka, Debezium, HDFS, Spark, ClickHouse, Airflow, Superset, OpenMetadata.

```bash
# Khởi động Option 1
./start-stack.sh 1
```

### 🔹 [Option 2]: Cụm Fast-Path Logs & S3 Files (NiFi ➔ ClickHouse) + OpenMetadata
> **Thành phần:** MinIO S3, Apache NiFi, ClickHouse, Superset, OpenMetadata (Bỏ qua Postgres, Kafka, HDFS, Spark để tiết kiệm RAM).

```bash
# Khởi động Option 2
./start-stack.sh 2
```

### 🔹 Các lệnh quản lý chung:
```bash
./start-stack.sh status   # Xem trạng thái các container đang chạy
./start-stack.sh down     # Dừng toàn bộ hệ thống
./start-stack.sh          # Mở menu tương tác lựa chọn
```

---

## 🌐 3. Danh sách Web UI & Thông tin Đăng nhập

| Dịch vụ | Địa chỉ Web UI | Tài khoản / Mật khẩu | Áp dụng |
| :--- | :--- | :--- | :---: |
| **📊 Apache Superset** | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` | Cả 2 Option |
| **🛡️ OpenMetadata** | [http://localhost:8585](http://localhost:8585) | `admin` / `admin` | Cả 2 Option |
| **⚡ ClickHouse HTTP** | [http://localhost:8123](http://localhost:8123) | `admin` / `Admin@123` | Cả 2 Option |
| **💧 Apache NiFi** | [https://localhost:8443/nifi](https://localhost:8443/nifi) | `admin` / `Admin@123456789` | Option 2 |
| **📦 MinIO S3 Console** | [http://localhost:9001](http://localhost:9001) | `admin` / `Admin@123` | Option 2 |
| **🌀 Apache Airflow** | [http://localhost:8089](http://localhost:8089) | `admin` / `Admin@123` | Option 1 |
| **📨 Kafka UI** | [http://localhost:8080](http://localhost:8080) | *(Không yêu cầu)* | Option 1 |
| **⚡ Spark Master UI** | [http://localhost:8085](http://localhost:8085) | *(Không yêu cầu)* | Option 1 |
| **🏛️ HDFS NameNode UI**| [http://localhost:9870](http://localhost:9870) | *(Không yêu cầu)* | Option 1 |
| **🗄️ pgAdmin** | [http://localhost:5050](http://localhost:5050) | `admin@admin.com` / `Admin@123` | Option 1 |

---

## 🛠️ 4. Hướng dẫn Sinh dữ liệu (`seed.sh`)

Script [`seed.sh`](seed.sh) hỗ trợ sinh dữ liệu đa nguồn: **PostgreSQL CDC**, **IoT Camera Logs** (`.json`), và **Tệp CSV đối soát cước** (`.csv`).

```bash
# 1. Sinh toàn bộ dữ liệu lịch sử từ đầu năm đến nay (~8 tháng, từ 01/01/2026)
./seed.sh --from-start-of-year

# 2. Xóa sạch DB cũ và sinh lại toàn bộ lịch sử từ đầu năm
./seed.sh --reset --from-start-of-year

# 3. Sinh dữ liệu lịch sử của 30 ngày gần nhất
./seed.sh --days 30

# 4. Sinh dữ liệu trong một khoảng ngày cụ thể
./seed.sh --start-date 2026-03-01 --end-date 2026-06-30

# 5. Chạy giả lập Traffic Streaming liên tục theo thời gian thực (nhấn Ctrl+C để dừng)
./seed.sh --stream --interval 1.5

# 6. Chạy Batch nhanh 20 bản ghi tại thời điểm hiện tại
./seed.sh --count 20
```

*File Logs và CSV đối soát sẽ được tự động xuất vào thư mục `./data_lake_source/`.*

---

## ⚙️ 5. Hướng dẫn Chạy Pipeline Spark Lakehouse (`run-spark.sh`)

Sử dụng khi chạy **Option 1**:

```bash
# 1. Chạy job Streaming kéo dữ liệu từ Kafka vào HDFS Bronze (Chế độ chạy ngầm)
./run-spark.sh bronze -d

# 2. Chạy job Batch làm sạch, khử trùng lặp và Merge dữ liệu sang HDFS Silver
./run-spark.sh silver

# 3. Chạy job tổng hợp KPI và đẩy dữ liệu sang ClickHouse Gold Layer
./run-spark.sh gold

# 4. Kiểm tra dữ liệu hiện có trong tầng Silver
./run-spark.sh check
```

---

## 🔌 6. Hướng dẫn Đăng ký Kafka Connectors thủ công (Nếu cần)

Các connector đã được tự động đăng ký qua `kafka-connect-init`, tuy nhiên bạn có thể đăng ký lại thủ công bằng các script:

```bash
# Đăng ký Debezium Source Connector cho bảng
./register-debezium.sh --connect-host=localhost --connect-port=8083 --table=public.users
./register-debezium.sh --connect-host=localhost --connect-port=8083 --table=public.devices
./register-debezium.sh --connect-host=localhost --connect-port=8083 --table=public.cam_bills

# Đăng ký JDBC Sink Connector đồng bộ sang postgres-big-data
./sink-connector.sh --connect-host=localhost --connect-port=8083 --table=users --topic=cdc_data.public.users
./sink-connector.sh --connect-host=localhost --connect-port=8083 --table=devices --topic=cdc_data.public.devices
./sink-connector.sh --connect-host=localhost --connect-port=8083 --table=cam_bills --topic=cdc_data.public.cam_bills
```

---

## 🧪 7. Quy trình Kiểm thử End-to-End (E2E Workflow)

### Quy trình 1: Kiểm thử Option 1 (CDC & Lakehouse Pipeline)
1. **Khởi động**: `./start-stack.sh 1`
2. **Sinh dữ liệu lịch sử**: `./seed.sh --reset --from-start-of-year`
3. **Chạy luồng Spark**:
   - `./run-spark.sh bronze -d`
   - `./run-spark.sh silver`
   - `./run-spark.sh gold`
4. **Kiểm tra kết quả**: Mở **Superset** (`http://localhost:8088`) kết nối ClickHouse (`gold_db`) để xem Dashboard doanh thu & thiết bị qua 8 tháng.
5. **Quản trị Data Governance**: Mở **OpenMetadata** (`http://localhost:8585`) để quét catalog và xem Data Lineage: `Postgres -> Kafka -> HDFS -> ClickHouse -> Superset`.

### Quy trình 2: Kiểm thử Option 2 (Fast-Path Logs & Files qua NiFi)
1. **Khởi động**: `./start-stack.sh 2`
2. **Sinh Logs & CSV**: `./seed.sh --from-start-of-year`
3. **Mở NiFi UI**: Truy cập `https://localhost:8443/nifi`, thiết lập Flow đọc file từ bucket MinIO `source-logs-bucket` và `source-files-bucket` $\rightarrow$ Nạp thẳng vào ClickHouse `fact_device_logs` và `fact_payment_reconciliation`.
4. **Kiểm tra trên Superset**: Xem Dashboard giám sát lỗi kết nối camera và đối soát cước thanh toán.
