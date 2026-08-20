#!/usr/bin/env bash
# ==============================================================================
# SCRIPT ĐIỀU KHIỂN KHỞI CHẠY HỆ THỐNG DOCKER COMPOSE THEO 2 CHẾ ĐỘ
# ==============================================================================
# Option 1: Hệ thống Big Data CDC & Lakehouse cốt lõi + Airflow + OpenMetadata
# Option 2: Luồng Nạp Nhanh Logs & Object Storage (NiFi -> ClickHouse) + OpenMetadata
# ==============================================================================

set -e

GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[1;33m"
CYAN="\033[0;36m"
RED="\033[0;31m"
BOLD="\033[1m"
NC="\033[0m"

COMPOSE_FILE="docker-compose.yaml"

# Danh sách containers Option 1
SERVICES_OPT1=(
  postgres-source
  postgres-big-data
  pgadmin
  kafka
  schema-registry
  kafka-connect
  kafka-connect-init
  kafka-ui
  namenode
  datanode
  spark-master
  spark-worker
  clickhouse
  superset
  airflow
  elasticsearch
  postgres-openmetadata
  openmetadata-server
  openmetadata-ingestion
)

# Danh sách containers Option 2
SERVICES_OPT2=(
  minio
  nifi
  clickhouse
  superset
  elasticsearch
  postgres-openmetadata
  openmetadata-server
  openmetadata-ingestion
)

show_header() {
  echo -e "${BLUE}${BOLD}"
  echo "=============================================================================="
  echo "         🚀 ENTERPRISE BIG DATA & LAKEHOUSE PLATFORM MANAGER"
  echo "=============================================================================="
  echo -e "${NC}"
}

print_opt1_endpoints() {
  echo -e "\n${GREEN}${BOLD}🎉 OPTION 1 ĐÃ KHỞI ĐỘNG THÀNH CÔNG! DANH SÁCH CÁC CỔNG GIAO DIỆN (WEB UI):${NC}"
  echo -e "${CYAN}------------------------------------------------------------------------------${NC}"
  printf "${BOLD}%-28s %-32s %-20s${NC}\n" "Dịch vụ" "Địa chỉ Web UI" "Tài khoản / Ghi chú"
  echo -e "${CYAN}------------------------------------------------------------------------------${NC}"
  printf "%-28s %-32s %-20s\n" "📊 Apache Superset" "http://localhost:8088" "admin / admin"
  printf "%-28s %-32s %-20s\n" "🛡️ OpenMetadata" "http://localhost:8585" "admin / admin"
  printf "%-28s %-32s %-20s\n" "🌀 Apache Airflow" "http://localhost:8089" "admin / Admin@123"
  printf "%-28s %-32s %-20s\n" "📨 Kafka UI" "http://localhost:8080" "Cluster quản lý"
  printf "%-28s %-32s %-20s\n" "⚡ Spark Master UI" "http://localhost:8085" "Giám sát Spark Jobs"
  printf "%-28s %-32s %-20s\n" "🏛️ HDFS NameNode UI" "http://localhost:9870" "HDFS Lakehouse"
  printf "%-28s %-32s %-20s\n" "🗄️ pgAdmin" "http://localhost:5050" "admin@admin.com / Admin@123"
  printf "%-28s %-32s %-20s\n" "⚡ ClickHouse HTTP" "http://localhost:8123" "admin / Admin@123"
  echo -e "${CYAN}------------------------------------------------------------------------------${NC}"
}

print_opt2_endpoints() {
  echo -e "\n${GREEN}${BOLD}🎉 OPTION 2 ĐÃ KHỞI ĐỘNG THÀNH CÔNG! DANH SÁCH CÁC CỔNG GIAO DIỆN (WEB UI):${NC}"
  echo -e "${CYAN}------------------------------------------------------------------------------${NC}"
  printf "${BOLD}%-28s %-32s %-20s${NC}\n" "Dịch vụ" "Địa chỉ Web UI" "Tài khoản / Ghi chú"
  echo -e "${CYAN}------------------------------------------------------------------------------${NC}"
  printf "%-28s %-32s %-20s\n" "💧 Apache NiFi" "https://localhost:8443/nifi" "admin / Admin@123456789"
  printf "%-28s %-32s %-20s\n" "📦 MinIO S3 Console" "http://localhost:9001" "admin / Admin@123"
  printf "%-28s %-32s %-20s\n" "📊 Apache Superset" "http://localhost:8088" "admin / admin"
  printf "%-28s %-32s %-20s\n" "🛡️ OpenMetadata" "http://localhost:8585" "admin / admin"
  printf "%-28s %-32s %-20s\n" "⚡ ClickHouse HTTP" "http://localhost:8123" "admin / Admin@123"
  echo -e "${CYAN}------------------------------------------------------------------------------${NC}"
}

run_option_1() {
  echo -e "${YELLOW}▶ [OPTION 1] Đang khởi động hệ thống Big Data CDC & Lakehouse + Airflow + OpenMetadata...${NC}"
  echo -e "Các container bao gồm: Postgres (Source & BigData), Kafka, Schema-Registry, Debezium Connect, HDFS, Spark, ClickHouse, Superset, Airflow, OpenMetadata, Elasticsearch."
  docker compose -f "$COMPOSE_FILE" up -d "${SERVICES_OPT1[@]}"
  print_opt1_endpoints
}

run_option_2() {
  echo -e "${YELLOW}▶ [OPTION 2] Đang khởi động luồng NiFi Fast-Path (Logs & Files -> MinIO -> NiFi -> ClickHouse) + OpenMetadata...${NC}"
  echo -e "Bỏ qua: PostgreSQL, Kafka, HDFS, Spark. Tiết kiệm tối đa tài nguyên!"
  docker compose -f "$COMPOSE_FILE" up -d "${SERVICES_OPT2[@]}"
  print_opt2_endpoints
}

stop_all() {
  echo -e "${YELLOW}⏹ Đang dừng toàn bộ các container trong cụm...${NC}"
  docker compose -f "$COMPOSE_FILE" down
  echo -e "${GREEN}✅ Đã dừng toàn bộ dịch vụ sạch sẽ!${NC}"
}

show_status() {
  echo -e "${YELLOW}🔍 Trạng thái các container đang chạy:${NC}"
  docker compose -f "$COMPOSE_FILE" ps
}

# ==============================================================================
# XỬ LÝ THAM SỐ ĐẦU VÀO
# ==============================================================================
show_header

if [ "$1" == "1" ] || [ "$1" == "--opt1" ] || [ "$1" == "-1" ]; then
  run_option_1
elif [ "$1" == "2" ] || [ "$1" == "--opt2" ] || [ "$1" == "-2" ]; then
  run_option_2
elif [ "$1" == "down" ] || [ "$1" == "stop" ]; then
  stop_all
elif [ "$1" == "status" ] || [ "$1" == "ps" ]; then
  show_status
else
  echo -e "Vui lòng chọn chế độ khởi chạy:"
  echo -e "  ${BOLD}[1] Option 1:${NC} Cụm Big Data CDC + HDFS Lakehouse + Spark + ClickHouse + Airflow + OpenMetadata"
  echo -e "  ${BOLD}[2] Option 2:${NC} Cụm Fast-Path: Source S3 (MinIO) + NiFi + ClickHouse + Superset + OpenMetadata (Không cần Postgres/Kafka/Spark/Hadoop)"
  echo -e "  ${BOLD}[3] Trạng thái:${NC} Xem danh sách container đang chạy (status / ps)"
  echo -e "  ${BOLD}[4] Dừng hệ thống:${NC} Dừng toàn bộ các container (stop / down)"
  echo -e "  ${BOLD}[0] Thoát${NC}"
  echo ""
  read -p "👉 Nhập lựa chọn của bạn [1/2/3/4/0]: " choice

  case "$choice" in
    1) run_option_1 ;;
    2) run_option_2 ;;
    3) show_status ;;
    4) stop_all ;;
    *) echo -e "${YELLOW}Đã thoát.${NC}" ;;
  esac
fi
