#!/bin/bash

# ==============================================================================
# Script Đăng ký JDBC Sink Connector đồng bộ dữ liệu vào PostgreSQL Data Warehouse
# ==============================================================================

# Giá trị mặc định
CONNECT_HOST="localhost"
CONNECT_PORT="8083"
DB_HOST="postgres-big-data"
DB_PORT="5432"
DB_USER="admin"
DB_PASS="Admin@123"
DB_NAME="bigdata_db"
TABLE_NAME="users"
TOPIC_NAME="cdc_data.public.users"
SCHEMA_REGISTRY_URL="http://schema-registry:8081"

# Đọc tham số truyền vào
while [ $# -gt 0 ]; do
  case "$1" in
    --connect-host=*) CONNECT_HOST="${1#*=}" ;;
    --connect-host)   CONNECT_HOST="$2"; shift ;;
    --connect-port=*) CONNECT_PORT="${1#*=}" ;;
    --connect-port)   CONNECT_PORT="$2"; shift ;;
    --db-host=*)      DB_HOST="${1#*=}" ;;
    --db-host)        DB_HOST="$2"; shift ;;
    --db-port=*)      DB_PORT="${1#*=}" ;;
    --db-port)        DB_PORT="$2"; shift ;;
    --db-name=*)      DB_NAME="${1#*=}" ;;
    --db-name)        DB_NAME="$2"; shift ;;
    --table=*)        TABLE_NAME="${1#*=}" ;;
    --table)          TABLE_NAME="$2"; shift ;;
    --topic=*)        TOPIC_NAME="${1#*=}" ;;
    --topic)          TOPIC_NAME="$2"; shift ;;
    --schema-registry=*) SCHEMA_REGISTRY_URL="${1#*=}" ;;
    --schema-registry)   SCHEMA_REGISTRY_URL="$2"; shift ;;
  esac
  shift
done

CONNECTOR_NAME="postgres-bigdata-${TABLE_NAME}-sink"

echo "Đang đăng ký JDBC Sink Connector cho bảng: ${TABLE_NAME} từ topic: ${TOPIC_NAME} (Connector: ${CONNECTOR_NAME})..."

curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  "http://${CONNECT_HOST}:${CONNECT_PORT}/connectors/" \
  -d '{
    "name": "'"$CONNECTOR_NAME"'",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
      "tasks.max": "1",
      "topics": "'"$TOPIC_NAME"'",
      "table.name.format": "'"$TABLE_NAME"'",
      
      "connection.url": "jdbc:postgresql://'"$DB_HOST"':'"$DB_PORT"'/'"$DB_NAME"'",
      "connection.user": "'"$DB_USER"'",
      "connection.password": "'"$DB_PASS"'",
      
      "insert.mode": "upsert",
      "pk.mode": "record_value",
      "pk.fields": "id",
      "auto.create": "true",
      "auto.evolve": "true",
      
      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "key.converter.schema.registry.url": "'"$SCHEMA_REGISTRY_URL"'",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "'"$SCHEMA_REGISTRY_URL"'",
      
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false"
    }
  }'
echo ""