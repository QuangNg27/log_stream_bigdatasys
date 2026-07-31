#!/bin/bash

# Giá trị mặc định
CONNECT_HOST="localhost"
CONNECT_PORT="8083"
DB_HOST="postgres-source"
DB_PORT="5432"
DB_USER="admin"
DB_PASS="Admin@123"
DB_NAME="app_db"
SCHEMA_TABLE="public.users"
TOPIC_PREFIX="cdc_data"
SCHEMA_REGISTRY_URL="http://schema-registry:8081"

# Đọc tham số có tên đi kèm (--key=value hoặc -key value)
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
    --db-user=*)      DB_USER="${1#*=}" ;;
    --db-user)        DB_USER="$2"; shift ;;
    --db-pass=*)      DB_PASS="${1#*=}" ;;
    --db-pass)        DB_PASS="$2"; shift ;;
    --db-name=*)      DB_NAME="${1#*=}" ;;
    --db-name)        DB_NAME="$2"; shift ;;
    --table=*)        SCHEMA_TABLE="${1#*=}" ;;
    --table)          SCHEMA_TABLE="$2"; shift ;;
    --prefix=*)       TOPIC_PREFIX="${1#*=}" ;;
    --prefix)         TOPIC_PREFIX="$2"; shift ;;
  esac
  shift
done

# Tự động lấy ngày tháng và tên bảng
CURRENT_DATE=$(date +%Y%m%d)
TABLE_ONLY=$(echo "$SCHEMA_TABLE" | awk -F'.' '{print $NF}')
CONNECTOR_NAME="postgres-${DB_NAME}-${TABLE_ONLY}-${CURRENT_DATE}-connector"

# Thực thi gửi request duy nhất
curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  "http://${CONNECT_HOST}:${CONNECT_PORT}/connectors/" \
  -d '{
  "name": "'"$CONNECTOR_NAME"'",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "plugin.name": "pgoutput",
    "database.hostname": "'"$DB_HOST"'",
    "database.port": "'"$DB_PORT"'",
    "database.user": "'"$DB_USER"'",
    "database.password": "'"$DB_PASS"'",
    "database.dbname": "'"$DB_NAME"'",
    "database.server.name": "pg_cdc",
    "topic.prefix": "'"$TOPIC_PREFIX"'",
    "table.include.list": "'"$SCHEMA_TABLE"'",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "disabled",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "'"$SCHEMA_REGISTRY_URL"'",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "'"$SCHEMA_REGISTRY_URL"'",
    "tombstones.on.delete": "true"
  }
}'