FROM confluentinc/cp-kafka-connect-base:7.5.0

# 1. Cài đặt Avro Converter từ Confluent Hub
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.5.0

# 2. Tải và giải nén trực tiếp Debezium Postgres Plugin vào thư mục plugin
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-postgres \
    && curl -sSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.4.0.Final/debezium-connector-postgres-2.4.0.Final-plugin.tar.gz \
       | tar -xz -C /usr/share/confluent-hub-components/debezium-connector-postgres --strip-components=1