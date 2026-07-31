curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ \
  -d '{
    "name": "postgres-bigdata-sink",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
      "tasks.max": "1",
      "topics": "cdc_data.public.users",
      
      "connection.url": "jdbc:postgresql://postgres-big-data:5432/bigdata_db",
      "connection.user": "admin",
      "connection.password": "Admin@123",
      
      "insert.mode": "upsert",
      "pk.mode": "record_key",
      "pk.fields": "id",
      "auto.create": "true",
      "auto.evolve": "true",
      
      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "key.converter.schema.registry.url": "http://schema-registry:8081",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "http://schema-registry:8081",
      
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false"
    }
  }'