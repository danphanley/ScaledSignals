java -jar lib/avro-tools-1.8.2.jar compile schema avro/metadata.avsc src
java -jar lib/avro-tools-1.8.2.jar compile schema avro/signals.avsc src

bin/kafka-avro-console-producer --broker-list localhost:9092 --topic metadata \
  --property parse.key=true \
  --property key.schema='{"type":"string"}' \
  --property value.schema='{"type":"record","name":"metadata","fields":[{"name":"signal1scale","type":"int"}]}'



"key1"  {"signal1scale": 1}
"key2"  {"signal1scale": 8}

bin/kafka-avro-console-consumer --topic metadata \
  --bootstrap-server localhost:9092 \
  --property print.key=true

bin/kafka-avro-console-producer --broker-list localhost:9092 --topic signals \
    --property parse.key=true \
    --property key.schema='{"type":"string"}' \
    --property value.schema='{"type":"record","name":"Signals","namespace":"dan.auditoy","fields":[{"name":"temp","type":"int"},{"name":"pressure","type":"int"}]}'

"key1"	{"signal1scale": 1}
"key2"  {"signal1scale": 8}

bin/kafka-avro-console-consumer --topic signals \
  --bootstrap-server localhost:9092 \
  --property print.key=true

bin/kafka-avro-console-consumer --topic scaledsignals2 \
    --bootstrap-server localhost:9092 \
    --property print.key=true

bin/kafka-avro-console-consumer --topic metadata2 \
        --bootstrap-server localhost:9092 \
        --property print.key=true