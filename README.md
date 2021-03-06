# Simple Scaler

This project is intended to show how Signal data may be scaled by using a table of scale factors.

The DualProducer class writes messages to a Signals stream and a Metadata stream.
The ScaleStream builds a table from Metadata and uses the metadata to scale the Signal values before writing them out to a new ScaledSignals stream.
The ScaleStreamScala does the same but in Scala using Ligthbend's Scala wrappers

In order to keep things simple, the same scale factor is used for all signals, and all records are hardcoded to the same key.

---
To build:

```console
mvn clean compile
```

To run:
```console
confluent start
mvn exec:java
```

To monitor:
```console
bin/kafka-avro-console-consumer --topic metadata \
    --bootstrap-server localhost:9092 \
    --property print.key=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 

bin/kafka-avro-console-consumer --topic signals \
    --bootstrap-server localhost:9092 \
    --property print.key=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
          

bin/kafka-avro-console-consumer --topic scaled_signals \
    --bootstrap-server localhost:9092 \
    --property print.key=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer   

bin/kafka-avro-console-consumer --topic scaled_signals_scala \
    --bootstrap-server localhost:9092 \
    --property print.key=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDes <....

```