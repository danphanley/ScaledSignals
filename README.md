# Simple Scaler

This project is intended to show how Signal data may be scaled by using a table of scale factors.

The DualProducer class writes messages to a Signals stream and a Metadata stream.
The StreamScaler builds a table from Metadata and uses the metadata to scale the Signal values before writing them out to a new ScaledSignals stream.

*Status* Broken

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
bin/kafka-avro-console-consumer --topic signals \
  --bootstrap-server localhost:9092 \
  --property print.key=true

bin/kafka-avro-console-consumer --topic scaledsignals \
    --bootstrap-server localhost:9092 \
    --property print.key=true

bin/kafka-avro-console-consumer --topic metadata \
        --bootstrap-server localhost:9092 \
        --property print.key=true
```