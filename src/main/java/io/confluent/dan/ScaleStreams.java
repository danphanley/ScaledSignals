package io.confluent.dan;

import io.confluent.dan.generated.Metadata;
import io.confluent.dan.generated.Signals;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;


import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ScaleStreams {

    public void run () {
        System.out.println("ScaleStreams: Starting");
        Properties props = Configuration.invoke();
        StreamsConfig config = new StreamsConfig(props);
        StreamsBuilder builder = new StreamsBuilder();

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getProperty("schema.registry.url"));
        Serde<String> keySerdes = Serdes.String();
        keySerdes.configure(serdeConfig, true);

        final SpecificAvroSerde<Metadata> metadataValueSerdes = new SpecificAvroSerde<>();
        metadataValueSerdes.configure(serdeConfig, false);

        final SpecificAvroSerde<Signals> signalsValueSerdes = new SpecificAvroSerde<>();
        signalsValueSerdes.configure(serdeConfig, false);

        KTable<String, Metadata> metadata = builder.table("metadata", Consumed.with(keySerdes, metadataValueSerdes));
        KStream<String, Signals> signals = builder.stream("signals", Consumed.with(keySerdes, signalsValueSerdes));

        signals.join(metadata,
                        (s, m) -> {
                            System.out.println("S");
                            return new Signals(
                                s.getTemp()*m.getSignal1scale(),
                                s.getPressure()*m.getSignal1scale()
                        );}, Joined.with(keySerdes,signalsValueSerdes,metadataValueSerdes))
                .to("scaledsignals", Produced.with(keySerdes, signalsValueSerdes));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }

    private static class Configuration {
        private static Properties invoke() {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "auditoy");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put("schema.registry.url", "http://localhost:8081");
            return props;
        }
    }
}

