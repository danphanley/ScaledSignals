package io.confluent.dan;

import io.confluent.dan.generated.Metadata;
import io.confluent.dan.generated.Signals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;


import java.util.Properties;

public class ScaleStreams {

    public void run () {
        System.out.println("ScaleStreams: Starting");
        StreamsConfig config = new StreamsConfig(Configuration.invoke());
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Metadata> metadata = builder.table("metadata");
        KStream<String, Signals> signals = builder.stream("signals");

        signals.join(metadata,
                        (s, m) -> {
                            s.setTemp(s.getTemp()*m.getSignal1scale());
                            s.setPressure(s.getPressure()*m.getSignal1scale());
                            return s;
                        })
                .to("scaledsignals");

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
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
            return props;
        }
    }
}

