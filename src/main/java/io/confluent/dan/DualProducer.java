package io.confluent.dan;

import io.confluent.dan.generated.Metadata;
import io.confluent.dan.generated.Signals;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DualProducer {

    public static Topic<String, Metadata> METADATA;
    public static Topic<String, Signals> SIGNALS;

    public void run() {

        System.out.println("DualProducer: Starting");
        Properties props = Configuration.invoke();

        METADATA = new Topic<>("metadata", Serdes.String(), new SpecificAvroSerde<Metadata>());
        SIGNALS = new Topic<>("signals", Serdes.String(), new SpecificAvroSerde<Signals>());

        Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getProperty("schema.registry.url"));

        METADATA.keySerde().configure(serdeConfig, true);
        METADATA.valueSerde().configure(serdeConfig, false);
        SIGNALS.keySerde().configure(serdeConfig, true);
        SIGNALS.valueSerde().configure(serdeConfig, false);


        Producer<String, Metadata> metadataProducer = new KafkaProducer<>(props, METADATA.keySerde().serializer(),
                METADATA.valueSerde().serializer());

        Producer<String, Signals> signalsProducer = new KafkaProducer<>(props, SIGNALS.keySerde().serializer(),
                SIGNALS.valueSerde().serializer());

        ScheduledExecutorService metadataExecutor = Executors.newScheduledThreadPool(1);
        ScheduledExecutorService signalsExecutor = Executors.newScheduledThreadPool(1);

        final CountDownLatch latch = new CountDownLatch(5000);

        Runnable metadataTask = () -> {
            Metadata meta = MetadataGenerator.getNext();
            //System.out.println("Generated event " + meta.toString());
            ProducerRecord<String, Metadata> record = new ProducerRecord<>(METADATA.name(), "key1", meta);
            metadataProducer.send(record);
            latch.countDown();
        };

        Runnable signalsTask = () -> {
            Signals signals = SignalsGenerator.getNext();
            //System.out.println("Generated event " + signals.toString());
            ProducerRecord<String, Signals> record = new ProducerRecord<>(SIGNALS.name(), "key1", signals);
            signalsProducer.send(record);
        };

        int metadataInitialDelay = 1;
        int signalsInitialDelay = 2;
        int metadataPeriod = 5;
        int signalsPeriod = 1;

        metadataExecutor.scheduleAtFixedRate(metadataTask, metadataInitialDelay, metadataPeriod, TimeUnit.SECONDS);
        signalsExecutor.scheduleAtFixedRate(signalsTask, signalsInitialDelay, signalsPeriod, TimeUnit.MILLISECONDS);

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("DualProducer: Stopping production");

        metadataExecutor.shutdownNow();
        signalsExecutor.shutdownNow();

        System.out.println("DualProducer: Exiting");

    }

    private static class Configuration {
        private static Properties invoke() {
            Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put("acks", "all");
            props.put("schema.registry.url", "http://localhost:8081");
            props.put("retries", 0);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor" );
            return props;
        }
    }
}

class MetadataGenerator {
    public static Metadata getNext() {
        Random r = new Random();
        return new Metadata(r.nextInt(10));
    }
}

class SignalsGenerator {
    public static Signals getNext() {
        Random r = new Random();
        return new Signals(r.nextInt(10), r.nextInt(10));
    }
}