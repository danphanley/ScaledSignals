package io.confluent.dan;

import io.confluent.dan.generated.Metadata;
import io.confluent.dan.generated.Signals;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DualProducer {

    public void run() {

        System.out.println("DualProducer: Starting");
        Properties props = Configuration.invoke();

        // Hard coding topics
        String metadataTopic = "metadata";
        String signalsTopic = "signals";

        Producer<String, Metadata> metadataProducer = new KafkaProducer<>(props);
        Producer<String, Signals> signalsProducer = new KafkaProducer<>(props);

        ScheduledExecutorService metadataExecutor = Executors.newScheduledThreadPool(2);
        ScheduledExecutorService signalsExecutor = Executors.newScheduledThreadPool(2);

        final CountDownLatch latch = new CountDownLatch(5);

        Runnable metadataTask = () -> {
            Metadata meta = MetadataGenerator.getNext();
            System.out.println("Generated event " + meta.toString());
            ProducerRecord<String, Metadata> record = new ProducerRecord<String, Metadata>(metadataTopic, "key1", meta);
            metadataProducer.send(record);
            latch.countDown();
        };

        Runnable signalsTask = () -> {
            Signals signals = SignalsGenerator.getNext();
            System.out.println("Generated event " + signals.toString());
            ProducerRecord<String, Signals> record = new ProducerRecord<>(signalsTopic, "key1", signals);
            signalsProducer.send(record);
        };

        int metadataInitialDelay = 1;
        int signalsInitialDelay = 2;
        int metadataPeriod = 5;
        int signalsPeriod = 1;

        metadataExecutor.scheduleAtFixedRate(metadataTask, metadataInitialDelay, metadataPeriod, TimeUnit.SECONDS);
        signalsExecutor.scheduleAtFixedRate(signalsTask, signalsInitialDelay, signalsPeriod, TimeUnit.SECONDS);

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
            String schemaUrl = "http://localhost:8081";
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put("schema.registry.url", schemaUrl);
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