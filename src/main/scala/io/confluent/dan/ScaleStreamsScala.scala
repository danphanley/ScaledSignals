package io.confluent.dan

import java.util
import java.util.{Collections, Properties}

import com.lightbend.kafka.scala.streams.{KStreamS, StreamsBuilderS}
import io.confluent.dan.generated.{Metadata, Signals}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsConfig}

class ScaleStreamsScala {
  def run(): Unit = {

    println("StreamJoiner: Starting")
    val streamsConfiguration: Properties = configure

    val serdeConfig: util.Map[String, String] = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, streamsConfiguration.getProperty("schema.registry.url"))

    implicit val keySerde: Serde[String] = Serdes.String
    keySerde.configure(serdeConfig, true)

    implicit val signalsValueSerde: Serde[Signals] = new SpecificAvroSerde[Signals]
    signalsValueSerde.configure(serdeConfig, false)

    implicit val metadataValueSerde: Serde[Metadata] = new SpecificAvroSerde[Metadata]
    metadataValueSerde.configure(serdeConfig, false)

    val builder = new StreamsBuilderS

    val signals = builder.stream[String, Signals]("signals", Consumed.`with`(keySerde, signalsValueSerde))
    val metadata =  builder.table[String, Metadata]("metadata", Consumed.`with`(keySerde, metadataValueSerde))

    val scaledSignals: KStreamS[String, Signals] = signals.join(metadata,
       (s: Signals, m: Metadata) => new Signals(s.getTemp * m.getSignalscalefactor,
                                                 s.getPressure * m.getSignalscalefactor))

    scaledSignals.to("scaled_signals_scala", Produced.`with`(keySerde, signalsValueSerde))

    val streams = new KafkaStreams(builder.build, streamsConfiguration)
    streams.start()

  }

  private def configure = {
    val streamsConfiguration = new Properties()
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "auditoyScala")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamsConfiguration.put("schema.registry.url", "http://localhost:8081")
    streamsConfiguration.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor")
    streamsConfiguration.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor")
    streamsConfiguration
  }
}

