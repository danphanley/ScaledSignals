package io.confluent.dan;

import org.apache.kafka.common.serialization.Serde;

public class Topic<K, V> {

    private String name;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;

    Topic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public String name() {
        return name;
    }

    public String toString() {
        return name;
    }
}