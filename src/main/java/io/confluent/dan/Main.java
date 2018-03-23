package io.confluent.dan;

import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        Executors.newSingleThreadExecutor()
            .submit(() -> {
                new DualProducer().run();
        });

        Executors.newSingleThreadExecutor()
            .submit(() -> {
                new ScaleStreams().run();
        });
    }
}
