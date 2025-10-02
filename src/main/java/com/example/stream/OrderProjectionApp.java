package com.example.stream;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public final class OrderProjectionApp {
    private OrderProjectionApp() {
    }

    public static void main(String[] args) throws Exception {
        Properties props = loadProperties(args);
        applyDefaults(props);

        OrderProjectionTopology topologyFactory = new OrderProjectionTopology();
        Topology topology = topologyFactory.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            latch.countDown();
        }));

        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Properties loadProperties(String[] args) throws Exception {
        Properties props = new Properties();
        if (args.length == 0) {
            return props;
        }
        Path path = Path.of(args[0]);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Properties file not found: " + path);
        }
        try (FileInputStream input = new FileInputStream(path.toFile())) {
            props.load(input);
        }
        return props;
    }

    private static void applyDefaults(Properties props) {
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG,
                envOrDefault("KAFKA_STREAMS_APP_ID", "order-projection-app"));
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"));
        props.putIfAbsent(StreamsConfig.STATE_DIR_CONFIG, envOrDefault("KAFKA_STATE_DIR", "/tmp/kafka-streams"));
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    }

    private static String envOrDefault(String key, String defaultValue) {
        String fromEnv = System.getenv(key);
        return fromEnv == null || fromEnv.isBlank() ? defaultValue : fromEnv;
    }
}
