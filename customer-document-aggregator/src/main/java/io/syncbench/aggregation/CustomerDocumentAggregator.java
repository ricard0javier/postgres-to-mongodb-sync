package io.syncbench.aggregation;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Application entry point for the Kafka Streams customer-document aggregator.
 *
 * <p>The application ID controls the Kafka Streams consumer group and state-store changelog
 * topics. {@code KAFKA_BOOTSTRAP_SERVERS} and {@code KAFKA_APPLICATION_ID} can override the local
 * Docker Compose defaults. The shutdown hook closes the stream cleanly so offsets and state are
 * committed before the container stops.
 */
public final class CustomerDocumentAggregator {
  /** Prevents construction because the class only provides the application entry point. */
  private CustomerDocumentAggregator() {
  }

  /** Builds the topology, starts processing CDC records, and installs graceful shutdown handling. */
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
        environment("KAFKA_APPLICATION_ID", "customer-document-aggregator"));
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        environment("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
        positiveInteger("KAFKA_STREAM_THREADS", 1));
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
    properties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);

    int maxMessageBytes = positiveInteger("KAFKA_MAX_MESSAGE_BYTES", 67_108_864);
    properties.put(StreamsConfig.producerPrefix("max.request.size"), maxMessageBytes);
    properties.put(StreamsConfig.consumerPrefix("max.partition.fetch.bytes"), maxMessageBytes);
    properties.put(StreamsConfig.consumerPrefix("fetch.max.bytes"), maxMessageBytes * 2);

    CountDownLatch shutdownLatch = new CountDownLatch(1);
    KafkaStreams streams = new KafkaStreams(CustomerDocumentTopology.build(), properties);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      streams.close(Duration.ofSeconds(15));
      shutdownLatch.countDown();
    }));
    try (streams) {
      streams.start();
      try {
        shutdownLatch.await();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Reads a non-blank environment value while preserving a local-development default. */
  private static String environment(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  /** Reads a positive integer environment value for tunable Kafka Streams concurrency. */
  private static int positiveInteger(String name, int defaultValue) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value);
      if (parsed > 0) {
        return parsed;
      }
    } catch (NumberFormatException ignored) {
      // The message below gives the operator the actionable configuration failure.
    }
    throw new IllegalArgumentException(name + " must be a positive integer.");
  }
}
