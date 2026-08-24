package io.syncbench.aggregation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/** Routes a CDC row or Debezium transaction marker to the partition selected by transaction ID. */
final class TransactionRouterProcessor implements Processor<String, String, String, String> {
  private static final ObjectMapper JSON = new ObjectMapper();

  private final String sourceTopic;
  private ProcessorContext<String, String> context;

  /** Creates a router for records received from the given CDC source topic. */
  TransactionRouterProcessor(String sourceTopic) {
    this.sourceTopic = sourceTopic;
  }

  /** Initializes forwarding for this processor task. */
  @Override
  public void init(ProcessorContext<String, String> context) {
    this.context = context;
  }

  /**
   * Wraps source metadata with the original event. PostgreSQL row and END IDs have different LSN
   * suffixes, so both are keyed by the prefix before their first colon.
   */
  @Override
  public void process(Record<String, String> record) {
    if (record.value() == null) {
      return;
    }
    JsonNode event = parse(record.value());
    String transactionId = CustomerDocumentTopology.TRANSACTION_TOPIC.equals(sourceTopic)
        ? normalizedTransactionId(text(event.get("id")))
        : normalizedTransactionId(text(event.path("transaction").get("id")));
    ObjectNode envelope = JSON.createObjectNode();
    envelope.put("source", sourceTopic);
    if (record.key() != null) {
      envelope.put("key", record.key());
    }
    envelope.set("event", event);
    if (transactionId != null) {
      envelope.put("transactionId", transactionId);
    }
    String partitionKey = transactionId == null
        ? "snapshot:" + sourceTopic + ":" + String.valueOf(record.key()) : transactionId;
    context.forward(new Record<>(partitionKey, serialize(envelope), record.timestamp()));
  }

  /** Removes the LSN suffix from a Debezium transaction identifier. */
  static String normalizedTransactionId(String transactionId) {
    if (transactionId == null) {
      return null;
    }
    int separator = transactionId.indexOf(':');
    return separator < 0 ? transactionId : transactionId.substring(0, separator);
  }

  /** Returns a non-blank textual JSON value, or {@code null}. */
  private static String text(JsonNode value) {
    if (value == null || value.isNull()) {
      return null;
    }
    String result = value.asText();
    return result.isBlank() ? null : result;
  }

  /** Parses a JSON event received from Debezium. */
  private static JsonNode parse(String json) {
    try {
      return JSON.readTree(json);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Invalid JSON from Debezium", exception);
    }
  }

  /** Serializes an event envelope for the next topology stage. */
  private static String serialize(JsonNode value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to serialize transaction event", exception);
    }
  }
}
