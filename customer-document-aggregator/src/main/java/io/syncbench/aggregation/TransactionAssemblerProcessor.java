package io.syncbench.aggregation;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Assembles all CDC rows for one PostgreSQL transaction before emitting customer-specific bundles.
 *
 * <p>The input is keyed by normalized transaction ID, so one task sees the complete row count and
 * Debezium END marker even when the original CDC topics have many unrelated partitions.
 */
final class TransactionAssemblerProcessor implements Processor<String, String, String, String> {
  static final String TRANSACTIONS_STORE = "transactions-by-id";
  private static final ObjectMapper JSON = new ObjectMapper();

  private ProcessorContext<String, String> context;
  private KeyValueStore<String, String> transactions;

  /** Initializes access to the transaction staging store. */
  @Override
  public void init(ProcessorContext<String, String> context) {
    this.context = context;
    transactions = context.getStateStore(TRANSACTIONS_STORE);
  }

  /** Stages a row event or handles a transaction completion marker. */
  @Override
  public void process(Record<String, String> record) {
    if (record.value() == null) {
      return;
    }
    JsonNode envelope = parse(record.value());
    String source = text(envelope.get("source"));
    JsonNode event = object(envelope.get("event"));
    if (source == null || event == null) {
      return;
    }
    if (CustomerDocumentTopology.TRANSACTION_TOPIC.equals(source)) {
      handleBoundary(record.key(), event);
      return;
    }
    String transactionId = text(envelope.get("transactionId"));
    if (transactionId == null) {
      emitBundles(null, java.util.List.of(envelope));
      return;
    }
    ObjectNode transaction = storedObject(transactions.get(transactionId));
    ArrayNode events = arrayOrCreate(transaction, "events");
    events.add(envelope);
    transaction.put("seen", transaction.path("seen").asLong() + 1);
    transactions.put(transactionId, serialize(transaction));
    flushIfComplete(transactionId);
  }

  /** Records the expected row count supplied by a Debezium END marker. */
  private void handleBoundary(String transactionId, JsonNode event) {
    if (transactionId == null || !"END".equals(event.path("status").asText())) {
      return;
    }
    ObjectNode transaction = storedObject(transactions.get(transactionId));
    transaction.put("expected", event.path("event_count").asLong(-1));
    transactions.put(transactionId, serialize(transaction));
    flushIfComplete(transactionId);
  }

  /** Emits only when the number of staged row events exactly matches Debezium's END event count. */
  private void flushIfComplete(String transactionId) {
    ObjectNode transaction = storedObject(transactions.get(transactionId));
    long expected = transaction.path("expected").asLong(-1);
    if (expected < 0 || transaction.path("seen").asLong() != expected) {
      return;
    }
    ArrayNode events = arrayOrCreate(transaction, "events");
    emitBundles(transactionId, events);
    transactions.delete(transactionId);
  }

  /** Groups raw row events by every customer affected by the row, including reassigned children. */
  private void emitBundles(String transactionId, Iterable<JsonNode> events) {
    Map<String, ArrayNode> byCustomer = new LinkedHashMap<>();
    for (JsonNode envelope : events) {
      for (String customerId : affectedCustomerIds(envelope)) {
        byCustomer.computeIfAbsent(customerId, ignored -> JSON.createArrayNode()).add(envelope);
      }
    }
    for (Map.Entry<String, ArrayNode> entry : byCustomer.entrySet()) {
      ObjectNode bundle = JSON.createObjectNode();
      if (transactionId != null) {
        bundle.put("transactionId", transactionId);
      }
      bundle.set("events", entry.getValue());
      context.forward(new Record<>(entry.getKey(), serialize(bundle), context.currentSystemTimeMs()));
    }
  }

  /** Identifies customers whose document is affected by a CDC row. */
  private static java.util.Set<String> affectedCustomerIds(JsonNode envelope) {
    java.util.Set<String> customerIds = new java.util.LinkedHashSet<>();
    String source = text(envelope.get("source"));
    JsonNode event = object(envelope.get("event"));
    if (source == null || event == null) {
      return customerIds;
    }
    JsonNode after = object(event.get("after"));
    JsonNode before = object(event.get("before"));
    String key = text(envelope.get("key"));
    if (CustomerDocumentTopology.CUSTOMERS_TOPIC.equals(source)) {
      add(customerIds, identifier(after, key, "id"));
      add(customerIds, identifier(before, key, "id"));
    } else {
      add(customerIds, identifier(after, key, "customer_id"));
      add(customerIds, identifier(before, key, "customer_id"));
    }
    return customerIds;
  }

  /** Adds a present customer identifier to a collection. */
  private static void add(java.util.Set<String> values, String value) {
    if (value != null) {
      values.add(value);
    }
  }

  /** Returns an array field, creating it when absent or malformed. */
  private static ArrayNode arrayOrCreate(ObjectNode parent, String field) {
    JsonNode value = parent.get(field);
    if (value instanceof ArrayNode array) {
      return array;
    }
    ArrayNode array = JSON.createArrayNode();
    parent.set(field, array);
    return array;
  }

  /** Resolves an identifier from a row, falling back to the Debezium key. */
  private static String identifier(JsonNode row, String key, String field) {
    if (row != null && row.hasNonNull(field)) {
      return row.get(field).asText();
    }
    if (key == null) {
      return null;
    }
    JsonNode parsedKey = parse(key);
    return parsedKey.hasNonNull(field) ? parsedKey.get(field).asText() : null;
  }

  /** Returns an object JSON node, or {@code null} for other values. */
  private static ObjectNode object(JsonNode value) {
    return value != null && value.isObject() ? (ObjectNode) value : null;
  }

  /** Returns a non-blank textual JSON value, or {@code null}. */
  private static String text(JsonNode value) {
    if (value == null || value.isNull()) {
      return null;
    }
    String result = value.asText();
    return result.isBlank() ? null : result;
  }

  /** Deserializes stored JSON into an object, returning an empty object when unavailable. */
  private static ObjectNode storedObject(String json) {
    JsonNode value = json == null ? null : object(parse(json));
    return value == null ? JSON.createObjectNode() : (ObjectNode) value;
  }

  /** Parses a JSON value from a topology record or state store. */
  private static JsonNode parse(String json) {
    try {
      return JSON.readTree(json);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Invalid JSON from Debezium", exception);
    }
  }

  /** Serializes a state-store value or customer bundle. */
  private static String serialize(JsonNode value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to serialize transaction bundle", exception);
    }
  }
}
