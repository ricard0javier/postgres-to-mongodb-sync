package io.syncbench.aggregation;

import java.util.List;

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
 *
 * <p>Row events are staged per customer so large transactions (for example delete-all) never write a
 * single multi-megabyte value to the state-store changelog.
 */
final class TransactionAssemblerProcessor implements Processor<String, String, String, String> {
  static final String TRANSACTIONS_STORE = "transactions-by-id";
  private static final String META_PREFIX = "meta:";
  private static final String EVENTS_PREFIX = "events:";
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
      emitBundle(null, affectedCustomerIds(envelope), List.of(envelope));
      return;
    }
    stageRow(transactionId, envelope);
    flushIfComplete(transactionId);
  }

  /** Appends one row event to each affected customer's staged bundle for the transaction. */
  private void stageRow(String transactionId, JsonNode envelope) {
    ObjectNode meta = storedObject(transactions.get(metaKey(transactionId)));
    meta.put("seen", meta.path("seen").asLong() + 1);
    ArrayNode customers = arrayOrCreate(meta, "customers");
    for (String customerId : affectedCustomerIds(envelope)) {
      addCustomer(customers, customerId);
      ArrayNode events = storedArray(transactions.get(eventsKey(transactionId, customerId)));
      events.add(envelope);
      transactions.put(eventsKey(transactionId, customerId), serialize(events));
    }
    transactions.put(metaKey(transactionId), serialize(meta));
  }

  /** Records the expected row count supplied by a Debezium END marker. */
  private void handleBoundary(String transactionId, JsonNode event) {
    if (transactionId == null || !"END".equals(event.path("status").asText())) {
      return;
    }
    ObjectNode meta = storedObject(transactions.get(metaKey(transactionId)));
    meta.put("expected", event.path("event_count").asLong(-1));
    transactions.put(metaKey(transactionId), serialize(meta));
    flushIfComplete(transactionId);
  }

  /** Emits only when the number of staged row events exactly matches Debezium's END event count. */
  private void flushIfComplete(String transactionId) {
    ObjectNode meta = storedObject(transactions.get(metaKey(transactionId)));
    long expected = meta.path("expected").asLong(-1);
    if (expected < 0 || meta.path("seen").asLong() != expected) {
      return;
    }
    ArrayNode customers = arrayOrCreate(meta, "customers");
    for (JsonNode customerNode : customers) {
      String customerId = customerNode.asText();
      ArrayNode events = storedArray(transactions.get(eventsKey(transactionId, customerId)));
      emitBundle(transactionId, customerId, events);
      transactions.delete(eventsKey(transactionId, customerId));
    }
    transactions.delete(metaKey(transactionId));
  }

  /** Forwards one customer bundle assembled from staged row events. */
  private void emitBundle(String transactionId, String customerId, ArrayNode events) {
    ObjectNode bundle = JSON.createObjectNode();
    if (transactionId != null) {
      bundle.put("transactionId", transactionId);
    }
    bundle.set("events", events);
    context.forward(new Record<>(customerId, serialize(bundle), context.currentSystemTimeMs()));
  }

  /** Forwards the same envelope to every customer affected by a non-transactional row event. */
  private void emitBundle(String transactionId, java.util.Set<String> customerIds, List<JsonNode> envelopes) {
    ArrayNode events = JSON.createArrayNode();
    envelopes.forEach(events::add);
    for (String customerId : customerIds) {
      emitBundle(transactionId, customerId, events);
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

  /** Tracks a customer ID once inside transaction metadata. */
  private static void addCustomer(ArrayNode customers, String customerId) {
    for (JsonNode existing : customers) {
      if (customerId.equals(existing.asText())) {
        return;
      }
    }
    customers.add(customerId);
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

  /** Returns the metadata key used for one PostgreSQL transaction. */
  static String metaKey(String transactionId) {
    return META_PREFIX + transactionId;
  }

  /** Returns the staging key used for one customer within a transaction. */
  static String eventsKey(String transactionId, String customerId) {
    return EVENTS_PREFIX + transactionId + ":" + customerId;
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

  /** Deserializes stored JSON into an array, returning an empty array when unavailable. */
  private static ArrayNode storedArray(String json) {
    JsonNode value = json == null ? null : parse(json);
    return value instanceof ArrayNode array ? array : JSON.createArrayNode();
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
