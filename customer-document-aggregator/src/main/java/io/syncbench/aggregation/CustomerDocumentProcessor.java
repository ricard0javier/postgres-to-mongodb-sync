package io.syncbench.aggregation;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
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
 * Applies complete, customer-keyed transaction bundles to normalized customer state.
 *
 * <p>Every instance owns a disjoint set of customer IDs because the bundle topic is keyed by
 * customer ID. It applies all rows in one bundle before emitting exactly one replacement document
 * or one root soft-delete marker.
 */
final class CustomerDocumentProcessor implements Processor<String, String, String, String> {
  static final String CUSTOMERS_STORE = "customers-by-id";
  static final String PROFILES_STORE = "profiles-by-customer";
  static final String ADDRESSES_STORE = "addresses-by-customer";
  static final String CONTACTS_STORE = "contacts-by-customer";
  static final String PREFERENCES_STORE = "preferences-by-customer";

  private static final ObjectMapper JSON = new ObjectMapper();

  private ProcessorContext<String, String> context;
  private KeyValueStore<String, String> customers;
  private KeyValueStore<String, String> profiles;
  private KeyValueStore<String, String> addresses;
  private KeyValueStore<String, String> contacts;
  private KeyValueStore<String, String> preferences;

  /** Initializes the normalized customer state stores. */
  @Override
  public void init(ProcessorContext<String, String> context) {
    this.context = context;
    customers = context.getStateStore(CUSTOMERS_STORE);
    profiles = context.getStateStore(PROFILES_STORE);
    addresses = context.getStateStore(ADDRESSES_STORE);
    contacts = context.getStateStore(CONTACTS_STORE);
    preferences = context.getStateStore(PREFERENCES_STORE);
  }

  /** Applies every CDC row in a bundle and emits the customer's final transaction state once. */
  @Override
  public void process(Record<String, String> record) {
    if (record.key() == null || record.value() == null) {
      return;
    }
    String customerId = record.key();
    JsonNode bundle = parse(record.value());
    JsonNode events = bundle.get("events");
    if (events == null || !events.isArray()) {
      return;
    }

    boolean rootDeleted = false;
    for (JsonNode envelope : events) {
      JsonNode event = object(envelope.get("event"));
      if (event == null) {
        continue;
      }
      JsonNode after = object(event.get("after"));
      JsonNode before = object(event.get("before"));
      boolean deleted = "d".equals(event.path("op").asText()) || after == null;
      String source = text(envelope.get("source"));
      String key = text(envelope.get("key"));
      if (CustomerDocumentTopology.CUSTOMERS_TOPIC.equals(source)) {
        rootDeleted |= handleCustomer(customerId, key, after, before, deleted);
      } else if (CustomerDocumentTopology.PROFILES_TOPIC.equals(source)) {
        handleSingleChild(profiles, customerId, key, after, before, deleted);
      } else if (CustomerDocumentTopology.PREFERENCES_TOPIC.equals(source)) {
        handleSingleChild(preferences, customerId, key, after, before, deleted);
      } else if (CustomerDocumentTopology.ADDRESSES_TOPIC.equals(source)) {
        handleCollectionChild(addresses, customerId, key, after, before, deleted);
      } else if (CustomerDocumentTopology.CONTACTS_TOPIC.equals(source)) {
        handleCollectionChild(contacts, customerId, key, after, before, deleted);
      }
    }
    emit(customerId, rootDeleted && customers.get(customerId) == null);
  }

  /** Updates root customer state and clears dependent state when the root is deleted. */
  private boolean handleCustomer(String customerId, String key, JsonNode after, JsonNode before,
      boolean deleted) {
    JsonNode row = deleted ? before : after;
    if (!customerId.equals(identifier(row, key, "id"))) {
      return false;
    }
    if (!deleted) {
      customers.put(customerId, serialize(after));
      return false;
    }
    customers.delete(customerId);
    profiles.delete(customerId);
    addresses.delete(customerId);
    contacts.delete(customerId);
    preferences.delete(customerId);
    return true;
  }

  /** Updates or removes a customer-owned singleton child row. */
  private void handleSingleChild(KeyValueStore<String, String> store, String customerId, String key,
      JsonNode after, JsonNode before, boolean deleted) {
    String afterCustomerId = identifier(after, key, "customer_id");
    String beforeCustomerId = identifier(before, key, "customer_id");
    if (customerId.equals(afterCustomerId) && !deleted) {
      store.put(customerId, serialize(after));
    } else if (customerId.equals(beforeCustomerId)) {
      store.delete(customerId);
    }
  }

  /** Updates or removes one row in a customer-owned collection. */
  private void handleCollectionChild(KeyValueStore<String, String> store, String customerId, String key,
      JsonNode after, JsonNode before, boolean deleted) {
    String rowId = identifier(deleted ? before : after, key, "id");
    if (rowId == null) {
      return;
    }
    String afterCustomerId = identifier(after, null, "customer_id");
    String beforeCustomerId = identifier(before, null, "customer_id");
    if (customerId.equals(afterCustomerId) && !deleted) {
      ObjectNode rows = storedObject(store.get(customerId));
      rows.set(rowId, after);
      store.put(customerId, serialize(rows));
    } else if (customerId.equals(beforeCustomerId)) {
      removeCollectionRow(store, customerId, rowId);
    }
  }

  /** Forwards either a customer document or its root soft-delete marker. */
  private void emit(String customerId, boolean deleted) {
    if (deleted) {
      ObjectNode softDelete = JSON.createObjectNode();
      softDelete.put("_id", customerId);
      softDelete.put("deleted", true);
      forward(customerId, softDelete);
    } else {
      emitDocument(customerId);
    }
  }

  /** Removes one collection row and clears its store entry when it becomes empty. */
  private void removeCollectionRow(KeyValueStore<String, String> store, String customerId, String rowId) {
    ObjectNode rows = storedObject(store.get(customerId));
    rows.remove(rowId);
    if (rows.isEmpty()) {
      store.delete(customerId);
    } else {
      store.put(customerId, serialize(rows));
    }
  }

  /** Builds and forwards the current embedded document for a customer. */
  private void emitDocument(String customerId) {
    String customerJson = customers.get(customerId);
    if (customerJson == null) {
      return;
    }
    ObjectNode customer = storedObject(customerJson);
    ObjectNode document = JSON.createObjectNode();
    document.put("_id", customerId);
    copy(document, "id", customer, "id");
    copy(document, "customer_number", customer, "customer_number");
    copy(document, "first_name", customer, "first_name");
    copy(document, "last_name", customer, "last_name");
    copyDate(document, "date_of_birth", customer.get("date_of_birth"));
    copy(document, "status", customer, "status");
    document.set("profile", profile(customerId));
    document.set("addresses", addresses(customerId));
    document.set("contacts", contacts(customerId));
    document.set("preferences", preferences(customerId));
    copyTimestamp(document, "created_at", customer.get("created_at"));
    String updatedAt = latestUpdatedAt(customerId, customer);
    if (updatedAt != null) {
      document.put("updated_at", updatedAt);
    }
    forward(customerId, document);
  }

  /** Maps stored profile data to the document profile shape. */
  private ObjectNode profile(String customerId) {
    ObjectNode source = storedObject(profiles.get(customerId));
    ObjectNode result = JSON.createObjectNode();
    copyNonNull(result, "preferredLanguage", source, "preferred_language");
    copyNonNull(result, "occupation", source, "occupation");
    copyNonNull(result, "annualIncome", source, "annual_income");
    copyNonNull(result, "taxResidencyCountry", source, "tax_residency_country");
    copyNonNull(result, "riskRating", source, "risk_rating");
    return result;
  }

  /** Maps stored address rows to the document address array. */
  private ArrayNode addresses(String customerId) {
    List<JsonNode> rows = collectionRows(addresses.get(customerId), "address_type");
    ArrayNode result = JSON.createArrayNode();
    for (JsonNode source : rows) {
      ObjectNode address = JSON.createObjectNode();
      copyNonNull(address, "type", source, "address_type");
      copyNonNull(address, "line1", source, "line_1");
      copyNonNull(address, "line2", source, "line_2");
      copyNonNull(address, "city", source, "city");
      copyNonNull(address, "region", source, "region");
      copyNonNull(address, "postalCode", source, "postal_code");
      copyNonNull(address, "countryCode", source, "country_code");
      copyDateNonNull(address, "validFrom", source.get("valid_from"));
      copyDateNonNull(address, "validTo", source.get("valid_to"));
      result.add(address);
    }
    return result;
  }

  /** Maps stored contact rows to the document contact array. */
  private ArrayNode contacts(String customerId) {
    List<JsonNode> rows = collectionRows(contacts.get(customerId), "contact_type");
    ArrayNode result = JSON.createArrayNode();
    for (JsonNode source : rows) {
      ObjectNode contact = JSON.createObjectNode();
      copyNonNull(contact, "type", source, "contact_type");
      copyNonNull(contact, "value", source, "contact_value");
      copyNonNull(contact, "isPrimary", source, "is_primary");
      copyTimestampNonNull(contact, "verifiedAt", source.get("verified_at"));
      result.add(contact);
    }
    return result;
  }

  /** Maps stored preferences to the document preferences shape. */
  private ObjectNode preferences(String customerId) {
    ObjectNode source = storedObject(preferences.get(customerId));
    ObjectNode result = JSON.createObjectNode();
    copyNonNull(result, "marketingEmailOptIn", source, "marketing_email_opt_in");
    copyNonNull(result, "marketingSmsOptIn", source, "marketing_sms_opt_in");
    copyNonNull(result, "paperlessStatements", source, "paperless_statements");
    JsonNode channels = source.get("notification_channels");
    if (channels != null && !channels.isNull()) {
      result.set("notificationChannels", jsonValue(channels));
    }
    return result;
  }

  /** Finds the newest update timestamp across a customer's stored rows. */
  private String latestUpdatedAt(String customerId, ObjectNode customer) {
    Instant latest = timestamp(customer.get("updated_at"));
    latest = later(latest, timestamp(storedObject(profiles.get(customerId)).get("updated_at")));
    latest = later(latest, timestamp(storedObject(preferences.get(customerId)).get("updated_at")));
    for (JsonNode row : collectionRows(addresses.get(customerId), "address_type")) {
      latest = later(latest, timestamp(row.get("updated_at")));
    }
    for (JsonNode row : collectionRows(contacts.get(customerId), "contact_type")) {
      latest = later(latest, timestamp(row.get("updated_at")));
    }
    return latest == null ? null : latest.toString();
  }

  /** Returns the later of two nullable instants. */
  private static Instant later(Instant first, Instant second) {
    return first == null || second != null && second.isAfter(first) ? second : first;
  }

  /** Deserializes and deterministically sorts stored collection rows. */
  private List<JsonNode> collectionRows(String json, String sortField) {
    List<JsonNode> rows = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> fields = storedObject(json).fields();
    fields.forEachRemaining(entry -> rows.add(entry.getValue()));
    rows.sort(Comparator.comparing((JsonNode row) -> row.path(sortField).asText())
        .thenComparing(row -> row.path("id").asText()));
    return rows;
  }

  /** Serializes and forwards a customer document to the sink. */
  private void forward(String key, ObjectNode value) {
    context.forward(new Record<>(key, serialize(value), context.currentSystemTimeMs()));
  }

  /** Copies a source field when present, retaining explicit JSON nulls. */
  private static void copy(ObjectNode target, String targetField, JsonNode source, String sourceField) {
    JsonNode value = source.get(sourceField);
    if (value != null) {
      target.set(targetField, value);
    }
  }

  /** Copies a source field only when it has a non-null value. */
  private static void copyNonNull(ObjectNode target, String targetField, JsonNode source,
      String sourceField) {
    JsonNode value = source.get(sourceField);
    if (value != null && !value.isNull()) {
      target.set(targetField, value);
    }
  }

  /** Copies a date, converting epoch-day values and retaining nulls. */
  private static void copyDate(ObjectNode target, String targetField, JsonNode value) {
    if (value == null || value.isNull()) {
      target.putNull(targetField);
    } else {
      target.put(targetField, date(value));
    }
  }

  /** Copies a non-null date, converting epoch-day values. */
  private static void copyDateNonNull(ObjectNode target, String targetField, JsonNode value) {
    if (value != null && !value.isNull()) {
      target.put(targetField, date(value));
    }
  }

  /** Copies a timestamp, converting epoch-millisecond values and retaining nulls. */
  private static void copyTimestamp(ObjectNode target, String targetField, JsonNode value) {
    if (value == null || value.isNull()) {
      target.putNull(targetField);
    } else {
      target.put(targetField, timestampText(value));
    }
  }

  /** Copies a non-null timestamp, converting epoch-millisecond values. */
  private static void copyTimestampNonNull(ObjectNode target, String targetField, JsonNode value) {
    if (value != null && !value.isNull()) {
      target.put(targetField, timestampText(value));
    }
  }

  /** Converts an epoch-day or textual JSON value to an ISO date string. */
  private static String date(JsonNode value) {
    return value.isNumber() ? LocalDate.ofEpochDay(value.asLong()).toString() : value.asText();
  }

  /** Converts a timestamp value to canonical ISO-8601 text when possible. */
  private static String timestampText(JsonNode value) {
    Instant parsed = timestamp(value);
    return parsed == null ? value.asText() : parsed.toString();
  }

  /** Parses an epoch-millisecond or ISO-8601 timestamp value. */
  private static Instant timestamp(JsonNode value) {
    if (value == null || value.isNull()) {
      return null;
    }
    try {
      return value.isNumber() ? Instant.ofEpochMilli(value.asLong()) : Instant.parse(value.asText());
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  /** Parses textual JSON values while leaving other values unchanged. */
  private static JsonNode jsonValue(JsonNode value) {
    if (!value.isTextual()) {
      return value;
    }
    try {
      return JSON.readTree(value.asText());
    } catch (JsonProcessingException ignored) {
      return value;
    }
  }

  /** Returns an object JSON node, or {@code null} for other values. */
  private static JsonNode object(JsonNode value) {
    return value != null && value.isObject() ? value : null;
  }

  /** Returns a non-blank textual JSON value, or {@code null}. */
  private static String text(JsonNode value) {
    if (value == null || value.isNull()) {
      return null;
    }
    String result = value.asText();
    return result.isBlank() ? null : result;
  }

  /** Resolves an identifier from a row, falling back to the Debezium key. */
  private static String identifier(JsonNode row, String key, String field) {
    if (row != null && row.hasNonNull(field)) {
      return row.get(field).asText();
    }
    if (key != null) {
      JsonNode parsedKey = parse(key);
      if (parsedKey.hasNonNull(field)) {
        return parsedKey.get(field).asText();
      }
    }
    return null;
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

  /** Serializes a state-store value or customer document. */
  private static String serialize(JsonNode value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Unable to serialize customer document", exception);
    }
  }
}
