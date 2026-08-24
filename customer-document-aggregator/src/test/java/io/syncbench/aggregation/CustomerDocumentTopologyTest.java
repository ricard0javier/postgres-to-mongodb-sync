package io.syncbench.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

/** Verifies transaction and customer repartitioning with the Kafka Streams test driver. */
class CustomerDocumentTopologyTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  /** Verifies snapshot rows build an embedded document and root deletion emits a marker. */
  @Test
  void embeds_related_snapshot_rows_and_emits_a_soft_delete_for_the_root() throws Exception {
    try (TopologyTestDriver driver = driver("customer-document-snapshot-test")) {
      TestInputTopic<String, String> customers = input(driver, CustomerDocumentTopology.CUSTOMERS_TOPIC);
      TestInputTopic<String, String> profiles = input(driver, CustomerDocumentTopology.PROFILES_TOPIC);
      TestInputTopic<String, String> addresses = input(driver, CustomerDocumentTopology.ADDRESSES_TOPIC);
      TestInputTopic<String, String> contacts = input(driver, CustomerDocumentTopology.CONTACTS_TOPIC);
      TestInputTopic<String, String> preferences = input(driver, CustomerDocumentTopology.PREFERENCES_TOPIC);
      TestOutputTopic<String, String> documents = output(driver);

      customers.pipeInput("{\"id\":7}", event("r", "after", """
          {"id":7,"customer_number":"CUS-7","first_name":"Ada","last_name":"Lovelace","date_of_birth":-51965,"status":"active","created_at":1704067200000,"updated_at":1704067200000}"""));
      profiles.pipeInput("{\"customer_id\":7}", event("r", "after", """
          {"customer_id":7,"preferred_language":"en","occupation":null,"annual_income":"120000.00","tax_residency_country":"GB","risk_rating":"low","updated_at":1704153600000}"""));
      addresses.pipeInput("{\"id\":11}", event("r", "after", """
          {"id":11,"customer_id":7,"address_type":"residential","line_1":"10 Market St","line_2":null,"city":"London","region":null,"postal_code":"SW1A 1AA","country_code":"GB","valid_from":19723,"valid_to":null,"updated_at":1704240000000}"""));
      contacts.pipeInput("{\"id\":12}", event("r", "after", """
          {"id":12,"customer_id":7,"contact_type":"email","contact_value":"ada@example.test","is_primary":true,"verified_at":1704240000000,"updated_at":1704240000000}"""));
      preferences.pipeInput("{\"customer_id\":7}", event("r", "after", """
          {"customer_id":7,"marketing_email_opt_in":true,"marketing_sms_opt_in":false,"paperless_statements":true,"notification_channels":{"email":true,"sms":false},"updated_at":1704240000000}"""));

      List<String> values = documents.readValuesToList();
      JsonNode document = JSON.readTree(values.get(values.size() - 1));
      assertEquals("7", document.path("_id").asText());
      assertEquals("Ada", document.path("first_name").asText());
      assertEquals("120000.00", document.path("profile").path("annualIncome").asText());
      assertEquals("10 Market St", document.path("addresses").get(0).path("line1").asText());
      assertEquals("2024-01-01", document.path("addresses").get(0).path("validFrom").asText());
      assertEquals("ada@example.test", document.path("contacts").get(0).path("value").asText());
      assertTrue(document.path("preferences").path("notificationChannels").path("email").asBoolean());

      customers.pipeInput("{\"id\":7}", event("d", "before", "{" + "\"id\":7}"));
      JsonNode softDelete = JSON.readTree(documents.readValue());
      assertEquals("7", softDelete.path("_id").asText());
      assertTrue(softDelete.path("deleted").asBoolean());
    }
  }

  /** Proves row LSN suffixes and the END LSN suffix are normalized to one transaction key. */
  @Test
  void assembles_multi_table_transaction_after_end_and_outputs_customer_key() throws Exception {
    try (TopologyTestDriver driver = driver("customer-document-transaction-test")) {
      TestInputTopic<String, String> customers = input(driver, CustomerDocumentTopology.CUSTOMERS_TOPIC);
      TestInputTopic<String, String> profiles = input(driver, CustomerDocumentTopology.PROFILES_TOPIC);
      TestInputTopic<String, String> addresses = input(driver, CustomerDocumentTopology.ADDRESSES_TOPIC);
      TestInputTopic<String, String> contacts = input(driver, CustomerDocumentTopology.CONTACTS_TOPIC);
      TestInputTopic<String, String> preferences = input(driver, CustomerDocumentTopology.PREFERENCES_TOPIC);
      TestInputTopic<String, String> transactions = input(driver, CustomerDocumentTopology.TRANSACTION_TOPIC);
      TestOutputTopic<String, String> documents = output(driver);
      TestOutputTopic<String, String> bundles = output(driver,
          CustomerDocumentTopology.CUSTOMER_BUNDLES_TOPIC);

      customers.pipeInput("{\"id\":9}", transactionalEvent("742:100", "c", "after", """
          {"id":9,"customer_number":"CUS-9","first_name":"Grace","last_name":"Hopper","date_of_birth":null,"status":"active","created_at":1704067200000,"updated_at":1704067200000}"""));
      profiles.pipeInput("{\"customer_id\":9}", transactionalEvent("742:101", "c", "after", """
          {"customer_id":9,"preferred_language":"en","occupation":"Engineer","annual_income":"120000.00","tax_residency_country":"US","risk_rating":"standard","updated_at":1704067200000}"""));
      addresses.pipeInput("{\"id\":19}", transactionalEvent("742:102", "c", "after", """
          {"id":19,"customer_id":9,"address_type":"work","line_1":"1 Compiler Way","city":"Arlington","country_code":"US","updated_at":1704067200000}"""));
      contacts.pipeInput("{\"id\":29}", transactionalEvent("742:103", "c", "after", """
          {"id":29,"customer_id":9,"contact_type":"email","contact_value":"grace@example.test","is_primary":true,"updated_at":1704067200000}"""));
      preferences.pipeInput("{\"customer_id\":9}", transactionalEvent("742:104", "c", "after", """
          {"customer_id":9,"marketing_email_opt_in":true,"updated_at":1704067200000}"""));

      assertTrue(documents.isEmpty());
      assertEquals(5, JSON.readTree(driver.<String, String>getKeyValueStore(
          TransactionAssemblerProcessor.TRANSACTIONS_STORE).get("742")).path("seen").asInt());
      transactions.pipeInput("742", "{\"status\":\"END\",\"id\":\"742:999\",\"event_count\":5}");

      KeyValue<String, String> bundle = bundles.readKeyValue();
      assertEquals("9", bundle.key);
      assertEquals(5, JSON.readTree(bundle.value).path("events").size());
      assertTrue(bundles.isEmpty());
      KeyValue<String, String> output = documents.readKeyValue();
      JsonNode document = JSON.readTree(output.value);
      assertEquals("9", output.key);
      assertEquals("Grace", document.path("first_name").asText());
      assertEquals("Engineer", document.path("profile").path("occupation").asText());
      assertEquals("1 Compiler Way", document.path("addresses").get(0).path("line1").asText());
      assertEquals("grace@example.test", document.path("contacts").get(0).path("value").asText());
      assertTrue(document.path("preferences").path("marketingEmailOptIn").asBoolean());
      assertTrue(documents.isEmpty());
    }
  }

  /** A multi-customer transaction emits one bundle/document per customer, each keyed by customer ID. */
  @Test
  void emits_one_customer_keyed_document_for_each_customer_affected_by_a_transaction() throws Exception {
    try (TopologyTestDriver driver = driver("customer-document-customer-key-test")) {
      TestInputTopic<String, String> customers = input(driver, CustomerDocumentTopology.CUSTOMERS_TOPIC);
      TestInputTopic<String, String> transactions = input(driver, CustomerDocumentTopology.TRANSACTION_TOPIC);
      TestOutputTopic<String, String> documents = output(driver);
      TestOutputTopic<String, String> bundles = output(driver,
          CustomerDocumentTopology.CUSTOMER_BUNDLES_TOPIC);

      customers.pipeInput("{\"id\":11}", transactionalEvent("800:10", "c", "after", """
          {"id":11,"customer_number":"CUS-11","first_name":"Lin","last_name":"One","status":"active"}"""));
      customers.pipeInput("{\"id\":12}", transactionalEvent("800:11", "c", "after", """
          {"id":12,"customer_number":"CUS-12","first_name":"Sam","last_name":"Two","status":"active"}"""));
      transactions.pipeInput("800", "{\"status\":\"END\",\"id\":\"800:90\",\"event_count\":2}");

      assertEquals(List.of("11", "12"), bundles.readKeyValuesToList().stream()
          .map(bundle -> bundle.key).toList());
      List<KeyValue<String, String>> outputs = documents.readKeyValuesToList();
      assertEquals(List.of("11", "12"), outputs.stream().map(output -> output.key).toList());
      assertEquals("Lin", JSON.readTree(outputs.get(0).value).path("first_name").asText());
      assertEquals("Sam", JSON.readTree(outputs.get(1).value).path("first_name").asText());
    }
  }

  /** Creates a test driver with the supplied isolated application ID. */
  private static TopologyTestDriver driver(String applicationId) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:9092");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    return new TopologyTestDriver(CustomerDocumentTopology.build(), properties);
  }

  /** Creates a string-serialized input topic for the topology test driver. */
  private static TestInputTopic<String, String> input(TopologyTestDriver driver, String topic) {
    return driver.createInputTopic(topic, Serdes.String().serializer(), Serdes.String().serializer());
  }

  /** Creates the default customer-document output topic. */
  private static TestOutputTopic<String, String> output(TopologyTestDriver driver) {
    return output(driver, CustomerDocumentTopology.DOCUMENTS_TOPIC);
  }

  /** Creates a string-deserialized output topic for the topology test driver. */
  private static TestOutputTopic<String, String> output(TopologyTestDriver driver, String topic) {
    return driver.createOutputTopic(topic,
        Serdes.String().deserializer(), Serdes.String().deserializer());
  }

  /** Builds a non-transactional Debezium row event. */
  private static String event(String operation, String rowName, String row) {
    String before = "before".equals(rowName) ? row : "null";
    String after = "after".equals(rowName) ? row : "null";
    return "{\"op\":\"" + operation + "\",\"before\":" + before + ",\"after\":" + after + "}";
  }

  /** Builds a Debezium row event associated with a transaction. */
  private static String transactionalEvent(String transactionId, String operation, String rowName, String row) {
    String before = "before".equals(rowName) ? row : "null";
    String after = "after".equals(rowName) ? row : "null";
    return "{\"op\":\"" + operation + "\",\"before\":" + before + ",\"after\":" + after
        + ",\"transaction\":{\"id\":\"" + transactionId + "\"}}";
  }
}
