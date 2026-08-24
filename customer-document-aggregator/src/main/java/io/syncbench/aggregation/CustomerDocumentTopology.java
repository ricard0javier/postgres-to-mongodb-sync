package io.syncbench.aggregation;

import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

/**
 * Declares a partition-safe, two-stage topology for customer-document aggregation.
 *
 * <p>CDC rows and Debezium transaction markers are first repartitioned by normalized PostgreSQL
 * transaction ID. One assembler task can therefore observe every row and the matching END marker,
 * independent of the source-topic partition that supplied the row. Completed transactions are then
 * repartitioned by customer ID before customer state is updated. This prevents one customer's row
 * state from being split across transaction partitions.
 */
public final class CustomerDocumentTopology {
  public static final String CUSTOMERS_TOPIC = "app.public.customers";
  public static final String PROFILES_TOPIC = "app.public.customer_profiles";
  public static final String ADDRESSES_TOPIC = "app.public.customer_addresses";
  public static final String CONTACTS_TOPIC = "app.public.customer_contacts";
  public static final String PREFERENCES_TOPIC = "app.public.customer_preferences";
  public static final String TRANSACTION_TOPIC = "app.transaction";
  /** Internal topic that co-locates all CDC records for one PostgreSQL transaction. */
  public static final String TRANSACTION_EVENTS_TOPIC = "app.customer.transaction.events";
  /** Internal topic that co-locates complete transaction bundles for one customer. */
  public static final String CUSTOMER_BUNDLES_TOPIC = "app.customer.transaction.bundles";
  /** External topic consumed by the MongoDB sink connector. */
  public static final String DOCUMENTS_TOPIC = "app.customer.documents";

  private static final List<String> CDC_TOPICS = List.of(CUSTOMERS_TOPIC, PROFILES_TOPIC,
      ADDRESSES_TOPIC, CONTACTS_TOPIC, PREFERENCES_TOPIC, TRANSACTION_TOPIC);
  private static final List<String> CUSTOMER_STORES = List.of(CustomerDocumentProcessor.CUSTOMERS_STORE,
      CustomerDocumentProcessor.PROFILES_STORE, CustomerDocumentProcessor.ADDRESSES_STORE,
      CustomerDocumentProcessor.CONTACTS_STORE, CustomerDocumentProcessor.PREFERENCES_STORE);

  /** Prevents construction because this class only declares the topology. */
  private CustomerDocumentTopology() {
  }

  /**
   * Creates source routing, transaction assembly, and customer-state stages.
   *
   * <p>Both intermediate topics are provisioned by {@code connectors/create-topics.sh}; their
   * partition count can be increased without relying on source CDC topics having one partition.
   */
  public static Topology build() {
    Topology topology = new Topology();
    for (int index = 0; index < CDC_TOPICS.size(); index++) {
      String sourceTopic = CDC_TOPICS.get(index);
      String source = "cdc-source-" + index;
      String router = "transaction-router-" + index;
      topology.addSource(source, Serdes.String().deserializer(), Serdes.String().deserializer(), sourceTopic);
      topology.addProcessor(router, () -> new TransactionRouterProcessor(sourceTopic), source);
      topology.addSink("transaction-events-sink-" + index, TRANSACTION_EVENTS_TOPIC,
          Serdes.String().serializer(), Serdes.String().serializer(), router);
    }

    topology.addSource("transaction-events-source", Serdes.String().deserializer(),
        Serdes.String().deserializer(), TRANSACTION_EVENTS_TOPIC);
    topology.addProcessor("transaction-assembler", TransactionAssemblerProcessor::new,
        "transaction-events-source");
    topology.addSink("customer-bundles-sink", CUSTOMER_BUNDLES_TOPIC, Serdes.String().serializer(),
        Serdes.String().serializer(), "transaction-assembler");
    topology.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(TransactionAssemblerProcessor.TRANSACTIONS_STORE), Serdes.String(),
        Serdes.String()), "transaction-assembler");

    topology.addSource("customer-bundles-source", Serdes.String().deserializer(),
        Serdes.String().deserializer(), CUSTOMER_BUNDLES_TOPIC);
    topology.addProcessor("customer-state", CustomerDocumentProcessor::new, "customer-bundles-source");
    topology.addSink("documents-sink", DOCUMENTS_TOPIC, Serdes.String().serializer(),
        Serdes.String().serializer(), "customer-state");
    for (String store : CUSTOMER_STORES) {
      topology.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(store),
          Serdes.String(), Serdes.String()), "customer-state");
    }
    return topology;
  }
}
