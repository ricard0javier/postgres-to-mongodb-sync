/**
 * Kafka Streams aggregation for the normalized PostgreSQL customer model.
 *
 * <p>Debezium publishes one CDC topic for each source table: customers, profiles, addresses,
 * contacts, and preferences. {@link io.syncbench.aggregation.CustomerDocumentTopology} first routes every
 * transactional row and transaction marker through {@code app.customer.transaction.events}, keyed
 * by normalized PostgreSQL transaction ID. An assembler waits until the Debezium END event count
 * is satisfied, then writes one bundle per affected customer to
 * {@code app.customer.transaction.bundles}, keyed by customer ID. Customer-state processors own
 * disjoint customer keys and emit one replacement document to {@code app.customer.documents}.
 * Snapshot records have no PostgreSQL transaction ID and are emitted as single-row bundles.
 *
 * <p>The replacement document embeds the profile, address array, contact array, and preferences
 * under the authoritative customer row. This keeps PostgreSQL normalized and makes MongoDB a
 * read-only, one-document-per-customer replica. Deleting the root customer emits a document with
 * {@code _id} and {@code deleted: true}; the MongoDB reader filters this marker from the UI.
 *
 * <p>Set the final partition count at bootstrap. Changing partition counts on a live topology
 * requires a new {@code KAFKA_APPLICATION_ID} so Kafka Streams creates compatible changelog topics.
 * The service can run multiple instances with Docker Compose scaling; each instance uses
 * {@code KAFKA_STREAM_THREADS} stream threads and Kafka assigns transaction and customer bundles
 * by their respective keys.
 */
package io.syncbench.aggregation;
