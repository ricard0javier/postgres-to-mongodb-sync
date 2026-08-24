#!/bin/sh
set -eu

# These topics deliberately use a configurable count. Source CDC topics may have any partitioning.
partitions="${AGGREGATION_TOPIC_PARTITIONS:-3}"

case "$partitions" in
  ''|*[!0-9]*|0) printf '%s\n' 'AGGREGATION_TOPIC_PARTITIONS must be a positive integer' >&2; exit 1 ;;
esac

create_delete_retained_topic() {
  topic="$1"
  topic_partitions="$2"

  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions "$topic_partitions" --replication-factor 1 \
    --config cleanup.policy=delete
}

# Kafka Streams subscribes to every Debezium topic at startup. Create them before the aggregator
# starts so its first assignment cannot fail while the connector is creating source topics.
# CDC records must not be compacted: a transaction END event's event_count requires every row event.
for topic in \
  app.public.customers \
  app.public.customer_profiles \
  app.public.customer_addresses \
  app.public.customer_contacts \
  app.public.customer_preferences \
  app.transaction; do
  create_delete_retained_topic "$topic" 1
done

for topic in app.customer.transaction.events app.customer.transaction.bundles app.customer.documents; do
  create_delete_retained_topic "$topic" "$partitions"
done

create_delete_retained_topic app.customer.documents.dlq 1
