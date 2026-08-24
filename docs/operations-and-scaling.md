# Operations And Scaling

Local reference for running and scaling the pipeline. See [`blog_extended.md`](blog_extended.md) for migration phases.

## Startup

```bash
docker compose up -d --build
cd demo && npm run dev
```

Sequence: create aggregation topics → start aggregator → register Debezium and MongoDB connectors. CDC source topics must exist before Kafka Streams starts.

Debezium performs an `initial` snapshot on first registration, then tails WAL. Snapshot rows apply individually. Re-running `connector-init` updates config only; it does not resnapshot. To rebuild MongoDB, reset connector offsets, aggregator state, and the target collection, then re-register.

To pick up schema changes, reset persisted volumes:

```bash
docker compose down -v
docker compose up -d --build
```

## Health

```bash
docker compose ps
curl --fail --silent http://localhost:8083/connectors/postgres-source/status
curl --fail --silent http://localhost:8083/connectors/mongodb-sink/status
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group customer-document-aggregator
```

Aggregator `LAG` should converge to `0`. Nonzero lag means MongoDB is behind; PostgreSQL writes are not blocked.

## Scaling

Default internal topics (3 partitions each):

```text
app.customer.transaction.events
app.customer.transaction.bundles
```

```bash
AGGREGATION_TOPIC_PARTITIONS=6 docker compose up -d --build
docker compose up -d --scale customer-document-aggregator=3
KAFKA_STREAM_THREADS=3 docker compose up -d customer-document-aggregator
```

Do not change partition counts on a live topology. Use a new versioned `KAFKA_APPLICATION_ID` instead.

## Processing Guarantees

Kafka Streams uses `exactly_once_v2` for internal aggregation. The MongoDB sink is at-least-once; stable `_id` replacement makes retries converge. Local Compose uses replication factor `1`; production needs multi-broker Kafka.

## Recovery

If Debezium, Kafka, or the aggregator falls behind: PostgreSQL keeps committing, WAL and Kafka retain data until consumed, and MongoDB converges after the backlog drains. Watch PostgreSQL slot WAL retention and Kafka topic retention during sustained outages.

## Parity

During shadow and read-migration phases, run scheduled PostgreSQL-vs-MongoDB diffs. Parity passes are required before advancing cohorts.

## Demo Caveats

The server-side generator (`/api/simulation`) is process-local. Do not run multiple Next.js instances with the generator active unless simulation moves to a coordinated worker.

## Troubleshooting

| Symptom                  | Check                         | Action                                                         |
| ------------------------ | ----------------------------- | -------------------------------------------------------------- |
| MongoDB stale            | Aggregator and sink lag       | Restore service; drain backlog                                 |
| Aggregator won't start   | Logs, partition counts        | Verify `AGGREGATION_TOPIC_PARTITIONS`; use new application ID  |
| Child delete persists    | Debezium, replica identity    | Reset volumes; verify `001-customers.sql` applied at bootstrap |
| Connector failed         | Connect status, Debezium logs | Fix config/connectivity; re-register                           |
| Duplicate MongoDB writes | `_id` on documents            | Expected; replacements converge by `_id`                       |
