# Architecture

PostgreSQL stays normalized and authoritative for writes during migration. Kafka Streams aggregates Debezium CDC events into one embedded MongoDB customer document per transaction. See [`blog_extended.md`](blog_extended.md) for migration phases.

## Data Ownership

| System                | Role                                              | Writable by app  |
| --------------------- | ------------------------------------------------- | ---------------- |
| PostgreSQL            | Normalized source during migration                | Yes              |
| Kafka / Kafka Streams | CDC transport and aggregation                     | No               |
| MongoDB               | Embedded replica; system of record after cut-over | No until Phase 3 |
| Next.js               | CRUD UI and comparison                            | PostgreSQL only  |

## PostgreSQL Model

| Table                  | Cardinality | Contents                                  |
| ---------------------- | ----------- | ----------------------------------------- |
| `customers`            | One         | Identity, status, audit timestamps        |
| `customer_profiles`    | Zero or one | Language, occupation, risk, tax residency |
| `customer_addresses`   | Many        | Residential, mailing, work                |
| `customer_contacts`    | Many        | Email and phone                           |
| `customer_preferences` | Zero or one | Marketing and notification choices        |

`customer_addresses` and `customer_contacts` use `REPLICA IDENTITY FULL` so delete events retain `customer_id`.

## CDC Topics

```text
app.public.customers
app.public.customer_profiles
app.public.customer_addresses
app.public.customer_contacts
app.public.customer_preferences
app.transaction
```

Debezium publishes with `provide.transaction.metadata=true`. Source topics are provisioned before Kafka Streams starts and use delete retention (not compaction) so transaction assembly never loses row events.

## Aggregation Pipeline

```text
Debezium CDC + app.transaction
  -> repartition by transaction ID -> app.customer.transaction.events
  -> wait for all rows + transaction END -> app.customer.transaction.bundles
  -> repartition by customer ID -> RocksDB state -> app.customer.documents
  -> MongoDB Kafka sink -> app.customers
```

**Transaction stage:** Normalizes `transaction-id:row-lsn` and `transaction-id:commit-lsn` to the same key. Emits one bundle per customer only after `END.event_count` row events are staged.

**Customer stage:** Co-locates all state for a customer key. Applies the full bundle and emits one complete replacement document.

## MongoDB Document

```json
{
  "_id": "150",
  "id": 150,
  "customer_number": "CUS-150",
  "first_name": "Ada",
  "last_name": "Lovelace",
  "profile": {},
  "addresses": [],
  "contacts": [],
  "preferences": {}
}
```

Root delete: `{ "_id": "150", "deleted": true }`. The UI filters `deleted: true`.

## Consistency

PostgreSQL commits are atomic; Debezium publishes after commit; the aggregator waits for the full transaction before emitting. MongoDB is eventually consistent.

Initial snapshot rows have no transaction boundary and are applied individually. No readers migrate until snapshot drain and parity verification pass.
