# Zero-Downtime Migration From PostgreSQL To MongoDB: An Extended Walkthrough

PostgreSQL has served the customer domain well, but it has become the constraint. Scaling it further is expensive, evolving the schema is slow, and the workloads the business now wants — vector search, full-text search at scale, and native document access — are not first-class in PostgreSQL. MongoDB fits those requirements, so the customer domain is being migrated.

The migration must be **zero-downtime**. Applications keep writing to PostgreSQL today; readers gradually move to MongoDB; eventually writes cut over. During the transition, the two stores must agree on every customer, even though the write model is spread across five relational tables and the target model is a single document.

This extended blog explains why we are moving, the alternatives we considered for the sync layer, the design we selected (Debezium + Kafka Streams aggregation + MongoDB sink), the infrastructure it requires, and how data flows through it end-to-end.

---

## 1. The Problem

### 1.1 Why PostgreSQL Is Being Sunset

The legacy PostgreSQL customer database is functional but hitting known limits:

- **Scale.** Vertical scaling has diminishing returns; sharding is invasive and operationally heavy.
- **Evolution.** Schema changes across five related tables require coordinated migrations, downtime windows, and careful backfills.
- **Feature gap.** The roadmap needs:
  - **Vector search** for semantic customer lookup and recommendations.
  - **Full-text search** at scale across profiles, addresses, and contacts.
  - **Native document support** so aggregates can be stored, read, and evolved without joins.
    PostgreSQL has partial answers (pgvector, `tsvector`, `JSONB`) but not at the scale, operability, or ergonomics required.
- **Operational cost.** Backups, upgrades, and DR of a large relational store dominate the team's time.

MongoDB addresses each of these directly: horizontal scale via sharding, flexible document evolution, Atlas Vector Search, native text search, and an operational model already familiar to the team.

### 1.2 Why We Cannot Just Cut Over

A hard cut-over would require:

- Freezing writes to PostgreSQL.
- Exporting and transforming all customers into documents.
- Importing into MongoDB.
- Repointing every reader and writer.
- Accepting downtime for every consumer during the window.

The customer domain is too central for a maintenance window. Any migration must keep PostgreSQL fully operational while MongoDB is populated and validated in parallel, so consumers can migrate one at a time and the cut-over becomes a routing change, not an outage.

### 1.3 What "One Customer" Means

A customer today is normalized across five PostgreSQL tables:

| Table                  | Cardinality | Contents                                                 |
| ---------------------- | ----------- | -------------------------------------------------------- |
| `customers`            | one         | Identity, status, audit timestamps                       |
| `customer_profiles`    | zero or one | Language, occupation, income, tax residency, risk rating |
| `customer_addresses`   | many        | Residential, mailing, work addresses                     |
| `customer_contacts`    | many        | Emails and phones with primary and verification flags    |
| `customer_preferences` | zero or one | Marketing, paperless, notification channels              |

The target MongoDB shape is a single document per customer that embeds the profile, addresses, contacts, and preferences. A realistic update can touch several PostgreSQL tables in one transaction; the MongoDB replica must never expose half of that change.

---

## 2. Candidate Solutions For The Sync Layer

Four alternatives were considered honestly before landing on CDC + Kafka Streams.

### 2.1 Option A — Bulk Backfill Plus Periodic Refresh

Snapshot PostgreSQL to MongoDB on a schedule.

**Pros**

- Simple; no new infrastructure beyond a job runner.
- Isolated from the transactional path.

**Cons**

- Staleness is minutes to hours.
- Full rebuilds are expensive; incremental rebuilds re-invent CDC poorly.
- Deletes require full-table diffs.
- Not viable for a zero-downtime migration where readers need current data.

### 2.2 Option B — Dual Writes From The Application

Every writer commits to PostgreSQL and MongoDB in the same request.

**Pros**

- Conceptually simple.
- Low latency between the two stores when both succeed.

**Cons**

- Two independent failure paths; one write can succeed while the other fails.
- No shared transaction; retries and compensations become application concerns.
- Every writing service must reimplement the same logic correctly.
- No natural ordering guarantee under concurrency.
- Blocks a phased migration: writers must be changed first, before MongoDB is trusted.

Explicitly rejected.

### 2.3 Option C — Row-Level CDC Directly Into MongoDB

Debezium reads the WAL; a MongoDB sink writes each row change as-is.

**Pros**

- No dual writes.
- Near-real-time.
- Minimal custom code.

**Cons**

- Each row becomes its own operation, so a single logical customer transaction produces several intermediate MongoDB states.
- Readers can observe a customer with a new profile but stale addresses, then the reverse a moment later.
- Child-table deletes require replica-identity handling and nested-array update logic in the sink.
- The MongoDB shape ends up mirroring the PostgreSQL shape row-for-row, defeating the reason for the migration.

### 2.4 Option D — CDC + Stream Processing Aggregation (Selected)

Debezium captures committed row changes. A Kafka Streams service waits for each PostgreSQL transaction to complete, groups the affected rows per customer, and emits one full replacement document. A MongoDB sink connector applies it.

**Pros**

- Single durable write in PostgreSQL; no dual writes anywhere.
- Readers never see half-applied transactions.
- Aggregation lives outside both databases, so neither schema is compromised during the migration.
- Horizontally scalable by partition.
- Exactly-once processing inside the pipeline.
- Enables phased reader migration: MongoDB is populated continuously and can be validated against PostgreSQL at any moment.
- Prepares the target shape for vector search, text search, and document reads on day one.

**Cons**

- More moving parts: Debezium, Kafka, Kafka Streams, MongoDB sink connector.
- Operational surface area grows (topics, connectors, consumer lag, state stores).
- Eventual consistency between PostgreSQL and MongoDB during the migration must be explicit to consumers.

Accepted because it is the only option that makes a zero-downtime migration safe.

---

## 3. Selected Solution

During the migration, PostgreSQL remains the authority for writes. Debezium streams committed changes. A Kafka Streams application performs a transaction-aware, customer-keyed aggregation. MongoDB stores one document per customer, replaced atomically. When MongoDB has been validated and every reader has migrated, writers cut over and PostgreSQL is retired.

### 3.1 Migration Contract During The Transition

- PostgreSQL is immediately correct after commit.
- MongoDB converges asynchronously — typically within seconds.
- The UI (and any migration-phase consumer) shows the delay rather than hiding it.
- Readers migrate one at a time. Writers migrate last.

### 3.2 Two-Stage Aggregation

The aggregator uses two keyed stages so it can scale without losing transaction atomicity or splitting customer state.

**Stage 1 — Transaction stage.** Repartition every CDC event and the transaction `END` marker by normalized transaction ID. One task sees the whole transaction. Wait until the `END` marker arrives and until the count of staged row events equals `END.event_count`. Emit one bundle per affected customer.

**Stage 2 — Customer stage.** Repartition bundles by customer ID. Kafka assigns each customer key to exactly one task, so the customer's row, profile, contacts, addresses, and preferences remain co-located in one RocksDB state store. Apply the bundle and emit one complete replacement document to `app.customer.documents`.

Stage 1 guarantees transactional coherence. Stage 2 guarantees customer-level state locality while allowing horizontal scale.

---

## 4. Required Infrastructure

| Component                                                 | Role                                                                         |
| --------------------------------------------------------- | ---------------------------------------------------------------------------- |
| PostgreSQL (legacy)                                       | Authoritative write model during migration; logical replication enabled      |
| Debezium PostgreSQL connector                             | Reads the WAL, publishes row events and a `app.transaction` metadata topic   |
| Apache Kafka                                              | Durable transport and state changelog backing store                          |
| Kafka Streams aggregator (`customer-document-aggregator`) | Two-stage transaction- and customer-keyed aggregation with `exactly_once_v2` |
| MongoDB Kafka sink connector                              | Applies replacement documents by stable `_id`                                |
| MongoDB (target)                                          | Read replica during migration; becomes the system of record after cut-over   |
| Next.js UI                                                | CRUD against PostgreSQL, reads from MongoDB, surfaces replication delay      |

### 4.1 PostgreSQL Requirements

- Logical replication (`wal_level = logical`).
- `REPLICA IDENTITY FULL` on `customer_addresses` and `customer_contacts` so delete events carry `customer_id`, letting the aggregator remove the correct nested item.
- Debezium `provide.transaction.metadata=true`, which enables the `app.transaction` topic and adds transaction metadata (including expected `event_count`) to each row event.

### 4.2 Kafka Topics

Source topics published by Debezium:

```text
app.public.customers
app.public.customer_profiles
app.public.customer_addresses
app.public.customer_contacts
app.public.customer_preferences
app.transaction
```

Internal aggregation topics (default three partitions, configurable via `AGGREGATION_TOPIC_PARTITIONS`):

```text
app.customer.transaction.events   # keyed by transaction ID
app.customer.transaction.bundles  # keyed by customer ID
```

Output topic consumed by the MongoDB sink:

```text
app.customer.documents            # keyed by customer ID
```

### 4.3 MongoDB Requirements

- Collection `app.customers` with `_id` equal to the PostgreSQL customer ID (as a string).
- Indexes appropriate to the new workloads: full-text on searchable fields, Atlas Vector Search on any embedded vectors, standard indexes on `customer_number` and status.
- Sink configured for replacement-by-`_id` (idempotent).

---

## 5. Data Flow

```text
             +---------------------+
             |    Next.js UI       |
             |  CRUD + read view   |
             +----------+----------+
                        |
             writes     |     reads (migrating)
                        v
             +---------------------+
             |  PostgreSQL (legacy)|
             | normalized tables   |
             +----------+----------+
                        |
                        | WAL (logical replication)
                        v
             +---------------------+
             |      Debezium       |
             +----------+----------+
                        |
   app.public.* (row events)   app.transaction (BEGIN/END + count)
                        |
                        v
             +---------------------+
             |  Kafka (broker)     |
             +----------+----------+
                        |
        repartition by transaction ID
                        v
        app.customer.transaction.events
                        |
        wait for END + expected event count
        group by affected customer
                        v
        app.customer.transaction.bundles
                        |
        repartition by customer ID
        apply to per-customer RocksDB state
        emit full replacement document
                        v
            app.customer.documents
                        |
                        v
             +---------------------+
             | MongoDB Kafka sink  |
             +----------+----------+
                        |
                        v
             +---------------------+
             |  MongoDB (target)   |
             |  app.customers      |
             +---------------------+
```

### 5.1 Walkthrough Of A Single Update

1. The user edits a customer in the UI. Next.js executes one PostgreSQL transaction that updates `customer_profiles` and inserts a row in `customer_addresses`.
2. PostgreSQL commits. The WAL contains both changes plus the transaction boundary.
3. Debezium reads the WAL and publishes two row events plus a transaction `END` event carrying `event_count = 2`.
4. The aggregator repartitions all three records by transaction ID. One task now holds the whole transaction.
5. Once the `END` marker is present and two row events are staged, the task groups them by `customer_id` and emits one bundle for that customer.
6. The bundle is repartitioned by `customer_id`. A single task owns that customer's state.
7. The customer processor merges the new profile and appends the address to its state store, then emits a complete replacement document to `app.customer.documents`.
8. The MongoDB sink replaces the document with `_id = <customer_id>`.
9. The UI's next MongoDB read sees the fully updated customer.

At no point does a MongoDB reader observe the profile change without the new address, or vice versa.

### 5.2 Deletes

A root `customers` delete produces:

```json
{ "_id": "150", "deleted": true }
```

The UI query filters `deleted = true`. This keeps the sink idempotent (stable `_id`, always a replacement) and preserves the tombstone for audit and replay.

Child deletes (`customer_addresses`, `customer_contacts`) rely on `REPLICA IDENTITY FULL` so the aggregator receives the `customer_id` in the delete event and can remove the correct nested item from the customer's state.

### 5.3 Initial Backfill

The initial backfill uses the same CDC pipeline as ongoing replication. Before Kafka Streams starts, the deployment explicitly creates all six Debezium input topics, including `app.transaction`. This avoids a startup race: Kafka Streams fails when a declared source topic is missing.

The input topics use delete retention, not compaction. Transaction assembly relies on seeing every row event that Debezium counts in an `END.event_count`; compacting a raw CDC topic could remove an earlier update for the same primary key before the aggregator consumes it.

Once the aggregator is subscribed, `connector-init` registers Debezium. On a new connector Debezium takes its `initial` snapshot and then continues from the captured WAL position. Snapshot rows do not have transaction boundaries, so the aggregator applies them individually to customer-keyed state. A document can briefly be incomplete while related snapshot rows are still arriving. That is acceptable in the shadow phase because no production reader has moved to MongoDB; the Phase 1 exit gate is a drained snapshot plus successful parity verification.

Registering an already configured connector does not create another snapshot. A rebuild is a controlled operation: reset the source connector offsets and replication slot, reset the Kafka Streams state, clear the target collection, then register the source connector to take a fresh snapshot.

---

## 6. Migration Phases

The pipeline supports a four-phase migration.

### Phase 1 — Stand Up The Pipeline (Shadow)

- Deploy Debezium, Kafka, the aggregator, and the MongoDB sink.
- Backfill: trigger a Debezium snapshot so every existing customer flows through the aggregator and lands in MongoDB.
- No consumer reads from MongoDB yet.
- Success criterion: MongoDB matches PostgreSQL for every customer, verified by a comparison job.

### Phase 2 — Read Migration

- Route non-critical readers to MongoDB (search, listings, analytics).
- Keep critical readers on PostgreSQL.
- The UI shows both, side by side, so replication delay is visible and observable.
- Success criterion: parity holds under production write load; MongoDB lag stays inside SLO.

### Phase 3 — Write Cut-Over

- Route writes to MongoDB. Reverse-CDC (MongoDB → PostgreSQL) is not required if PostgreSQL is being retired; otherwise, add a reverse sync symmetric to this one.
- Freeze schema changes on PostgreSQL.
- Success criterion: no application writes reach PostgreSQL for a sustained window.

### Phase 4 — Retire PostgreSQL

- Archive the PostgreSQL cluster.
- Remove Debezium and the aggregator; keep MongoDB, its indexes, and its new-workload features (vector search, text search).
- Success criterion: PostgreSQL is offline; MongoDB is the sole customer store.

---

## 7. Consistency And Correctness

### 7.1 Guarantees

- **Atomic per PostgreSQL transaction.** No bundle is emitted until the transaction stage has both the `END` marker and every expected row.
- **One task per customer.** Customer-keyed partitioning ensures state locality.
- **Exactly-once inside the pipeline.** `exactly_once_v2` commits source offsets, state changes, and produce records in a single Kafka transaction.
- **Idempotent sink.** MongoDB replaces by stable `_id`. Duplicate deliveries converge.

### 7.2 What Is Not Guaranteed

- **Synchronous consistency between PostgreSQL and MongoDB during the migration.** MongoDB is eventually consistent. Consumers who require read-after-write must read from PostgreSQL until Phase 3.
- **Ordering across unrelated customers.** Two customers can converge in any order, which is fine because they are independent.

---

## 8. Scaling Model

Scale horizontally by running more aggregator instances up to the number of active partitions:

```bash
docker compose up -d --scale customer-document-aggregator=3
```

Or vertically inside one instance:

```bash
KAFKA_STREAM_THREADS=3 docker compose up -d customer-document-aggregator
```

Choose partition count based on:

- Expected transaction throughput (drives partitions on `app.customer.transaction.events`).
- Customer-key cardinality and per-customer update rate (drives partitions on `app.customer.transaction.bundles` and `app.customer.documents`).

Partition counts of active changelog topics cannot be changed in place. A topology migration uses a new versioned `KAFKA_APPLICATION_ID`, runs both versions until the new one is caught up, then retires the old.

---

## 9. Operations

### 9.1 Startup

```bash
docker compose up -d --build
cd demo && npm run dev
```

### 9.2 Health

```bash
docker compose ps
curl --fail --silent http://localhost:8083/connectors/postgres-source/status
curl --fail --silent http://localhost:8083/connectors/mongodb-sink/status
```

### 9.3 Lag

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --describe \
  --group customer-document-aggregator
```

Nonzero `LAG` means MongoDB is behind, not that PostgreSQL writes are blocked.

### 9.4 Backlog And Recovery

1. PostgreSQL keeps accepting writes.
2. WAL retains changes until Debezium's replication slot advances.
3. Kafka retains CDC records until the aggregator consumes them.
4. The aggregator resumes from committed offsets and RocksDB changelog topics.
5. MongoDB converges when the backlog drains.

Monitor WAL retention on PostgreSQL and topic retention on Kafka during sustained outages. If either boundary is exhausted, plan a controlled resnapshot.

### 9.5 Parity Verification

Run a scheduled comparison job during Phases 1 and 2:

- Sample or scan customers in PostgreSQL.
- Rebuild the expected document shape.
- Diff against MongoDB.
- Alert on any mismatch older than the lag SLO.

Parity verification is the gate for advancing between phases.

---

## 10. Trade-Offs, Revisited

| Concern                                               | This design                | Dual writes                  | Direct CDC sink    | Batch ETL        |
| ----------------------------------------------------- | -------------------------- | ---------------------------- | ------------------ | ---------------- |
| Write path complexity                                 | Low (single DB)            | High (two DBs)               | Low                | Low              |
| Read model coherence                                  | Per PostgreSQL transaction | Best effort                  | Per row            | Per batch        |
| Latency to MongoDB                                    | Seconds                    | Sub-second when both succeed | Sub-second per row | Minutes to hours |
| Failure isolation                                     | Strong                     | Weak                         | Medium             | Strong           |
| Supports zero-downtime migration                      | Yes                        | No                           | Partially          | No               |
| Target shape ready for vector/text/document workloads | Yes                        | Yes                          | No                 | Yes              |
| Operational surface                                   | Highest                    | Lowest                       | Medium             | Low              |

The operational surface is the price paid for a safe, phased migration and a MongoDB shape ready for the workloads that motivated the move.

---

## 11. When Not To Use This Pattern

- The migration can tolerate downtime → a one-shot dump and load is cheaper.
- The source aggregate is a single row → direct CDC to a document store is simpler.
- No new workloads require MongoDB features → the migration itself may not be justified.
- Team has no appetite for Kafka operations → invest in that capability first, or use a managed CDC pipeline.

Use this pattern when the legacy store cannot be paused, the target shape is a document, and the two must never disagree within a transaction during the migration.

---

## 12. References

- [Architecture](architecture.md)
- [Operations and scaling](operations-and-scaling.md)
- [Product overview](PRODUCT.md)
- [Design notes](DESIGN.md)
