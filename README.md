# PostgreSQL to MongoDB Sync

A reference implementation for **continuously syncing data from PostgreSQL into MongoDB** — turning many normalized SQL tables into a single embedded document — with no downtime and no custom sync scripts.

Built entirely on open-source components (Debezium, Kafka, Kafka Streams, MongoDB Kafka Connector). Swap the source connector to support MySQL, Oracle, SQL Server, and other databases — making this a reusable pattern for SQL-to-MongoDB migrations that span weeks, months, or years.

The included **Sync Bench** demo writes to PostgreSQL only and compares both stores side-by-side, so replication lag and parity are visible in real time.

## The Problem

Moving a relational system to MongoDB is rarely a one-weekend cutover. While it runs, both databases have to hold the same truth. Four things make that hard:

- **Multi-table to single document** — `customers`, `addresses`, `profiles`, `preferences`, and `contacts` all have to collapse into one MongoDB `Customer` document.
- **Transactions** — rows changed together in one SQL transaction must arrive together, not as a stream of half-applied states.
- **Deletes** — when a row disappears at the source, it has to disappear downstream too, with minimal delay.
- **Scale** — peak traffic needs horizontal scaling, and quiet periods should shrink back down.

Because cutovers can take weeks, months, or years, both stores must stay in sync the entire time. PostgreSQL remains the write authority until the migration completes.

## The Solution

Change Data Capture (CDC) reads every row-level change from PostgreSQL. A three-stage Kafka Streams pipeline re-keys, bundles, and aggregates those changes into one document per customer, then a MongoDB sink connector writes the result.

![Debezium captures five customer tables plus transaction metadata into Kafka topics; the document aggregator writes one document per customer to the sink topic, which the MongoDB connector replicates](docs/hla.png)

| Component               | What it does                                                                            |
| ----------------------- | --------------------------------------------------------------------------------------- |
| **Debezium source**     | Captures inserts, updates, and deletes from PostgreSQL logical replication (`pgoutput`) |
| **Document aggregator** | Kafka Streams service that rebuilds each customer document (scales to N replicas)       |
| **MongoDB sink**        | Upserts the aggregated document into `app.customers` via `ReplaceOneDefaultStrategy`    |
| **Dead-letter queue**   | Captures sink records MongoDB rejects, so one bad document cannot stall the pipeline    |

### Inside the aggregator

The aggregator is where transactional correctness is enforced. It runs three stages, each handing off through its own Kafka topic:

```mermaid
flowchart LR
  K1["app.public.*<br/>app.transaction"] --> S1["Stage 1: router<br/>rekey by transaction id"]
  S1 --> T1["app.customer.transaction.events"]
  T1 --> S2["Stage 2: assembler<br/>wait for txn END marker"]
  S2 --> T2["app.customer.transaction.bundles"]
  T2 --> S3["Stage 3: customer state<br/>build embedded document"]
  S3 --> K2["app.customer.documents"]
```

| Stage                        | What it does                                                                              |
| ---------------------------- | ----------------------------------------------------------------------------------------- |
| **Stage 1 — Router**         | Re-keys events by transaction ID so all rows in one SQL transaction co-locate             |
| **Stage 2 — Assembler**      | Waits for Debezium's transaction END marker, then emits a complete event bundle           |
| **Stage 3 — Customer state** | Maintains per-customer state stores and emits one replacement document (or delete marker) |

### The result

Five normalized tables arrive in MongoDB as one document, keyed by the PostgreSQL customer ID:

```json
{
  "_id": "1042",
  "id": 1042,
  "customer_number": "CUS-1042",
  "first_name": "Ada",
  "last_name": "Lovelace",
  "date_of_birth": "1815-12-10",
  "status": "active",
  "profile": {
    "preferredLanguage": "en",
    "occupation": "Mathematician",
    "annualIncome": "125000.00",
    "taxResidencyCountry": "GB",
    "riskRating": "low"
  },
  "addresses": [
    {
      "type": "residential",
      "line1": "12 St James's Square",
      "city": "London",
      "postalCode": "SW1Y 4LE",
      "countryCode": "GB",
      "validFrom": "2019-04-01"
    }
  ],
  "contacts": [
    {
      "type": "email",
      "value": "ada@example.com",
      "isPrimary": true,
      "verifiedAt": "2024-02-11T09:31:00Z"
    },
    { "type": "mobile_phone", "value": "+44 7700 900123", "isPrimary": true }
  ],
  "preferences": {
    "marketingEmailOptIn": true,
    "marketingSmsOptIn": false,
    "paperlessStatements": true,
    "notificationChannels": { "email": true, "sms": false }
  },
  "created_at": "2019-04-01T10:15:00Z",
  "updated_at": "2024-02-11T09:31:00Z"
}
```

Deleting the root row emits `{ "_id": "1042", "deleted": true }` instead. Document assembly lives in [`CustomerDocumentProcessor.java`][processor].

### Guarantees and limits

- Kafka Streams runs with `EXACTLY_ONCE_V2`, so a bundle is applied once even across rebalances and restarts.
- A bundle is only released after Debezium's transaction END marker confirms every row in the transaction arrived, so MongoDB never observes a partially applied transaction for a given customer.
- **Atomicity is per document, not per transaction.** A SQL transaction touching three customers produces three independent sink writes. Readers can briefly see one customer updated and another not. Cross-document atomicity would require MongoDB multi-document transactions in the sink, which this reference does not use.
- The local stack runs single-broker with `replication-factor 1` and no standby replicas — fine for a demo, not a production topology. See [Production readiness](docs/production-readiness.md).

The local Docker stack is **greenfield** — schema, connectors, and Kafka Streams state bootstrap from scratch on first start. The [migration phases](#migration-phases) below describe the cutover process for a real legacy PostgreSQL system.

## Configuration

### Services

All services and ports are defined in [`compose.yaml`][compose].

| Service                        | Image / build               | Port    | Role                                                                                        |
| ------------------------------ | --------------------------- | ------- | ------------------------------------------------------------------------------------------- |
| `postgres`                     | PostgreSQL 17               | `5432`  | Source database (`app`, `wal_level=logical`)                                                |
| `kafka`                        | Apache Kafka 4.3 (KRaft)    | `9092`  | Event bus                                                                                   |
| `debezium`                     | Debezium Connect            | `8083`  | CDC source connector ([`Dockerfile.debezium`][dbz-dockerfile])                              |
| `customer-document-aggregator` | Java 21 / Kafka Streams 3.9 | —       | Three-stage aggregation ([`pom.xml`][pom], [`CustomerDocumentAggregator.java`][aggregator]) |
| `mongodb-atlas-local`          | MongoDB Atlas Local 8.3     | `27017` | Target database (`app`)                                                                     |
| `aggregation-topic-init`       | one-shot                    | —       | Provisions Kafka topics ([`create-topics.sh`][topics])                                      |
| `connector-init`               | one-shot                    | —       | Registers both connectors ([`register.sh`][register])                                       |
| Demo UI (Next.js)              | —                           | `3000`  | Side-by-side parity viewer ([`demo/`](demo/README.md))                                      |

### Source — PostgreSQL tables

Schema owned by [`postgres/init/001-customers.sql`][schema]. Which tables get captured is set by `table.include.list` in [`postgres-source.json`][source-connector].

| Table                  | Relationship                                      |
| ---------------------- | ------------------------------------------------- |
| `customers`            | Root entity                                       |
| `customer_profiles`    | 1:1, `ON DELETE CASCADE`                          |
| `customer_addresses`   | 1:N, `ON DELETE CASCADE`, `REPLICA IDENTITY FULL` |
| `customer_contacts`    | 1:N, `ON DELETE CASCADE`, `REPLICA IDENTITY FULL` |
| `customer_preferences` | 1:1, `ON DELETE CASCADE`                          |

`REPLICA IDENTITY FULL` is required on the collection children so delete events carry `customer_id` and the pipeline knows which document to update.

### Target — MongoDB collection

| Setting        | Value                                | Owned by                                                        |
| -------------- | ------------------------------------ | --------------------------------------------------------------- |
| Database       | `app`                                | [`mongodb-sink.json`][sink-connector]                           |
| Collection     | `customers`                          | [`mongodb-sink.json`][sink-connector]                           |
| Document `_id` | PostgreSQL `customers.id`            | `document.id.strategy` in [`mongodb-sink.json`][sink-connector] |
| Document shape | All child rows embedded              | [`CustomerDocumentProcessor.java`][processor]                   |
| Root delete    | `{ "_id": "<id>", "deleted": true }` | [`CustomerDocumentProcessor.java`][processor]                   |

### Kafka topics

Topic names and partition counts are created by [`create-topics.sh`][topics]. CDC topic names derive from `topic.prefix` in [`postgres-source.json`][source-connector]; pipeline topic names are constants in [`CustomerDocumentTopology.java`][topology].

| Topic                              | Partitions | Purpose                       |
| ---------------------------------- | ---------- | ----------------------------- |
| `app.public.customers`             | 1          | CDC — root table              |
| `app.public.customer_profiles`     | 1          | CDC — profiles                |
| `app.public.customer_addresses`    | 1          | CDC — addresses               |
| `app.public.customer_contacts`     | 1          | CDC — contacts                |
| `app.public.customer_preferences`  | 1          | CDC — preferences             |
| `app.transaction`                  | 1          | Debezium transaction metadata |
| `app.customer.transaction.events`  | 12         | Stage 1 output                |
| `app.customer.transaction.bundles` | 12         | Stage 2 output                |
| `app.customer.documents`           | 12         | Stage 3 output → MongoDB sink |
| `app.customer.documents.dlq`       | 1          | Dead-letter queue             |

The three pipeline topics use `AGGREGATION_TOPIC_PARTITIONS`, defaulting to 12 in [`compose.yaml`][compose].

### Connectors

Both are registered against Kafka Connect by [`register.sh`][register].

| Connector         | Key settings                                                          | Config                                     |
| ----------------- | --------------------------------------------------------------------- | ------------------------------------------ |
| `postgres-source` | `pgoutput`, slot `debezium_app`, `provide.transaction.metadata: true` | [`postgres-source.json`][source-connector] |
| `mongodb-sink`    | `ReplaceOneDefaultStrategy`, `tasks.max: 12`, DLQ enabled             | [`mongodb-sink.json`][sink-connector]      |

### Scaling knobs

| Variable                                 | Default | Effect                              | Owned by                                                                   |
| ---------------------------------------- | ------- | ----------------------------------- | -------------------------------------------------------------------------- |
| `AGGREGATION_TOPIC_PARTITIONS`           | `12`    | Partition count for pipeline topics | [`compose.yaml`][compose], [`create-topics.sh`][topics]                    |
| `KAFKA_STREAM_THREADS`                   | `1`     | Threads per aggregator instance     | [`compose.yaml`][compose], [`CustomerDocumentAggregator.java`][aggregator] |
| `--scale customer-document-aggregator=N` | `1`     | Horizontal aggregator replicas      | `docker compose` CLI                                                       |

Partition count caps total parallelism: replicas times threads should not exceed `AGGREGATION_TOPIC_PARTITIONS`. See [Operations and scaling](docs/operations-and-scaling.md).

## Run Locally

**Prerequisites:** Docker (roughly 8 GB available to the Docker VM — five containers including Kafka and MongoDB) and Node.js 20.9+ for the demo.

```bash
docker compose up -d --build

cd demo
cp .env.example .env.local
npm ci && npm run dev
```

Open **http://localhost:3000**. The UI writes PostgreSQL only; MongoDB is read-only until write cut-over.

`POSTGRES_URL` and `MONGODB_URI` are required — the demo throws on startup without them, which is why `.env.local` is created above.

Sync Bench shows the PostgreSQL source and the MongoDB replica side by side, with the record and document counts diverging while the pipeline converges:

![Sync Bench UI](docs/demo.png)

### Verify the stack is healthy

Bootstrap is asynchronous — topics, then connectors, then the first snapshot. Confirm each step rather than waiting on the UI:

```bash
# All services up, one-shot init containers exited 0
docker compose ps

# Both connectors registered
curl -s localhost:8083/connectors

# Neither connector is FAILED
curl -s localhost:8083/connectors/postgres-source/status
curl -s localhost:8083/connectors/mongodb-sink/status

# Documents are landing in MongoDB
docker exec mongodb-atlas-local mongosh --quiet \
  -u root -p root --authenticationDatabase admin app \
  --eval 'db.customers.countDocuments()'
```

An empty connector list means `connector-init` ran before Kafka Connect was ready — check `docker compose logs connector-init`.

### Reset

Schema or topology changes require dropping persisted volumes:

```bash
docker compose down -v && docker compose up -d --build
```

### Scale

```bash
docker compose up -d --scale customer-document-aggregator=3
```

### Demo environment

Defaults in [`demo/.env.example`][demo-env] match the local stack:

```bash
POSTGRES_URL=postgres://postgres:postgres@localhost:5432/app
MONGODB_URI=mongodb://root:root@localhost:27017/?authSource=admin&directConnection=true
MONGODB_DB=app
```

## Repository Layout

```text
compose.yaml                     Full local stack
Dockerfile.debezium              Debezium image + MongoDB Kafka Connector
postgres/init/                   Source schema, applied on first start
connectors/                      Topic provisioning, connector registration and configs
customer-document-aggregator/    Java 21 Kafka Streams aggregation service
demo/                            Next.js "Sync Bench" parity UI
docs/                            Architecture, operations, sizing, migration guides
```

## Documentation

Full index: [docs/README.md](docs/README.md)

| Guide                                                                | Topic                              |
| -------------------------------------------------------------------- | ---------------------------------- |
| [Architecture](docs/architecture.md)                                 | Data model, CDC, aggregation       |
| [Operations and scaling](docs/operations-and-scaling.md)             | Startup, health, scaling, recovery |
| [Production readiness](docs/production-readiness.md)                 | Launch gates and gaps              |
| [Capacity and hardware sizing](docs/capacity-and-hardware-sizing.md) | S/M/L hardware envelopes           |
| [Migration walkthrough](docs/blog_extended.md)                       | Rationale and phased plan          |
| [Migration summary](docs/blog.md)                                    | Condensed narrative                |
| [Demo notes](docs/demo-notes.md)                                     | Next.js API ownership              |
| [Demo app](demo/README.md)                                           | UI and API                         |

## Migration Phases

These phases apply when cutting over a **production legacy PostgreSQL** system — not when resetting the local demo stack.

1. **Shadow** — pipeline up, snapshot backfill, parity verified
2. **Read migration** — readers move to MongoDB one cohort at a time
3. **Write cut-over** — writes move to MongoDB
4. **Retire PostgreSQL** — MongoDB is sole store

## License

[MIT](LICENSE)

[compose]: compose.yaml
[dbz-dockerfile]: Dockerfile.debezium
[schema]: postgres/init/001-customers.sql
[topics]: connectors/create-topics.sh
[register]: connectors/register.sh
[source-connector]: connectors/postgres-source.json
[sink-connector]: connectors/mongodb-sink.json
[pom]: customer-document-aggregator/pom.xml
[aggregator]: customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentAggregator.java
[topology]: customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentTopology.java
[processor]: customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentProcessor.java
[demo-env]: demo/.env.example
