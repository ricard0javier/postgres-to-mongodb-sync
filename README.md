# PostgreSQL to MongoDB Sync

A reference implementation for **continuously syncing data from PostgreSQL into MongoDB** — turning many normalized SQL tables into a single embedded document — with no downtime and no custom sync scripts.

Built entirely on open-source components (Debezium, Kafka, Kafka Streams, MongoDB Kafka Connector). Swap the source connector to support MySQL, Oracle, SQL Server, and other databases — making this a reusable pattern for SQL-to-MongoDB migrations that span weeks, months, or years.

The included **Sync Bench** demo writes to PostgreSQL only and compares both stores side-by-side, so replication lag and parity are visible in real time.

## The Problem

Real-world SQL-to-MongoDB migrations hit four hard problems:

| Challenge                         | Example                                                                                                     |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **Multi-table → single document** | `customers`, `addresses`, `profiles`, `preferences`, `contacts` must become one MongoDB `Customer` document |
| **Transactions**                  | Related row changes in one SQL transaction must land in MongoDB as one atomic write                         |
| **Deletes**                       | Source deletes must propagate to MongoDB with minimal delay                                                 |
| **Scale**                         | Peak traffic requires horizontal scaling; idle periods should shrink back                                   |

Because cutovers could take weeks, months or years, both stores must stay in sync the entire time — PostgreSQL remains the write authority until the migration completes.

## The Solution

Change Data Capture (CDC) reads every row-level change from PostgreSQL. A three-stage Kafka Streams pipeline re-keys, bundles, and aggregates those changes into one document per customer, then a MongoDB sink connector writes the result.

```mermaid
flowchart LR
  PG[("PostgreSQL<br/>5 customer tables")] -->|Debezium CDC| K1["Kafka<br/>app.public.*<br/>app.transaction"]
  K1 --> S1["Stage 1: router<br/>rekey by transaction id"]
  S1 --> S2["Stage 2: assembler<br/>wait for txn END marker"]
  S2 --> S3["Stage 3: customer state<br/>build embedded document"]
  S3 --> K2["app.customer.documents"]
  K2 -->|MongoDB Kafka sink| M[("MongoDB<br/>app.customers")]
```

| Stage                        | What it does                                                                              |
| ---------------------------- | ----------------------------------------------------------------------------------------- |
| **Debezium CDC**             | Captures inserts, updates, and deletes from PostgreSQL logical replication (`pgoutput`)   |
| **Stage 1 — Router**         | Re-keys events by transaction ID so all rows in one SQL transaction co-locate             |
| **Stage 2 — Assembler**      | Waits for Debezium's transaction END marker, then emits a complete event bundle           |
| **Stage 3 — Customer state** | Maintains per-customer state stores and emits one replacement document (or delete marker) |
| **MongoDB sink**             | Upserts the aggregated document into `app.customers` via `ReplaceOneDefaultStrategy`      |

The local Docker stack is **greenfield** — schema, connectors, and Kafka Streams state bootstrap from scratch on first start. The [migration phases](#migration-phases) below describe the cutover process for a real legacy PostgreSQL system.

## Configuration

### Services (local stack)

Defined in [`compose.yaml`](compose.yaml).

| Service                        | Image / build               | Port    | Role                                         | Config                                                                                                                                                                                                                      |
| ------------------------------ | --------------------------- | ------- | -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `postgres`                     | PostgreSQL 17               | `5432`  | Source database (`app`, `wal_level=logical`) | [`compose.yaml`](compose.yaml)                                                                                                                                                                                              |
| `kafka`                        | Apache Kafka 4.3 (KRaft)    | `9092`  | Event bus                                    | [`compose.yaml`](compose.yaml)                                                                                                                                                                                              |
| `debezium`                     | Debezium Connect            | `8083`  | CDC source connector                         | [`compose.yaml`](compose.yaml), [`Dockerfile.debezium`](Dockerfile.debezium)                                                                                                                                                |
| `customer-document-aggregator` | Java 21 / Kafka Streams 3.9 | —       | Three-stage aggregation (`EXACTLY_ONCE_V2`)  | [`compose.yaml`](compose.yaml), [`pom.xml`](customer-document-aggregator/pom.xml), [`CustomerDocumentAggregator.java`](customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentAggregator.java) |
| `mongodb-atlas-local`          | MongoDB Atlas Local 8.3     | `27017` | Target database (`app`)                      | [`compose.yaml`](compose.yaml)                                                                                                                                                                                              |
| `aggregation-topic-init`       | one-shot                    | —       | Provisions Kafka topics                      | [`compose.yaml`](compose.yaml), [`create-topics.sh`](connectors/create-topics.sh)                                                                                                                                           |
| `connector-init`               | one-shot                    | —       | Registers Debezium + MongoDB connectors      | [`compose.yaml`](compose.yaml), [`register.sh`](connectors/register.sh)                                                                                                                                                     |
| Demo UI (Next.js)              | —                           | `3000`  | Side-by-side parity viewer                   | [`demo/package.json`](demo/package.json), [`demo/.env.example`](demo/.env.example)                                                                                                                                          |

### Source — PostgreSQL tables

Schema in [`postgres/init/001-customers.sql`](postgres/init/001-customers.sql). Captured tables in [`connectors/postgres-source.json`](connectors/postgres-source.json) (`table.include.list`).

| Table                  | Relationship                                      | Config                                                 |
| ---------------------- | ------------------------------------------------- | ------------------------------------------------------ |
| `customers`            | Root entity                                       | [`001-customers.sql`](postgres/init/001-customers.sql) |
| `customer_profiles`    | 1:1, `ON DELETE CASCADE`                          | [`001-customers.sql`](postgres/init/001-customers.sql) |
| `customer_addresses`   | 1:N, `ON DELETE CASCADE`, `REPLICA IDENTITY FULL` | [`001-customers.sql`](postgres/init/001-customers.sql) |
| `customer_contacts`    | 1:N, `ON DELETE CASCADE`, `REPLICA IDENTITY FULL` | [`001-customers.sql`](postgres/init/001-customers.sql) |
| `customer_preferences` | 1:1, `ON DELETE CASCADE`                          | [`001-customers.sql`](postgres/init/001-customers.sql) |

### Target — MongoDB collection

| Setting        | Value                                   | Config                                                                                                                                                                                                                          |
| -------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database       | `app`                                   | [`connectors/mongodb-sink.json`](connectors/mongodb-sink.json)                                                                                                                                                                  |
| Collection     | `customers`                             | [`connectors/mongodb-sink.json`](connectors/mongodb-sink.json)                                                                                                                                                                  |
| Document `_id` | PostgreSQL `customers.id`               | [`connectors/mongodb-sink.json`](connectors/mongodb-sink.json) (`document.id.strategy`), [`CustomerDocumentProcessor.java`](customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentProcessor.java) |
| Shape          | All child rows embedded in one document | [`CustomerDocumentProcessor.java`](customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentProcessor.java)                                                                                          |
| Root delete    | `{ "_id": "<id>", "deleted": true }`    | [`CustomerDocumentProcessor.java`](customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentProcessor.java)                                                                                          |

### Kafka topics

| Topic                              | Partitions | Purpose                       | Config                                                                                                                                                                                                                                       |
| ---------------------------------- | ---------- | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `app.public.customers`             | 1          | CDC — root table              | [`create-topics.sh`](connectors/create-topics.sh), [`postgres-source.json`](connectors/postgres-source.json) (`topic.prefix`)                                                                                                                |
| `app.public.customer_profiles`     | 1          | CDC — profiles                | [`create-topics.sh`](connectors/create-topics.sh), [`postgres-source.json`](connectors/postgres-source.json)                                                                                                                                 |
| `app.public.customer_addresses`    | 1          | CDC — addresses               | [`create-topics.sh`](connectors/create-topics.sh), [`postgres-source.json`](connectors/postgres-source.json)                                                                                                                                 |
| `app.public.customer_contacts`     | 1          | CDC — contacts                | [`create-topics.sh`](connectors/create-topics.sh), [`postgres-source.json`](connectors/postgres-source.json)                                                                                                                                 |
| `app.public.customer_preferences`  | 1          | CDC — preferences             | [`create-topics.sh`](connectors/create-topics.sh), [`postgres-source.json`](connectors/postgres-source.json)                                                                                                                                 |
| `app.transaction`                  | 1          | Debezium transaction metadata | [`create-topics.sh`](connectors/create-topics.sh), [`postgres-source.json`](connectors/postgres-source.json) (`provide.transaction.metadata`)                                                                                                |
| `app.customer.transaction.events`  | 12         | Stage 1 output                | [`create-topics.sh`](connectors/create-topics.sh), [`CustomerDocumentTopology.java`](customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentTopology.java)                                                      |
| `app.customer.transaction.bundles` | 12         | Stage 2 output                | [`create-topics.sh`](connectors/create-topics.sh), [`CustomerDocumentTopology.java`](customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentTopology.java)                                                      |
| `app.customer.documents`           | 12         | Stage 3 output → MongoDB sink | [`create-topics.sh`](connectors/create-topics.sh), [`CustomerDocumentTopology.java`](customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentTopology.java), [`mongodb-sink.json`](connectors/mongodb-sink.json) |
| `app.customer.documents.dlq`       | 1          | Dead-letter queue             | [`create-topics.sh`](connectors/create-topics.sh), [`mongodb-sink.json`](connectors/mongodb-sink.json)                                                                                                                                       |

Pipeline topic partition count defaults to **12** via `AGGREGATION_TOPIC_PARTITIONS` in [`compose.yaml`](compose.yaml) and [`create-topics.sh`](connectors/create-topics.sh).

### Connectors

Registered by [`connectors/register.sh`](connectors/register.sh).

| Connector         | Key settings                                                          | Config                                                               |
| ----------------- | --------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `postgres-source` | `pgoutput`, slot `debezium_app`, `provide.transaction.metadata: true` | [`connectors/postgres-source.json`](connectors/postgres-source.json) |
| `mongodb-sink`    | `ReplaceOneDefaultStrategy`, `tasks.max: 12`                          | [`connectors/mongodb-sink.json`](connectors/mongodb-sink.json)       |

### Scaling knobs

| Variable                                                      | Default | Effect                              | Config                                                                                                                                                                   |
| ------------------------------------------------------------- | ------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `AGGREGATION_TOPIC_PARTITIONS`                                | `12`    | Partition count for pipeline topics | [`compose.yaml`](compose.yaml), [`create-topics.sh`](connectors/create-topics.sh)                                                                                        |
| `KAFKA_STREAM_THREADS`                                        | `1`     | Threads per aggregator instance     | [`compose.yaml`](compose.yaml), [`CustomerDocumentAggregator.java`](customer-document-aggregator/src/main/java/io/syncbench/aggregation/CustomerDocumentAggregator.java) |
| `docker compose up -d --scale customer-document-aggregator=N` | —       | Horizontal aggregator replicas      | [`compose.yaml`](compose.yaml), [Operations and scaling](docs/operations-and-scaling.md)                                                                                 |

See [Operations and scaling](docs/operations-and-scaling.md) for details.

## Run Locally

**Prerequisites:** Docker, Node.js 20+.

```bash
docker compose up -d --build
cd demo && npm ci && npm run dev
```

Open **http://localhost:3000**. The UI writes PostgreSQL only; MongoDB is read-only until write cut-over.

**Reset** (schema or topology changes):

```bash
docker compose down -v && docker compose up -d --build
```

**Scale** the aggregator for higher throughput:

```bash
docker compose up -d --scale customer-document-aggregator=3
```

Demo env vars (see [`demo/.env.example`](demo/.env.example)):

```
POSTGRES_URL=postgres://postgres:postgres@localhost:5432/app
MONGODB_URI=mongodb://root:root@localhost:27017/?authSource=admin&directConnection=true
MONGODB_DB=app
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
