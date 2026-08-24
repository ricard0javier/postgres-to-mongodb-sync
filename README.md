# PostgreSQL To MongoDB Sync

Zero-downtime migration reference: normalized PostgreSQL customer tables → one embedded MongoDB document via Debezium, Kafka, and Kafka Streams.

The **local Docker stack is greenfield** — schema, connectors, and Kafka Streams state bootstrap from scratch on first start. The **migration phases below** describe the cutover process for a real legacy PostgreSQL system being moved to MongoDB.

PostgreSQL remains authoritative for writes during migration. The Next.js demo writes PostgreSQL only and compares both stores so replication delay is visible.

## Quick Start

```bash
docker compose up -d --build
cd demo && npm ci && npm run dev
```

Open `http://localhost:3000`.

To pick up schema or topology changes, reset persisted volumes: `docker compose down -v && docker compose up -d --build`.

## Pipeline

```text
PostgreSQL -> Debezium CDC -> Kafka Streams aggregation -> app.customer.documents -> MongoDB
```

## Documentation

See [docs/README.md](docs/README.md) for the full index.

| Guide                                                   | Topic                        |
| ------------------------------------------------------- | ---------------------------- |
| [Architecture](docs/architecture.md)                    | Data model, CDC, aggregation |
| [Operations](docs/operations-and-scaling.md)            | Startup, health, scaling     |
| [Production readiness](docs/production-readiness.md)    | Launch gates and gaps        |
| [Capacity sizing](docs/capacity-and-hardware-sizing.md) | S/M/L hardware envelopes     |
| [Migration walkthrough](docs/blog_extended.md)          | Rationale and phased plan    |
| [Demo](demo/README.md)                                  | UI and API                   |

## Migration Phases

These phases apply when cutting over a **production legacy PostgreSQL** system — not when resetting the local demo stack.

1. **Shadow** — pipeline up, snapshot backfill, parity verified
2. **Read migration** — readers move to MongoDB one cohort at a time
3. **Write cut-over** — writes move to MongoDB
4. **Retire PostgreSQL** — MongoDB is sole store
