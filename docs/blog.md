# From Relational Rows To One Customer Document

The customer domain is being migrated from a legacy PostgreSQL database to MongoDB. PostgreSQL is hard to scale and evolve and lacks first-class support for the workloads the business now needs — vector search, text search, and native document access at scale. The migration must be zero-downtime, so during the transition PostgreSQL keeps taking writes while MongoDB is populated and validated in parallel.

Customer data rarely starts life as one convenient record. Core identity, contact details, addresses, risk information, and communication preferences tend to belong in different relational tables because each part has its own constraints, lifecycle, and cardinality. That model fits PostgreSQL; it does not fit a target that needs the complete customer in one document. This project demonstrates a way to bridge those models without denormalizing the write side or letting application code maintain a second database.

For a full walkthrough of the problem, alternatives, selected design, and migration phases, see [`blog_extended.md`](blog_extended.md).

## The Starting Point

The writable customer model is deliberately normalized:

- `customers` owns identity and status.
- `customer_profiles` owns risk and residency information.
- `customer_addresses` and `customer_contacts` are collections.
- `customer_preferences` owns communication choices.

PostgreSQL is the authority for every write. The Next.js application never writes MongoDB directly.

For the initial migration backfill, the deployment provisions the Debezium source topics before Kafka Streams starts. Debezium then snapshots the normalized tables and switches to WAL streaming. Snapshot rows do not have transaction boundaries, so they are individually applied to customer state; MongoDB is not exposed to migrated readers until the snapshot drains and parity checks pass.

## Change Data Capture, Not Dual Writes

Writing the same customer to PostgreSQL and MongoDB in one request creates two independent failure paths. One write can succeed while the other fails, leaving the systems inconsistent.

Instead, Debezium reads PostgreSQL's write-ahead log and publishes a CDC event for each affected row. The application has one durable write, and replication begins only after PostgreSQL commits it.

That gives the system a clear contract: PostgreSQL is immediately correct; MongoDB converges asynchronously.

## Why Aggregation Belongs In Kafka

The challenge is not moving a row. The challenge is turning several row changes into one customer document.

The aggregation service uses Kafka Streams in two stages:

1. It repartitions CDC events by PostgreSQL transaction ID.
2. It waits for Debezium's transaction `END` event and expected event count.
3. It groups the completed transaction into one bundle for each affected customer.
4. It repartitions bundles by customer ID and updates customer-keyed state.
5. It emits one complete replacement document to `app.customer.documents`.

This avoids temporary MongoDB documents with a customer name but missing addresses or preferences.

## Scaling Without Splitting State

Partitioning only by transaction ID is not enough. Two different transactions for the same customer could land on different tasks, splitting that customer's state.

The second customer-keyed stage solves that problem. Kafka assigns each customer key to one stateful task, so the customer row, profile, contacts, addresses, and preferences remain co-located while the service scales horizontally.

The two intermediate topics are configurable and default to three partitions:

```text
app.customer.transaction.events
app.customer.transaction.bundles
```

Run more aggregator instances when the partition count permits it:

```bash
docker compose up -d --scale customer-document-aggregator=3
```

## The Read Model

MongoDB receives one document per customer. The document embeds the profile, address array, contact array, and preferences. A root customer delete is represented as a stable-ID delete marker, which keeps the sink idempotent and allows the UI to hide deleted customers.

The result is a practical separation of concerns during the migration:

- PostgreSQL keeps the integrity and flexibility of a relational write model while it is still the authority.
- Kafka carries committed facts and manages aggregation.
- MongoDB is populated as a single-document replica optimized for complete-customer reads, and prepared for vector search, text search, and document workloads.
- The UI makes the replication delay visible rather than pretending the systems are synchronous, so consumers can decide when they are ready to migrate their reads.

After every reader has moved and writes cut over, the CDC stack is decommissioned and MongoDB becomes the sole customer store.

For implementation details, see [Architecture](architecture.md) and [Operations and scaling](operations-and-scaling.md).
