# Production Readiness

Baseline for running the PostgreSQL-to-MongoDB migration pipeline in production. `compose.yaml` is local development only.

```text
PostgreSQL -> Debezium -> Kafka -> Kafka Streams -> MongoDB sink -> MongoDB
```

The repo already provides transaction-aware aggregation, exactly-once Streams processing, stable MongoDB `_id`s, and a parity gate. Production work must preserve those properties.

## Non-Negotiables

1. PostgreSQL is the sole write authority until approved write cut-over.
2. MongoDB is eventually consistent during migration; consumers need lag tolerance and a PostgreSQL fallback.
3. Kafka topics require durable multi-AZ replication with managed retention.
4. A transaction publishes as one complete customer update or not at all.
5. PostgreSQL replication slots and Kafka retention are capacity-managed; exhausting either is an incident.
6. Phase transitions require evidence, owner, rollback plan, and change record.

## Release Gates

| Area         | Required before launch                                            |
| ------------ | ----------------------------------------------------------------- |
| Business     | Service owner, data classification, regulatory scope              |
| Availability | SLOs for writes, reads, CDC freshness, error budget               |
| Recovery     | RTO/RPO for PostgreSQL, Kafka, MongoDB, connectors, pipeline      |
| Consistency  | Per-consumer staleness limits and fallback behavior               |
| Capacity     | Peak/sustained row changes, document size, growth with headroom   |
| Security     | Threat model, least privilege, key management, security review    |
| Data         | Source-to-target mapping, retention/deletion, reconciliation spec |
| Operations   | 24x7 ownership, runbooks, dashboards, paging, incident exercise   |
| Delivery     | IaC, immutable artifacts, rollback, production access controls    |
| Validation   | Load, failure, DR, security, and migration rehearsal results      |

## Current Gaps (Local Stack)

| Gap                       | Required outcome                                               |
| ------------------------- | -------------------------------------------------------------- |
| Single-node services      | Multi-AZ managed or clustered production topology              |
| `latest` images           | Pinned digests, SBOMs, patch cadence                           |
| Plaintext credentials     | Vault secrets, TLS, authenticated Kafka                        |
| No IaC                    | Reviewed Terraform/Helm/Kubernetes definitions                 |
| RF=1, no Streams standbys | RF≥3, min ISR≥2, persistent state, standby replicas            |
| No observability          | Metrics, tracing, alerts, structured logs                      |
| Demo API surface          | Auth, rate limits, audit; remove bulk delete/generator in prod |
| No CI/CD or load tests    | Gated pipeline and recurring operational exercises             |

## Production Topology

| Component      | Minimum                                                                     |
| -------------- | --------------------------------------------------------------------------- |
| PostgreSQL     | HA primary + standby, PITR, logical replication for Debezium                |
| Kafka          | ≥3 brokers/controllers across AZs, TLS, ACLs, no auto-create topics         |
| Kafka Connect  | ≥3 workers; one PostgreSQL source task; sink tasks ≤ document partitions    |
| Aggregator     | ≥3 replicas, persistent state, standby replicas, graceful shutdown          |
| MongoDB        | ≥3-node replica set, private endpoints, PITR, indexes before read migration |
| Application    | Stateless replicas, SSO, per-route auth, bounded connection pools           |
| Reconciliation | Scheduled independent PostgreSQL-vs-MongoDB job with repair workflow        |

## Component Essentials

- **PostgreSQL:** Dedicated Debezium role, slot/WAL sizing for max outage, tested failover with slot intact.
- **Debezium/Connect:** Version-controlled connector config, DLQ for sink failures, no public Connect REST.
- **Kafka:** Delete retention on CDC/transaction topics; compacted changelog topics; partition changes via new application ID.
- **Aggregator:** Bounded transaction assembly, readiness after state restore, JMX/Prometheus metrics.
- **MongoDB:** Separate sink/reader roles, indexes from real query patterns, document size limits.
- **Application:** No demo generator or bulk delete in prod; SSE needs connection limits and auth.
- **Reconciliation:** Full scan after snapshot/resnapshot and before each cohort; repair via replay, not direct MongoDB edits.

## Phase Gates

| Phase                 | Exit criteria                                                      |
| --------------------- | ------------------------------------------------------------------ |
| Shadow                | Snapshot drained; full parity; freshness SLO sustained             |
| Read migration        | Per-cohort latency, freshness, and parity targets met              |
| Write cut-over        | All readers migrated; MongoDB write path proven; explicit approval |
| PostgreSQL retirement | Retention complete; archives verified; sign-off                    |

Write cut-over requires a documented MongoDB write API and truthful rollback strategy. The current one-way CDC pipeline is not a write cut-over design.

## Acceptance Checklist

- [ ] SLO, RTO, RPO, ownership, and escalation approved
- [ ] Multi-AZ infrastructure provisioned through IaC
- [ ] Images pinned, scanned, signed; no production `latest`
- [ ] TLS, private networking, least privilege, audit logging verified
- [ ] Kafka/Streams/Connect topics: correct RF, ISR, retention, ACLs
- [ ] PostgreSQL slot/WAL capacity tested during connector outage
- [ ] Aggregator: persistent state, standbys, bounded transactions, telemetry
- [ ] MongoDB: HA, PITR, indexes, restore validated
- [ ] Application hardened; demo endpoints removed or isolated
- [ ] Reconciliation, canary freshness, and migration control plane operating
- [ ] Load, failover, DR, and cut-over rehearsals meet objectives

Assign named owners for PostgreSQL, CDC, Kafka, Streams, MongoDB, application, reconciliation, security, and incident command.
