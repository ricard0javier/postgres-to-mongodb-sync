# Capacity And Hardware Sizing

Initial S/M/L envelopes for the transitional CDC pipeline. Replace planning assumptions with measured pre-production data before procurement. Assumes controls in [Production Readiness](production-readiness.md): multi-AZ, RF 3, TLS, PITR, persistent Streams state, one standby replica, 30% free disk.

## Sizes

| Size | Documents | Sustained writes | Peak (1 h) |
| ---- | --------: | ---------------: | ---------: |
| S    |      250k |             10/s |       50/s |
| M    |        5M |             75/s |      375/s |
| L    |       50M |            300/s |    1,500/s |

## Scaling Constraints

1. **One Debezium source task** — logical replication is ordered; workers add availability, not capture parallelism.
2. **Transaction and customer stages scale** via partitions, Streams threads, and sink tasks after repartitioning.
3. **Kafka disk** is usually the first bottleneck — CDC records are retained on source and transaction topics with RF 3.
4. **PostgreSQL slot WAL** must cover the worst credible Debezium outage.
5. **MongoDB** — size for document + index working set; stay under 16 MiB per document.

## Planning Assumptions

| Assumption                        | Value                                                   |
| --------------------------------- | ------------------------------------------------------- |
| CDC records per write transaction | 5 (4 rows + 1 END)                                      |
| Average CDC record                | 3 KiB                                                   |
| Average customer document         | 6 KiB                                                   |
| Kafka retention                   | 7/7/7/14 days (source/txn/bundle/document)              |
| Peak factor                       | 5× sustained for ≥1 h; recovery must exceed peak ingest |

## Sizing Summary

| Dimension                         |           S |           M |        L |
| --------------------------------- | ----------: | ----------: | -------: |
| Kafka disk (RF 3, with allowance) |    ~1.3 TiB |      ~9 TiB |  ~35 TiB |
| PostgreSQL data + indexes         |      ~8 GiB |    ~150 GiB | ~1.5 TiB |
| MongoDB data + indexes            |      ~4 GiB |     ~75 GiB | ~750 GiB |
| Data-plane vCPU (approx.)         |       58–64 |     114–126 |  368–380 |
| Data-plane RAM (approx.)          | 224–248 GiB | 444–492 GiB |  1.4 TiB |

## Partition Plan

| Size | Aggregation + document partitions | Streams replicas | Sink tasks (max) |
| ---- | --------------------------------: | ---------------: | ---------------: |
| S    |                                 6 |                3 |                3 |
| M    |                                12 |                6 |                6 |
| L    |                                24 |               12 |               12 |

Create topics through IaC. Validate key distribution and skew before increasing partitions. Use a versioned application ID for topology changes.

## Per-Size Starting Point

**S:** 3-AZ; PostgreSQL 4 vCPU/16 GiB/1 TiB; Kafka 3×4 vCPU/2 TiB; Connect 3×2 vCPU; Streams 3×4 vCPU/200 GiB state; MongoDB 3×4 vCPU/500 GiB; app 2×2 vCPU.

**M:** Double compute/storage floors; 6 Streams replicas; 12 partitions.

**L:** 6 Kafka brokers; 12 Streams replicas; formal assessment of source WAL throughput, inter-AZ bandwidth, and state-restore RTO. If one logical stream saturates, split the source domain or change migration sequence.

## Scale Triggers

| Component  | Trigger                                           | Action                                          |
| ---------- | ------------------------------------------------- | ----------------------------------------------- |
| PostgreSQL | Slot WAL >50% reserved, lag >15 min, CPU >60%     | Resize/tune source; do not add Connect workers  |
| Debezium   | Cannot keep up with peak WAL                      | Optimize source; split domain if needed         |
| Kafka      | Disk/network >60%, ISR shrinkage                  | Add brokers/storage first                       |
| Streams    | CPU >65%, restore exceeds RTO, hot keys           | Add replicas/SSD; versioned partition migration |
| MongoDB    | CPU/IOPS >60%, oplog below recovery window        | Resize; review indexes/sharding                 |
| App/SSE    | Pool saturation, connections over load-test limit | Add replicas; shared event fan-out              |

Alert at 55% disk; treat 70% as urgent. Thresholds leave headroom for AZ failure and replay.

## Validation Before Sign-Off

1. Measure 7-day source workload: WAL rate, records/transaction, bytes/record, key skew.
2. Build production-scale MongoDB corpus; measure document/index size and restore time.
3. Load-test sustained rate, 5× peak, and outage recovery at chosen size.
4. Test loss of one broker, Connect worker, Streams instance, MongoDB node, and PostgreSQL primary.
5. Run snapshot and full reconciliation at target volume.
6. Recalculate formulas with p95/p99 values plus growth margin.

These sizes cover the transitional CDC architecture only. Re-size MongoDB independently after write cut-over.
