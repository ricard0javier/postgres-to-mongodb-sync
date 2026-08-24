# Documentation

## Guides

- [Architecture](architecture.md): data model, CDC topics, aggregation, document shape.
- [Operations and scaling](operations-and-scaling.md): startup, health, scaling, recovery.
- [Production readiness](production-readiness.md): launch gates, gaps, phase controls.
- [Capacity and hardware sizing](capacity-and-hardware-sizing.md): S/M/L envelopes and scale triggers.
- [Migration walkthrough](blog_extended.md): problem, alternatives, phased plan.
- [Migration summary](blog.md): condensed narrative.
- [Demo notes](demo-notes.md): Next.js API ownership.

## Quick Start

```bash
docker compose up -d --build
cd demo && npm ci && npm run dev
```

Open `http://localhost:3000`. The UI writes PostgreSQL only; MongoDB is read-only until write cut-over. See [demo/README.md](../demo/README.md) for API details.

On a fresh stack, topics are provisioned before the aggregator starts, then Debezium snapshots PostgreSQL. Do not migrate readers until snapshot drain and parity verification pass.
