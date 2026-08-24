# Sync Bench Demo

Next.js workspace comparing PostgreSQL (writable source) with the MongoDB replica during migration. See [`../docs/blog_extended.md`](../docs/blog_extended.md) for the phased plan.

Do not treat MongoDB as migration-ready until snapshot drain and parity verification pass.

## Run

1. `docker compose up -d` from the repository root.
2. `npm run dev` in this directory → `http://localhost:3000`.

To pick up schema changes after pulling updates, reset persisted volumes: `docker compose down -v && docker compose up -d --build`.

Environment variables: `.env.example`.

## API

| Method   | Path                                        | Purpose                            |
| -------- | ------------------------------------------- | ---------------------------------- |
| GET      | `/api/customers?postgresPage=&mongodbPage=` | Paged PostgreSQL and MongoDB lists |
| POST     | `/api/customers`                            | Create customer in PostgreSQL      |
| PUT      | `/api/customers/:id`                        | Update customer                    |
| DELETE   | `/api/customers/:id`                        | Delete customer                    |
| DELETE   | `/api/customers`                            | Delete all customers               |
| GET      | `/api/customers/sample`                     | Generated sample payload           |
| GET/POST | `/api/simulation`                           | Generator status / start / stop    |
| GET      | `/api/events`                               | SSE event stream                   |

Root deletes appear in MongoDB as `{ "_id": "<id>", "deleted": true }`.

## Docs

- [Architecture](../docs/architecture.md)
- [Operations](../docs/operations-and-scaling.md)
