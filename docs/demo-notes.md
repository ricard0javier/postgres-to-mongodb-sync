# Demo Notes

- `src/app/api/customers` — PostgreSQL CRUD across normalized customer tables.
- `src/app/api/simulation` — process-local generator; browser controls start/stop only.
- `src/app/api/events` — SSE for PostgreSQL `NOTIFY` and MongoDB change streams.

PostgreSQL is authoritative during migration; MongoDB is read-only until write cut-over. See [`blog_extended.md`](blog_extended.md).
