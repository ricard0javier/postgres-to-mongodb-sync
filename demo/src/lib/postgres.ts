import { Pool, types } from "pg";

// PostgreSQL DATE has no timezone; preserve its literal calendar value.
types.setTypeParser(1082, (value) => value);

declare global { var postgresPool: Pool | undefined; }

export function getPostgresPool() {
  if (!process.env.POSTGRES_URL) throw new Error("POSTGRES_URL is not configured.");
  if (!global.postgresPool) global.postgresPool = new Pool({ connectionString: process.env.POSTGRES_URL });
  return global.postgresPool;
}
