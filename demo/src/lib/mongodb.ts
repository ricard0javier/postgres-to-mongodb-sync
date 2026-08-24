import { Db, MongoClient } from "mongodb";

declare global { var mongoClientPromise: Promise<MongoClient> | undefined; }

export function getMongoClient() {
  if (!process.env.MONGODB_URI) throw new Error("MONGODB_URI is not configured.");
  if (!global.mongoClientPromise) global.mongoClientPromise = new MongoClient(process.env.MONGODB_URI).connect();
  return global.mongoClientPromise;
}

export async function getMongoDb(): Promise<Db> { return (await getMongoClient()).db(process.env.MONGODB_DB ?? "app"); }
