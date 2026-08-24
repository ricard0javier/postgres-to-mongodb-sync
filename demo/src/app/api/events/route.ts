import type { ChangeStream } from "mongodb";
import type { PoolClient } from "pg";
import { getMongoDb } from "@/lib/mongodb";
import { getPostgresPool } from "@/lib/postgres";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";
const encoder = new TextEncoder();

export async function GET(request: Request) {
  let postgresClient: PoolClient | undefined;
  let changeStream: ChangeStream | undefined;
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  const stream = new ReadableStream({
    start(controller) {
      let closed = false;
      const send = (event: Record<string, unknown>) => {
        if (!closed) controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
      };
      const close = () => {
        if (closed) return;
        closed = true;
        if (heartbeat) clearInterval(heartbeat);
        void changeStream?.close();
        if (postgresClient) void postgresClient.query("UNLISTEN customer_changes").finally(() => postgresClient?.release());
        controller.close();
      };
      void (async () => {
        let postgresConnected = false;
        let mongodbConnected = false;
        try {
          postgresClient = await getPostgresPool().connect();
          await postgresClient.query("LISTEN customer_changes");
          postgresConnected = true;
           postgresClient.on("notification", (notification) => {
             const payload = JSON.parse(notification.payload ?? "{}") as { operation?: string; customer_number?: string };
             send({ source: "postgres", operation: payload.operation ?? "change", customerNumber: payload.customer_number, at: new Date().toISOString() });
           });
           postgresClient.on("error", (error) => {
             console.error("PostgreSQL notification listener failed", error);
             send({ source: "system", operation: "postgres_unavailable", at: new Date().toISOString() });
           });
        } catch (error) {
          console.error("Unable to subscribe to PostgreSQL notifications", error);
          send({ source: "system", operation: "postgres_unavailable", at: new Date().toISOString() });
        }
        try {
          changeStream = (await getMongoDb()).collection("customers").watch();
          mongodbConnected = true;
          changeStream.on("change", (change) => send({ source: "mongodb", operation: change.operationType, at: new Date().toISOString() }));
          changeStream.on("error", (error) => {
            console.error("MongoDB change stream failed", error);
            send({ source: "system", operation: "mongodb_unavailable", at: new Date().toISOString() });
          });
        } catch (error) {
          console.error("Unable to subscribe to MongoDB changes", error);
          send({ source: "system", operation: "mongodb_unavailable", at: new Date().toISOString() });
        }
        send({ source: "system", operation: postgresConnected && mongodbConnected ? "connected" : "degraded", at: new Date().toISOString() });
      })();
      heartbeat = setInterval(() => send({ source: "system", operation: "heartbeat", at: new Date().toISOString() }), 15000);
      request.signal.addEventListener("abort", close);
    },
    cancel() {
      if (heartbeat) clearInterval(heartbeat);
      void changeStream?.close();
      if (postgresClient) void postgresClient.query("UNLISTEN customer_changes").finally(() => postgresClient?.release());
    },
  });
  return new Response(stream, { headers: { "Cache-Control": "no-cache, no-transform", Connection: "keep-alive", "Content-Type": "text/event-stream", "X-Accel-Buffering": "no" } });
}
