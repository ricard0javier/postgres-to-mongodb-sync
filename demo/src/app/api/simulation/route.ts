import { getCustomerSimulationStatus, startCustomerSimulation, stopCustomerSimulation } from "@/lib/customer-simulation";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  return Response.json(getCustomerSimulationStatus());
}

export async function POST(request: Request) {
  const body = await request.json().catch(() => null) as { action?: unknown; intervalMs?: unknown } | null;
  if (!body || (body.action !== "start" && body.action !== "stop")) return Response.json({ error: "Action must be start or stop." }, { status: 400 });
  if (body.action === "stop") return Response.json(stopCustomerSimulation());
  const intervalMs = Number(body.intervalMs ?? 1000);
  if (!Number.isInteger(intervalMs) || intervalMs < 1 || intervalMs > 1000) return Response.json({ error: "Rate must be between 1 and 1000 customers per second." }, { status: 400 });
  return Response.json(startCustomerSimulation(intervalMs));
}
