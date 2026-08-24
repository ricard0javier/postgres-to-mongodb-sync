import { createPostgresCustomer, generateCustomerInput } from "@/lib/customer";
import type { CustomerSimulationStatus } from "@/types/customer";

type SimulationState = CustomerSimulationStatus & { timer: ReturnType<typeof setTimeout> | null; generation: number };

declare global { var customerSimulation: SimulationState | undefined; }

function state() {
  if (!global.customerSimulation) {
    global.customerSimulation = { running: false, intervalMs: 1000, created: 0, startedAt: null, lastCreatedAt: null, lastError: null, timer: null, generation: 0 };
  }
  return global.customerSimulation;
}
function status(current: SimulationState): CustomerSimulationStatus {
  return { running: current.running, intervalMs: current.intervalMs, created: current.created, startedAt: current.startedAt, lastCreatedAt: current.lastCreatedAt, lastError: current.lastError };
}
function schedule(current: SimulationState, generation: number) {
  current.timer = setTimeout(() => void generateNext(current, generation), current.intervalMs);
}
async function generateNext(current: SimulationState, generation: number) {
  try {
    await createPostgresCustomer(generateCustomerInput());
    current.created += 1;
    current.lastCreatedAt = new Date().toISOString();
    current.lastError = null;
  } catch (error) {
    console.error("Customer simulation write failed", error);
    current.lastError = error instanceof Error ? error.message : "Unable to create a generated customer.";
  } finally {
    if (current.running && current.generation === generation) schedule(current, generation);
  }
}

export function getCustomerSimulationStatus() { return status(state()); }

export function startCustomerSimulation(intervalMs: number) {
  const current = state();
  if (current.timer) clearTimeout(current.timer);
  current.running = true;
  current.intervalMs = intervalMs;
  current.startedAt ??= new Date().toISOString();
  current.lastError = null;
  current.generation += 1;
  void generateNext(current, current.generation);
  return status(current);
}

export function stopCustomerSimulation() {
  const current = state();
  if (current.timer) clearTimeout(current.timer);
  current.timer = null;
  current.running = false;
  current.generation += 1;
  return status(current);
}
