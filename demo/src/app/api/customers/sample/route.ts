import { generateCustomerInput } from "@/lib/customer";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  return Response.json(generateCustomerInput());
}
