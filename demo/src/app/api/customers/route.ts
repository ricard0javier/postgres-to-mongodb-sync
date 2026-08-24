import { createPostgresCustomer, deleteAllPostgresCustomers, listMongoCustomers, listPostgresCustomers, parseCustomerInput } from "@/lib/customer";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

function page(value: string | null) { return Math.max(1, Number.parseInt(value ?? "1", 10) || 1); }

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const postgresPage = page(url.searchParams.get("postgresPage"));
    const mongodbPage = page(url.searchParams.get("mongodbPage"));
    const [postgres, mongodb] = await Promise.all([listPostgresCustomers(postgresPage), listMongoCustomers(mongodbPage)]);
    return Response.json({ postgres: postgres.items, mongodb: mongodb.items, pagination: { postgres: { page: postgres.page, total: postgres.total, totalPages: postgres.totalPages }, mongodb: { page: mongodb.page, total: mongodb.total, totalPages: mongodb.totalPages } } });
  } catch (error) {
    console.error("Unable to list customers", error);
    return Response.json({ error: "Unable to read the connected databases." }, { status: 500 });
  }
}

export async function DELETE() {
  try { return Response.json({ deleted: await deleteAllPostgresCustomers() }); }
  catch (error) { console.error("Unable to delete all customers", error); return Response.json({ error: "Unable to delete PostgreSQL customers." }, { status: 500 }); }
}

export async function POST(request: Request) {
  const parsed = parseCustomerInput(await request.json());
  if ("error" in parsed) return Response.json(parsed, { status: 400 });
  try {
    return Response.json(await createPostgresCustomer(parsed.data), { status: 201 });
  } catch (error) {
    if ((error as { code?: string }).code === "23505") return Response.json({ error: "Customer number must be unique." }, { status: 409 });
    console.error("Unable to create customer", error);
    return Response.json({ error: "Unable to create the customer." }, { status: 500 });
  }
}
