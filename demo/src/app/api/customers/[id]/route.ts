import { deletePostgresCustomer, parseCustomerInput, updatePostgresCustomer } from "@/lib/customer";

export const runtime = "nodejs";

export async function PUT(request: Request, context: RouteContext<"/api/customers/[id]">) {
  const parsed = parseCustomerInput(await request.json());
  if ("error" in parsed) return Response.json(parsed, { status: 400 });
  const { id } = await context.params;
  try {
    const customer = await updatePostgresCustomer(id, parsed.data);
    return customer ? Response.json(customer) : Response.json({ error: "Customer not found." }, { status: 404 });
  } catch (error) {
    if ((error as { code?: string }).code === "23505") return Response.json({ error: "Customer number must be unique." }, { status: 409 });
    console.error("Unable to update customer", error);
    return Response.json({ error: "Unable to update the customer." }, { status: 500 });
  }
}

export async function DELETE(_request: Request, context: RouteContext<"/api/customers/[id]">) {
  const { id } = await context.params;
  try {
    return (await deletePostgresCustomer(id)) ? new Response(null, { status: 204 }) : Response.json({ error: "Customer not found." }, { status: 404 });
  } catch (error) {
    console.error("Unable to delete customer", error);
    return Response.json({ error: "Unable to delete the customer." }, { status: 500 });
  }
}
