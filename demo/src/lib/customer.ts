import { randomUUID } from "crypto";
import type { Document } from "mongodb";
import type { PoolClient, QueryResultRow } from "pg";
import { getMongoDb } from "@/lib/mongodb";
import { getPostgresPool } from "@/lib/postgres";
import {
  customerAddressTypes,
  customerContactTypes,
  customerRiskRatings,
  customerStatuses,
  type Customer,
  type CustomerAddressInput,
  type CustomerContactInput,
  type CustomerInput,
  type CustomerPage,
  type CustomerProfileInput,
  type CustomerStatus,
} from "@/types/customer";

type CustomerRow = QueryResultRow & {
  id: string | number; customer_number: string; first_name: string; last_name: string;
  date_of_birth: string | null; status: CustomerStatus; created_at: Date | string; updated_at: Date | string;
  profile: unknown; addresses: unknown; contacts: unknown; preferences: unknown;
};

const defaultProfile: CustomerProfileInput = { preferredLanguage: "en", occupation: null, annualIncome: null, taxResidencyCountry: null, riskRating: "standard" };
const defaultPreferences = { marketingEmailOptIn: false, marketingSmsOptIn: false, paperlessStatements: true, notificationChannels: { email: true, sms: false, push: false } };

function toIso(value: unknown) { return value ? (value instanceof Date ? value.toISOString() : new Date(String(value)).toISOString()) : ""; }
function toDate(value: unknown) {
  if (!value) return null;
  if (typeof value === "number") return new Date(Date.UTC(1970, 0, 1) + value * 86400000).toISOString().slice(0, 10);
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : toIso(value).slice(0, 10);
}
function asObject(value: unknown): Record<string, unknown> {
  if (typeof value === "string") { try { return asObject(JSON.parse(value)); } catch { return {}; } }
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}
function asArray(value: unknown) {
  if (typeof value === "string") { try { return asArray(JSON.parse(value)); } catch { return []; } }
  return Array.isArray(value) ? value.map(asObject) : [];
}
function stringOrNull(value: unknown) { const result = String(value ?? "").trim(); return result || null; }
function asBoolean(value: unknown, fallback = false) { return typeof value === "boolean" ? value : fallback; }

function mapProfile(value: unknown): CustomerProfileInput {
  const profile = asObject(value);
  const riskRating = String(profile.riskRating ?? "standard");
  return {
    preferredLanguage: String(profile.preferredLanguage ?? "en"),
    occupation: stringOrNull(profile.occupation),
    annualIncome: stringOrNull(profile.annualIncome),
    taxResidencyCountry: stringOrNull(profile.taxResidencyCountry),
    riskRating: customerRiskRatings.includes(riskRating as CustomerProfileInput["riskRating"]) ? riskRating as CustomerProfileInput["riskRating"] : "standard",
  };
}
function mapAddresses(value: unknown): CustomerAddressInput[] {
  return asArray(value).flatMap((address) => {
    const type = String(address.type ?? "");
    if (!customerAddressTypes.includes(type as CustomerAddressInput["type"])) return [];
    return [{ type: type as CustomerAddressInput["type"], line1: String(address.line1 ?? ""), line2: stringOrNull(address.line2), city: String(address.city ?? ""), region: stringOrNull(address.region), postalCode: String(address.postalCode ?? ""), countryCode: String(address.countryCode ?? ""), validFrom: toDate(address.validFrom) ?? "", validTo: toDate(address.validTo) }];
  });
}
function mapContacts(value: unknown): CustomerContactInput[] {
  return asArray(value).flatMap((contact) => {
    const type = String(contact.type ?? "");
    if (!customerContactTypes.includes(type as CustomerContactInput["type"])) return [];
    return [{ type: type as CustomerContactInput["type"], value: String(contact.value ?? ""), isPrimary: asBoolean(contact.isPrimary), verifiedAt: contact.verifiedAt ? toIso(contact.verifiedAt) : null }];
  });
}
function mapPreferences(value: unknown) {
  const preferences = asObject(value);
  const channels = asObject(preferences.notificationChannels);
  return {
    marketingEmailOptIn: asBoolean(preferences.marketingEmailOptIn),
    marketingSmsOptIn: asBoolean(preferences.marketingSmsOptIn),
    paperlessStatements: asBoolean(preferences.paperlessStatements, true),
    notificationChannels: Object.fromEntries(Object.entries(channels).filter(([, enabled]) => typeof enabled === "boolean")) as Record<string, boolean>,
  };
}
function toCustomer(row: CustomerRow): Customer {
  return {
    id: String(row.id), customerNumber: row.customer_number, firstName: row.first_name, lastName: row.last_name,
    dateOfBirth: toDate(row.date_of_birth), status: row.status, profile: mapProfile(row.profile), addresses: mapAddresses(row.addresses), contacts: mapContacts(row.contacts), preferences: mapPreferences(row.preferences),
    createdAt: toIso(row.created_at), updatedAt: toIso(row.updated_at),
  };
}

const customerSelect = `
  SELECT c.id, c.customer_number, c.first_name, c.last_name, c.date_of_birth, c.status, c.created_at, c.updated_at,
    COALESCE((SELECT jsonb_strip_nulls(jsonb_build_object('preferredLanguage', p.preferred_language, 'occupation', p.occupation, 'annualIncome', p.annual_income, 'taxResidencyCountry', p.tax_residency_country, 'riskRating', p.risk_rating)) FROM public.customer_profiles p WHERE p.customer_id = c.id), '{}'::jsonb) AS profile,
    COALESCE((SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object('type', a.address_type, 'line1', a.line_1, 'line2', a.line_2, 'city', a.city, 'region', a.region, 'postalCode', a.postal_code, 'countryCode', a.country_code, 'validFrom', a.valid_from, 'validTo', a.valid_to)) ORDER BY a.address_type) FROM public.customer_addresses a WHERE a.customer_id = c.id), '[]'::jsonb) AS addresses,
    COALESCE((SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object('type', ct.contact_type, 'value', ct.contact_value, 'isPrimary', ct.is_primary, 'verifiedAt', ct.verified_at)) ORDER BY ct.contact_type, ct.id) FROM public.customer_contacts ct WHERE ct.customer_id = c.id), '[]'::jsonb) AS contacts,
    COALESCE((SELECT jsonb_build_object('marketingEmailOptIn', cp.marketing_email_opt_in, 'marketingSmsOptIn', cp.marketing_sms_opt_in, 'paperlessStatements', cp.paperless_statements, 'notificationChannels', cp.notification_channels) FROM public.customer_preferences cp WHERE cp.customer_id = c.id), '{}'::jsonb) AS preferences
  FROM public.customers c`;

export async function listPostgresCustomers(page = 1, pageSize = 10): Promise<CustomerPage> {
  const pool = getPostgresPool();
  const [count, result] = await Promise.all([
    pool.query<{ count: string }>("SELECT COUNT(*) FROM public.customers"),
    pool.query<CustomerRow>(`${customerSelect} ORDER BY c.updated_at DESC, c.id DESC LIMIT $1 OFFSET $2`, [pageSize, (page - 1) * pageSize]),
  ]);
  const total = Number(count.rows[0].count);
  return { items: result.rows.map(toCustomer), page, total, totalPages: Math.max(1, Math.ceil(total / pageSize)) };
}
async function getPostgresCustomer(id: string | number) {
  const result = await getPostgresPool().query<CustomerRow>(`${customerSelect} WHERE c.id = $1`, [id]);
  return result.rows[0] ? toCustomer(result.rows[0]) : null;
}

async function writeCustomerDetails(client: PoolClient, customerId: string | number, input: CustomerInput) {
  await client.query(
    `INSERT INTO public.customer_profiles (customer_id, preferred_language, occupation, annual_income, tax_residency_country, risk_rating)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (customer_id) DO UPDATE SET preferred_language = EXCLUDED.preferred_language, occupation = EXCLUDED.occupation, annual_income = EXCLUDED.annual_income, tax_residency_country = EXCLUDED.tax_residency_country, risk_rating = EXCLUDED.risk_rating, updated_at = CURRENT_TIMESTAMP`,
    [customerId, input.profile.preferredLanguage, input.profile.occupation, input.profile.annualIncome, input.profile.taxResidencyCountry, input.profile.riskRating],
  );
  await client.query("DELETE FROM public.customer_addresses WHERE customer_id = $1", [customerId]);
  for (const address of input.addresses) {
    await client.query(
      `INSERT INTO public.customer_addresses (customer_id, address_type, line_1, line_2, city, region, postal_code, country_code, valid_from, valid_to)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [customerId, address.type, address.line1, address.line2, address.city, address.region, address.postalCode, address.countryCode, address.validFrom, address.validTo],
    );
  }
  await client.query("DELETE FROM public.customer_contacts WHERE customer_id = $1", [customerId]);
  for (const contact of input.contacts) {
    await client.query(
      `INSERT INTO public.customer_contacts (customer_id, contact_type, contact_value, is_primary, verified_at)
       VALUES ($1, $2, $3, $4, $5)`,
      [customerId, contact.type, contact.value, contact.isPrimary, contact.verifiedAt],
    );
  }
  await client.query(
    `INSERT INTO public.customer_preferences (customer_id, marketing_email_opt_in, marketing_sms_opt_in, paperless_statements, notification_channels)
     VALUES ($1, $2, $3, $4, $5::jsonb)
     ON CONFLICT (customer_id) DO UPDATE SET marketing_email_opt_in = EXCLUDED.marketing_email_opt_in, marketing_sms_opt_in = EXCLUDED.marketing_sms_opt_in, paperless_statements = EXCLUDED.paperless_statements, notification_channels = EXCLUDED.notification_channels, updated_at = CURRENT_TIMESTAMP`,
    [customerId, input.preferences.marketingEmailOptIn, input.preferences.marketingSmsOptIn, input.preferences.paperlessStatements, JSON.stringify(input.preferences.notificationChannels)],
  );
}

export async function createPostgresCustomer(input: CustomerInput) {
  const client = await getPostgresPool().connect();
  try {
    await client.query("BEGIN");
    const result = await client.query<CustomerRow>(
      `INSERT INTO public.customers (customer_number, first_name, last_name, date_of_birth, status)
       VALUES ($1, $2, $3, $4, $5) RETURNING id`,
      [input.customerNumber, input.firstName, input.lastName, input.dateOfBirth, input.status],
    );
    await writeCustomerDetails(client, result.rows[0].id, input);
    await client.query("COMMIT");
    return (await getPostgresCustomer(result.rows[0].id))!;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally { client.release(); }
}

export async function updatePostgresCustomer(id: string, input: CustomerInput) {
  const client = await getPostgresPool().connect();
  try {
    await client.query("BEGIN");
    const result = await client.query<CustomerRow>(
      `UPDATE public.customers SET customer_number = $1, first_name = $2, last_name = $3, date_of_birth = $4, status = $5, updated_at = CURRENT_TIMESTAMP
       WHERE id = $6 RETURNING id`,
      [input.customerNumber, input.firstName, input.lastName, input.dateOfBirth, input.status, id],
    );
    if (!result.rows[0]) { await client.query("ROLLBACK"); return null; }
    await writeCustomerDetails(client, id, input);
    await client.query("COMMIT");
    return getPostgresCustomer(id);
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally { client.release(); }
}

export async function deletePostgresCustomer(id: string) {
  return ((await getPostgresPool().query("DELETE FROM public.customers WHERE id = $1", [id])).rowCount ?? 0) > 0;
}

export async function deleteAllPostgresCustomers() {
  return (await getPostgresPool().query("DELETE FROM public.customers")).rowCount ?? 0;
}

function mongoField(document: Document, name: string) {
  const after = document.after as Document | undefined;
  return document[name] ?? after?.[name];
}

export async function listMongoCustomers(page = 1, pageSize = 10): Promise<CustomerPage> {
  const collection = (await getMongoDb()).collection<Document>("customers");
  const filter = { deleted: { $ne: true } };
  const [total, documents] = await Promise.all([collection.countDocuments(filter), collection.find(filter).sort({ updated_at: -1, id: -1 }).skip((page - 1) * pageSize).limit(pageSize).toArray()]);
  return { items: documents.map((document) => {
    const documentId = document._id as Document | string | number;
    const primaryKey = mongoField(document, "id") ?? (typeof documentId === "object" ? documentId.id : documentId);
    return {
      id: String(primaryKey ?? document._id), customerNumber: String(mongoField(document, "customer_number") ?? ""), firstName: String(mongoField(document, "first_name") ?? ""), lastName: String(mongoField(document, "last_name") ?? ""), dateOfBirth: toDate(mongoField(document, "date_of_birth")), status: (mongoField(document, "status") ?? "active") as CustomerStatus,
      profile: mapProfile(mongoField(document, "profile")), addresses: mapAddresses(mongoField(document, "addresses")), contacts: mapContacts(mongoField(document, "contacts")), preferences: mapPreferences(mongoField(document, "preferences")),
      createdAt: toIso(mongoField(document, "created_at")), updatedAt: toIso(mongoField(document, "updated_at")),
    };
  }), page, total, totalPages: Math.max(1, Math.ceil(total / pageSize)) };
}

function validDate(value: string | null) { return !value || /^\d{4}-\d{2}-\d{2}$/.test(value); }
function parseObject(value: unknown) { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null; }

export function parseCustomerInput(value: unknown): { data: CustomerInput } | { error: string } {
  const input = parseObject(value);
  if (!input) return { error: "A customer payload is required." };
  const customerNumber = String(input.customerNumber ?? "").trim();
  const firstName = String(input.firstName ?? "").trim();
  const lastName = String(input.lastName ?? "").trim();
  const dateOfBirth = stringOrNull(input.dateOfBirth);
  const status = String(input.status ?? "") as CustomerStatus;
  if (!customerNumber || !firstName || !lastName) return { error: "Customer number, first name, and last name are required." };
  if (!validDate(dateOfBirth)) return { error: "Date of birth must use YYYY-MM-DD." };
  if (!customerStatuses.includes(status)) return { error: "Status must be active, inactive, or suspended." };

  const rawProfile = parseObject(input.profile);
  const riskRating = String(rawProfile?.riskRating ?? "") as CustomerProfileInput["riskRating"];
  const annualIncome = stringOrNull(rawProfile?.annualIncome);
  const taxResidencyCountry = stringOrNull(rawProfile?.taxResidencyCountry)?.toUpperCase() ?? null;
  if (!rawProfile || !String(rawProfile.preferredLanguage ?? "").trim() || !customerRiskRatings.includes(riskRating)) return { error: "Profile language and a valid risk rating are required." };
  if (annualIncome && (!/^\d+(\.\d{1,2})?$/.test(annualIncome) || Number(annualIncome) < 0)) return { error: "Annual income must be a positive currency amount." };
  if (taxResidencyCountry && !/^[A-Z]{2}$/.test(taxResidencyCountry)) return { error: "Tax residency country must be a two-letter code." };

  if (!Array.isArray(input.addresses)) return { error: "Addresses must be an array." };
  const addresses: CustomerAddressInput[] = [];
  const addressTypes = new Set<string>();
  for (const rawAddress of input.addresses) {
    const address = parseObject(rawAddress);
    const type = String(address?.type ?? "") as CustomerAddressInput["type"];
    const validFrom = stringOrNull(address?.validFrom);
    const validTo = stringOrNull(address?.validTo);
    if (!address || !customerAddressTypes.includes(type) || addressTypes.has(type) || !String(address.line1 ?? "").trim() || !String(address.city ?? "").trim() || !String(address.postalCode ?? "").trim() || !/^[A-Za-z]{2}$/.test(String(address.countryCode ?? "")) || !validFrom || !validDate(validFrom) || !validDate(validTo)) return { error: "Each address needs a unique type, street, city, postal code, country, and valid-from date." };
    addressTypes.add(type);
    addresses.push({ type, line1: String(address.line1).trim(), line2: stringOrNull(address.line2), city: String(address.city).trim(), region: stringOrNull(address.region), postalCode: String(address.postalCode).trim(), countryCode: String(address.countryCode).trim().toUpperCase(), validFrom, validTo });
  }

  if (!Array.isArray(input.contacts)) return { error: "Contacts must be an array." };
  const contacts: CustomerContactInput[] = [];
  const primaryTypes = new Set<string>();
  for (const rawContact of input.contacts) {
    const contact = parseObject(rawContact);
    const type = String(contact?.type ?? "") as CustomerContactInput["type"];
    const verifiedAt = stringOrNull(contact?.verifiedAt);
    const isPrimary = contact?.isPrimary === true;
    if (!contact || !customerContactTypes.includes(type) || !String(contact.value ?? "").trim() || (verifiedAt && Number.isNaN(Date.parse(verifiedAt))) || (isPrimary && primaryTypes.has(type))) return { error: "Each contact needs a type and value, with at most one primary contact per type." };
    if (isPrimary) primaryTypes.add(type);
    contacts.push({ type, value: String(contact.value).trim(), isPrimary, verifiedAt: verifiedAt ? new Date(verifiedAt).toISOString() : null });
  }

  const rawPreferences = parseObject(input.preferences);
  const rawChannels = parseObject(rawPreferences?.notificationChannels);
  if (!rawPreferences || !rawChannels || typeof rawPreferences.marketingEmailOptIn !== "boolean" || typeof rawPreferences.marketingSmsOptIn !== "boolean" || typeof rawPreferences.paperlessStatements !== "boolean" || Object.values(rawChannels).some((enabled) => typeof enabled !== "boolean")) return { error: "All preference switches must be true or false." };

  return {
    data: {
      customerNumber, firstName, lastName, dateOfBirth, status,
      profile: { preferredLanguage: String(rawProfile.preferredLanguage).trim(), occupation: stringOrNull(rawProfile.occupation), annualIncome, taxResidencyCountry, riskRating },
      addresses, contacts,
      preferences: { marketingEmailOptIn: rawPreferences.marketingEmailOptIn, marketingSmsOptIn: rawPreferences.marketingSmsOptIn, paperlessStatements: rawPreferences.paperlessStatements, notificationChannels: rawChannels as Record<string, boolean> },
    },
  };
}

const names = [["Ada", "Lovelace"], ["Grace", "Hopper"], ["Katherine", "Johnson"], ["Alan", "Turing"], ["Evelyn", "Boyd"], ["Maya", "Chen"]] as const;
const cities = [["London", "Greater London", "SW1A 1AA", "GB"], ["New York", "NY", "10001", "US"], ["Toronto", "ON", "M5V 2T6", "CA"], ["Singapore", "Singapore", "018956", "SG"]] as const;
const pick = <T,>(items: readonly T[]) => items[Math.floor(Math.random() * items.length)];

export function generateCustomerInput(): CustomerInput {
  const [firstName, lastName] = pick(names);
  const [city, region, postalCode, countryCode] = pick(cities);
  const suffix = randomUUID().slice(0, 8).toUpperCase();
  const birthYear = 1955 + Math.floor(Math.random() * 40);
  const birthDate = `${birthYear}-${String(1 + Math.floor(Math.random() * 12)).padStart(2, "0")}-${String(1 + Math.floor(Math.random() * 28)).padStart(2, "0")}`;
  const today = new Date().toISOString().slice(0, 10);
  return {
    customerNumber: `CUS-${suffix}`, firstName, lastName, dateOfBirth: birthDate, status: "active",
    profile: { preferredLanguage: "en", occupation: pick(["Engineer", "Analyst", "Researcher", "Consultant"]), annualIncome: String(60000 + Math.floor(Math.random() * 150000)), taxResidencyCountry: countryCode, riskRating: pick(customerRiskRatings) },
    addresses: customerAddressTypes.map((type, index) => ({ type, line1: `${10 + index * 22} Market Street`, line2: index === 1 ? "Suite 400" : null, city, region, postalCode, countryCode, validFrom: today, validTo: null })),
    contacts: [
      { type: "email", value: `${firstName.toLowerCase()}.${lastName.toLowerCase()}.${suffix.toLowerCase()}@example.test`, isPrimary: true, verifiedAt: new Date().toISOString() },
      { type: "mobile_phone", value: `+1-555-${String(1000 + Math.floor(Math.random() * 9000))}`, isPrimary: true, verifiedAt: new Date().toISOString() },
      { type: "home_phone", value: `+1-212-${String(1000 + Math.floor(Math.random() * 9000))}`, isPrimary: false, verifiedAt: null },
    ],
    preferences: { ...defaultPreferences, notificationChannels: { email: true, sms: Math.random() > 0.5, push: Math.random() > 0.5 } },
  };
}

export function emptyCustomerInput(): CustomerInput {
  return { customerNumber: "", firstName: "", lastName: "", dateOfBirth: null, status: "active", profile: { ...defaultProfile }, addresses: [], contacts: [], preferences: { ...defaultPreferences, notificationChannels: { ...defaultPreferences.notificationChannels } } };
}
