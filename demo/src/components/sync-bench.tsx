"use client";

import { type FormEvent, useEffect, useState } from "react";
import {
  customerAddressTypes,
  customerContactTypes,
  customerRiskRatings,
  customerStatuses,
  type Customer,
  type CustomerAddressInput,
  type CustomerContactInput,
  type CustomerDashboard,
  type CustomerInput,
  type CustomerSimulationStatus,
  type DatabaseEvent,
} from "@/types/customer";

const emptyDraft: CustomerInput = {
  customerNumber: "", firstName: "", lastName: "", dateOfBirth: null, status: "active",
  profile: { preferredLanguage: "en", occupation: null, annualIncome: null, taxResidencyCountry: null, riskRating: "standard" },
  addresses: [], contacts: [],
  preferences: { marketingEmailOptIn: false, marketingSmsOptIn: false, paperlessStatements: true, notificationChannels: { email: true, sms: false, push: false } },
};
const blankAddress = (type: CustomerAddressInput["type"]): CustomerAddressInput => ({ type, line1: "", line2: null, city: "", region: null, postalCode: "", countryCode: "", validFrom: new Date().toISOString().slice(0, 10), validTo: null });
const blankContact = (): CustomerContactInput => ({ type: "email", value: "", isPrimary: true, verifiedAt: null });

async function requestDashboard(postgresPage: number, mongodbPage: number) {
  const response = await fetch(`/api/customers?postgresPage=${postgresPage}&mongodbPage=${mongodbPage}`, { cache: "no-store" });
  const body = (await response.json()) as CustomerDashboard & { error?: string };
  if (!response.ok) throw new Error(body.error ?? "Unable to read the customers.");
  return body;
}
async function requestSimulationStatus() {
  const response = await fetch("/api/simulation", { cache: "no-store" });
  const body = (await response.json()) as CustomerSimulationStatus & { error?: string };
  if (!response.ok) throw new Error(body.error ?? "Unable to read simulation status.");
  return body;
}
function displayTime(value: string) { return value ? new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(new Date(value)) : "Pending"; }
function inputDateTime(value: string | null) { return value ? new Date(value).toISOString().slice(0, 16) : ""; }
const MIN_SIMULATION_RATE = 1;
const MAX_SIMULATION_RATE = 1000;
function clampSimulationRate(value: number) { if (!Number.isFinite(value)) return MIN_SIMULATION_RATE; return Math.min(MAX_SIMULATION_RATE, Math.max(MIN_SIMULATION_RATE, Math.round(value))); }
function rateToIntervalMs(rate: number) { return Math.max(1, Math.round(1000 / clampSimulationRate(rate))); }
function intervalMsToRate(intervalMs: number) { return clampSimulationRate(Math.round(1000 / Math.max(1, intervalMs))); }

function CustomerRows({ customers, writable, emptyMessage = "No records yet. Add a customer to the Postgres source.", onEdit, onDelete }: { customers: Customer[]; writable?: boolean; emptyMessage?: string; onEdit?: (customer: Customer) => void; onDelete?: (customer: Customer) => void }) {
  if (!customers.length) return <p className="empty-state">{emptyMessage}</p>;
  return <div className="table-wrap"><table><thead><tr><th>Customer</th><th>Number</th><th>Status</th><th>Changed</th>{writable && <th><span className="sr-only">Actions</span></th>}</tr></thead><tbody>{customers.map((customer) => <tr key={customer.id}><td><strong>{customer.firstName} {customer.lastName}</strong><span>{customer.profile.riskRating} risk · {customer.addresses.length} addresses · {customer.contacts.length} contacts</span></td><td>{customer.customerNumber}</td><td><span className={`status ${customer.status}`}>{customer.status}</span></td><td>{displayTime(customer.updatedAt)}</td>{writable && <td className="row-actions"><button type="button" className="text-button" onClick={() => onEdit?.(customer)}>Edit</button><button type="button" className="text-button danger-text" onClick={() => onDelete?.(customer)}>Delete</button></td>}</tr>)}</tbody></table></div>;
}

function Pager({ page, totalPages, onChange }: { page: number; totalPages: number; onChange: (page: number) => void }) {
  return <nav className="pager" aria-label="Customer records pages"><button type="button" className="text-button" disabled={page === 1} onClick={() => onChange(page - 1)}>Previous</button><span>Page {page} of {totalPages}</span><button type="button" className="text-button" disabled={page === totalPages} onClick={() => onChange(page + 1)}>Next</button></nav>;
}

export function SyncBench() {
  const [data, setData] = useState<CustomerDashboard>({ postgres: [], mongodb: [], pagination: { postgres: { page: 1, total: 0, totalPages: 1 }, mongodb: { page: 1, total: 0, totalPages: 1 } } });
  const [draft, setDraft] = useState<CustomerInput>(emptyDraft);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editorOpen, setEditorOpen] = useState(false);
  const [lastEvent, setLastEvent] = useState<DatabaseEvent | null>(null);
  const [simulation, setSimulation] = useState<CustomerSimulationStatus>({ running: false, intervalMs: 1000, created: 0, startedAt: null, lastCreatedAt: null, lastError: null });
  const [simulationRate, setSimulationRate] = useState(1);
  const [notice, setNotice] = useState("Connecting to both databases...");
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [isGenerating, setIsGenerating] = useState(false);
  const [isSimulationChanging, setIsSimulationChanging] = useState(false);
  const [observedSources, setObservedSources] = useState({ postgres: false, mongodb: false });

  async function refresh(reason = "manual refresh") {
    setIsLoading(true);
    try { setData(await requestDashboard(data.pagination.postgres.page, data.pagination.mongodb.page)); setNotice(reason === "manual refresh" ? "Panels refreshed." : `Refreshed after ${reason}.`); }
    catch (error) { setNotice(error instanceof Error ? error.message : "Unable to refresh the panels."); }
    finally { setIsLoading(false); }
  }
  // Keep one event-stream subscription for the workspace lifetime.
  useEffect(() => {
    const initialLoad = window.setTimeout(() => { void refresh("initial connection"); void requestSimulationStatus().then((status) => { setSimulation(status); setSimulationRate(intervalMsToRate(status.intervalMs)); }).catch(() => undefined); }, 0);
    const events = new EventSource("/api/events");
    events.onmessage = (message) => { const event = JSON.parse(message.data) as DatabaseEvent; if (event.operation === "heartbeat") return; setLastEvent(event); if (event.operation === "connected") { setObservedSources({ postgres: true, mongodb: true }); return; } if (event.operation === "postgres_unavailable") setObservedSources((sources) => ({ ...sources, postgres: false })); else if (event.operation === "mongodb_unavailable") setObservedSources((sources) => ({ ...sources, mongodb: false })); else if (event.source === "postgres") setObservedSources((sources) => ({ ...sources, postgres: true })); else if (event.source === "mongodb") setObservedSources((sources) => ({ ...sources, mongodb: true })); if (event.operation !== "degraded") void refresh(`${event.source} ${event.operation.toLowerCase()}`); };
    events.onerror = () => { setObservedSources({ postgres: false, mongodb: false }); setNotice("Event bridge reconnecting. Use refresh if needed."); };
    const simulationPoll = window.setInterval(() => void requestSimulationStatus().then((status) => { setSimulation(status); if (!status.running) setSimulationRate(intervalMsToRate(status.intervalMs)); }).catch(() => undefined), 2500);
    return () => { window.clearTimeout(initialLoad); window.clearInterval(simulationPoll); events.close(); };
  // The subscription intentionally stays open while pagination changes.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function edit(customer: Customer) { setEditingId(customer.id); setEditorOpen(true); setDraft({ customerNumber: customer.customerNumber, firstName: customer.firstName, lastName: customer.lastName, dateOfBirth: customer.dateOfBirth, status: customer.status, profile: { ...customer.profile }, addresses: customer.addresses.map((address) => ({ ...address })), contacts: customer.contacts.map((contact) => ({ ...contact })), preferences: { ...customer.preferences, notificationChannels: { ...customer.preferences.notificationChannels } } }); setNotice(`Editing ${customer.firstName} ${customer.lastName} in PostgreSQL.`); }
  function resetForm() { setEditingId(null); setEditorOpen(false); setDraft(emptyDraft); }
  function openCreate() { setEditingId(null); setDraft(emptyDraft); setEditorOpen(true); }
  function updateAddress(index: number, changes: Partial<CustomerAddressInput>) { setDraft({ ...draft, addresses: draft.addresses.map((address, addressIndex) => addressIndex === index ? { ...address, ...changes } : address) }); }
  function updateContact(index: number, changes: Partial<CustomerContactInput>) { setDraft({ ...draft, contacts: draft.contacts.map((contact, contactIndex) => contactIndex === index ? { ...contact, ...changes } : contact) }); }
  async function fillGeneratedValues() {
    setIsGenerating(true);
    try {
      const response = await fetch("/api/customers/sample", { cache: "no-store" });
      const body = (await response.json()) as CustomerInput & { error?: string };
      if (!response.ok) throw new Error(body.error ?? "Unable to generate a customer.");
      setDraft(body); setEditingId(null); setNotice("Generated all customer fields. Review or create the PostgreSQL source record.");
    } catch (error) { setNotice(error instanceof Error ? error.message : "Unable to generate a customer."); }
    finally { setIsGenerating(false); }
  }
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); setIsSaving(true);
    try { const response = await fetch(editingId ? `/api/customers/${editingId}` : "/api/customers", { method: editingId ? "PUT" : "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(draft) }); const body = (await response.json()) as Customer & { error?: string }; if (!response.ok) throw new Error(body.error ?? "Unable to save the customer."); const didEdit = Boolean(editingId); resetForm(); setNotice(didEdit ? "PostgreSQL customer updated." : "PostgreSQL customer created."); await refresh("PostgreSQL write"); }
    catch (error) { setNotice(error instanceof Error ? error.message : "Unable to save the customer."); }
    finally { setIsSaving(false); }
  }
  async function remove(customer: Customer) {
    setIsSaving(true);
    try { const response = await fetch(`/api/customers/${customer.id}`, { method: "DELETE" }); if (!response.ok) { const body = (await response.json()) as { error?: string }; throw new Error(body.error ?? "Unable to delete the customer."); } if (editingId === customer.id) resetForm(); setNotice("PostgreSQL customer deleted."); await refresh("PostgreSQL delete"); }
    catch (error) { setNotice(error instanceof Error ? error.message : "Unable to delete the customer."); }
    finally { setIsSaving(false); }
  }
  async function deleteAll() {
    setIsSaving(true);
    try { const response = await fetch("/api/customers", { method: "DELETE" }); const body = (await response.json()) as { deleted?: number; error?: string }; if (!response.ok) throw new Error(body.error ?? "Unable to delete PostgreSQL customers."); setNotice(`${body.deleted ?? 0} PostgreSQL customers deleted.`); await refresh("PostgreSQL bulk delete"); }
    catch (error) { setNotice(error instanceof Error ? error.message : "Unable to delete PostgreSQL customers."); }
    finally { setIsSaving(false); }
  }
  async function changePage(source: "postgres" | "mongodb", page: number) {
    setIsLoading(true);
    try { const next = { postgres: data.pagination.postgres.page, mongodb: data.pagination.mongodb.page, [source]: page }; setData(await requestDashboard(next.postgres, next.mongodb)); }
    catch (error) { setNotice(error instanceof Error ? error.message : "Unable to change page."); }
    finally { setIsLoading(false); }
  }
  async function setSimulationRunning(action: "start" | "stop") {
    setIsSimulationChanging(true);
    try {
      const response = await fetch("/api/simulation", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action, intervalMs: rateToIntervalMs(simulationRate) }) });
      const body = (await response.json()) as CustomerSimulationStatus & { error?: string };
      if (!response.ok) throw new Error(body.error ?? "Unable to update the generator.");
      setSimulation(body); setSimulationRate(intervalMsToRate(body.intervalMs)); setNotice(action === "start" ? "Server-side continuous customer generation started." : "Server-side customer generation stopped.");
    } catch (error) { setNotice(error instanceof Error ? error.message : "Unable to update the generator."); }
    finally { setIsSimulationChanging(false); }
  }

  const sourceTotal = data.pagination.postgres.total;
  const replicaTotal = data.pagination.mongodb.total;
  const recordDelta = sourceTotal - replicaTotal;
  const countParity = sourceTotal === replicaTotal;
  const observingBoth = observedSources.postgres && observedSources.mongodb;
  const latestObservation = lastEvent
    ? `${lastEvent.source === "postgres" ? "PostgreSQL" : lastEvent.source === "mongodb" ? "MongoDB" : "System"} ${lastEvent.operation}`
    : "Awaiting the first database event";

  return <main className="bench-shell">
    <header className="topbar"><div className="brand-lockup"><svg aria-hidden="true" className="circuit-icon" viewBox="0 0 42 42"><path d="M4 9h14v9h11v15h9M4 33h8V22h7V8h19" /><circle cx="4" cy="9" r="2" /><circle cx="4" cy="33" r="2" /><circle cx="38" cy="33" r="2" /><circle cx="38" cy="8" r="2" /></svg><div><p className="product-name">Sync Bench</p><p className="product-subtitle">Customer replication workspace</p></div></div><div className="health-line" aria-live="polite"><span className="pulse-dot" /> <span>{isLoading ? "Refreshing records" : observingBoth ? "Both observation feeds active" : "Observation feed reconnecting"}</span><button type="button" className="refresh-button" onClick={() => void refresh()} disabled={isLoading}>Refresh</button></div></header>
    <section className="architecture-rail" aria-labelledby="pipeline-title"><div className="rail-intro"><h1 id="pipeline-title">Customer replication path</h1><p>A committed PostgreSQL transaction becomes one replaceable MongoDB customer document.</p></div><ol className="pipeline-stages"><li><strong>PostgreSQL</strong><span>Writable normalized source</span></li><li><strong>Debezium CDC</strong><span>Five table topics + transaction metadata</span></li><li><strong>Transaction barrier</strong><span>Waits for every row and the transaction END</span></li><li><strong>Customer projector</strong><span>Customer-keyed state emits a full document</span></li><li><strong>MongoDB</strong><span>Read-only, eventually consistent replica</span></li></ol></section>
    <p className="notice" aria-live="polite">{notice}</p>
    <section className="bench-grid">
      <section className="database-panel" aria-labelledby="postgres-title"><div className="panel-heading"><div><p className="panel-kicker">Authoritative source</p><h1 id="postgres-title">PostgreSQL customers</h1></div><div className="panel-actions"><span className="record-count">{data.pagination.postgres.total} records</span><button type="button" className="add-button" onClick={openCreate}>Create customer</button><button type="button" className="text-button danger-text" onClick={() => void deleteAll()} disabled={isSaving || data.pagination.postgres.total === 0}>Delete all</button></div></div><CustomerRows customers={data.postgres} writable onEdit={edit} onDelete={remove} /><Pager {...data.pagination.postgres} onChange={(page) => void changePage("postgres", page)} />
        {editorOpen && <div className="modal-backdrop" role="presentation"><section className="customer-modal" role="dialog" aria-modal="true" aria-labelledby="customer-editor-title"><form className="customer-form" onSubmit={submit}>
          <div className="form-heading"><h2 id="customer-editor-title">{editingId ? "Edit customer" : "Create complete customer"}</h2><div>{!editingId && <button type="button" className="text-button" onClick={() => void fillGeneratedValues()} disabled={isGenerating}>{isGenerating ? "Generating..." : "Fill generated values"}</button>}<button type="button" className="text-button" onClick={resetForm}>Close</button></div></div>
          <div className="form-grid"><label>Customer number<input required value={draft.customerNumber} onChange={(event) => setDraft({ ...draft, customerNumber: event.target.value })} placeholder="CUS-1001" /></label><label>First name<input required value={draft.firstName} onChange={(event) => setDraft({ ...draft, firstName: event.target.value })} placeholder="Ada" /></label><label>Last name<input required value={draft.lastName} onChange={(event) => setDraft({ ...draft, lastName: event.target.value })} placeholder="Lovelace" /></label><label>Date of birth<input type="date" value={draft.dateOfBirth ?? ""} onChange={(event) => setDraft({ ...draft, dateOfBirth: event.target.value || null })} /></label><label>Status<select value={draft.status} onChange={(event) => setDraft({ ...draft, status: event.target.value as CustomerInput["status"] })}>{customerStatuses.map((status) => <option key={status} value={status}>{status}</option>)}</select></label></div>
          <fieldset><legend>Customer profile</legend><div className="form-grid"><label>Preferred language<input required value={draft.profile.preferredLanguage} onChange={(event) => setDraft({ ...draft, profile: { ...draft.profile, preferredLanguage: event.target.value } })} /></label><label>Occupation<input value={draft.profile.occupation ?? ""} onChange={(event) => setDraft({ ...draft, profile: { ...draft.profile, occupation: event.target.value || null } })} /></label><label>Annual income<input inputMode="decimal" value={draft.profile.annualIncome ?? ""} onChange={(event) => setDraft({ ...draft, profile: { ...draft.profile, annualIncome: event.target.value || null } })} placeholder="75000.00" /></label><label>Tax residency country<input maxLength={2} value={draft.profile.taxResidencyCountry ?? ""} onChange={(event) => setDraft({ ...draft, profile: { ...draft.profile, taxResidencyCountry: event.target.value.toUpperCase() || null } })} placeholder="GB" /></label><label>Risk rating<select value={draft.profile.riskRating} onChange={(event) => setDraft({ ...draft, profile: { ...draft.profile, riskRating: event.target.value as CustomerInput["profile"]["riskRating"] } })}>{customerRiskRatings.map((risk) => <option key={risk} value={risk}>{risk}</option>)}</select></label></div></fieldset>
          <fieldset><legend>Addresses</legend><div className="repeater-list">{draft.addresses.map((address, index) => <div className="repeater-row" key={`${address.type}-${index}`}><div className="repeater-heading"><strong>Address {index + 1}</strong><button type="button" className="text-button danger-text" onClick={() => setDraft({ ...draft, addresses: draft.addresses.filter((_, addressIndex) => addressIndex !== index) })}>Remove</button></div><div className="form-grid"><label>Address type<select value={address.type} onChange={(event) => updateAddress(index, { type: event.target.value as CustomerAddressInput["type"] })}>{customerAddressTypes.map((type) => <option key={type} value={type}>{type}</option>)}</select></label><label>Line 1<input required value={address.line1} onChange={(event) => updateAddress(index, { line1: event.target.value })} /></label><label>Line 2<input value={address.line2 ?? ""} onChange={(event) => updateAddress(index, { line2: event.target.value || null })} /></label><label>City<input required value={address.city} onChange={(event) => updateAddress(index, { city: event.target.value })} /></label><label>Region<input value={address.region ?? ""} onChange={(event) => updateAddress(index, { region: event.target.value || null })} /></label><label>Postal code<input required value={address.postalCode} onChange={(event) => updateAddress(index, { postalCode: event.target.value })} /></label><label>Country code<input required maxLength={2} value={address.countryCode} onChange={(event) => updateAddress(index, { countryCode: event.target.value.toUpperCase() })} /></label><label>Valid from<input required type="date" value={address.validFrom} onChange={(event) => updateAddress(index, { validFrom: event.target.value })} /></label><label>Valid to<input type="date" value={address.validTo ?? ""} onChange={(event) => updateAddress(index, { validTo: event.target.value || null })} /></label></div></div>)}</div><button type="button" className="add-button" disabled={draft.addresses.length === customerAddressTypes.length} onClick={() => { const type = customerAddressTypes.find((candidate) => !draft.addresses.some((address) => address.type === candidate)); if (type) setDraft({ ...draft, addresses: [...draft.addresses, blankAddress(type)] }); }}>Add address</button></fieldset>
          <fieldset><legend>Contacts</legend><div className="repeater-list">{draft.contacts.map((contact, index) => <div className="repeater-row compact" key={`${contact.type}-${index}`}><div className="repeater-heading"><strong>Contact {index + 1}</strong><button type="button" className="text-button danger-text" onClick={() => setDraft({ ...draft, contacts: draft.contacts.filter((_, contactIndex) => contactIndex !== index) })}>Remove</button></div><div className="form-grid"><label>Contact type<select value={contact.type} onChange={(event) => updateContact(index, { type: event.target.value as CustomerContactInput["type"] })}>{customerContactTypes.map((type) => <option key={type} value={type}>{type}</option>)}</select></label><label>Value<input required value={contact.value} onChange={(event) => updateContact(index, { value: event.target.value })} /></label><label>Verified at<input type="datetime-local" value={inputDateTime(contact.verifiedAt)} onChange={(event) => updateContact(index, { verifiedAt: event.target.value || null })} /></label><label className="check-label"><input type="checkbox" checked={contact.isPrimary} onChange={(event) => updateContact(index, { isPrimary: event.target.checked })} />Primary contact</label></div></div>)}</div><button type="button" className="add-button" onClick={() => setDraft({ ...draft, contacts: [...draft.contacts, blankContact()] })}>Add contact</button></fieldset>
          <fieldset><legend>Communication preferences</legend><div className="toggle-grid"><label className="check-label"><input type="checkbox" checked={draft.preferences.marketingEmailOptIn} onChange={(event) => setDraft({ ...draft, preferences: { ...draft.preferences, marketingEmailOptIn: event.target.checked } })} />Marketing email</label><label className="check-label"><input type="checkbox" checked={draft.preferences.marketingSmsOptIn} onChange={(event) => setDraft({ ...draft, preferences: { ...draft.preferences, marketingSmsOptIn: event.target.checked } })} />Marketing SMS</label><label className="check-label"><input type="checkbox" checked={draft.preferences.paperlessStatements} onChange={(event) => setDraft({ ...draft, preferences: { ...draft.preferences, paperlessStatements: event.target.checked } })} />Paperless statements</label>{Object.entries(draft.preferences.notificationChannels).map(([channel, enabled]) => <label className="check-label" key={channel}><input type="checkbox" checked={enabled} onChange={(event) => setDraft({ ...draft, preferences: { ...draft.preferences, notificationChannels: { ...draft.preferences.notificationChannels, [channel]: event.target.checked } } })} />Notify by {channel}</label>)}</div></fieldset>
          <button className="primary-button" disabled={isSaving}>{isSaving ? "Writing..." : editingId ? "Save PostgreSQL customer" : "Create PostgreSQL customer"}</button>
        </form></section></div>}
        <section className="simulation-control" aria-labelledby="simulation-title"><div><h2 id="simulation-title">Continuous generator</h2><p>Runs in the Next.js Node.js server, not in this browser.</p></div><div className="simulation-actions"><label className="simulation-rate">Customers per second<div className="simulation-rate-controls"><input type="range" min={MIN_SIMULATION_RATE} max={MAX_SIMULATION_RATE} step={1} value={simulationRate} disabled={simulation.running || isSimulationChanging} onChange={(event) => setSimulationRate(clampSimulationRate(Number(event.target.value)))} aria-valuemin={MIN_SIMULATION_RATE} aria-valuemax={MAX_SIMULATION_RATE} aria-valuenow={simulationRate} /><input type="number" min={MIN_SIMULATION_RATE} max={MAX_SIMULATION_RATE} step={1} value={simulationRate} disabled={simulation.running || isSimulationChanging} onChange={(event) => setSimulationRate(clampSimulationRate(Number(event.target.value)))} aria-label="Customers per second" /></div></label><button type="button" className="simulation-button" disabled={isSimulationChanging} onClick={() => void setSimulationRunning(simulation.running ? "stop" : "start")}>{isSimulationChanging ? "Updating..." : simulation.running ? "Stop generator" : "Start generator"}</button></div><p className="simulation-status" aria-live="polite">{simulation.running ? `Server generating at ${intervalMsToRate(simulation.intervalMs)}/s (${simulation.intervalMs} ms interval)` : "Server generator stopped"} · {simulation.created} created this process{simulation.lastError ? ` · ${simulation.lastError}` : ""}</p></section>
      </section>
      <section className="control-plane" aria-labelledby="control-plane-title"><header className="control-heading"><div><h2 id="control-plane-title">Replication control plane</h2><p>Live browser observations, with the architecture decisions that make the replica safe to evaluate.</p></div><span className={`bridge-status ${observingBoth ? "connected" : "degraded"}`}><i />{observingBoth ? "Both feeds observed" : "Observation degraded"}</span></header><section className="observed-state" aria-labelledby="observed-state-title"><h3 id="observed-state-title">Observed now</h3><dl className="observation-grid"><div><dt>PostgreSQL feed</dt><dd className={observedSources.postgres ? "good-value" : "watch-value"}>{observedSources.postgres ? "Listening" : "Unavailable"}</dd><small>PostgreSQL NOTIFY subscription</small></div><div><dt>MongoDB feed</dt><dd className={observedSources.mongodb ? "good-value" : "watch-value"}>{observedSources.mongodb ? "Listening" : "Unavailable"}</dd><small>MongoDB change stream subscription</small></div><div><dt>Latest event</dt><dd>{latestObservation}</dd><small>{lastEvent?.customerNumber ? `Customer ${lastEvent.customerNumber}` : "SSE event stream"}</small></div><div><dt>Last observed</dt><dd>{lastEvent ? displayTime(lastEvent.at) : "Pending"}</dd><small>{lastEvent ? "Browser receipt time" : "Waiting for a write or replica change"}</small></div><div><dt>Source / replica</dt><dd>{sourceTotal} / {replicaTotal}</dd><small>Customer records / documents</small></div><div><dt>Count delta</dt><dd className={countParity ? "good-value" : "watch-value"}>{countParity ? "0" : `${recordDelta > 0 ? "+" : ""}${recordDelta}`}</dd><small>{countParity ? "Count parity observed" : "Investigate convergence"}</small></div></dl><p className={`parity-callout ${countParity ? "parity-observed" : "parity-pending"}`}><strong>{countParity ? "Counts align" : "Counts are not aligned"}</strong>{countParity ? " This is a volume check only. Field-level parity is not measured by this workspace." : " PostgreSQL remains authoritative; allow the pipeline to converge, then validate the customer document contents."}</p></section><section className="control-mechanisms" aria-labelledby="mechanism-title"><h3 id="mechanism-title">What protects the projection</h3><ol><li><span>1</span><div><strong>Capture all normalized changes</strong><p>Debezium publishes customers, profile, addresses, contacts, preferences, and transaction metadata.</p></div></li><li><span>2</span><div><strong>Respect commit boundaries</strong><p>The transaction stage waits for its declared event count and END marker before it creates a customer bundle.</p></div></li><li><span>3</span><div><strong>Replace by customer identity</strong><p>Kafka Streams applies the complete bundle to customer-keyed state, then emits one full document for the MongoDB sink.</p></div></li></ol></section><aside className="migration-gate"><h3>Reader migration is blocked</h3><dl><div><dt>Initial snapshot</dt><dd>Must drain</dd></div><div><dt>Field-level parity</dt><dd>Not measured here</dd></div><div><dt>Write authority</dt><dd>PostgreSQL</dd></div></dl><p>Reader routing changes only after independent reconciliation produces approved parity evidence.</p></aside></section>
      <section className="database-panel" aria-labelledby="mongodb-title"><div className="panel-heading"><div><p className="panel-kicker">Read-only replica</p><h2 id="mongodb-title">MongoDB customers</h2></div><span className="record-count">{data.pagination.mongodb.total} documents</span></div><CustomerRows customers={data.mongodb} emptyMessage="No replica document yet. Kafka Streams emits it after the normalized changes arrive." /><Pager {...data.pagination.mongodb} onChange={(page) => void changePage("mongodb", page)} /><div className="projection-note"><strong>Kafka aggregate per customer</strong><p>Each MongoDB record embeds the profile, address array, contact array, and communication preferences.</p></div><div className="replica-note"><span className="pulse-dot amber" /><div><strong>Eventual replication</strong><p>MongoDB updates after Kafka Streams aggregates the Debezium change events.</p></div></div></section>
    </section>
  </main>;
}
