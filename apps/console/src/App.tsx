import { useEffect, useMemo, useState } from "react";

type RunEvent = { at: string; type: string; detail: string; traceId?: string };
type Run = { id: string; state: string; workflowId: string; budgetCents: number; events: RunEvent[] };

const demoWorkflow = {
  name: "research-demo",
  version: "v1",
  budgetCents: 100,
  allowedHosts: ["example.test"],
  steps: [
    { kind: "tool", tool: "mock_data_read", sideEffect: false },
    { kind: "transform", operation: "extract_json", input: "summarize fixture findings" },
    { kind: "approval", reason: "Confirm mock ticket creation" },
    { kind: "tool", tool: "mock_ticket_write", sideEffect: true }
  ]
};

function stateLabel(state?: string) {
  return state?.replaceAll("_", " ") ?? "not started";
}

export function App() {
  const [apiUrl, setApiUrl] = useState("http://127.0.0.1:3001");
  const [tenantId, setTenantId] = useState("demo-tenant");
  const [apiKey, setApiKey] = useState("replace-with-a-long-local-key");
  const [run, setRun] = useState<Run>();
  const [message, setMessage] = useState("Connect locally, then create a synthetic approval-gated run.");
  const [busy, setBusy] = useState(false);

  const headers = useMemo(() => ({ "content-type": "application/json", "x-tenant-id": tenantId, "x-api-key": apiKey }), [tenantId, apiKey]);

  async function request(path: string, init: RequestInit = {}) {
    const requestHeaders = { ...headers, ...(init.headers ?? {}) } as Record<string, string>;
    // Fastify rejects an empty body declared as JSON. Approval and cancellation
    // intentionally have no request body, so only advertise JSON when one is
    // actually supplied.
    if (!init.body) delete requestHeaders["content-type"];
    const response = await fetch(`${apiUrl}${path}`, { ...init, headers: requestHeaders });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.error ?? `Request failed (${response.status})`);
    return body;
  }

  async function createDemoRun() {
    setBusy(true);
    setMessage("Creating immutable workflow definition…");
    try {
      const workflow = await request("/v1/workflows", {
        method: "POST",
        body: JSON.stringify({ ...demoWorkflow, name: `research-demo-${crypto.randomUUID().slice(0, 8)}` })
      });
      setMessage("Queueing an idempotent run. No provider key or real external action is used.");
      const created = await request("/v1/runs", {
        method: "POST",
        headers: { "idempotency-key": crypto.randomUUID() },
        body: JSON.stringify({ workflowId: workflow.workflow.id, input: { query: "synthetic safe fixture" } })
      });
      setRun(created.run);
      setMessage("Run queued. The worker will stop at approval before the mock side effect.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Could not create run");
    } finally {
      setBusy(false);
    }
  }

  async function refresh() {
    if (!run) return;
    setBusy(true);
    try {
      const result = await request(`/v1/runs/${run.id}`);
      setRun(result.run);
      setMessage("Run refreshed.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Could not refresh run");
    } finally {
      setBusy(false);
    }
  }

  async function approve() {
    if (!run) return;
    setBusy(true);
    try {
      const result = await request(`/v1/runs/${run.id}/approve`, { method: "POST" });
      setRun(result.run);
      setMessage("Approval recorded. The run is safely re-queued at the next step.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Could not approve run");
    } finally {
      setBusy(false);
    }
  }

  async function cancel() {
    if (!run) return;
    setBusy(true);
    try {
      const result = await request(`/v1/runs/${run.id}/cancel`, { method: "POST" });
      setRun(result.run);
      setMessage("Cancellation recorded. No new work will be leased.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Could not cancel run");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    if (!run || ["succeeded", "failed", "cancelled", "uncertain", "awaiting_approval"].includes(run.state)) return;
    const timer = window.setInterval(() => void refresh(), 1500);
    return () => window.clearInterval(timer);
  }, [run?.id, run?.state]);

  return (
    <main>
      <header>
        <p className="eyebrow">Developer infrastructure / v0.1</p>
        <h1>Durable Agent Runtime</h1>
        <p className="lede">Recoverable agent workflows with explicit approval, tenant boundaries, and evidence-first operations.</p>
      </header>
      <section className="card connection">
        <div><label>Control-plane URL<input value={apiUrl} onChange={(event) => setApiUrl(event.target.value)} /></label></div>
        <div><label>Tenant<input value={tenantId} onChange={(event) => setTenantId(event.target.value)} /></label></div>
        <div><label>Local API key<input type="password" value={apiKey} onChange={(event) => setApiKey(event.target.value)} /></label></div>
        <button onClick={() => void createDemoRun()} disabled={busy}>{busy ? "Working…" : "Create synthetic run"}</button>
      </section>
      <p className="notice">{message}</p>
      <section className="grid">
        <article className="card">
          <p className="eyebrow">Safety controls</p>
          <ul>
            <li>Server-registered tools only</li>
            <li>Idempotent external effects</li>
            <li>Approval before side effects</li>
            <li>Per-run budgets and tenant scope</li>
          </ul>
        </article>
        <article className="card">
          <p className="eyebrow">Run status</p>
          <h2>{stateLabel(run?.state)}</h2>
          <p>{run ? `Run ${run.id.slice(0, 8)} · budget ${(run.budgetCents / 100).toFixed(2)} USD` : "No run created yet"}</p>
          {run && <div className="actions"><button className="secondary" onClick={() => void refresh()} disabled={busy}>Refresh timeline</button>{run.state === "awaiting_approval" && <button onClick={() => void approve()} disabled={busy}>Approve mock ticket</button>}{!["succeeded", "failed", "cancelled", "uncertain"].includes(run.state) && <button className="danger" onClick={() => void cancel()} disabled={busy}>Cancel run</button>}</div>}
        </article>
      </section>
      <section className="card timeline">
        <p className="eyebrow">Redacted event timeline</p>
        {run?.events.length ? <ol>{run.events.map((event) => <li key={`${event.at}-${event.type}`}><time>{new Date(event.at).toLocaleTimeString()}</time><strong>{stateLabel(event.type)}</strong><span>{event.detail}</span></li>)}</ol> : <p>Events will appear here. Provider credentials are never sent to this view.</p>}
      </section>
    </main>
  );
}
