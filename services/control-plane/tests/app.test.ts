import { describe, expect, it } from "vitest";
import { createApp } from "../src/app.js";
import { InMemoryDurableStore } from "../src/store.js";

const headers = { "x-tenant-id": "tenant-a", "x-api-key": "tenant-a-local-secret-key" };
const workflow = {
  name: "research-demo",
  version: "v1",
  budgetCents: 100,
  allowedHosts: ["example.test"],
  steps: [
    { kind: "tool", tool: "mock_data_read", sideEffect: false },
    { kind: "approval", reason: "Confirm mock ticket creation" },
    { kind: "tool", tool: "mock_ticket_write", sideEffect: true }
  ]
};

describe("control plane", () => {
  it("exposes only route/status metrics without requiring tenant credentials", async () => {
    const store = new InMemoryDurableStore({ tenantId: "tenant-a", apiKey: "tenant-a-local-secret-key" });
    const app = createApp(store);
    await app.inject({ method: "GET", url: "/healthz" });
    const metrics = await app.inject({ method: "GET", url: "/metrics" });
    expect(metrics.statusCode).toBe(200);
    expect(metrics.body).toContain("dar_http_requests_total");
    expect(metrics.body).not.toContain("tenant-a");
    await app.close();
  });

  it("creates an idempotent tenant-scoped run without persisting provider credentials", async () => {
    const store = new InMemoryDurableStore({ tenantId: "tenant-a", apiKey: "tenant-a-local-secret-key" });
    const app = createApp(store);
    const createdWorkflow = await app.inject({ method: "POST", url: "/v1/workflows", headers, payload: workflow });
    expect(createdWorkflow.statusCode).toBe(201);
    const workflowId = createdWorkflow.json().workflow.id as string;
    const payload = { workflowId, input: { query: "safe fixture" }, providerCredential: "provider-secret-never-persisted" };
    const first = await app.inject({ method: "POST", url: "/v1/runs", headers: { ...headers, "idempotency-key": "idempotency-key-0001" }, payload });
    const second = await app.inject({ method: "POST", url: "/v1/runs", headers: { ...headers, "idempotency-key": "idempotency-key-0001" }, payload });
    expect(first.statusCode).toBe(201);
    expect(second.statusCode).toBe(200);
    expect(first.json().run.id).toBe(second.json().run.id);
    expect(JSON.stringify(first.json())).not.toContain("provider-secret-never-persisted");
    await app.close();
  });

  it("denies cross-tenant reads", async () => {
    const store = new InMemoryDurableStore({ tenantId: "tenant-a", apiKey: "tenant-a-local-secret-key" });
    const app = createApp(store);
    const result = await app.inject({ method: "GET", url: "/v1/runs/00000000-0000-0000-0000-000000000000", headers: { "x-tenant-id": "tenant-b", "x-api-key": "tenant-b-local-secret-key" } });
    expect(result.statusCode).toBe(401);
    await app.close();
  });
});
