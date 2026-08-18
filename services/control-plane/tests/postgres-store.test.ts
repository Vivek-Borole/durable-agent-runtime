import { randomBytes } from "node:crypto";
import { connect } from "nats";
import { afterAll, describe, expect, it } from "vitest";
import { OutboxPublisher } from "../src/outbox-publisher.js";
import { PostgresDurableStore } from "../src/postgres-store.js";

const databaseUrl = process.env.POSTGRES_URL;
const natsUrl = process.env.NATS_URL;
const integration = databaseUrl ? describe : describe.skip;

const workflow = {
  name: "postgres-fixture",
  version: "v1",
  budgetCents: 100,
  allowedHosts: ["example.test"],
  steps: [
    { kind: "tool", tool: "mock_data_read", sideEffect: false },
    { kind: "approval", reason: "Approve synthetic fixture" },
    { kind: "tool", tool: "mock_ticket_write", sideEffect: true }
  ]
} as const;

integration("PostgreSQL durable store", () => {
  const store = new PostgresDurableStore(databaseUrl!);
  const suffix = randomBytes(6).toString("hex");
  const tenantSlug = `test-${suffix}`;
  const apiKey = `postgres-test-${suffix}-local-key`;
  const secondTenantSlug = `other-${suffix}`;
  const secondApiKey = `postgres-other-${suffix}-local-key`;

  afterAll(async () => {
    await store.pool.query("delete from tenants where slug = any($1::text[])", [[tenantSlug, secondTenantSlug]]);
    await store.close();
  });

  it("enforces a tenant-scoped idempotency key and RLS reads", async () => {
    await store.bootstrap({ tenantSlug, apiKey });
    await store.bootstrap({ tenantSlug: secondTenantSlug, apiKey: secondApiKey });
    const principal = await store.authenticate(tenantSlug, apiKey);
    const otherPrincipal = await store.authenticate(secondTenantSlug, secondApiKey);
    expect(principal).toBeDefined();
    expect(otherPrincipal).toBeDefined();
    if (!principal || !otherPrincipal) throw new Error("fixture authentication failed");

    const createdWorkflow = await store.createWorkflow(principal, workflow);
    const first = await store.createRun(principal, { workflowId: createdWorkflow.id, input: { query: "fixture" } }, "postgres-idempotency-0001");
    const replay = await store.createRun(principal, { workflowId: createdWorkflow.id, input: { query: "changed-but-replayed" } }, "postgres-idempotency-0001");

    expect(first.replayed).toBe(false);
    expect(replay.replayed).toBe(true);
    expect(replay.run.id).toBe(first.run.id);
    expect(replay.run.input).toEqual({ query: "fixture" });
    expect(await store.readRun(otherPrincipal, first.run.id)).toBeUndefined();
  });

  it.skipIf(!natsUrl)("publishes a committed outbox entry to JetStream", async () => {
    const principal = await store.authenticate(tenantSlug, apiKey);
    if (!principal) throw new Error("fixture authentication failed");
    const outboxWorkflow = await store.createWorkflow(principal, { ...workflow, name: "outbox-fixture" });
    await store.createRun(principal, { workflowId: outboxWorkflow.id, input: { query: "outbox fixture" } }, "postgres-outbox-key-0001");

    const connection = await connect({ servers: natsUrl!, name: "dar-outbox-test" });
    const manager = await connection.jetstreamManager();
    await manager.streams.delete("DAR_TEST").catch(() => undefined);
    await manager.streams.add({ name: "DAR_TEST", subjects: ["dar.run.queued"], storage: "memory" });
    try {
      const publisher = new OutboxPublisher(store, connection.jetstream());
      // The preceding idempotency fixture intentionally created one queued
      // run, so this verifies the publisher drains the durable backlog too.
      await expect(publisher.publishPending()).resolves.toBe(2);
      await expect(manager.streams.info("DAR_TEST")).resolves.toMatchObject({ state: { messages: 2 } });
    } finally {
      await manager.streams.delete("DAR_TEST").catch(() => undefined);
      await connection.drain();
    }
  });

  it("allows one worker lease at a time and recovers an expired lease", async () => {
    const principal = await store.authenticate(tenantSlug, apiKey);
    if (!principal) throw new Error("fixture authentication failed");
    const leaseWorkflow = await store.createWorkflow(principal, { ...workflow, name: "lease-fixture" });
    const created = await store.createRun(principal, { workflowId: leaseWorkflow.id, input: { query: "lease fixture" } }, "postgres-lease-key-0001");

    const first = await store.claimRunLease(created.run.id, "worker-a", 30);
    expect(first).toMatchObject({ leaseOwner: "worker-a", currentStep: 0, run: { id: created.run.id, state: "leased" } });
    await expect(store.claimRunLease(created.run.id, "worker-b", 30)).resolves.toBeUndefined();

    await store.pool.query("update workflow_runs set lease_expires_at = now() - interval '1 second' where id = $1", [created.run.id]);
    await expect(store.claimRunLease(created.run.id, "worker-b", 30)).resolves.toMatchObject({ leaseOwner: "worker-b" });
  });
});
