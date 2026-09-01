import { randomBytes } from "node:crypto";
import { connect } from "nats";
import { Pool } from "pg";
import { afterAll, describe, expect, it } from "vitest";
import { OutboxPublisher } from "../src/outbox-publisher.js";
import { PostgresDurableStore } from "../src/postgres-store.js";

const databaseUrl = process.env.POSTGRES_URL;
const systemDatabaseUrl =
  process.env.DAR_TEST_ADMIN_POSTGRES_URL ?? databaseUrl;
const workerDatabaseUrl =
  process.env.DAR_WORKER_POSTGRES_URL ?? systemDatabaseUrl;
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
    { kind: "tool", tool: "mock_ticket_write", sideEffect: true },
  ],
} as const;

integration("PostgreSQL durable store", () => {
  const store = new PostgresDurableStore(databaseUrl!);
  const systemPool = new Pool({ connectionString: systemDatabaseUrl });
  const workerStore = new PostgresDurableStore(workerDatabaseUrl!);
  const suffix = randomBytes(6).toString("hex");
  const tenantSlug = `test-${suffix}`;
  const apiKey = `postgres-test-${suffix}-local-key`;
  const secondTenantSlug = `other-${suffix}`;
  const secondApiKey = `postgres-other-${suffix}-local-key`;

  afterAll(async () => {
    await systemPool.query("delete from tenants where slug = any($1::text[])", [
      [tenantSlug, secondTenantSlug],
    ]);
    await store.close();
    await workerStore.close();
    await systemPool.end();
  });

  it("enforces a tenant-scoped idempotency key and RLS reads", async () => {
    await store.bootstrap({ tenantSlug, apiKey });
    await store.bootstrap({
      tenantSlug: secondTenantSlug,
      apiKey: secondApiKey,
    });
    const principal = await store.authenticate(tenantSlug, apiKey);
    const otherPrincipal = await store.authenticate(
      secondTenantSlug,
      secondApiKey,
    );
    expect(principal).toBeDefined();
    expect(otherPrincipal).toBeDefined();
    if (!principal || !otherPrincipal)
      throw new Error("fixture authentication failed");

    const createdWorkflow = await store.createWorkflow(principal, workflow);
    const first = await store.createRun(
      principal,
      { workflowId: createdWorkflow.id, input: { query: "fixture" } },
      "postgres-idempotency-0001",
    );
    const replay = await store.createRun(
      principal,
      {
        workflowId: createdWorkflow.id,
        input: { query: "changed-but-replayed" },
      },
      "postgres-idempotency-0001",
    );

    expect(first.replayed).toBe(false);
    expect(replay.replayed).toBe(true);
    expect(replay.run.id).toBe(first.run.id);
    expect(replay.run.input).toEqual({ query: "fixture" });
    expect(await store.readRun(otherPrincipal, first.run.id)).toBeUndefined();
  });

  it("keeps outbox and worker lease columns unavailable to the control-plane role", async () => {
    await expect(
      store.pool.query("select * from workflow_outbox limit 1"),
    ).rejects.toThrow("permission denied");
    await expect(
      store.pool.query(
        "update workflow_runs set lease_owner = 'forbidden' where false",
      ),
    ).rejects.toThrow("permission denied");
  });

  it.skipIf(!natsUrl)(
    "publishes a committed outbox entry to JetStream",
    async () => {
      const principal = await store.authenticate(tenantSlug, apiKey);
      if (!principal) throw new Error("fixture authentication failed");
      const outboxWorkflow = await store.createWorkflow(principal, {
        ...workflow,
        name: "outbox-fixture",
      });
      await store.createRun(
        principal,
        { workflowId: outboxWorkflow.id, input: { query: "outbox fixture" } },
        "postgres-outbox-key-0001",
      );
      await systemPool.query(
        "update workflow_outbox set subject = 'dar.test.run.queued' where tenant_id = $1 and published_at is null",
        [principal.tenantId],
      );

      const connection = await connect({
        servers: natsUrl!,
        name: "dar-outbox-test",
      });
      const manager = await connection.jetstreamManager();
      await manager.streams.delete("DAR_TEST").catch(() => undefined);
      await manager.streams.add({
        name: "DAR_TEST",
        subjects: ["dar.test.run.queued"],
        storage: "memory",
      });
      try {
        const publisher = new OutboxPublisher(
          workerStore,
          connection.jetstream(),
        );
        // The preceding idempotency fixture intentionally created one queued
        // run, so this verifies the publisher drains the durable backlog too.
        await expect(publisher.publishPending()).resolves.toBe(2);
        await expect(manager.streams.info("DAR_TEST")).resolves.toMatchObject({
          state: { messages: 2 },
        });
      } finally {
        await manager.streams.delete("DAR_TEST").catch(() => undefined);
        await connection.drain();
      }
    },
  );

  it("allows one worker lease at a time and recovers an expired lease", async () => {
    const principal = await store.authenticate(tenantSlug, apiKey);
    if (!principal) throw new Error("fixture authentication failed");
    const leaseWorkflow = await store.createWorkflow(principal, {
      ...workflow,
      name: "lease-fixture",
    });
    const created = await store.createRun(
      principal,
      { workflowId: leaseWorkflow.id, input: { query: "lease fixture" } },
      "postgres-lease-key-0001",
    );

    const concurrent = await Promise.all([
      workerStore.claimRunLease(created.run.id, "worker-a", 30),
      workerStore.claimRunLease(created.run.id, "worker-b", 30),
    ]);
    expect(concurrent.filter(Boolean)).toHaveLength(1);
    const first = concurrent.find(Boolean);
    expect(first).toMatchObject({
      currentStep: 0,
      run: { id: created.run.id, state: "leased" },
    });

    await systemPool.query(
      "update workflow_runs set lease_expires_at = now() - interval '1 second' where id = $1",
      [created.run.id],
    );
    await expect(
      workerStore.claimRunLease(created.run.id, "worker-recovery", 30),
    ).resolves.toMatchObject({ leaseOwner: "worker-recovery" });
  });

  it("requeues an approved run at the step after its approval and emits a new outbox item", async () => {
    const principal = await store.authenticate(tenantSlug, apiKey);
    if (!principal) throw new Error("fixture authentication failed");
    const approvalWorkflow = await store.createWorkflow(principal, {
      ...workflow,
      name: "approval-fixture",
    });
    const created = await store.createRun(
      principal,
      { workflowId: approvalWorkflow.id, input: {} },
      "postgres-approval-key-0001",
    );
    const now = new Date().toISOString();

    await store.transition(principal, created.run.id, "leased", {
      at: now,
      type: "leased",
      detail: "test lease",
    });
    await store.transition(principal, created.run.id, "running", {
      at: now,
      type: "started",
      detail: "test start",
    });
    await store.transition(principal, created.run.id, "awaiting_approval", {
      at: now,
      type: "approval_requested",
      detail: "test approval",
    });
    await store.transition(principal, created.run.id, "queued", {
      at: now,
      type: "approved",
      detail: "test approved",
    });

    await expect(
      systemPool.query<{ current_step: number }>(
        "select current_step from workflow_runs where id = $1",
        [created.run.id],
      ),
    ).resolves.toMatchObject({ rows: [{ current_step: 1 }] });
    await expect(
      systemPool.query<{ count: string }>(
        "select count(*) from workflow_outbox where run_id = $1",
        [created.run.id],
      ),
    ).resolves.toMatchObject({ rows: [{ count: "2" }] });
  });

  it("enforces active run limits under the tenant transaction lock", async () => {
    const principal = await store.authenticate(secondTenantSlug, secondApiKey);
    if (!principal) throw new Error("fixture authentication failed");
    const limitedWorkflow = await store.createWorkflow(principal, {
      ...workflow,
      name: "limit-fixture",
    });
    await systemPool.query(
      "update tenant_runtime_limits set max_active_runs = 1, max_runs_per_day = 1000 where tenant_id = $1",
      [principal.tenantId],
    );
    await store.createRun(
      principal,
      { workflowId: limitedWorkflow.id, input: {} },
      "postgres-limit-key-0001",
    );
    await expect(
      store.createRun(
        principal,
        { workflowId: limitedWorkflow.id, input: {} },
        "postgres-limit-key-0002",
      ),
    ).rejects.toThrow("Active run quota exceeded");
  });
});
