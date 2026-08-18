import { randomUUID, timingSafeEqual, createHash } from "node:crypto";
import { Pool, type PoolClient } from "pg";
import { assertTransition, runStateSchema, workflowDefinitionSchema, type CreateRun, type RunEvent, type RunState, type WorkflowDefinition } from "@dar/contracts";
import { hashApiKey, type AuditRecord, type Principal, type Role, type RuntimeStore, type StoredRun, type StoredWorkflow } from "./store.js";

type RunRow = {
  id: string;
  tenant_id: string;
  workflow_id: string;
  state: string;
  input: Record<string, unknown>;
  budget_cents: number;
  idempotency_key: string;
  created_at: Date;
};

type EventRow = { created_at: Date; event_type: RunEvent["type"]; detail: string; trace_id: string | null };

export type PendingOutboxMessage = {
  id: number;
  tenantId: string;
  subject: string;
  payload: Record<string, unknown>;
};

export type WorkerLease = {
  run: StoredRun;
  definition: WorkflowDefinition;
  currentStep: number;
  leaseOwner: string;
  leaseExpiresAt: string;
};

function equalHash(left: string, right: string): boolean {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}

function toEvent(row: EventRow): RunEvent {
  return { at: row.created_at.toISOString(), type: row.event_type, detail: row.detail, ...(row.trace_id ? { traceId: row.trace_id } : {}) };
}

function toRun(row: RunRow, events: RunEvent[]): StoredRun {
  return {
    id: row.id,
    tenantId: row.tenant_id,
    workflowId: row.workflow_id,
    state: runStateSchema.parse(row.state),
    input: row.input,
    budgetCents: row.budget_cents,
    idempotencyKey: row.idempotency_key,
    createdAt: row.created_at.toISOString(),
    events
  };
}

export class PostgresDurableStore implements RuntimeStore {
  readonly pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString, max: 12 });
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  async bootstrap(seed: { tenantSlug: string; apiKey: string; role?: Role; subject?: string }): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("begin");
      const tenant = await client.query<{ id: string }>(
        "insert into tenants (slug) values ($1) on conflict (slug) do update set slug = excluded.slug returning id",
        [seed.tenantSlug]
      );
      const tenantId = tenant.rows[0]!.id;
      // The control-plane role remains subject to RLS even while provisioning
      // its own tenant-scoped runtime limit record.
      await client.query("select set_config('app.tenant_id', $1, true)", [tenantId]);
      await client.query("insert into tenant_runtime_limits (tenant_id) values ($1) on conflict (tenant_id) do nothing", [tenantId]);
      const principal = await client.query<{ id: string }>(
        `insert into principals (tenant_id, subject, role) values ($1, $2, $3::dar_role)
         on conflict (tenant_id, subject) do update set role = excluded.role returning id`,
        [tenantId, seed.subject ?? "bootstrap-owner", seed.role ?? "owner"]
      );
      await client.query(
        `insert into api_keys (tenant_id, principal_id, key_hash) values ($1, $2, $3)
         on conflict (key_hash) do update set revoked_at = null`,
        [tenantId, principal.rows[0]!.id, hashApiKey(seed.apiKey)]
      );
      await client.query("commit");
    } catch (error) {
      await client.query("rollback");
      throw error;
    } finally {
      client.release();
    }
  }

  async authenticate(tenantSlug: string, apiKey: string | undefined): Promise<Principal | undefined> {
    if (!apiKey) return undefined;
    const result = await this.pool.query<{ tenant_id: string; role: Role; subject: string; key_hash: string }>(
      `select p.tenant_id, p.role, p.subject, k.key_hash
       from api_keys k
       join principals p on p.id = k.principal_id
       join tenants t on t.id = p.tenant_id
       where t.slug = $1 and k.revoked_at is null`,
      [tenantSlug]
    );
    const candidate = result.rows.find((row) => equalHash(row.key_hash, hashApiKey(apiKey)));
    return candidate ? { tenantId: candidate.tenant_id, role: candidate.role, subject: candidate.subject } : undefined;
  }

  async createWorkflow(principal: Principal, definition: WorkflowDefinition): Promise<StoredWorkflow> {
    return this.withTenant(principal.tenantId, async (client) => {
      const result = await client.query<{ id: string; tenant_id: string; created_at: Date }>(
        `insert into workflow_definitions (tenant_id, name, version, definition, budget_cents)
         values ($1, $2, $3, $4::jsonb, $5) returning id, tenant_id, created_at`,
        [principal.tenantId, definition.name, definition.version, JSON.stringify(definition), definition.budgetCents]
      );
      await this.audit(client, principal, "workflow.created", result.rows[0]!.id);
      return { id: result.rows[0]!.id, tenantId: result.rows[0]!.tenant_id, definition, createdAt: result.rows[0]!.created_at.toISOString() };
    });
  }

  async createRun(principal: Principal, request: Omit<CreateRun, "providerCredential"> & { providerCredentialHandle?: string }, idempotencyKey: string): Promise<{ run: StoredRun; replayed: boolean }> {
    return this.withTenant(principal.tenantId, async (client) => {
      const existing = await client.query<RunRow>(
        `select id, tenant_id, workflow_id, state, input, budget_cents, idempotency_key, created_at
         from workflow_runs where tenant_id = $1 and idempotency_key = $2`,
        [principal.tenantId, idempotencyKey]
      );
      if (existing.rows[0]) return { run: await this.readRunInTransaction(client, principal.tenantId, existing.rows[0]!), replayed: true };

      const limits = await client.query<{ max_active_runs: number; max_runs_per_day: number }>(
        "select max_active_runs, max_runs_per_day from tenant_runtime_limits where tenant_id = $1 for update",
        [principal.tenantId]
      );
      if (!limits.rows[0]) throw new Error("Tenant runtime limits missing");
      const usage = await client.query<{ active: string; created_today: string }>(
        `select count(*) filter (where state in ('queued', 'leased', 'running', 'awaiting_approval')) as active,
                count(*) filter (where created_at >= date_trunc('day', now() at time zone 'utc')) as created_today
         from workflow_runs where tenant_id = $1`,
        [principal.tenantId]
      );
      if (Number(usage.rows[0]!.active) >= limits.rows[0].max_active_runs) throw new Error("Active run quota exceeded");
      if (Number(usage.rows[0]!.created_today) >= limits.rows[0].max_runs_per_day) throw new Error("Daily run quota exceeded");

      const workflow = await client.query<{ id: string; budget_cents: number }>(
        "select id, budget_cents from workflow_definitions where id = $1 and tenant_id = $2",
        [request.workflowId, principal.tenantId]
      );
      if (!workflow.rows[0]) throw new Error("Workflow not found");
      const inserted = await client.query<RunRow>(
        `insert into workflow_runs (tenant_id, workflow_id, idempotency_key, input, budget_cents, provider_credential_handle)
         values ($1, $2, $3, $4::jsonb, $5, $6::uuid)
         on conflict (tenant_id, idempotency_key) do nothing
         returning id, tenant_id, workflow_id, state, input, budget_cents, idempotency_key, created_at`,
        [principal.tenantId, request.workflowId, idempotencyKey, JSON.stringify(request.input), workflow.rows[0].budget_cents, request.providerCredentialHandle ?? null]
      );
      if (!inserted.rows[0]) {
        const raced = await client.query<RunRow>(
          `select id, tenant_id, workflow_id, state, input, budget_cents, idempotency_key, created_at
           from workflow_runs where tenant_id = $1 and idempotency_key = $2`,
          [principal.tenantId, idempotencyKey]
        );
        return { run: await this.readRunInTransaction(client, principal.tenantId, raced.rows[0]!), replayed: true };
      }
      const row = inserted.rows[0];
      await client.query("insert into run_events (tenant_id, run_id, event_type, detail) values ($1, $2, 'created', 'Run queued')", [principal.tenantId, row.id]);
      await client.query("select queue_run_outbox($1::uuid, $2::uuid)", [principal.tenantId, row.id]);
      await this.audit(client, principal, "run.created", row.id);
      return { run: await this.readRunInTransaction(client, principal.tenantId, row), replayed: false };
    });
  }

  async readRun(principal: Principal, runId: string): Promise<StoredRun | undefined> {
    return this.withTenant(principal.tenantId, async (client) => {
      const result = await client.query<RunRow>(
        `select id, tenant_id, workflow_id, state, input, budget_cents, idempotency_key, created_at
         from workflow_runs where id = $1 and tenant_id = $2`,
        [runId, principal.tenantId]
      );
      return result.rows[0] ? this.readRunInTransaction(client, principal.tenantId, result.rows[0]) : undefined;
    });
  }

  async transition(principal: Principal, runId: string, state: RunState, event: RunEvent): Promise<StoredRun> {
    return this.withTenant(principal.tenantId, async (client) => {
      const locked = await client.query<RunRow>(
        `select id, tenant_id, workflow_id, state, input, budget_cents, idempotency_key, created_at
         from workflow_runs where id = $1 and tenant_id = $2 for update`,
        [runId, principal.tenantId]
      );
      const current = locked.rows[0];
      if (!current) throw new Error("Run not found");
      assertTransition(runStateSchema.parse(current.state), state);
      await client.query(
        state === "queued" && event.type === "approved"
          ? "update workflow_runs set state = $1::dar_run_state, current_step = current_step + 1, updated_at = now() where id = $2"
          : "update workflow_runs set state = $1::dar_run_state, updated_at = now() where id = $2",
        [state, runId]
      );
      await client.query("insert into run_events (tenant_id, run_id, event_type, detail, trace_id) values ($1, $2, $3, $4, $5)", [principal.tenantId, runId, event.type, event.detail, event.traceId ?? null]);
      if (state === "queued" && event.type === "approved") {
        await client.query("select queue_run_outbox($1::uuid, $2::uuid)", [principal.tenantId, runId]);
      }
      await this.audit(client, principal, `run.${event.type}`, runId);
      const updated = { ...current, state };
      return this.readRunInTransaction(client, principal.tenantId, updated);
    });
  }

  async listAudit(principal: Principal): Promise<AuditRecord[]> {
    return this.withTenant(principal.tenantId, async (client) => {
      const records = await client.query<{ created_at: Date; tenant_id: string; subject: string | null; action: string; resource_id: string | null }>(
        `select a.created_at, a.tenant_id, p.subject, a.action, a.resource_id
         from audit_records a left join principals p on p.id = a.principal_id
         where a.tenant_id = $1 order by a.id`,
        [principal.tenantId]
      );
      return records.rows.map((row) => ({ at: row.created_at.toISOString(), tenantId: row.tenant_id, subject: row.subject ?? "system", action: row.action, resourceId: row.resource_id ?? "" }));
    });
  }

  // This system-only path uses the worker database credential, never a tenant
  // API key. A crash between NATS acknowledgement and the published marker
  // may redeliver a message; the worker lease and effect ledger handle that.
  async claimPendingOutbox(limit = 50): Promise<PendingOutboxMessage[]> {
    const client = await this.pool.connect();
    try {
      await client.query("begin");
      const rows = await client.query<{ id: string; tenant_id: string; subject: string; payload: Record<string, unknown> }>(
        `select id, tenant_id, subject, payload from workflow_outbox
         where published_at is null
         order by id
         for update skip locked
         limit $1`,
        [limit]
      );
      if (rows.rows.length > 0) {
        await client.query("update workflow_outbox set attempts = attempts + 1 where id = any($1::bigint[])", [rows.rows.map((row) => row.id)]);
      }
      await client.query("commit");
      return rows.rows.map((row) => ({ id: Number(row.id), tenantId: row.tenant_id, subject: row.subject, payload: row.payload }));
    } catch (error) {
      await client.query("rollback");
      throw error;
    } finally {
      client.release();
    }
  }

  async markOutboxPublished(ids: number[]): Promise<void> {
    if (ids.length === 0) return;
    await this.pool.query("update workflow_outbox set published_at = now() where id = any($1::bigint[])", [ids]);
  }

  async claimRunLease(runId: string, workerId: string, ttlSeconds = 30): Promise<WorkerLease | undefined> {
    const client = await this.pool.connect();
    try {
      await client.query("begin");
      const run = await client.query<RunRow & { current_step: number; lease_expires_at: Date | null }>(
        `select id, tenant_id, workflow_id, state, input, budget_cents, idempotency_key, created_at, current_step, lease_expires_at
         from workflow_runs
         where id = $1 and (state = 'queued' or (state in ('leased', 'running') and lease_expires_at < now()))
         for update skip locked`,
        [runId]
      );
      if (!run.rows[0]) {
        await client.query("commit");
        return undefined;
      }
      const row = run.rows[0];
      const workflow = await client.query<{ definition: WorkflowDefinition }>("select definition from workflow_definitions where id = $1 and tenant_id = $2", [row.workflow_id, row.tenant_id]);
      if (!workflow.rows[0]) throw new Error("Workflow definition missing for leased run");
      const updated = await client.query<{ lease_expires_at: Date }>(
        `update workflow_runs set state = 'leased', lease_owner = $1,
         lease_expires_at = now() + ($2::text || ' seconds')::interval, updated_at = now()
         where id = $3 returning lease_expires_at`,
        [workerId, ttlSeconds, runId]
      );
      await client.query("insert into run_events (tenant_id, run_id, event_type, detail) values ($1, $2, 'leased', 'Worker lease acquired')", [row.tenant_id, runId]);
      await client.query("commit");
      return {
        run: await this.readRunSystem({ ...row, state: "leased" }),
        definition: workflowDefinitionSchema.parse(workflow.rows[0].definition),
        currentStep: row.current_step,
        leaseOwner: workerId,
        leaseExpiresAt: updated.rows[0]!.lease_expires_at.toISOString()
      };
    } catch (error) {
      await client.query("rollback");
      throw error;
    } finally {
      client.release();
    }
  }

  private async readRunInTransaction(client: PoolClient, tenantId: string, row: RunRow): Promise<StoredRun> {
    const events = await client.query<EventRow>("select created_at, event_type, detail, trace_id from run_events where tenant_id = $1 and run_id = $2 order by id", [tenantId, row.id]);
    return toRun(row, events.rows.map(toEvent));
  }

  private async readRunSystem(row: RunRow): Promise<StoredRun> {
    const events = await this.pool.query<EventRow>("select created_at, event_type, detail, trace_id from run_events where tenant_id = $1 and run_id = $2 order by id", [row.tenant_id, row.id]);
    return toRun(row, events.rows.map(toEvent));
  }

  private async audit(client: PoolClient, principal: Principal, action: string, resourceId: string): Promise<void> {
    const principalRow = await client.query<{ id: string }>("select id from principals where tenant_id = $1 and subject = $2", [principal.tenantId, principal.subject]);
    await client.query("insert into audit_records (tenant_id, principal_id, action, resource_id) values ($1, $2, $3, $4)", [principal.tenantId, principalRow.rows[0]?.id ?? null, action, resourceId]);
  }

  private async withTenant<T>(tenantId: string, operation: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("begin");
      await client.query("select set_config('app.tenant_id', $1, true)", [tenantId]);
      const result = await operation(client);
      await client.query("commit");
      return result;
    } catch (error) {
      await client.query("rollback");
      throw error;
    } finally {
      client.release();
    }
  }
}

// Kept explicit so every production caller uses the same SHA-256 key envelope.
export function providerKeyFingerprint(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 12);
}
