import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import {
  assertTransition,
  type CreateRun,
  type RunEvent,
  type RunState,
  type WorkflowDefinition
} from "@dar/contracts";

export type Role = "owner" | "operator" | "viewer";

export interface Principal {
  tenantId: string;
  role: Role;
  subject: string;
}

export interface StoredWorkflow {
  id: string;
  tenantId: string;
  definition: WorkflowDefinition;
  createdAt: string;
}

export interface StoredRun {
  id: string;
  tenantId: string;
  workflowId: string;
  state: RunState;
  input: Record<string, unknown>;
  budgetCents: number;
  idempotencyKey: string;
  createdAt: string;
  events: RunEvent[];
}

export interface AuditRecord {
  at: string;
  tenantId: string;
  subject: string;
  action: string;
  resourceId: string;
}

export interface RuntimeStore {
  authenticate(tenantSlug: string, apiKey: string | undefined): Principal | undefined | Promise<Principal | undefined>;
  createWorkflow(principal: Principal, definition: WorkflowDefinition): StoredWorkflow | Promise<StoredWorkflow>;
  createRun(principal: Principal, request: Omit<CreateRun, "providerCredential"> & { providerCredentialHandle?: string }, idempotencyKey: string): { run: StoredRun; replayed: boolean } | Promise<{ run: StoredRun; replayed: boolean }>;
  readRun(principal: Principal, runId: string): StoredRun | undefined | Promise<StoredRun | undefined>;
  transition(principal: Principal, runId: string, state: RunState, event: RunEvent): StoredRun | Promise<StoredRun>;
  listAudit(principal: Principal): AuditRecord[] | Promise<AuditRecord[]>;
}

export function hashApiKey(key: string): string {
  return createHash("sha256").update(key).digest("hex");
}

function isoNow(): string {
  return new Date().toISOString();
}

function constantTimeEqual(left: string, right: string): boolean {
  const leftBytes = Buffer.from(left);
  const rightBytes = Buffer.from(right);
  return leftBytes.length === rightBytes.length && timingSafeEqual(leftBytes, rightBytes);
}

export class InMemoryDurableStore implements RuntimeStore {
  readonly workflows = new Map<string, StoredWorkflow>();
  readonly runs = new Map<string, StoredRun>();
  readonly audits: AuditRecord[] = [];
  private readonly idempotency = new Map<string, string>();
  private readonly credentials = new Map<string, { hash: string; role: Role; subject: string }>();

  constructor(seed: { tenantId: string; apiKey: string; role?: Role; subject?: string }) {
    this.credentials.set(seed.tenantId, {
      hash: hashApiKey(seed.apiKey),
      role: seed.role ?? "owner",
      subject: seed.subject ?? "bootstrap-owner"
    });
  }

  authenticate(tenantId: string, apiKey: string | undefined): Principal | undefined {
    const record = this.credentials.get(tenantId);
    if (!record || !apiKey || !constantTimeEqual(record.hash, hashApiKey(apiKey))) return undefined;
    return { tenantId, role: record.role, subject: record.subject };
  }

  createWorkflow(principal: Principal, definition: WorkflowDefinition): StoredWorkflow {
    const id = randomUUID();
    const workflow: StoredWorkflow = { id, tenantId: principal.tenantId, definition, createdAt: isoNow() };
    this.workflows.set(id, workflow);
    this.audit(principal, "workflow.created", id);
    return workflow;
  }

  createRun(principal: Principal, request: CreateRun, idempotencyKey: string): { run: StoredRun; replayed: boolean } {
    const key = `${principal.tenantId}:${idempotencyKey}`;
    const priorId = this.idempotency.get(key);
    if (priorId) {
      const prior = this.runs.get(priorId);
      if (!prior) throw new Error("Idempotency record refers to a missing run");
      return { run: prior, replayed: true };
    }

    const workflow = this.workflows.get(request.workflowId);
    if (!workflow || workflow.tenantId !== principal.tenantId) throw new Error("Workflow not found");
    const now = isoNow();
    const id = randomUUID();
    const run: StoredRun = {
      id,
      tenantId: principal.tenantId,
      workflowId: workflow.id,
      state: "queued",
      input: request.input,
      budgetCents: workflow.definition.budgetCents,
      idempotencyKey,
      createdAt: now,
      events: [{ at: now, type: "created", detail: "Run queued" }]
    };
    this.runs.set(id, run);
    this.idempotency.set(key, id);
    this.audit(principal, "run.created", id);
    return { run, replayed: false };
  }

  readRun(principal: Principal, runId: string): StoredRun | undefined {
    const run = this.runs.get(runId);
    return run?.tenantId === principal.tenantId ? run : undefined;
  }

  transition(principal: Principal, runId: string, state: RunState, event: RunEvent): StoredRun {
    const run = this.readRun(principal, runId);
    if (!run) throw new Error("Run not found");
    assertTransition(run.state, state);
    run.state = state;
    run.events.push(event);
    this.audit(principal, `run.${event.type}`, runId);
    return run;
  }

  audit(principal: Principal, action: string, resourceId: string): void {
    this.audits.push({ at: isoNow(), tenantId: principal.tenantId, subject: principal.subject, action, resourceId });
  }

  listAudit(principal: Principal): AuditRecord[] {
    return this.audits.filter((item) => item.tenantId === principal.tenantId);
  }
}
