import Fastify, { type FastifyInstance, type FastifyRequest } from "fastify";
import { createRunSchema, runStateSchema, workflowDefinitionSchema } from "@dar/contracts";
import { InMemoryDurableStore, type Principal, type Role } from "./store.js";

declare module "fastify" {
  interface FastifyRequest {
    principal?: Principal;
  }
}

const privilegedRoles: Role[] = ["owner", "operator"];

function runEvent(type: "approved" | "cancelled", detail: string) {
  return { at: new Date().toISOString(), type, detail } as const;
}

function requirePrincipal(request: FastifyRequest): Principal {
  if (!request.principal) throw new Error("Unauthenticated request");
  return request.principal;
}

function requireRole(principal: Principal, roles: Role[]): void {
  if (!roles.includes(principal.role)) throw new Error("Insufficient role");
}

export function createApp(store = new InMemoryDurableStore({ tenantId: "demo-tenant", apiKey: "replace-with-a-long-local-key" })): FastifyInstance {
  const app = Fastify({ logger: false });

  app.addHook("preHandler", async (request, reply) => {
    if (request.url === "/healthz") return;
    const tenantId = request.headers["x-tenant-id"];
    const apiKey = request.headers["x-api-key"];
    const tenant = typeof tenantId === "string" ? tenantId : undefined;
    const key = typeof apiKey === "string" ? apiKey : undefined;
    const principal = tenant ? store.authenticate(tenant, key) : undefined;
    if (!principal) return reply.code(401).send({ error: "unauthenticated" });
    request.principal = principal;
  });

  app.get("/healthz", async () => ({ status: "ok" }));

  app.post("/v1/workflows", async (request, reply) => {
    const principal = requirePrincipal(request);
    requireRole(principal, privilegedRoles);
    const parsed = workflowDefinitionSchema.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ error: "invalid_workflow", details: parsed.error.issues });
    const workflow = store.createWorkflow(principal, parsed.data);
    return reply.code(201).send({ workflow });
  });

  app.post("/v1/runs", async (request, reply) => {
    const principal = requirePrincipal(request);
    requireRole(principal, privilegedRoles);
    const key = request.headers["idempotency-key"];
    if (typeof key !== "string" || key.length < 16 || key.length > 128) {
      return reply.code(400).send({ error: "idempotency_key_required" });
    }
    const parsed = createRunSchema.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ error: "invalid_run", details: parsed.error.issues });
    try {
      const { providerCredential: _providerCredential, ...persisted } = parsed.data;
      const result = store.createRun(principal, persisted, key);
      return reply.code(result.replayed ? 200 : 201).send({ run: result.run, replayed: result.replayed });
    } catch (error) {
      return reply.code(404).send({ error: error instanceof Error ? error.message : "run_creation_failed" });
    }
  });

  app.get("/v1/runs/:runId", async (request, reply) => {
    const principal = requirePrincipal(request);
    const run = store.readRun(principal, (request.params as { runId: string }).runId);
    return run ? reply.send({ run }) : reply.code(404).send({ error: "run_not_found" });
  });

  app.post("/v1/runs/:runId/approve", async (request, reply) => {
    const principal = requirePrincipal(request);
    requireRole(principal, privilegedRoles);
    const runId = (request.params as { runId: string }).runId;
    const run = store.readRun(principal, runId);
    if (!run) return reply.code(404).send({ error: "run_not_found" });
    if (run.state !== "awaiting_approval") return reply.code(409).send({ error: "run_not_awaiting_approval" });
    const updated = store.transition(principal, runId, "queued", runEvent("approved", "Approval recorded; run re-queued"));
    return reply.send({ run: updated });
  });

  app.post("/v1/runs/:runId/cancel", async (request, reply) => {
    const principal = requirePrincipal(request);
    requireRole(principal, privilegedRoles);
    const runId = (request.params as { runId: string }).runId;
    const run = store.readRun(principal, runId);
    if (!run) return reply.code(404).send({ error: "run_not_found" });
    const parsed = runStateSchema.safeParse(run.state);
    if (!parsed.success || ["succeeded", "failed", "cancelled", "uncertain"].includes(parsed.data)) {
      return reply.code(409).send({ error: "terminal_run" });
    }
    const updated = store.transition(principal, runId, "cancelled", runEvent("cancelled", "Cancellation requested"));
    return reply.send({ run: updated });
  });

  app.get("/v1/audit", async (request, reply) => {
    const principal = requirePrincipal(request);
    requireRole(principal, ["owner"]);
    return reply.send({ records: store.listAudit(principal) });
  });

  return app;
}

