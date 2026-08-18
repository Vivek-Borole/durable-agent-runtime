import cors from "@fastify/cors";
import Fastify, { type FastifyInstance, type FastifyRequest } from "fastify";
import { createRunSchema, runStateSchema, workflowDefinitionSchema } from "@dar/contracts";
import { InMemoryDurableStore, type Principal, type Role, type RuntimeStore } from "./store.js";
import { CredentialBroker } from "./credential-broker.js";
import { httpDuration, httpRequests, metrics } from "./metrics.js";
import { SpanStatusCode, type Span } from "@opentelemetry/api";
import { requestTracer } from "./telemetry.js";

declare module "fastify" {
  interface FastifyRequest {
    principal?: Principal;
    telemetrySpan?: Span;
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

export function createApp(store: RuntimeStore = new InMemoryDurableStore({ tenantId: "demo-tenant", apiKey: "replace-with-a-long-local-key" }), broker = new CredentialBroker()): FastifyInstance {
  const app = Fastify({ logger: false });
  void app.register(cors, {
    origin: (origin, callback) => {
      const allowed = (process.env.CONSOLE_ORIGIN ?? "http://127.0.0.1:5173").split(",");
      callback(null, !origin || allowed.includes(origin));
    },
    credentials: false
  });

  app.addHook("preHandler", async (request, reply) => {
    if (request.url === "/healthz" || request.url === "/metrics" || request.url.startsWith("/internal/")) return;
    const tenantId = request.headers["x-tenant-id"];
    const apiKey = request.headers["x-api-key"];
    const tenant = typeof tenantId === "string" ? tenantId : undefined;
    const key = typeof apiKey === "string" ? apiKey : undefined;
    const principal = tenant ? await store.authenticate(tenant, key) : undefined;
    if (!principal) return reply.code(401).send({ error: "unauthenticated" });
    request.principal = principal;
  });

  app.addHook("onRequest", async (request) => {
    request.telemetrySpan = requestTracer().startSpan("http.request", {
      attributes: { "http.request.method": request.method, "url.path": request.routeOptions.url ?? request.url.split("?")[0] }
    });
  });

  app.addHook("onResponse", async (request, reply) => {
    const route = request.routeOptions.url ?? "unmatched";
    const status = String(reply.statusCode);
    httpRequests.inc({ route, status });
    httpDuration.observe({ route, status }, reply.elapsedTime / 1_000);
    request.telemetrySpan?.setAttribute("http.response.status_code", reply.statusCode);
    request.telemetrySpan?.setStatus({ code: reply.statusCode >= 500 ? SpanStatusCode.ERROR : SpanStatusCode.UNSET });
    request.telemetrySpan?.end();
  });

  app.get("/healthz", async () => ({ status: "ok" }));
  app.get("/metrics", async (_request, reply) => reply.type(metrics.contentType).send(await metrics.metrics()));

  app.post("/v1/workflows", async (request, reply) => {
    const principal = requirePrincipal(request);
    requireRole(principal, privilegedRoles);
    const parsed = workflowDefinitionSchema.safeParse(request.body);
    if (!parsed.success) return reply.code(400).send({ error: "invalid_workflow", details: parsed.error.issues });
    const workflow = await store.createWorkflow(principal, parsed.data);
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
      const { providerCredential, ...persisted } = parsed.data;
      const providerCredentialHandle = providerCredential ? broker.issue(providerCredential) : undefined;
      const result = await store.createRun(principal, { ...persisted, providerCredentialHandle }, key);
      if (result.replayed && providerCredentialHandle) broker.discard(providerCredentialHandle);
      return reply.code(result.replayed ? 200 : 201).send({ run: result.run, replayed: result.replayed });
    } catch (error) {
      const message = error instanceof Error ? error.message : "run_creation_failed";
      return reply.code(message.includes("quota") ? 429 : message === "Workflow not found" ? 404 : 500).send({ error: message });
    }
  });

  app.post("/internal/credentials/:handle/consume", async (request, reply) => {
    const internalToken = process.env.DAR_INTERNAL_TOKEN;
    if (!internalToken || request.headers["x-runtime-internal-token"] !== internalToken) return reply.code(401).send({ error: "internal_unauthenticated" });
    const credential = broker.consume((request.params as { handle: string }).handle);
    return credential ? reply.send({ credential }) : reply.code(404).send({ error: "provider_credential_unavailable" });
  });

  app.get("/v1/runs/:runId", async (request, reply) => {
    const principal = requirePrincipal(request);
    const run = await store.readRun(principal, (request.params as { runId: string }).runId);
    return run ? reply.send({ run }) : reply.code(404).send({ error: "run_not_found" });
  });

  app.post("/v1/runs/:runId/approve", async (request, reply) => {
    const principal = requirePrincipal(request);
    requireRole(principal, privilegedRoles);
    const runId = (request.params as { runId: string }).runId;
    const run = await store.readRun(principal, runId);
    if (!run) return reply.code(404).send({ error: "run_not_found" });
    if (run.state !== "awaiting_approval") return reply.code(409).send({ error: "run_not_awaiting_approval" });
    const updated = await store.transition(principal, runId, "queued", runEvent("approved", "Approval recorded; run re-queued"));
    return reply.send({ run: updated });
  });

  app.post("/v1/runs/:runId/cancel", async (request, reply) => {
    const principal = requirePrincipal(request);
    requireRole(principal, privilegedRoles);
    const runId = (request.params as { runId: string }).runId;
    const run = await store.readRun(principal, runId);
    if (!run) return reply.code(404).send({ error: "run_not_found" });
    const parsed = runStateSchema.safeParse(run.state);
    if (!parsed.success || ["succeeded", "failed", "cancelled", "uncertain"].includes(parsed.data)) {
      return reply.code(409).send({ error: "terminal_run" });
    }
    const updated = await store.transition(principal, runId, "cancelled", runEvent("cancelled", "Cancellation requested"));
    return reply.send({ run: updated });
  });

  app.get("/v1/audit", async (request, reply) => {
    const principal = requirePrincipal(request);
    requireRole(principal, ["owner"]);
    return reply.send({ records: await store.listAudit(principal) });
  });

  return app;
}
