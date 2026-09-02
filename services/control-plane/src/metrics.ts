import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from "prom-client";

export const metrics = new Registry();
collectDefaultMetrics({ register: metrics, prefix: "dar_control_plane_" });

export const httpRequests = new Counter({
  name: "dar_http_requests_total",
  help: "Control-plane HTTP responses by route and status only.",
  labelNames: ["route", "status"] as const,
  registers: [metrics],
});

export const httpDuration = new Histogram({
  name: "dar_http_request_duration_seconds",
  help: "Control-plane HTTP duration without tenant or request-content labels.",
  labelNames: ["route", "status"] as const,
  buckets: [0.005, 0.01, 0.05, 0.1, 0.5, 1, 5],
  registers: [metrics],
});

export const runCreations = new Counter({
  name: "dar_run_creations_total",
  help: "Runs accepted by the control plane, split only by replay status.",
  labelNames: ["replayed"] as const,
  registers: [metrics],
});

export const quotaRejections = new Counter({
  name: "dar_quota_rejections_total",
  help: "Database-authoritative run quota rejections.",
  registers: [metrics],
});

export const approvalDecisions = new Counter({
  name: "dar_approval_decisions_total",
  help: "Approved workflow pauses.",
  registers: [metrics],
});

export const cancellations = new Counter({
  name: "dar_cancellations_total",
  help: "Accepted run cancellation requests.",
  registers: [metrics],
});

export const outboxPublished = new Counter({
  name: "dar_outbox_published_total",
  help: "Outbox messages acknowledged by JetStream.",
  registers: [metrics],
});

export const outboxPublishFailures = new Counter({
  name: "dar_outbox_publish_failures_total",
  help: "Safe-classified outbox publication failures.",
  registers: [metrics],
});

export const outboxBacklog = new Gauge({
  name: "dar_outbox_backlog",
  help: "Durable outbox rows awaiting publication.",
  registers: [metrics],
});
