import { Counter, Histogram, Registry, collectDefaultMetrics } from "prom-client";

export const metrics = new Registry();
collectDefaultMetrics({ register: metrics, prefix: "dar_control_plane_" });

export const httpRequests = new Counter({
  name: "dar_http_requests_total",
  help: "Control-plane HTTP responses by route and status only.",
  labelNames: ["route", "status"] as const,
  registers: [metrics]
});

export const httpDuration = new Histogram({
  name: "dar_http_request_duration_seconds",
  help: "Control-plane HTTP duration without tenant or request-content labels.",
  labelNames: ["route", "status"] as const,
  buckets: [0.005, 0.01, 0.05, 0.1, 0.5, 1, 5],
  registers: [metrics]
});
