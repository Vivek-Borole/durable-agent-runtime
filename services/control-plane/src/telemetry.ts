import { trace } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
  BatchSpanProcessor,
  NodeTracerProvider,
} from "@opentelemetry/sdk-trace-node";

let shutdown: (() => Promise<void>) | undefined;

export function startTelemetry(): void {
  const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
  if (!endpoint || shutdown) return;
  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({
      "service.name": "durable-agent-runtime-control-plane",
    }),
    spanProcessors: [
      new BatchSpanProcessor(
        new OTLPTraceExporter({
          url: `${endpoint.replace(/\/$/, "")}/v1/traces`,
        }),
      ),
    ],
  });
  provider.register();
  shutdown = () => provider.shutdown();
}

export function requestTracer() {
  return trace.getTracer("durable-agent-runtime.control-plane");
}

export async function shutdownTelemetry(): Promise<void> {
  await shutdown?.();
}
