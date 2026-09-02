import { runOutboxPublisher } from "./outbox-publisher.js";
import { createServer } from "node:http";
import { metrics } from "./metrics.js";

const databaseUrl = process.env.DAR_WORKER_POSTGRES_URL;
const natsUrl = process.env.NATS_URL ?? "nats://127.0.0.1:4222";

if (!databaseUrl)
  throw new Error(
    "DAR_WORKER_POSTGRES_URL is required for the outbox publisher",
  );

const abort = new AbortController();
process.once("SIGINT", () => abort.abort());
process.once("SIGTERM", () => abort.abort());
const metricsServer = createServer(async (request, response) => {
  if (request.url === "/metrics") {
    response.writeHead(200, { "content-type": metrics.contentType });
    response.end(await metrics.metrics());
  } else if (
    request.url === "/health/live" ||
    request.url === "/health/ready"
  ) {
    response.writeHead(200).end("ok");
  } else {
    response.writeHead(404).end();
  }
});
metricsServer.listen(
  Number(process.env.DAR_PUBLISHER_HTTP_PORT ?? 9092),
  process.env.HOST ?? "0.0.0.0",
);
await runOutboxPublisher({ databaseUrl, natsUrl, signal: abort.signal });
await new Promise<void>((resolve, reject) =>
  metricsServer.close((error) => (error ? reject(error) : resolve())),
);
