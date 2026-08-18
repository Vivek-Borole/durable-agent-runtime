import { shutdownTelemetry, startTelemetry } from "./telemetry.js";
startTelemetry();
const { createApp } = await import("./app.js");
import { CredentialBroker } from "./credential-broker.js";
import { PostgresDurableStore } from "./postgres-store.js";
import { InMemoryDurableStore } from "./store.js";

const tenantId = process.env.DAR_BOOTSTRAP_TENANT ?? "demo-tenant";
const apiKey = process.env.DAR_BOOTSTRAP_API_KEY ?? "replace-with-a-long-local-key";
const store = process.env.POSTGRES_URL
  ? new PostgresDurableStore(process.env.POSTGRES_URL)
  : new InMemoryDurableStore({ tenantId, apiKey });

if (store instanceof PostgresDurableStore) {
  await store.bootstrap({ tenantSlug: tenantId, apiKey });
}

const app = createApp(store, new CredentialBroker());

await app.listen({ host: "127.0.0.1", port: Number(process.env.PORT ?? 3001) });

let closing = false;
async function closeGracefully(): Promise<void> {
  if (closing) return;
  closing = true;
  await app.close();
  await shutdownTelemetry();
}

process.once("SIGINT", () => void closeGracefully());
process.once("SIGTERM", () => void closeGracefully());
