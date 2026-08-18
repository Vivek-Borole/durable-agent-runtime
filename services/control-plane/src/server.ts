import { createApp } from "./app.js";
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

const app = createApp(store);

await app.listen({ host: "127.0.0.1", port: Number(process.env.PORT ?? 3001) });
