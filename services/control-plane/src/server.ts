import { createApp } from "./app.js";
import { InMemoryDurableStore } from "./store.js";

const tenantId = process.env.DAR_BOOTSTRAP_TENANT ?? "demo-tenant";
const apiKey = process.env.DAR_BOOTSTRAP_API_KEY ?? "replace-with-a-long-local-key";
const app = createApp(new InMemoryDurableStore({ tenantId, apiKey }));

await app.listen({ host: "127.0.0.1", port: Number(process.env.PORT ?? 3001) });

