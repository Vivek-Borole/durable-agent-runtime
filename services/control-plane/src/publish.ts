import { runOutboxPublisher } from "./outbox-publisher.js";

const databaseUrl = process.env.DAR_WORKER_POSTGRES_URL;
const natsUrl = process.env.NATS_URL ?? "nats://127.0.0.1:4222";

if (!databaseUrl) throw new Error("DAR_WORKER_POSTGRES_URL is required for the outbox publisher");

await runOutboxPublisher({ databaseUrl, natsUrl });
