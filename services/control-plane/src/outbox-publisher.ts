import { connect, JSONCodec, type JetStreamClient, type NatsConnection } from "nats";
import { PostgresDurableStore } from "./postgres-store.js";

export class OutboxPublisher {
  private readonly codec = JSONCodec<Record<string, unknown>>();

  constructor(
    private readonly store: Pick<PostgresDurableStore, "claimPendingOutbox" | "markOutboxPublished">,
    private readonly jetstream: Pick<JetStreamClient, "publish">
  ) {}

  async publishPending(limit = 50): Promise<number> {
    const messages = await this.store.claimPendingOutbox(limit);
    const delivered: number[] = [];
    for (const message of messages) {
      await this.jetstream.publish(message.subject, this.codec.encode({ ...message.payload, tenantId: message.tenantId }));
      delivered.push(message.id);
    }
    await this.store.markOutboxPublished(delivered);
    return delivered.length;
  }
}

export async function runOutboxPublisher(options: { databaseUrl: string; natsUrl: string; intervalMs?: number }): Promise<never> {
  const store = new PostgresDurableStore(options.databaseUrl);
  const connection: NatsConnection = await connect({ servers: options.natsUrl, name: "dar-outbox-publisher" });
  const publisher = new OutboxPublisher(store, connection.jetstream());
  const interval = options.intervalMs ?? 250;
  try {
    for (;;) {
      await publisher.publishPending();
      await new Promise((resolve) => setTimeout(resolve, interval));
    }
  } finally {
    await connection.drain();
    await store.close();
  }
}
