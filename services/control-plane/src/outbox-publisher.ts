import {
  connect,
  JSONCodec,
  type JetStreamClient,
  type NatsConnection,
} from "nats";
import { PostgresDurableStore } from "./postgres-store.js";
import {
  outboxBacklog,
  outboxPublished,
  outboxPublishFailures,
} from "./metrics.js";

type PublisherStore = Pick<
  PostgresDurableStore,
  "claimPendingOutbox" | "markOutboxPublished"
> & { outboxBacklog?: () => Promise<number> };

export class OutboxPublisher {
  private readonly codec = JSONCodec<Record<string, unknown>>();

  constructor(
    private readonly store: PublisherStore,
    private readonly jetstream: Pick<JetStreamClient, "publish">,
  ) {}

  async publishPending(limit = 50): Promise<number> {
    const messages = await this.store.claimPendingOutbox(limit);
    const delivered: number[] = [];
    for (const message of messages) {
      try {
        await this.jetstream.publish(
          message.subject,
          this.codec.encode({ ...message.payload, tenantId: message.tenantId }),
        );
      } catch (error) {
        outboxPublishFailures.inc();
        throw error;
      }
      delivered.push(message.id);
    }
    await this.store.markOutboxPublished(delivered);
    outboxPublished.inc(delivered.length);
    if (this.store.outboxBacklog)
      outboxBacklog.set(await this.store.outboxBacklog());
    return delivered.length;
  }
}

export async function runOutboxPublisher(options: {
  databaseUrl: string;
  natsUrl: string;
  intervalMs?: number;
  signal?: AbortSignal;
}): Promise<void> {
  const store = new PostgresDurableStore(options.databaseUrl);
  const connection: NatsConnection = await connect({
    servers: options.natsUrl,
    name: "dar-outbox-publisher",
  });
  const publisher = new OutboxPublisher(store, connection.jetstream());
  const interval = options.intervalMs ?? 250;
  try {
    while (!options.signal?.aborted) {
      await publisher.publishPending();
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, interval);
        options.signal?.addEventListener(
          "abort",
          () => {
            clearTimeout(timer);
            resolve();
          },
          { once: true },
        );
      });
    }
  } finally {
    await connection.drain();
    await store.close();
  }
}
