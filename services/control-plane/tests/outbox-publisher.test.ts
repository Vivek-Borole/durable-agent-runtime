import { describe, expect, it } from "vitest";
import { OutboxPublisher } from "../src/outbox-publisher.js";

describe("outbox publisher", () => {
  it("marks only messages acknowledged by JetStream as published", async () => {
    const marked: number[][] = [];
    const sent: string[] = [];
    const store = {
      claimPendingOutbox: async () => [
        { id: 1, tenantId: "tenant-a", subject: "dar.run.queued", payload: { runId: "run-a" } },
        { id: 2, tenantId: "tenant-b", subject: "dar.run.queued", payload: { runId: "run-b" } }
      ],
      markOutboxPublished: async (ids: number[]) => {
        marked.push(ids);
      }
    };
    const jetstream = {
      publish: async (subject: string, payload: Uint8Array) => {
        sent.push(`${subject}:${new TextDecoder().decode(payload)}`);
        return { stream: "DAR", seq: sent.length, duplicate: false };
      }
    };
    const publisher = new OutboxPublisher(store, jetstream);

    await expect(publisher.publishPending()).resolves.toBe(2);
    expect(sent).toHaveLength(2);
    expect(sent[0]).toContain('"tenantId":"tenant-a"');
    expect(marked).toEqual([[1, 2]]);
  });

  it("does not mark an interrupted delivery as published", async () => {
    const marked: number[][] = [];
    const store = {
      claimPendingOutbox: async () => [
        { id: 1, tenantId: "tenant-a", subject: "dar.run.queued", payload: { runId: "run-a" } },
        { id: 2, tenantId: "tenant-a", subject: "dar.run.queued", payload: { runId: "run-b" } }
      ],
      markOutboxPublished: async (ids: number[]) => {
        marked.push(ids);
      }
    };
    let delivery = 0;
    const jetstream = {
      publish: async () => {
        delivery += 1;
        if (delivery === 2) throw new Error("simulated NATS interruption");
        return { stream: "DAR", seq: delivery, duplicate: false };
      }
    };
    const publisher = new OutboxPublisher(store, jetstream);

    await expect(publisher.publishPending()).rejects.toThrow("simulated NATS interruption");
    expect(marked).toEqual([]);
  });
});
