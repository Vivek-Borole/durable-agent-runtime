import { describe, expect, it } from "vitest";
import { CredentialBroker } from "../src/credential-broker.js";

describe("credential broker", () => {
  it("returns a credential once and never exposes its value in the handle", () => {
    const broker = new CredentialBroker();
    const handle = broker.issue("provider-secret-never-persisted");
    expect(handle).not.toContain("provider-secret-never-persisted");
    expect(broker.consume(handle)).toBe("provider-secret-never-persisted");
    expect(broker.consume(handle)).toBeUndefined();
  });

  it("expires credentials from process memory", async () => {
    const broker = new CredentialBroker(1);
    const handle = broker.issue("expired-secret");
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(broker.consume(handle)).toBeUndefined();
  });
});
