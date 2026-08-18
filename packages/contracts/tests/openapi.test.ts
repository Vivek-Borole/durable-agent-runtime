import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const contract = readFileSync(resolve(import.meta.dirname, "../../../contracts/openapi.yaml"), "utf8");

describe("OpenAPI v1 compatibility surface", () => {
  it("retains every supported public route and idempotency requirement", () => {
    for (const route of ["/healthz:", "/metrics:", "/v1/workflows:", "/v1/runs:", "/v1/runs/{runId}:", "/v1/runs/{runId}/approve:", "/v1/runs/{runId}/cancel:", "/v1/audit:"]) {
      expect(contract).toContain(route);
    }
    expect(contract).toContain("Idempotency-Key");
    expect(contract).toContain("minLength: 16");
    expect(contract).toContain("maxLength: 128");
  });

  it("documents credential non-persistence and every legal run state", () => {
    expect(contract).toContain("writeOnly: true");
    expect(contract).toContain("Never persisted or returned.");
    for (const state of ["queued", "leased", "running", "awaiting_approval", "succeeded", "failed", "cancelled", "uncertain"]) expect(contract).toContain(state);
  });
});
