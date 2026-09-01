import { describe, expect, it } from "vitest";
import { assertTransition, workflowDefinitionSchema } from "../src/index.js";

describe("workflow contracts", () => {
  it("requires approval immediately before a side-effecting tool", () => {
    const parsed = workflowDefinitionSchema.safeParse({
      name: "research-demo",
      version: "v1",
      budgetCents: 100,
      allowedHosts: ["example.test"],
      steps: [{ kind: "tool", tool: "mock_ticket_write", sideEffect: true }],
    });
    expect(parsed.success).toBe(false);
  });

  it("rejects terminal state mutations", () => {
    expect(() => assertTransition("succeeded", "queued")).toThrow(
      "Invalid run transition",
    );
    expect(() => assertTransition("queued", "leased")).not.toThrow();
  });

  it("interprets v0.1 definitions as schema 1 and rejects future schemas", () => {
    const legacy = workflowDefinitionSchema.parse({
      name: "legacy-demo",
      version: "v1",
      budgetCents: 1,
      steps: [{ kind: "tool", tool: "mock_data_read", sideEffect: false }],
    });
    expect(legacy.schemaVersion).toBe("1");
    expect(
      workflowDefinitionSchema.safeParse({ ...legacy, schemaVersion: "2" })
        .success,
    ).toBe(false);
  });
});
