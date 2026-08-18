import { describe, expect, it } from "vitest";
import { redactOperationalText, safeErrorClass } from "../src/redaction.js";

describe("operational redaction", () => {
  it("removes seeded credentials and email addresses", () => {
    const raw = "Bearer sk-test-123 token=abc123 email=person@example.test";
    const safe = redactOperationalText(raw);
    expect(safe).not.toContain("sk-test-123");
    expect(safe).not.toContain("abc123");
    expect(safe).not.toContain("person@example.test");
  });

  it("never returns arbitrary exception text as an error class", () => {
    expect(safeErrorClass(new Error("password=do-not-log"))).toBe("runtime_operation_failed");
  });
});
