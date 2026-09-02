import { z } from "zod";

export const runStates = [
  "queued",
  "leased",
  "running",
  "awaiting_approval",
  "succeeded",
  "failed",
  "cancelled",
  "uncertain",
] as const;
export const runStateSchema = z.enum(runStates);
export type RunState = z.infer<typeof runStateSchema>;

export const workflowStepSchema = z.discriminatedUnion("kind", [
  z.object({
    kind: z.literal("model"),
    provider: z.enum(["mock", "openai_compatible"]),
    promptTemplate: z.string().min(1).max(20_000),
  }),
  z.object({
    kind: z.literal("transform"),
    operation: z.enum(["extract_json", "template"]),
    input: z.string().min(1).max(4_000),
  }),
  z.object({
    kind: z.literal("tool"),
    tool: z.enum([
      "allowlisted_http_fetch",
      "mock_ticket_write",
      "mock_data_read",
    ]),
    sideEffect: z.boolean().default(false),
  }),
  z.object({ kind: z.literal("approval"), reason: z.string().min(1).max(500) }),
]);

export const workflowDefinitionSchema = z
  .object({
    schemaVersion: z.enum(["1"]).default("1"),
    name: z
      .string()
      .min(3)
      .max(80)
      .regex(/^[a-z0-9-]+$/),
    version: z.string().regex(/^v\d+$/),
    steps: z.array(workflowStepSchema).min(1).max(20),
    budgetCents: z.number().int().min(0).max(100_000),
    allowedHosts: z.array(z.string().min(1)).max(20).default([]),
  })
  .superRefine((workflow, context) => {
    workflow.steps.forEach((step, index) => {
      if (
        step.kind === "tool" &&
        step.sideEffect &&
        workflow.steps[index - 1]?.kind !== "approval"
      ) {
        context.addIssue({
          code: "custom",
          message:
            "A side-effecting tool must immediately follow an approval step",
          path: ["steps", index],
        });
      }
    });
  });
export type WorkflowDefinition = z.infer<typeof workflowDefinitionSchema>;

export const createRunSchema = z.object({
  workflowId: z.uuid(),
  input: z.record(z.string(), z.unknown()).default({}),
  providerCredential: z.string().min(20).max(512).optional(),
});
export type CreateRun = z.infer<typeof createRunSchema>;

export const runEventSchema = z.object({
  at: z.string().datetime(),
  type: z.enum([
    "created",
    "leased",
    "started",
    "approval_requested",
    "approved",
    "cancelled",
    "succeeded",
    "failed",
    "uncertain",
  ]),
  detail: z.string().max(500),
  traceId: z.string().optional(),
});
export type RunEvent = z.infer<typeof runEventSchema>;

export const allowedTransitions: Record<RunState, readonly RunState[]> = {
  queued: ["leased", "cancelled"],
  leased: ["running", "queued", "failed", "uncertain", "cancelled"],
  running: [
    "awaiting_approval",
    "succeeded",
    "failed",
    "uncertain",
    "cancelled",
  ],
  awaiting_approval: ["queued", "cancelled"],
  succeeded: [],
  failed: [],
  cancelled: [],
  uncertain: [],
};

export function assertTransition(from: RunState, to: RunState): void {
  if (!allowedTransitions[from].includes(to)) {
    throw new Error(`Invalid run transition: ${from} -> ${to}`);
  }
}
