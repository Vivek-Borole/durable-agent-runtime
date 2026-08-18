# Durable Agent Runtime

A developer-facing runtime for safe, recoverable, multi-tenant AI workflows.
It is intentionally constrained: every run is durable, tools are registered by
the server, side effects require explicit approval, and retries are idempotent.

## Status

The first vertical slice implements versioned workflow and run contracts, tenant
scoping, idempotency, run state transitions, approval/cancellation endpoints,
and an auditable in-memory development store. PostgreSQL, JetStream, Redis,
OpenTelemetry, and the Go worker are defined in the Compose topology and are
the next persistence/runtime milestone.

## Local prerequisites

- Node.js 22+
- pnpm 11+
- Go 1.26+
- Docker Desktop for Apple Silicon (required for Compose integration tests)

```bash
pnpm install
pnpm test:ts
pnpm typecheck
go test ./...
pnpm dev
```

## Safety model

- At-least-once task delivery; idempotency protects committed external effects.
- No shell execution, arbitrary browsing, unrestricted egress, payments, or
  autonomous messaging in v1.
- Model provider keys are supplied per run, used only in process memory, and
  excluded from persistence, audit records, and telemetry.
- All records are tenant-scoped. Cross-tenant reads or mutations fail closed.

Read [the architecture](docs/architecture.md), [threat model](docs/threat-model.md),
and [API contract](contracts/openapi.yaml) before extending the runtime.

