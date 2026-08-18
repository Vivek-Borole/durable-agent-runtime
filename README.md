# Durable Agent Runtime

A developer-facing runtime for safe, recoverable, multi-tenant AI workflows.
It is intentionally constrained: every run is durable, tools are registered by
the server, side effects require explicit approval, and retries are idempotent.

## Status

The current vertical slice includes versioned workflow/run contracts, tenant
scoping, API-key authentication, PostgreSQL persistence with forced RLS,
tenant-scoped idempotency, run state transitions, immutable workflows, a
transactional outbox, and a tested JetStream publisher. The Go worker effect
ledger has race-tested duplicate-commit protection; its full workflow executor
and benchmark gates are still in progress.

## Local prerequisites

- Node.js 22+
- pnpm 11+
- Go 1.26+
- Docker Desktop **or Colima** for Apple Silicon (required for Compose tests)

```bash
pnpm install
pnpm compose:up
pnpm compose:migrate
POSTGRES_URL=postgres://dar:dar@127.0.0.1:5432/dar NATS_URL=nats://127.0.0.1:4222 pnpm test
pnpm typecheck
POSTGRES_URL=postgres://dar:dar@127.0.0.1:5432/dar DAR_BOOTSTRAP_TENANT=demo-tenant DAR_BOOTSTRAP_API_KEY=replace-with-a-long-local-key pnpm dev
```

To publish committed runs locally, use a separate terminal with the system
database credential (never a tenant API key):

```bash
DAR_WORKER_POSTGRES_URL=postgres://dar:dar@127.0.0.1:5432/dar NATS_URL=nats://127.0.0.1:4222 pnpm --filter @dar/control-plane outbox
```

## Safety model

- At-least-once task delivery; idempotency protects committed external effects.
- No shell execution, arbitrary browsing, unrestricted egress, payments, or
  autonomous messaging in v1.
- Model provider keys are supplied per run, used only in process memory, and
  excluded from persistence, audit records, and telemetry.
- All records are tenant-scoped. Cross-tenant reads or mutations fail closed.

## Verification status

The current tests include in-memory API checks, PostgreSQL RLS/idempotency
integration, real JetStream outbox delivery, and a 1,000-concurrent-delivery
effect-ledger race test. The 100,000-attempt fault run, worker restart recovery,
1,000-active-workflow benchmark, full UI, and public release evidence remain
release gates, not verified claims.

Read [the architecture](docs/architecture.md), [threat model](docs/threat-model.md),
and [API contract](contracts/openapi.yaml) before extending the runtime.
