# Durable Agent Runtime

A developer-facing runtime for safe, recoverable, multi-tenant AI workflows.
It is intentionally constrained: every run is durable, tools are registered by
the server, side effects require explicit approval, and retries are idempotent.

## Status

The current vertical slice includes versioned workflow/run contracts, tenant
scoping, API-key authentication, PostgreSQL persistence with forced RLS,
tenant-scoped idempotency, run state transitions, immutable workflows, a
transactional outbox, a JetStream publisher, and a recoverable Go worker. The
worker supports only registered mock, transform, allowlisted-fetch, and approval
steps; it records idempotent mock-ticket effects at a PostgreSQL commit boundary.

## Local prerequisites

- Node.js 22+
- pnpm 11+
- Go 1.26+
- Docker Desktop **or Colima** for Apple Silicon (required for Compose tests)

```bash
pnpm install
pnpm compose:up
pnpm compose:migrate
POSTGRES_URL=postgres://dar_control:dar-control-local-only@127.0.0.1:5432/dar DAR_WORKER_POSTGRES_URL=postgres://dar_worker:dar-worker-local-only@127.0.0.1:5432/dar DAR_TEST_ADMIN_POSTGRES_URL=postgres://dar:dar@127.0.0.1:5432/dar NATS_URL=nats://127.0.0.1:4222 pnpm test
pnpm typecheck
POSTGRES_URL=postgres://dar_control:dar-control-local-only@127.0.0.1:5432/dar DAR_BOOTSTRAP_TENANT=demo-tenant DAR_BOOTSTRAP_API_KEY=replace-with-a-long-local-key pnpm dev
```

To publish committed runs locally, use a separate terminal with the system
database credential (never a tenant API key):

```bash
DAR_WORKER_POSTGRES_URL=postgres://dar_worker:dar-worker-local-only@127.0.0.1:5432/dar NATS_URL=nats://127.0.0.1:4222 pnpm --filter @dar/control-plane outbox
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
effect-ledger race test. The recorded synthetic evidence includes a
[100,000-attempt effect fault run](docs/evidence/effect-fault-report.json) and
a [1,000-resident / 10,000-backlog scheduling benchmark](docs/evidence/scheduling-benchmark-report.json),
an [expired-lease recovery simulation](docs/evidence/lease-recovery-report.json),
and [synthetic console screenshots](docs/evidence/screenshots/).
The evidence package also contains a real worker-process interruption recovery
run, exported OpenTelemetry trace output, a Grafana dashboard screenshot, a
clean-Compose smoke log, and an animated synthetic walkthrough. Remote CI,
GitHub Pages publication, a release tag, and the repository visibility change
remain release operations and are performed only after their checks pass.

Read [the architecture](docs/architecture.md), [threat model](docs/threat-model.md),
[API contract](contracts/openapi.yaml), [Compose quick start](docs/compose-quickstart.md),
and [API examples](docs/api-examples.md) before extending the runtime. The
published release also exposes a compact [evidence index](docs/index.html).
