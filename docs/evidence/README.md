# Synthetic benchmark evidence

These reports are local, synthetic evidence, not production capacity claims.
They were generated on the documented Apple Silicon development machine using
Docker Desktop, PostgreSQL, and NATS JetStream from `compose.yaml`.

Reproduce the scheduling report after starting Compose and applying migrations:

```bash
go build -o /tmp/durable-agent-runtime-worker ./services/worker
DAR_BENCHMARK_POSTGRES_URL=postgres://dar:dar@127.0.0.1:5432/dar \
DAR_WORKER_POSTGRES_URL=postgres://dar_worker:dar-worker-local-only@127.0.0.1:5432/dar \
DAR_WORKER_CONCURRENCY=32 \
go run ./cmd/scheduling-benchmark \
  -active=1000 -warmup=1000 -backlog=10000 -workers=4 \
  -output=docs/evidence/scheduling-benchmark-report.json
```

The benchmark isolates itself in a unique JetStream stream and consumer. It
keeps 1,000 synthetic workflow records in `awaiting_approval`, warms the worker
with 1,000 mock reads, then measures queued-to-started latency for 10,000 mock
read runs. It passes only when every measured run succeeds and p95 is at most
500 ms.

The committed `scheduling-benchmark-run.log` and `effect-fault-run.log` are
the raw stdout from the latest full synthetic runs. `clean-compose-smoke.log`
records the disposable-stack startup, migration replay, service health, and
evidence-schema verification.

Reproduce the idempotency fault report:

```bash
DAR_BENCHMARK_POSTGRES_URL=postgres://dar:dar@127.0.0.1:5432/dar \
DAR_WORKER_POSTGRES_URL=postgres://dar_worker:dar-worker-local-only@127.0.0.1:5432/dar \
go run ./cmd/effect-fault -attempts=100000 -workers=100 \
  -output=docs/evidence/effect-fault-report.json
```

That workload concurrently attempts one stable mock-ticket effect key 100,000
times. The PostgreSQL unique constraint is the durable commit boundary; passing
requires exactly one committed effect and no database-operation errors.

Reproduce the lease-recovery artifact:

```bash
DAR_BENCHMARK_POSTGRES_URL=postgres://dar:dar@127.0.0.1:5432/dar \
go run ./cmd/lease-recovery -output=docs/evidence/lease-recovery-report.json
pnpm evidence:check
```

This is a deterministic database-level simulation of a worker disappearing
after acquiring a lease, followed by a second worker recovering the expired
lease and committing exactly one mock effect. It is intentionally labelled as
a simulation; the Compose integration suite remains the place for a real
process-termination test.

Reproduce the real worker-process interruption report:

```bash
go build -o /tmp/durable-agent-runtime-worker ./services/worker
DAR_BENCHMARK_POSTGRES_URL=postgres://dar:dar@127.0.0.1:5432/dar \
DAR_WORKER_POSTGRES_URL=postgres://dar_worker:dar-worker-local-only@127.0.0.1:5432/dar \
DAR_WORKER_BINARY=/tmp/durable-agent-runtime-worker \
go run ./cmd/process-recovery
```

The fixture kills a local worker during a synthetic mock-step database write,
waits for its standard 30-second lease to expire, sends a duplicate delivery,
and verifies that the replacement worker succeeds without a committed effect.

`otel-control-plane-trace.log` records the local collector's synthetic request
spans. It contains only service name, method, route, and status—not headers,
tenant identifiers, prompts, provider credentials, or API keys.

## Screenshots

`screenshots/console-awaiting-approval.png` and
`screenshots/console-succeeded.png` are captured from the local console using
only the `synthetic-demo` tenant and a mock ticket tool. The API key field is
masked, no provider credential was used, and no external system was contacted.

`screenshots/grafana-runtime-overview.png` is the local provisioned Grafana
dashboard. Its HTTP panels are populated only by synthetic local requests.

`synthetic-demo.gif` is an animated local walkthrough of the captured console
states: approval pause, then resumed successful completion. Regenerate it with
`go run ./cmd/demo-gif`. It contains no real credentials, tenant data, provider
response, or external side effect.
