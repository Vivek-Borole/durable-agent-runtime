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

Reproduce the idempotency fault report:

```bash
DAR_WORKER_POSTGRES_URL=postgres://dar:dar@127.0.0.1:5432/dar \
go run ./cmd/effect-fault -attempts=100000 -workers=100 \
  -output=docs/evidence/effect-fault-report.json
```

That workload concurrently attempts one stable mock-ticket effect key 100,000
times. The PostgreSQL unique constraint is the durable commit boundary; passing
requires exactly one committed effect and no database-operation errors.
