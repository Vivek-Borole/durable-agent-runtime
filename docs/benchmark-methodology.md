# Benchmark methodology

Benchmarks are run only after the PostgreSQL, JetStream, Redis, and worker
integration milestone. Reports must record:

- Git commit, OS, CPU, RAM, Docker resource limits, image versions, and command.
- Tenant count, workflow count, queue backlog, model mode, tool mode, and test duration.
- p50/p95/p99 queue delay, worker recovery time, duplicate-commit count, failure count, and resource use.
- Whether every tool was synthetic, whether the run included injected worker or broker failure, and the exact acceptance result.

The first target is 1,000 active mock workflows plus a 10,000-run backlog after
warmup, with p95 scheduling delay at or below 500 ms. Failure runs are reported
separately and never blended into a success metric.
