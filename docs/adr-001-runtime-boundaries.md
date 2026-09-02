# ADR 001: PostgreSQL authority with JetStream delivery

## Decision

Use PostgreSQL for workflow definitions, run state, budgets, leases, the
transactional outbox, and committed-effect idempotency. Use NATS JetStream for
at-least-once delivery and Redis only for disposable quota pre-checks. Workers
may repeat execution after failure, but a stable effect key prevents a repeated
mock-ticket commit.

## Alternatives

- **Temporal** supplies a mature durable execution engine, SDK replay model,
  timers, and operational tooling. It is the appropriate default for many real
  products. Building the constrained core here makes transactional boundaries,
  leases, and ambiguity visible for portfolio evaluation.
- **Restate** provides durable handlers and lightweight service integration.
  It would remove much of the custom lease/outbox code but also hide the exact
  failure mechanisms this project demonstrates.
- **AWS Step Functions** provides managed orchestration and integrations but
  creates a paid cloud dependency and shifts the project away from local-first
  evidence.
- **A simple queue worker** is easier, but queue acknowledgement alone cannot
  atomically protect state changes and external effects.

## Consequences

The design is intentionally smaller than those products. It supports a linear
DSL, one region, registered tools, and synthetic effects. It does not claim
global exactly-once execution. PostgreSQL contention and the single in-memory
credential broker are explicit scaling limits; Kubernetes horizontal proof is
therefore focused on workers.
