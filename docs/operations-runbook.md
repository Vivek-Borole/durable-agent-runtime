# Operations and recovery runbook

## SLO signals

- Scheduling delay: `dar:scheduling_delay_seconds:p95_5m`, target at or below
  500 ms for the documented local workload.
- Uncertain runs: investigate every increment immediately; never retry an
  ambiguous side effect automatically.
- Outbox backlog: investigate sustained values above 100.
- Lease recoveries and effect replays: expected during controlled restarts;
  verify the effect ledger still has one committed row per effect key.

## Worker rollout

Workers stop receiving JetStream deliveries on `SIGTERM`, drain in-flight work
for up to 25 seconds, then cancel remaining work. A read-only step can recover
after lease expiry. Around a side effect, the effect ledger determines whether
the action committed; absence of evidence must become `uncertain`.

## Provider failure

Provider credentials exist only in one control-plane process for five minutes
and are consumed once. Credential loss, process restart, timeout, malformed
response, or unavailable provider produces a redacted uncertain outcome. Do
not put credentials in Kubernetes values, workflow inputs, logs, or traces.

## Release boundary

Compose and kind are single-region synthetic environments. Passing their
checks does not establish multi-region availability, production SaaS readiness,
or customer adoption.
