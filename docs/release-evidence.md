# Release evidence checklist

Completed synthetic evidence:

- [x] generated OpenAPI contract and API examples;
- [x] architecture and state-machine diagrams;
- [x] 100,000-attempt idempotent-effect fault report;
- [x] 1,000-resident / 10,000-backlog scheduling benchmark report.

Before the repository becomes public, still attach:

- threat model plus secret-redaction test output;
- worker-recovery report, plus raw benchmark/fault logs and a report-schema check;
- screenshots and a video made only with synthetic data;
- licence, contribution guide, and security contact.

Do not describe the runtime as production-ready, exactly-once, autonomous, or
capable of arbitrary tools until the associated implementation and evidence
exist.
