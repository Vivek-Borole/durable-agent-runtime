# Release evidence checklist

Completed synthetic evidence:

- [x] generated OpenAPI contract and API examples;
- [x] architecture and state-machine diagrams;
- [x] 100,000-attempt idempotent-effect fault report;
- [x] 1,000-resident / 10,000-backlog scheduling benchmark report.
- [x] expired-lease recovery simulation with one committed mock effect;
- [x] secret/PII operational-text redaction tests;
- [x] synthetic console screenshots of approval pause and successful resume.

Before the repository becomes public, still attach:

- raw benchmark/fault logs and a report-schema check;
- a short video made only with synthetic data;
- a clean-machine Compose smoke-test log;
- GitHub Actions verification, GitHub Pages publication, release assets, tag,
  and the explicit private-to-public repository change.

`LICENSE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and `SECURITY.md` are
already present and must be checked again at release time.

Do not describe the runtime as production-ready, exactly-once, autonomous, or
capable of arbitrary tools until the associated implementation and evidence
exist.
