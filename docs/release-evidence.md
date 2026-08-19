# Release evidence checklist

Completed synthetic evidence:

- [x] generated OpenAPI contract and API examples;
- [x] architecture and state-machine diagrams;
- [x] 100,000-attempt idempotent-effect fault report;
- [x] 1,000-resident / 10,000-backlog scheduling benchmark report.
- [x] expired-lease recovery simulation with one committed mock effect;
- [x] secret/PII operational-text redaction tests;
- [x] synthetic console screenshots of approval pause and successful resume.
- [x] real worker-process interruption/recovery report;
- [x] exported OpenTelemetry trace and synthetic Grafana screenshot;
- [x] clean-Compose smoke log;
- [x] animated synthetic walkthrough.

Before the repository becomes public, still attach:

- a passing GitHub Actions run for the exact release commit;
- GitHub Pages publication and release assets;
- the `v0.1.0` tag and explicit private-to-public repository change.

`LICENSE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and `SECURITY.md` are
already present and must be checked again at release time.

Do not describe the runtime as production-ready, exactly-once, autonomous, or
capable of arbitrary tools until the associated implementation and evidence
exist.
