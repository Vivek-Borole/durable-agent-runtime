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

Publication record (20 August 2026):

- [x] [passing GitHub Actions verification](https://github.com/Vivek-Borole/durable-agent-runtime/actions/runs/32244485396)
      for release commit `57d6972532ae4388302a2346ef1b764bb4da8026`;
- [x] [GitHub Pages evidence site](https://vivek-borole.github.io/durable-agent-runtime/)
      and [successful deployment](https://github.com/Vivek-Borole/durable-agent-runtime/actions/runs/32331865243);
- [x] [public `v0.1.0` release](https://github.com/Vivek-Borole/durable-agent-runtime/releases/tag/v0.1.0)
      with a SHA-256-attested evidence archive;
- [x] repository visibility changed from private to public only after the tag
      and release assets existed.

v0.2 candidate evidence (1 September 2026):

- [x] two-worker local-kind rollout completed an approval-gated run after one
      recovered attempt with exactly one committed mock effect;
- [x] 100,000 concurrent effect attempts produced one commit in 8.128 seconds;
- [x] two worker processes completed a 10,000-run backlog with 1,000 resident
      workflows at 2.749 ms p95 scheduling delay;
- [x] Helm lint, container builds, TypeScript tests, Go race tests, evidence
      validation, high-severity dependency audit, and local secret scan pass;
- [ ] remote GitHub Actions verification for the candidate commit;
- [ ] signed `v0.2.0` tag and release artifacts.

`LICENSE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and `SECURITY.md` are
already present and must be checked again at release time.

Do not describe the runtime as production-ready, exactly-once, autonomous, or
capable of arbitrary tools until the associated implementation and evidence
exist.
