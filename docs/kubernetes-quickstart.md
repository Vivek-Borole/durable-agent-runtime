# Local Kubernetes quick start

This path is free and uses Colima, kind, Helm, and synthetic credentials. It
does not provision a cloud service or execute a real external side effect.

```sh
colima start --cpu 6 --memory 8
./scripts/kind-smoke.sh
```

The smoke command builds four local images, creates or reuses the `dar-v02`
kind cluster, deploys two workers, runs the approval-gated synthetic workflow,
rolls both workers, and requires the run to finish with one ledger-backed mock
ticket. Set `DAR_SKIP_IMAGE_BUILD=1` to reuse images. Set
`DAR_KIND_CLEANUP=1` only when you want the cluster deleted after success.

Production chart values do not contain usable secrets or database endpoints.
They reference an existing Kubernetes Secret and externally managed
PostgreSQL, NATS, and Redis services. The single-node dependencies in
`values-kind.yaml` are fixtures, not a production topology.
