# Compose quick start

This is a synthetic local demonstration. It does not connect to production
providers or execute real external side effects.

```bash
pnpm install
pnpm compose:up
pnpm compose:migrate
```

In separate terminals, start the control plane, outbox publisher, and worker:

```bash
POSTGRES_URL=postgres://dar_control:dar-control-local-only@127.0.0.1:5432/dar \
DAR_BOOTSTRAP_TENANT=demo-tenant \
DAR_BOOTSTRAP_API_KEY=replace-with-a-long-local-key \
DAR_INTERNAL_TOKEN=local-development-token \
pnpm dev

DAR_WORKER_POSTGRES_URL=postgres://dar_worker:dar-worker-local-only@127.0.0.1:5432/dar \
NATS_URL=nats://127.0.0.1:4222 \
pnpm --filter @dar/control-plane outbox

DAR_WORKER_POSTGRES_URL=postgres://dar_worker:dar-worker-local-only@127.0.0.1:5432/dar \
NATS_URL=nats://127.0.0.1:4222 \
DAR_CONTROL_PLANE_INTERNAL_URL=http://127.0.0.1:3001 \
DAR_INTERNAL_TOKEN=local-development-token \
go run ./services/worker
```

Start the console with `pnpm --filter @dar/console dev`, then open the Vite URL.
Use the displayed demo tenant and local key. Create a synthetic workflow; it
will pause before the mock ticket write. Approving it resumes only the next
step. Prometheus is at `http://127.0.0.1:9090`; Grafana is at
`http://127.0.0.1:3000`.

Tear down generated local data with `pnpm compose:down`.
