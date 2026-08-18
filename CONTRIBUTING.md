# Contributing

This repository accepts changes that preserve the constrained runtime model.

1. Fork and create a focused branch.
2. Run `pnpm install`, `pnpm test`, `pnpm typecheck`, `pnpm build`, and
   `pnpm audit --audit-level=high`.
3. For database changes, run `pnpm compose:up`, `pnpm compose:migrate`, then
   the integration tests with `POSTGRES_URL` and `NATS_URL` set as documented.
4. Add tests and update the OpenAPI contract for every public API change.
5. Do not commit credentials, prompts containing secrets, customer data, or
   unmeasured performance claims.

Changes that add shell execution, arbitrary egress, browser automation,
financial actions, or automatic messaging are out of scope for v1.
