create extension if not exists pgcrypto;

create type dar_role as enum ('owner', 'operator', 'viewer');
create type dar_run_state as enum ('queued', 'leased', 'running', 'awaiting_approval', 'succeeded', 'failed', 'cancelled', 'uncertain');

create table tenants (
  id uuid primary key default gen_random_uuid(),
  slug text unique not null check (slug ~ '^[a-z0-9-]{3,80}$'),
  created_at timestamptz not null default now()
);

create table principals (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  subject text not null,
  role dar_role not null,
  created_at timestamptz not null default now(),
  unique (tenant_id, subject)
);

create table api_keys (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  principal_id uuid not null references principals(id) on delete cascade,
  key_hash text not null unique,
  created_at timestamptz not null default now(),
  revoked_at timestamptz
);

create table workflow_definitions (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  name text not null,
  version text not null check (version ~ '^v[0-9]+$'),
  definition jsonb not null,
  budget_cents integer not null check (budget_cents >= 0),
  created_at timestamptz not null default now(),
  unique (tenant_id, name, version)
);

create table workflow_runs (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  workflow_id uuid not null references workflow_definitions(id),
  idempotency_key text not null check (char_length(idempotency_key) between 16 and 128),
  state dar_run_state not null default 'queued',
  input jsonb not null default '{}'::jsonb,
  budget_cents integer not null check (budget_cents >= 0),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, idempotency_key)
);

create table run_events (
  id bigint generated always as identity primary key,
  tenant_id uuid not null references tenants(id) on delete cascade,
  run_id uuid not null references workflow_runs(id) on delete cascade,
  event_type text not null,
  detail text not null check (char_length(detail) <= 500),
  trace_id text,
  created_at timestamptz not null default now()
);

create table effect_commits (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  run_id uuid not null references workflow_runs(id) on delete cascade,
  step_index integer not null check (step_index >= 0),
  effect_key text not null,
  outcome jsonb not null,
  committed_at timestamptz not null default now(),
  unique (tenant_id, effect_key)
);

create table workflow_outbox (
  id bigint generated always as identity primary key,
  tenant_id uuid not null references tenants(id) on delete cascade,
  run_id uuid not null references workflow_runs(id) on delete cascade,
  subject text not null,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  published_at timestamptz,
  attempts integer not null default 0 check (attempts >= 0)
);

create table approvals (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  run_id uuid not null references workflow_runs(id) on delete cascade,
  step_index integer not null check (step_index >= 0),
  principal_id uuid not null references principals(id),
  decision text not null check (decision in ('approved', 'rejected')),
  created_at timestamptz not null default now(),
  unique (run_id, step_index)
);

create table audit_records (
  id bigint generated always as identity primary key,
  tenant_id uuid not null references tenants(id) on delete cascade,
  principal_id uuid references principals(id) on delete set null,
  action text not null,
  resource_id uuid,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index workflow_runs_tenant_state_idx on workflow_runs (tenant_id, state, created_at);
create index workflow_outbox_unpublished_idx on workflow_outbox (created_at) where published_at is null;
create index run_events_tenant_run_idx on run_events (tenant_id, run_id, id);

-- The control plane sets app.tenant_id with SET LOCAL for every transaction.
-- RLS provides a second boundary if a future query accidentally omits a tenant filter.
alter table workflow_definitions enable row level security;
alter table workflow_runs enable row level security;
alter table run_events enable row level security;
alter table effect_commits enable row level security;
alter table workflow_outbox enable row level security;
alter table approvals enable row level security;
alter table audit_records enable row level security;

create policy workflow_definitions_tenant on workflow_definitions using (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid) with check (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
create policy workflow_runs_tenant on workflow_runs using (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid) with check (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
create policy run_events_tenant on run_events using (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid) with check (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
create policy effect_commits_tenant on effect_commits using (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid) with check (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
create policy workflow_outbox_tenant on workflow_outbox using (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid) with check (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
create policy approvals_tenant on approvals using (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid) with check (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
create policy audit_records_tenant on audit_records using (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid) with check (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);

