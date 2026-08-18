create table tenant_runtime_limits (
  tenant_id uuid primary key references tenants(id) on delete cascade,
  max_active_runs integer not null default 100 check (max_active_runs between 1 and 10000),
  max_runs_per_day integer not null default 1000 check (max_runs_per_day between 1 and 100000),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table tenant_runtime_limits enable row level security;
alter table tenant_runtime_limits force row level security;
create policy tenant_runtime_limits_tenant on tenant_runtime_limits
  using (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid)
  with check (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
