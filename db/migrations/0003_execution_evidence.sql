alter table workflow_runs
  add column attempt_count integer not null default 0 check (attempt_count >= 0),
  add column last_error_code text,
  add column terminal_evidence jsonb not null default '{}'::jsonb;

create table run_step_results (
  id bigint generated always as identity primary key,
  tenant_id uuid not null references tenants(id) on delete cascade,
  run_id uuid not null references workflow_runs(id) on delete cascade,
  step_index integer not null check (step_index >= 0),
  result_kind text not null check (result_kind in ('model', 'transform', 'tool', 'approval')),
  status text not null check (status in ('succeeded', 'awaiting_approval', 'failed', 'uncertain')),
  output_ref text not null,
  evidence jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (tenant_id, run_id, step_index)
);

create index run_step_results_tenant_run_idx on run_step_results (tenant_id, run_id, step_index);

alter table run_step_results enable row level security;
alter table run_step_results force row level security;
create policy run_step_results_tenant on run_step_results
  using (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid)
  with check (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
