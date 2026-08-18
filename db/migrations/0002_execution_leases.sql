alter table workflow_runs
  add column current_step integer not null default 0 check (current_step >= 0),
  add column lease_owner text,
  add column lease_expires_at timestamptz;

create index workflow_runs_lease_idx on workflow_runs (state, lease_expires_at) where state in ('queued', 'leased', 'running');
