-- These credentials exist only for the checked-in local Compose environment.
-- Production deployments must provision equivalent roles with externally managed
-- secrets before starting the control plane or worker.
do $$ begin
  if not exists (select 1 from pg_roles where rolname = 'dar_control') then
    create role dar_control login nosuperuser nocreatedb nocreaterole noinherit password 'dar-control-local-only';
  end if;
  if not exists (select 1 from pg_roles where rolname = 'dar_worker') then
    create role dar_worker login nosuperuser nocreatedb nocreaterole noinherit password 'dar-worker-local-only';
  end if;
end $$;

grant usage on schema public to dar_control, dar_worker;

-- Tenant-facing control plane: no outbox access and no lease-owner/expiry
-- mutation. RLS remains the tenant boundary for its normal operations.
grant select, insert, update on tenants, principals, api_keys, tenant_runtime_limits to dar_control;
grant select, insert, update, delete on workflow_definitions, run_events, approvals, audit_records to dar_control;
grant select on workflow_runs to dar_control;
grant insert (tenant_id, workflow_id, idempotency_key, input, budget_cents, provider_credential_handle) on workflow_runs to dar_control;
grant update (state, current_step, updated_at) on workflow_runs to dar_control;
grant usage, select on all sequences in schema public to dar_control;

-- The worker/publisher is a separately configured system principal. It can
-- operate across tenants, but only on runtime tables; it receives no API-key or
-- principal-table privileges.
grant select on tenants to dar_worker;
grant select on workflow_definitions to dar_worker;
grant select, update on workflow_runs to dar_worker;
grant select, insert, update on run_events, effect_commits, run_step_results, workflow_outbox to dar_worker;
grant usage, select on all sequences in schema public to dar_worker;

create policy workflow_definitions_worker_system on workflow_definitions for all to dar_worker using (true) with check (true);
create policy workflow_runs_worker_system on workflow_runs for all to dar_worker using (true) with check (true);
create policy run_events_worker_system on run_events for all to dar_worker using (true) with check (true);
create policy effect_commits_worker_system on effect_commits for all to dar_worker using (true) with check (true);
create policy workflow_outbox_worker_system on workflow_outbox for all to dar_worker using (true) with check (true);
create policy run_step_results_worker_system on run_step_results for all to dar_worker using (true) with check (true);

create or replace function queue_run_outbox(p_tenant_id uuid, p_run_id uuid)
returns void
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if p_tenant_id is distinct from nullif(current_setting('app.tenant_id', true), '')::uuid then
    raise exception 'tenant context does not match queued run';
  end if;
  if not exists (select 1 from workflow_runs where id = p_run_id and tenant_id = p_tenant_id and state = 'queued') then
    raise exception 'queued run does not exist for tenant';
  end if;
  insert into workflow_outbox (tenant_id, run_id, subject, payload)
    values (p_tenant_id, p_run_id, 'dar.run.queued', jsonb_build_object('runId', p_run_id));
end;
$$;

revoke all on function queue_run_outbox(uuid, uuid) from public;
grant execute on function queue_run_outbox(uuid, uuid) to dar_control;
