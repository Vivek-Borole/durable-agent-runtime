alter table workflow_runs add column provider_credential_handle uuid;

create unique index workflow_runs_provider_credential_handle_idx
  on workflow_runs (provider_credential_handle)
  where provider_credential_handle is not null;
