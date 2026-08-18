# API examples

Set local-only fixture values:

```bash
export DAR_API=http://127.0.0.1:3001
export DAR_TENANT=demo-tenant
export DAR_KEY=replace-with-a-long-local-key
```

Register a workflow:

```bash
curl -X POST "$DAR_API/v1/workflows" \
  -H "x-tenant-id: $DAR_TENANT" -H "x-api-key: $DAR_KEY" \
  -H 'content-type: application/json' \
  --data '{"name":"safe-demo","version":"v1","budgetCents":100,"allowedHosts":["example.test"],"steps":[{"kind":"tool","tool":"mock_data_read","sideEffect":false},{"kind":"approval","reason":"Approve synthetic ticket"},{"kind":"tool","tool":"mock_ticket_write","sideEffect":true}]}'
```

Use the returned workflow ID when queueing a run. Every request must provide a
new `Idempotency-Key` with 16–128 characters. Read the run timeline, approve
only when the state is `awaiting_approval`, or cancel an unfinished run. See
`contracts/openapi.yaml` for the complete versioned contract.
