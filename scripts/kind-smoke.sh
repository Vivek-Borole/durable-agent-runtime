#!/bin/sh
set -eu

cluster=${DAR_KIND_CLUSTER:-dar-v02}
namespace=${DAR_KIND_NAMESPACE:-dar-system}
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)

for command in docker kind kubectl helm curl jq; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done

if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --name "$cluster" --wait 120s
fi

if [ "${DAR_SKIP_IMAGE_BUILD:-0}" != "1" ]; then
  docker build -f "$root/Dockerfile.worker" -t durable-agent-runtime-worker:dev "$root"
  docker build -f "$root/Dockerfile.control-plane" -t durable-agent-runtime-control-plane:dev "$root"
  docker build -f "$root/Dockerfile.console" -t durable-agent-runtime-console:dev "$root"
  docker build -f "$root/Dockerfile.migrate" -t durable-agent-runtime-migrate:dev "$root"
fi

for image in durable-agent-runtime-worker:dev durable-agent-runtime-control-plane:dev durable-agent-runtime-console:dev durable-agent-runtime-migrate:dev; do
  kind load docker-image --name "$cluster" "$image"
done

kubectl --context "kind-$cluster" create namespace "$namespace" --dry-run=client -o yaml | kubectl --context "kind-$cluster" apply -f -
helm upgrade --install dar "$root/deploy/helm/durable-agent-runtime" \
  --kube-context "kind-$cluster" --namespace "$namespace" \
  -f "$root/deploy/helm/durable-agent-runtime/values-kind.yaml" \
  --set-string "runtime.rolloutNonce=$(date +%s)" \
  --wait --wait-for-jobs --timeout 10m

for deployment in dar-control-plane dar-publisher dar-worker dar-console; do
  kubectl --context "kind-$cluster" -n "$namespace" rollout status "deployment/$deployment" --timeout=180s
done

kubectl --context "kind-$cluster" -n "$namespace" port-forward service/dar-control-plane 18081:3001 >/tmp/dar-kind-port-forward.log 2>&1 &
forward_pid=$!
trap 'kill "$forward_pid" 2>/dev/null || true' EXIT INT TERM
sleep 2
curl --fail --silent http://127.0.0.1:18081/health/ready >/dev/null

workflow_id=$(curl --fail --silent -X POST http://127.0.0.1:18081/v1/workflows \
  -H 'content-type: application/json' -H 'x-tenant-id: demo-tenant' -H 'x-api-key: replace-with-a-long-local-key' \
  --data "{\"schemaVersion\":\"1\",\"name\":\"kind-smoke-$(date +%s)\",\"version\":\"v1\",\"budgetCents\":10,\"steps\":[{\"kind\":\"tool\",\"tool\":\"mock_data_read\",\"sideEffect\":false},{\"kind\":\"approval\",\"reason\":\"synthetic kind smoke\"},{\"kind\":\"tool\",\"tool\":\"mock_ticket_write\",\"sideEffect\":true}]}" | jq -r '.workflow.id')
run_id=$(curl --fail --silent -X POST http://127.0.0.1:18081/v1/runs \
  -H 'content-type: application/json' -H 'x-tenant-id: demo-tenant' -H 'x-api-key: replace-with-a-long-local-key' \
  -H "idempotency-key: kind-smoke-$(date +%s)-0000000000000000" \
  --data "{\"workflowId\":\"$workflow_id\",\"input\":{\"fixture\":\"synthetic\"}}" | jq -r '.run.id')

for _ in $(seq 1 60); do
  state=$(curl --fail --silent http://127.0.0.1:18081/v1/runs/"$run_id" -H 'x-tenant-id: demo-tenant' -H 'x-api-key: replace-with-a-long-local-key' | jq -r '.run.state')
  [ "$state" = awaiting_approval ] && break
  sleep 1
done
[ "${state:-}" = awaiting_approval ] || { echo "run did not reach approval: ${state:-unknown}" >&2; exit 1; }

curl --fail --silent -X POST http://127.0.0.1:18081/v1/runs/"$run_id"/approve -H 'x-tenant-id: demo-tenant' -H 'x-api-key: replace-with-a-long-local-key' >/dev/null
kubectl --context "kind-$cluster" -n "$namespace" rollout restart deployment/dar-worker
kubectl --context "kind-$cluster" -n "$namespace" rollout status deployment/dar-worker --timeout=180s

for _ in $(seq 1 60); do
  state=$(curl --fail --silent http://127.0.0.1:18081/v1/runs/"$run_id" -H 'x-tenant-id: demo-tenant' -H 'x-api-key: replace-with-a-long-local-key' | jq -r '.run.state')
  [ "$state" = succeeded ] && break
  sleep 1
done
[ "${state:-}" = succeeded ] || { echo "run did not succeed after worker rollout: ${state:-unknown}" >&2; exit 1; }

echo "kind smoke passed: run=$run_id state=$state"
if [ "${DAR_KIND_CLEANUP:-0}" = "1" ]; then kind delete cluster --name "$cluster"; fi
