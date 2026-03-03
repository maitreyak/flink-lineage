#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="flink-lineage"
RELEASE_NAME="flink-lineage"

ENV="${1:-}"
if [[ -z "${ENV}" ]]; then
  echo "Usage: $0 <local|aws>"
  exit 1
fi

case "${ENV}" in
  local|aws) ;;
  *)
    echo "Unknown environment: ${ENV}"
    echo "Usage: $0 <local|aws>"
    exit 1
    ;;
esac

echo "=== Suspending Flink jobs (${ENV}) ==="
kubectl patch flinkdeployment "${RELEASE_NAME}" \
  -n "${NAMESPACE}" \
  --type merge --field-manager=helm \
  -p '{"spec":{"job":{"state":"suspended"}}}'

kubectl patch flinkdeployment "${RELEASE_NAME}-producer" \
  -n "${NAMESPACE}" \
  --type merge --field-manager=helm \
  -p '{"spec":{"job":{"state":"suspended"}}}'

echo ""
echo "=== Suspend initiated ==="
echo "Check status:   kubectl get flinkdeployment -n ${NAMESPACE}"
