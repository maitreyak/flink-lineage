#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="flink-lineage"
RELEASE_NAME="flink-lineage"

ENV="${1:-}"
if [[ -z "${ENV}" ]]; then
  echo "Usage: $0 <aws>"
  exit 1
fi

case "${ENV}" in
  aws) ;;
  *)
    echo "Unknown environment: ${ENV}"
    echo "Usage: $0 <aws>"
    exit 1
    ;;
esac

echo "=== Resuming Flink jobs (${ENV}) ==="
kubectl patch flinkdeployment "${RELEASE_NAME}" \
  -n "${NAMESPACE}" \
  --type merge --field-manager=helm \
  -p '{"spec":{"job":{"state":"running"}}}'

kubectl patch flinkdeployment "${RELEASE_NAME}-producer" \
  -n "${NAMESPACE}" \
  --type merge --field-manager=helm \
  -p '{"spec":{"job":{"state":"running"}}}'

echo ""
echo "=== Resume initiated ==="
echo "Check status:   kubectl get flinkdeployment -n ${NAMESPACE}"
