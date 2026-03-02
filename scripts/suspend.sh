#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CHART_DIR="${PROJECT_DIR}/helm/flink-lineage"
NAMESPACE="flink-lineage"
RELEASE_NAME="flink-lineage"

ENV="${1:-}"
if [[ -z "${ENV}" ]]; then
  echo "Usage: $0 <local|aws>"
  exit 1
fi

case "${ENV}" in
  local|aws)
    VALUES_FILE="${CHART_DIR}/values-${ENV}.yaml"
    ;;
  *)
    echo "Unknown environment: ${ENV}"
    echo "Usage: $0 <local|aws>"
    exit 1
    ;;
esac

echo "=== Suspending Flink jobs (${ENV}) ==="
helm upgrade "${RELEASE_NAME}" "${CHART_DIR}" \
  --namespace "${NAMESPACE}" \
  --reuse-values \
  --set flink.job.state=suspended \
  --set flinkProducer.job.state=suspended

echo ""
echo "=== Suspend initiated ==="
echo "Check status:   kubectl get flinkdeployment -n ${NAMESPACE}"
