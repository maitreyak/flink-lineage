#!/usr/bin/env bash
set -euo pipefail

# Run the gap checker inside EKS to avoid S3 egress costs.
#
# Usage: ./check-offset-gaps-cloud.sh [--build] <start_date> <end_date>
#        ./check-offset-gaps-cloud.sh [--build] --paths <path1> [path2 ...]
#
# Examples:
#   $0 --build --paths 03-06-2026          # build image + run
#   $0 --paths 03-06-2026/15/40            # run (image already in ECR)
#   $0 '03-06-2026' '03-06-2026'           # full day

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ECR_REGISTRY="${ECR_REGISTRY:-605618833247.dkr.ecr.us-east-1.amazonaws.com}"
AWS_REGION="${AWS_REGION:-us-east-1}"
IMAGE_REPO="flink-gap-checker"
IMAGE="${ECR_REGISTRY}/${IMAGE_REPO}:latest"
NAMESPACE="flink-lineage"
SERVICE_ACCOUNT="flink-workload"

# Parse --build flag, pass everything else through
BUILD=false
ARGS=()
for arg in "$@"; do
  if [[ "$arg" == "--build" ]]; then
    BUILD=true
  else
    ARGS+=("$arg")
  fi
done

if [[ ${#ARGS[@]} -eq 0 ]]; then
  echo "Usage: $0 [--build] <start_date> <end_date>"
  echo "       $0 [--build] --paths <path1> [path2 ...]"
  echo ""
  echo "Options:"
  echo "  --build  Build and push the gap-checker image to ECR before running"
  echo ""
  echo "Examples:"
  echo "  $0 --build --paths 03-06-2026"
  echo "  $0 --paths 03-06-2026/15/40"
  echo "  $0 '03-06-2026' '03-06-2026'"
  exit 1
fi

if [[ "${BUILD}" == true ]]; then
  echo "=== Building gap-checker image ==="
  docker build --platform linux/amd64 \
    -t "${IMAGE_REPO}:latest" \
    -f "${PROJECT_DIR}/Dockerfile.gap-checker" \
    "${PROJECT_DIR}"

  echo "=== Pushing to ECR ==="
  aws ecr get-login-password --region "${AWS_REGION}" | \
    docker login --username AWS --password-stdin "${ECR_REGISTRY}"
  docker tag "${IMAGE_REPO}:latest" "${IMAGE}"
  docker push "${IMAGE}"
  echo ""
fi

POD_NAME="gap-checker-$(date +%s)"

cleanup() { kubectl delete pod "${POD_NAME}" -n "${NAMESPACE}" --ignore-not-found=true >/dev/null 2>&1; }
trap cleanup EXIT

echo "=== Running gap checker in EKS ==="
kubectl run "${POD_NAME}" \
  --namespace "${NAMESPACE}" \
  --image "${IMAGE}" \
  --overrides="{\"spec\":{\"serviceAccountName\":\"${SERVICE_ACCOUNT}\"}}" \
  --restart=Never \
  -- aws "${ARGS[@]}"

kubectl wait --for=condition=Ready "pod/${POD_NAME}" -n "${NAMESPACE}" --timeout=60s >/dev/null 2>&1 || true
kubectl logs -f "${POD_NAME}" -n "${NAMESPACE}"
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${POD_NAME}" -n "${NAMESPACE}" --timeout=600s >/dev/null 2>&1
