#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CHART_DIR="${PROJECT_DIR}/helm/flink-lineage"
NAMESPACE="flink-lineage"
RELEASE_NAME="flink-lineage"

ENV="${1:-}"
if [[ -z "${ENV}" ]]; then
  echo "Usage: $0 [local|aws]"
  exit 1
fi

echo "=== Building Flink job JARs ==="
(cd "${PROJECT_DIR}/flink-job" && mvn clean package -DskipTests -q)
(cd "${PROJECT_DIR}/flink-producer" && mvn clean package -DskipTests -q)

PLATFORM_FLAG=""
if [[ "${ENV}" == "aws" ]]; then
  PLATFORM_FLAG="--platform linux/amd64"
fi

echo "=== Building Docker image ==="
docker build ${PLATFORM_FLAG} -t flink-lineage:latest -f "${PROJECT_DIR}/Dockerfile.flink" "${PROJECT_DIR}"

case "${ENV}" in
  local)
    CLUSTER_NAME="${KIND_CLUSTER_NAME:-flink-lineage}"
    echo "=== Loading image into Kind cluster '${CLUSTER_NAME}' ==="
    kind load docker-image flink-lineage:latest --name "${CLUSTER_NAME}"

    echo "=== Deploying with Helm (local) ==="
    helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" \
      --namespace "${NAMESPACE}" \
      --values "${CHART_DIR}/values.yaml" \
      --values "${CHART_DIR}/values-local.yaml"
    ;;

  aws)
    ECR_REGISTRY="${ECR_REGISTRY:-605618833247.dkr.ecr.us-east-1.amazonaws.com}"
    AWS_REGION="${AWS_REGION:-us-east-1}"

    echo "=== Pushing images to ECR ==="
    aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

    docker tag flink-lineage:latest "${ECR_REGISTRY}/flink-lineage:latest"
    docker push "${ECR_REGISTRY}/flink-lineage:latest"

    # Delete FlinkDeployments to avoid field manager conflicts from kubectl-patch
    # (suspend/resume workflow). Helm will recreate them and the Flink operator
    # restarts jobs from the latest checkpoint.
    kubectl delete flinkdeployment --all -n "${NAMESPACE}" --wait 2>/dev/null || true

    echo "=== Deploying with Helm (aws) ==="
    helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" \
      --namespace "${NAMESPACE}" \
      --values "${CHART_DIR}/values.yaml" \
      --values "${CHART_DIR}/values-aws.yaml" \
      --set flinkImage.repository="${ECR_REGISTRY}/flink-lineage"
    ;;

  *)
    echo "Unknown environment: ${ENV}"
    echo "Usage: $0 [local|aws]"
    exit 1
    ;;
esac

echo ""
echo "=== Deployment complete ==="
echo "Check status:   kubectl get pods -n ${NAMESPACE}"
echo "Flink status:   kubectl get flinkdeployment -n ${NAMESPACE}"
echo "Port-forward:   kubectl port-forward svc/${RELEASE_NAME}-rest 8081:8081 -n ${NAMESPACE}"
