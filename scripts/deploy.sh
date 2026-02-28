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

echo "=== Building Flink job JAR ==="
(cd "${PROJECT_DIR}/flink-job" && mvn clean package -DskipTests -q)

echo "=== Building Docker images ==="
docker build -t flink-lineage:latest -f "${PROJECT_DIR}/Dockerfile.flink" "${PROJECT_DIR}"
docker build -t flink-lineage-data-generator:latest "${PROJECT_DIR}/data-generator"

case "${ENV}" in
  local)
    CLUSTER_NAME="${KIND_CLUSTER_NAME:-flink-lineage}"
    echo "=== Loading images into Kind cluster '${CLUSTER_NAME}' ==="
    kind load docker-image flink-lineage:latest --name "${CLUSTER_NAME}"
    kind load docker-image flink-lineage-data-generator:latest --name "${CLUSTER_NAME}"

    echo "=== Deploying with Helm (local) ==="
    helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" \
      --namespace "${NAMESPACE}" \
      --values "${CHART_DIR}/values.yaml" \
      --values "${CHART_DIR}/values-local.yaml"
    ;;

  aws)
    ECR_REGISTRY="${ECR_REGISTRY:?ECR_REGISTRY must be set}"
    AWS_REGION="${AWS_REGION:?AWS_REGION must be set}"

    echo "=== Pushing images to ECR ==="
    aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

    docker tag flink-lineage:latest "${ECR_REGISTRY}/flink-lineage:latest"
    docker push "${ECR_REGISTRY}/flink-lineage:latest"

    docker tag flink-lineage-data-generator:latest "${ECR_REGISTRY}/flink-lineage-data-generator:latest"
    docker push "${ECR_REGISTRY}/flink-lineage-data-generator:latest"

    echo "=== Deploying with Helm (aws) ==="
    helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" \
      --namespace "${NAMESPACE}" \
      --values "${CHART_DIR}/values.yaml" \
      --values "${CHART_DIR}/values-aws.yaml" \
      --set flinkImage.repository="${ECR_REGISTRY}/flink-lineage" \
      --set dataGenerator.image.repository="${ECR_REGISTRY}/flink-lineage-data-generator"
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
