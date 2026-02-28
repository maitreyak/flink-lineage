#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-flink-lineage}"
NAMESPACE="flink-lineage"

echo "=== Creating Kind cluster '${CLUSTER_NAME}' ==="
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "Cluster '${CLUSTER_NAME}' already exists, skipping creation"
else
  kind create cluster --name "${CLUSTER_NAME}" --wait 60s
fi

echo "=== Creating namespace '${NAMESPACE}' ==="
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "=== Installing Flink Kubernetes Operator ==="
helm repo add flink-operator-repo https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.10.0/ 2>/dev/null || true
helm repo update

if helm status flink-kubernetes-operator -n "${NAMESPACE}" &>/dev/null; then
  echo "Flink Kubernetes Operator already installed, skipping"
else
  helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
    --namespace "${NAMESPACE}" \
    --set webhook.create=false
fi

echo "=== Waiting for operator to be ready ==="
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=flink-kubernetes-operator -n "${NAMESPACE}" --timeout=120s

echo ""
echo "=== Kind cluster '${CLUSTER_NAME}' is ready ==="
echo "Namespace:  ${NAMESPACE}"
echo "Next step:  scripts/deploy.sh local"
