#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXPERIMENTS_DIR="$REPO_ROOT/helm/chaos-mesh/experiments"
VALUES_FILE="$REPO_ROOT/helm/chaos-mesh/values.yaml"
NAMESPACE="chaos-mesh"

usage() {
    cat <<EOF
Usage: $0 <command> [args]

Commands:
  install                Install Chaos Mesh into the cluster
  uninstall              Remove Chaos Mesh from the cluster
  apply <experiment>     Apply a chaos experiment (e.g., network-block-s3)
  delete <experiment>    Delete a running chaos experiment
  status                 Show running chaos experiments

Available experiments:
  network-block-s3       Block S3 connectivity (2m)
  network-block-msk      Block MSK/Kafka connectivity (2m)
  pod-kill               Kill random pods every 2 minutes (scheduled)
  pod-failure            Make a random pod unavailable (30s)
EOF
    exit 1
}

cmd_install() {
    echo "Adding chaos-mesh Helm repo..."
    helm repo add chaos-mesh https://charts.chaos-mesh.org
    helm repo update chaos-mesh

    echo "Installing Chaos Mesh into namespace '$NAMESPACE'..."
    helm install chaos-mesh chaos-mesh/chaos-mesh \
        -n "$NAMESPACE" \
        --create-namespace \
        -f "$VALUES_FILE" \
        --wait

    echo "Chaos Mesh installed. Verify with: kubectl get pods -n $NAMESPACE"
}

cmd_uninstall() {
    echo "Uninstalling Chaos Mesh..."
    helm uninstall chaos-mesh -n "$NAMESPACE" || true

    echo "Deleting namespace '$NAMESPACE'..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found

    echo "Removing Chaos Mesh CRDs..."
    kubectl get crd -o name | grep chaos-mesh.org | xargs -r kubectl delete
    echo "Chaos Mesh removed."
}

resolve_experiment() {
    local name="$1"
    local file="$EXPERIMENTS_DIR/${name}.yaml"

    if [[ ! -f "$file" ]]; then
        echo "Error: experiment '$name' not found at $file" >&2
        echo "Available experiments:" >&2
        ls "$EXPERIMENTS_DIR"/*.yaml 2>/dev/null | xargs -I{} basename {} .yaml | sed 's/^/  /' >&2
        exit 1
    fi

    echo "$file"
}

cmd_apply() {
    local name="${1:?experiment name required}"
    local file
    file=$(resolve_experiment "$name")

    # For network-block-msk, regenerate from current values-aws.yaml broker endpoints
    if [[ "$name" == "network-block-msk" ]]; then
        local aws_values="$REPO_ROOT/helm/flink-lineage/values-aws.yaml"
        if [[ -f "$aws_values" ]]; then
            local brokers
            brokers=$(grep 'KAFKA_BOOTSTRAP_SERVERS' "$aws_values" | head -1 \
                | sed 's/.*"\(.*\)".*/\1/' \
                | tr ',' '\n' \
                | sed 's/:[0-9]*//' \
                | sort -u)

            if [[ -n "$brokers" ]]; then
                echo "Updating MSK experiment with brokers from values-aws.yaml:"
                echo "$brokers" | sed 's/^/  /'

                # Build externalTargets YAML block
                local targets=""
                while IFS= read -r broker; do
                    targets="${targets}    - ${broker}\n"
                done <<< "$brokers"

                # Create a temp file with updated targets
                local tmpfile
                tmpfile=$(mktemp)
                sed '/externalTargets:/,/duration:/{ /externalTargets:/{ p; d; }; /duration:/{ p; d; }; d; }' "$file" \
                    | sed "/externalTargets:/a\\
$(echo "$brokers" | sed 's/^/    - /')" > "$tmpfile"
                kubectl apply -f "$tmpfile"
                rm -f "$tmpfile"
                echo "Experiment '$name' applied."
                return
            fi
        fi
    fi

    echo "Applying experiment '$name'..."
    kubectl apply -f "$file"
    echo "Experiment '$name' applied."
}

cmd_delete() {
    local name="${1:?experiment name required}"
    local file
    file=$(resolve_experiment "$name")

    echo "Deleting experiment '$name'..."
    kubectl delete -f "$file" --ignore-not-found
    echo "Experiment '$name' deleted."
}

cmd_status() {
    echo "=== Chaos Experiments in flink-lineage ==="
    echo ""
    echo "--- NetworkChaos ---"
    kubectl get networkchaos -n flink-lineage 2>/dev/null || echo "  (none)"
    echo ""
    echo "--- PodChaos ---"
    kubectl get podchaos -n flink-lineage 2>/dev/null || echo "  (none)"
    echo ""
    echo "--- Schedules ---"
    kubectl get schedule -n flink-lineage 2>/dev/null || echo "  (none)"
}

[[ $# -lt 1 ]] && usage

case "$1" in
    install)    cmd_install ;;
    uninstall)  cmd_uninstall ;;
    apply)      cmd_apply "${2:-}" ;;
    delete)     cmd_delete "${2:-}" ;;
    status)     cmd_status ;;
    *)          usage ;;
esac
