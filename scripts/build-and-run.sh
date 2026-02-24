#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=== Step 1: Building Flink job JAR with Maven ==="
cd flink-job
mvn clean package -DskipTests -q
cd "$PROJECT_DIR"
echo "JAR built: flink-job/target/flink-lineage-job-1.0-SNAPSHOT.jar"

echo ""
echo "=== Step 2: Building Docker images ==="
docker compose build

echo ""
echo "=== Step 3: Starting all services ==="
docker compose up -d

echo ""
echo "=== Step 4: Waiting for services to be ready ==="
echo "Waiting for Flink JobManager..."
for i in $(seq 1 30); do
    if curl -s http://localhost:8081/overview > /dev/null 2>&1; then
        echo "Flink JobManager is ready!"
        break
    fi
    sleep 2
done

echo ""
echo "=== Step 5: Submitting Flink job ==="
docker compose exec -T flink-jobmanager flink run \
    /opt/flink/usrlib/flink-lineage-job.jar

echo ""
echo "============================================"
echo "Pipeline is running!"
echo ""
echo "  Flink Web UI:     http://localhost:8081"
echo "  MinIO Console:    http://localhost:9001"
echo "    Username:       minioadmin"
echo "    Password:       minioadmin"
echo "    Bucket:         flink-data"
echo ""
echo "  Output path:      flink-data/output/year=.../month=.../day=.../hour=.../"
echo "  Checkpoints:      flink-data/checkpoints/"
echo ""
echo "Wait ~2 minutes for data to appear in MinIO."
echo "============================================"
