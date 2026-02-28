# Flink Lineage Pipeline

Kafka-to-S3 pipeline that reads Avro messages from Kafka, enriches them with Kafka metadata (topic, partition, offset), and writes Parquet files to S3-compatible storage (MinIO). Includes a write-ahead commit log that maps each checkpoint to the S3 data files it committed.

## Prerequisites

- Docker
- Java 11+
- Maven 3.6+
- Standard Unix tools (for querying write-ahead commit log CSV files)

**For Kubernetes deployment (Kind):**
- [kind](https://kind.sigs.k8s.io/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm](https://helm.sh/)

## Quick Start (Docker Compose)

```bash
./scripts/build-and-run.sh
```

This builds the JAR and Docker images, starts all services (Kafka, ZooKeeper, MinIO, Flink, data generator), and submits the Flink job. Wait ~2 minutes for data to appear in MinIO.

## Kubernetes Deployment (Kind)

### 1. Create the cluster

```bash
scripts/kind-setup.sh
```

Creates a Kind cluster, the `flink-lineage` namespace, and installs the Flink Kubernetes Operator.

### 2. Build and deploy

```bash
scripts/deploy.sh local
```

Builds the JAR and Docker images, loads them into Kind, and deploys the Helm chart. This starts Kafka (KRaft mode), MinIO, the Flink job (via FlinkDeployment CRD), and the data generator.

### 3. Verify

```bash
# All pods should be Running
kubectl get pods -n flink-lineage

# FlinkDeployment should show RUNNING / STABLE
kubectl get flinkdeployment -n flink-lineage
```

### 4. Access UIs

```bash
# Flink Web UI — http://localhost:8081
kubectl port-forward svc/flink-lineage-rest 8081:8081 -n flink-lineage

# MinIO Console — http://localhost:9001 (minioadmin / minioadmin)
kubectl port-forward svc/minio 9001:9001 -n flink-lineage
```

### 5. Tear down

```bash
helm uninstall flink-lineage -n flink-lineage
kind delete cluster --name flink-lineage
```

## Architecture

```
Kafka (lineage-input topic, 10 partitions)
    |
    v
Flink: RawKafkaDeserializer (pass-through ConsumerRecord)
    |
    v
Flink: EnrichFunction
  - Deserializes Avro, adds kafka_topic/partition/offset
    |
    v
WriteAheadCommitLogFileSink (wraps FileSink)
  - Pre-commit topology: WriteAheadCommitLogOperator
    extracts (checkpoint_id, s3_key) from committable messages
    writes CSV write-ahead commit log eagerly in processElement()
    |
    v
FileSink --> s3://flink-data/output/                    (enriched data, partitioned by date/hour)
         +-> s3://flink-data/write-ahead-commit-log/    (write-ahead commit log CSV, partitioned by checkpoint)
```

### Data Flow

- **Data generator**: Produces ~10 Avro messages/sec across 10 Kafka partitions
- **Flink job**: Consumes from Kafka, enriches with metadata, writes Parquet to S3
- **Data sink**: Full enriched records partitioned by `year=/month=/day=/hour=`
- **Write-ahead commit log**: CSV files mapping each checkpoint to its committed S3 data files (written eagerly in `processElement()` before checkpoint completes; may contain entries for failed checkpoints, consumers filter by checking if the referenced s3_key exists)

## Services

**Docker Compose:**

| Service | URL | Credentials |
|---------|-----|-------------|
| Flink Web UI | http://localhost:8081 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka | localhost:29092 (host) / kafka:9092 (internal) | - |

**Kubernetes (via port-forward):**

| Service | Command | URL |
|---------|---------|-----|
| Flink Web UI | `kubectl port-forward svc/flink-lineage-rest 8081:8081 -n flink-lineage` | http://localhost:8081 |
| MinIO Console | `kubectl port-forward svc/minio 9001:9001 -n flink-lineage` | http://localhost:9001 |

## Output

### Data Files

Written to `s3://flink-data/output/` with date-hour partitioning:

```
output/year=2026/month=02/day=24/hour=20/part-*.parquet
```

Schema: `uuid` (string), `timestamp` (long), `kafka_topic` (string), `kafka_partition` (int), `kafka_offset` (long), `checkpoint_id` (long)

### Write-Ahead Commit Log Files

Written to `s3://flink-data/write-ahead-commit-log/` per checkpoint as CSV files:

```
write-ahead-commit-log/chk-1/subtask-0.csv
write-ahead-commit-log/chk-1/subtask-1.csv
write-ahead-commit-log/chk-2/subtask-0.csv
...
```

Columns: `checkpoint_id`, `s3_key`, `commit_timestamp`

The write-ahead commit log is written inside the pre-commit topology. `WriteAheadCommitLogOperator` eagerly writes CSV files in `processElement()` before the checkpoint completes, ensuring entries are durable even if the job crashes before `notifyCheckpointComplete`. Entries for failed checkpoints may exist; consumers filter by checking that the referenced `s3_key` is a finalized file (uncommitted files stay `.inprogress` and get cleaned up).

To query which files belong to a specific checkpoint:

```bash
# Copy write-ahead commit log files from MinIO
docker compose exec minio mc cp --recursive local/flink-data/write-ahead-commit-log/ /tmp/wal/
docker compose cp minio:/tmp/wal /tmp/write-ahead-commit-log

# List files committed by checkpoint 5
cat /tmp/write-ahead-commit-log/chk-5/subtask-*.csv
```

## Configuration

Environment variables (set in `docker-compose.yml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `lineage-input` | Source Kafka topic |
| `OUTPUT_PATH` | `s3://flink-data/output` | Data sink path |
| `WRITE_AHEAD_COMMIT_LOG_PATH` | `s3://flink-data/write-ahead-commit-log` | Write-ahead commit log base path |

Flink settings are in `flink-conf/flink-conf.yaml`:
- Checkpointing: 60s interval, EXACTLY_ONCE
- State backend: RocksDB with incremental checkpoints
- Parallelism: 2
- TaskManager: 2 slots, 2 GB memory

## Project Structure

```
flink-lineage/
├── avro/
│   └── input_event.avsc              # Avro schema for source messages
├── data-generator/
│   ├── Dockerfile
│   ├── generator.py                   # Kafka producer (10 msgs/sec)
│   └── requirements.txt
├── flink-conf/
│   └── flink-conf.yaml               # Flink runtime configuration
├── flink-job/
│   ├── pom.xml
│   └── src/main/java/com/lineage/
│       ├── AvroSchema.java                        # Schema definitions (input, enriched)
│       ├── WriteAheadCommitLogOperator.java       # Pre-commit operator (eager CSV write-ahead commit log)
│       ├── WriteAheadCommitLogFileSink.java        # FileSink wrapper with write-ahead commit log topology
│       ├── EnrichFunction.java                    # Avro deserialization + Kafka metadata enrichment
│       ├── EnrichedEvent.java                     # Enriched event POJO
│       ├── InputEvent.java                        # Input event POJO
│       └── LineageJob.java                        # Main Flink job (source, sink, wiring)
├── helm/flink-lineage/
│   ├── Chart.yaml
│   ├── values.yaml                       # Base defaults
│   ├── values-local.yaml                 # Kind + MinIO overrides
│   ├── values-aws.yaml                   # EKS + MSK + IRSA overrides
│   └── templates/
│       ├── _helpers.tpl                  # Naming, labels helpers
│       ├── serviceaccount.yaml           # With optional IRSA annotation
│       ├── rbac.yaml                     # Role/RoleBinding for Flink pods
│       ├── flink-deployment.yaml         # FlinkDeployment CRD
│       ├── data-generator.yaml           # Python data generator Deployment
│       ├── kafka.yaml                    # Kafka StatefulSet (local dev)
│       └── minio.yaml                    # MinIO Deployment + init Job (local dev)
├── scripts/
│   ├── build-and-run.sh              # Docker Compose build and deploy
│   ├── kind-setup.sh                 # Create Kind cluster + install operator
│   └── deploy.sh                     # Build images, load/push, helm install
├── docker-compose.yml
├── Dockerfile.flink
└── README.md
```

## Stopping

**Docker Compose:**

```bash
docker compose down       # keep data
docker compose down -v    # remove MinIO data volume
```

**Kubernetes:**

```bash
helm uninstall flink-lineage -n flink-lineage
kind delete cluster --name flink-lineage
```
