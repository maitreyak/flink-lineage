# Flink Lineage Pipeline

Kafka-to-S3 pipeline that reads Avro messages from Kafka, enriches them with Kafka metadata (topic, partition, offset), and writes Parquet files to S3-compatible storage (MinIO). Includes a write-ahead commit log that maps each checkpoint to the S3 data files it committed.

## Prerequisites

- Docker
- Java 11+
- Maven 3.6+
- [DuckDB](https://duckdb.org/) (for offset gap verification: `brew install duckdb`)

**For AWS deployment (EKS):**
- [AWS CLI](https://aws.amazon.com/cli/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/) configured for EKS
- [Helm](https://helm.sh/)

## Quick Start (Docker Compose)

```bash
./scripts/build-and-run.sh
```

This builds the JARs and Docker images, starts all services (Kafka, ZooKeeper, MinIO, Flink producer, Flink consumer), and submits the Flink job. Wait ~2 minutes for data to appear in MinIO.

## AWS Deployment (EKS + MSK + S3)

### Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/) configured with appropriate credentials
- An EKS cluster with `kubectl` context configured
- An MSK cluster with TLS listeners (port 9094)
- S3 buckets for data output and commit log
- An IAM role with S3 access and an OIDC trust policy for IRSA
- An ECR repository for `flink-lineage`

### 1. Create ECR repository

```bash
aws ecr create-repository --repository-name flink-lineage --region us-east-1
```

### 2. Install the Flink Operator

```bash
kubectl create namespace flink-lineage
helm repo add flink-operator-repo https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.10.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-lineage \
  --set webhook.create=false
```

### 3. Configure values-aws.yaml

Update `helm/flink-lineage/values-aws.yaml` with your:
- ECR image repository URIs
- MSK bootstrap server addresses
- S3 bucket names
- IRSA role ARN for the service account

### 4. Build and deploy

```bash
ECR_REGISTRY=<account-id>.dkr.ecr.<region>.amazonaws.com AWS_REGION=<region> scripts/deploy.sh aws
```

This builds the JARs and Docker image (targeting `linux/amd64`), pushes to ECR, and deploys the Helm chart with `values-aws.yaml`. Both the producer and consumer run from the same Docker image with different `entryClass` configurations.

### 5. Verify

```bash
# All pods should be Running
kubectl get pods -n flink-lineage

# FlinkDeployment should show RUNNING / STABLE
kubectl get flinkdeployment -n flink-lineage

# Check S3 for output
aws s3 ls s3://<output-bucket>/output/ --recursive
aws s3 ls s3://<commit-log-bucket>/write-ahead-commit-log/ --recursive
```

### 6. Tear down

```bash
helm uninstall flink-lineage -n flink-lineage
helm uninstall flink-kubernetes-operator -n flink-lineage
kubectl delete namespace flink-lineage
```

## Architecture

```
Flink ProducerJob (DataGeneratorSource, ~10 msgs/sec)
    |
    v
Kafka (lineage-input topic, 10 partitions)
    |
    v
Flink LineageJob: RawKafkaDeserializer (pass-through ConsumerRecord)
    |
    v
Flink LineageJob: EnrichFunction
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
         +-> s3://flink-data/write-ahead-commit-log/    (write-ahead commit log CSV, partitioned by date/hour/minute)
```

### Data Flow

- **Flink producer** (`ProducerJob`): Generates Avro messages at a configurable rate using `DataGeneratorSource` and writes them to Kafka via `KafkaSink`
- **Flink consumer** (`LineageJob`): Consumes from Kafka, enriches with metadata, writes Parquet to S3
- **Data sink**: Full enriched records partitioned by `year=/month=/day=/hour=`
- **Write-ahead commit log**: CSV files mapping each checkpoint to its committed S3 data files (written eagerly in `processElement()` before checkpoint completes; may contain entries for failed checkpoints, consumers filter by checking if the referenced s3_key exists)

## Services

**Docker Compose:**

| Service | URL | Credentials |
|---------|-----|-------------|
| Flink Web UI | http://localhost:8081 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka | localhost:29092 (host) / kafka:9092 (internal) | - |

**Kubernetes — AWS EKS (via port-forward):**

| Service | Command | URL |
|---------|---------|-----|
| Flink Consumer UI | `kubectl port-forward svc/flink-lineage-rest 8081:8081 -n flink-lineage` | http://localhost:8081 |
| Flink Producer UI | `kubectl port-forward svc/flink-lineage-producer-rest 8082:8081 -n flink-lineage` | http://localhost:8082 |

## Output

### Data Files

Written to `s3://flink-data/output/` with date-hour partitioning:

```
output/year=2026/month=02/day=24/hour=20/part-*.parquet
```

Schema: `uuid` (string), `timestamp` (long), `kafka_topic` (string), `kafka_partition` (int), `kafka_offset` (long), `checkpoint_id` (long)

### Write-Ahead Commit Log Files

Written to `s3://flink-data/write-ahead-commit-log/` with date/hour/minute partitioning (UTC) as CSV files:

```
write-ahead-commit-log/03-03-2026/00/01/output-subtask-0-<uuid>.csv
write-ahead-commit-log/03-03-2026/00/01/output-subtask-1-<uuid>.csv
write-ahead-commit-log/03-03-2026/00/02/output-subtask-0-<uuid>.csv
...
```

Columns: `checkpoint_id`, `s3_key`, `commit_timestamp`

The write-ahead commit log is written inside the pre-commit topology. `WriteAheadCommitLogOperator` eagerly writes CSV files in `processElement()` before the checkpoint completes, ensuring entries are durable even if the job crashes before `notifyCheckpointComplete`. Entries for failed checkpoints may exist; consumers filter by checking that the referenced `s3_key` is a finalized file (uncommitted files stay `.inprogress` and get cleaned up).

To query which files belong to a specific checkpoint:

```bash
# Copy write-ahead commit log files from MinIO
docker compose exec minio mc cp --recursive local/flink-data/write-ahead-commit-log/ /tmp/wal/
docker compose cp minio:/tmp/wal /tmp/write-ahead-commit-log

# List files committed by checkpoint 5 (search all date partitions, filter by checkpoint_id)
grep -r '^5,' /tmp/write-ahead-commit-log/
```

### Offset Gap Verification

Use `scripts/check-offset-gaps.sh` to verify there are no missing Kafka offsets across a date range. The script reads the write-ahead commit log CSVs for the specified day(s), downloads the referenced parquet files (both output and dropped), and uses DuckDB to check for gaps.

```bash
# Usage: ./scripts/check-offset-gaps.sh <docker|aws> <start_date> <end_date>
#   Date formats: 'MM-DD-YYYY' (full day) or 'MM-DD-YYYY HH:MM' (minute-level)

# Full day (all commit log entries for that day)
./scripts/check-offset-gaps.sh aws '03-03-2026' '03-03-2026'
./scripts/check-offset-gaps.sh docker '03-03-2026' '03-03-2026'

# Minute-level range (filters by commit_timestamp)
./scripts/check-offset-gaps.sh aws '03-03-2026 21:37' '03-03-2026 21:39'

# Override WAL path
WAL_S3_PATH=s3://my-bucket/wal ./scripts/check-offset-gaps.sh aws '03-03-2026' '03-03-2026'
```

Requires [DuckDB](https://duckdb.org/) (`brew install duckdb`).

## Configuration

### Producer (`ProducerJob`)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `lineage-input` | Target Kafka topic |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Kafka security protocol (`PLAINTEXT` or `SSL` for MSK TLS) |
| `RATE_PER_SEC` | `10` | Message generation rate per second |

### Consumer (`LineageJob`)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `lineage-input` | Source Kafka topic |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Kafka security protocol (`PLAINTEXT` or `SSL` for MSK TLS) |
| `OUTPUT_PATH` | `s3://flink-data/output` | Data sink path |
| `WRITE_AHEAD_COMMIT_LOG_PATH` | `s3://flink-data/write-ahead-commit-log` | Write-ahead commit log base path |
| `DROPPED_PATH` | `s3://flink-data/dropped` | Path for dropped/invalid records |

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
├── flink-conf/
│   └── flink-conf.yaml               # Flink runtime configuration
├── flink-job/
│   ├── pom.xml
│   └── src/main/java/com/lineage/
│       ├── AvroSchema.java                        # Schema definitions (input, enriched)
│       ├── WriteAheadCommitLogOperator.java       # Pre-commit operator (eager CSV write-ahead commit log)
│       ├── WriteAheadCommitLogFileSink.java       # FileSink wrapper with write-ahead commit log topology
│       ├── EnrichFunction.java                    # Avro deserialization + Kafka metadata enrichment
│       ├── EnrichedEvent.java                     # Enriched event POJO
│       ├── InputEvent.java                        # Input event POJO
│       └── LineageJob.java                        # Main Flink job (source, sink, wiring)
├── flink-producer/
│   ├── pom.xml
│   └── src/main/java/com/lineage/producer/
│       ├── ProducerJob.java                       # Flink DataGeneratorSource → KafkaSink
│       └── ByteArraySerializationSchema.java      # Byte array passthrough serializer
├── helm/flink-lineage/
│   ├── Chart.yaml
│   ├── values.yaml                       # Base defaults
│   ├── values-aws.yaml                   # EKS + MSK + IRSA overrides
│   └── templates/
│       ├── _helpers.tpl                  # Naming, labels helpers
│       ├── serviceaccount.yaml           # With optional IRSA annotation
│       ├── rbac.yaml                     # Role/RoleBinding for Flink pods
│       ├── flink-deployment.yaml         # FlinkDeployment CRD (consumer)
│       └── flink-producer.yaml           # FlinkDeployment CRD (producer)
├── scripts/
│   ├── build-and-run.sh              # Docker Compose build and deploy
│   ├── check-offset-gaps.sh          # Verify no offset gaps across checkpoints
│   └── deploy.sh                     # Build images, push to ECR, helm install
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

**Kubernetes (AWS EKS):**

```bash
helm uninstall flink-lineage -n flink-lineage
helm uninstall flink-kubernetes-operator -n flink-lineage
kubectl delete namespace flink-lineage
```
