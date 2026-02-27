# Flink Lineage Pipeline

Kafka-to-S3 pipeline that reads Avro messages from Kafka, enriches them with Kafka metadata (topic, partition, offset), and writes Parquet files to S3-compatible storage (MinIO). Includes a write-ahead commit log that maps each checkpoint to the S3 data files it committed.

## Prerequisites

- Docker and Docker Compose
- Java 11+
- Maven 3.6+
- Standard Unix tools (for querying write-ahead commit log CSV files)

## Quick Start

```bash
./scripts/build-and-run.sh
```

This script:
1. Builds the Flink job JAR with Maven
2. Builds Docker images
3. Starts all services (Kafka, ZooKeeper, MinIO, Flink, data generator)
4. Waits for Flink JobManager to be ready
5. Submits the Flink job

Wait ~2 minutes for data to appear in MinIO.

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
WriteAheadCommitLogSink (wraps FileSink)
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

| Service | URL | Credentials |
|---------|-----|-------------|
| Flink Web UI | http://localhost:8081 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka | localhost:29092 (host) / kafka:9092 (internal) | - |

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
│       ├── WriteAheadCommitLogSink.java           # FileSink wrapper with write-ahead commit log topology
│       ├── EnrichFunction.java                    # Avro deserialization + Kafka metadata enrichment
│       ├── EnrichedEvent.java                     # Enriched event POJO
│       ├── InputEvent.java                        # Input event POJO
│       └── LineageJob.java                        # Main Flink job (source, sink, wiring)
├── scripts/
│   └── build-and-run.sh              # One-command build and deploy
├── docker-compose.yml
├── Dockerfile.flink
└── README.md
```

## Stopping

```bash
docker compose down
```

Add `-v` to also remove the MinIO data volume:

```bash
docker compose down -v
```
