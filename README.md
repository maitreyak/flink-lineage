# Flink Lineage Pipeline

Kafka-to-S3 pipeline that reads Avro messages from Kafka, enriches them with Kafka metadata (topic, partition, offset), and writes Parquet files to S3-compatible storage (MinIO). Includes a commit log that maps each checkpoint to the S3 data files it committed.

## Prerequisites

- Docker and Docker Compose
- Java 11+
- Maven 3.6+
- DuckDB (for querying commit log Parquet files)

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
CommitLoggingFileSink (wraps FileSink)
  - Pre-commit topology: CommitLogExtractingOperator
    extracts (checkpoint_id, s3_key) from committable messages
    emits as side output → second transactional FileSink (Parquet)
    |
    v
FileSink --> s3://flink-data/output/          (enriched data, partitioned by date/hour)
FileSink --> s3://flink-data/commit-log/      (commit log Parquet, partitioned by checkpoint)
```

### Data Flow

- **Data generator**: Produces ~10 Avro messages/sec across 10 Kafka partitions
- **Flink job**: Consumes from Kafka, enriches with metadata, writes Parquet to S3
- **Data sink**: Full enriched records partitioned by `year=/month=/day=/hour=`
- **Commit log**: Parquet files mapping each checkpoint to its committed S3 data files (transactional — rolls back with the data sink)

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

### Commit Log Files

Written to `s3://flink-data/commit-log/` per checkpoint as Parquet files:

```
commit-log/chk-1/part-0-0.parquet
commit-log/chk-2/part-0-0.parquet
...
```

Schema: `checkpoint_id` (long), `s3_key` (string), `commit_timestamp` (timestamp-millis)

The commit log is written by a second transactional `FileSink` inside the pre-commit topology. `CommitLogExtractingOperator` emits commit log records as side output, which are captured and written to Parquet. Both the data sink and commit log sink participate in Flink's two-phase commit — on rollback, both roll back (no orphaned commit log files).

To query which files belong to a specific checkpoint:

```bash
# Copy commit log files from MinIO
docker compose exec minio mc cp --recursive local/flink-data/commit-log/ /tmp/cl/
docker compose cp minio:/tmp/cl /tmp/commit-log

# Query with DuckDB
duckdb -c "SELECT checkpoint_id, s3_key, commit_timestamp FROM read_parquet('/tmp/commit-log/chk-*/part-*.parquet') WHERE checkpoint_id = 5"
```

## Configuration

Environment variables (set in `docker-compose.yml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `lineage-input` | Source Kafka topic |
| `OUTPUT_PATH` | `s3://flink-data/output` | Data sink path |
| `COMMIT_LOG_PATH` | `s3://flink-data/commit-log` | Commit log base path |

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
│       ├── AvroSchema.java                      # Schema definitions (input, enriched)
│       ├── CommitLogExtractingOperator.java     # Pre-commit topology operator (side output)
│       ├── CommitLogParquetWriterOperator.java  # Parquet writer for commit log records
│       ├── CommitLoggingFileSink.java           # FileSink wrapper with commit log topology
│       ├── EnrichFunction.java                  # Avro deserialization + Kafka metadata enrichment
│       ├── EnrichedEvent.java                   # Enriched event POJO
│       ├── InputEvent.java                      # Input event POJO
│       └── LineageJob.java                      # Main Flink job (source, sink, wiring)
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
