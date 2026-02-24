# Flink Lineage Pipeline

Kafka-to-S3 pipeline that reads Avro messages from Kafka, enriches them with Kafka metadata (topic, partition, offset), and writes Parquet files to S3-compatible storage (MinIO). Includes checkpoint-aligned offset indexing for lightweight gap detection without full data scans.

## Prerequisites

- Docker and Docker Compose
- Java 11+
- Maven 3.6+
- [DuckDB CLI](https://duckdb.org/docs/installation/) (for querying Parquet output)

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
Flink: OffsetTrackingEnrichFunction
  - Deserializes Avro, adds kafka_topic/partition/offset
  - Tracks per-partition offset ranges in-memory
  - Writes Parquet index files on checkpoint completion (CheckpointListener)
    |
    v
FileSink --> s3://flink-data/output/          (enriched data, partitioned by date/hour)
         +-> s3://flink-data/offset-index/    (index files written directly on checkpoint)
```

### Data Flow

- **Data generator**: Produces ~10 Avro messages/sec across 10 Kafka partitions
- **Flink job**: Consumes from Kafka, enriches with metadata, writes Parquet to S3
- **Data sink**: Full enriched records partitioned by `year=/month=/day=/hour=`
- **Offset index**: Per-partition offset summaries written directly as Parquet on each checkpoint — no second Flink sink needed

### Offset Index Design

The `OffsetTrackingEnrichFunction` combines enrichment and offset tracking in a single operator:

1. **`processElement()`** — deserializes Avro, enriches with Kafka metadata, and updates in-memory per-partition accumulators (min/max offset, count)
2. **`snapshotState()`** — serializes accumulators to Flink `ListState` for fault tolerance and stashes a copy for the pending checkpoint
3. **`notifyCheckpointComplete()`** — writes a small Parquet file per subtask to `s3://flink-data/offset-index/chk-{id}/subtask-{index}.parquet` using Flink's `FileSystem` API

This replaces the previous approach of a separate `keyBy → OffsetIndexFunction → FileSink` branch, eliminating one operator, its keyed state, and a second file writer.

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

Schema: `uuid` (string), `timestamp` (long), `kafka_topic` (string), `kafka_partition` (int), `kafka_offset` (long)

### Offset Index Files

Written to `s3://flink-data/offset-index/` per checkpoint:

```
offset-index/chk-1/subtask-0.parquet
offset-index/chk-1/subtask-1.parquet
offset-index/chk-2/subtask-0.parquet
...
```

Schema: `kafka_partition` (int), `min_offset` (long), `max_offset` (long), `record_count` (long), `window_start` (timestamp)

Each file is ~1.6 KB. A new set of files is produced every checkpoint interval (60s).

## Gap Detection

Query the offset index to detect dropped Kafka messages without scanning data files:

```sql
SELECT kafka_partition,
       min(min_offset) AS first_offset,
       max(max_offset) AS last_offset,
       sum(record_count) AS total_rows,
       max(max_offset) - min(min_offset) + 1 AS expected_rows,
       max(max_offset) - min(min_offset) + 1 - sum(record_count) AS gaps
FROM read_parquet('offset-index/chk-*/*.parquet')
GROUP BY kafka_partition;
```

To run this locally after copying files from MinIO:

```bash
# Copy index files from the MinIO container to the host
docker compose exec minio mc cp --recursive local/flink-data/offset-index/ /tmp/oi/
docker compose cp minio:/tmp/oi /tmp/offset-index

# Run the gap detection query
duckdb -c "
SELECT kafka_partition,
       min(min_offset) AS first_offset,
       max(max_offset) AS last_offset,
       sum(record_count) AS total_rows,
       max(max_offset) - min(min_offset) + 1 AS expected_rows,
       max(max_offset) - min(min_offset) + 1 - sum(record_count) AS gaps
FROM read_parquet('/tmp/offset-index/chk-*/*.parquet')
GROUP BY kafka_partition
ORDER BY kafka_partition;
"
```

A `gaps` value of 0 means no dropped messages for that partition.

## Configuration

Environment variables (set in `docker-compose.yml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `lineage-input` | Source Kafka topic |
| `OUTPUT_PATH` | `s3://flink-data/output` | Data sink path |
| `OFFSET_INDEX_PATH` | `s3://flink-data/offset-index` | Offset index base path |

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
│       ├── AvroSchema.java                      # Schema definitions (input, enriched, offset index)
│       ├── EnrichedEvent.java                   # Enriched event POJO
│       ├── InputEvent.java                      # Input event POJO
│       ├── LineageJob.java                      # Main Flink job (source, sink, wiring)
│       └── OffsetTrackingEnrichFunction.java    # Enrich + checkpoint-based offset index
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
