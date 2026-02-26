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

Schema: `uuid` (string), `timestamp` (long), `kafka_topic` (string), `kafka_partition` (int), `kafka_offset` (long), `checkpoint_id` (long)

### Offset Index Files

Written to `s3://flink-data/offset-index/` per checkpoint:

```
offset-index/chk-1/subtask-0.parquet
offset-index/chk-1/subtask-1.parquet
offset-index/chk-2/subtask-0.parquet
...
```

Schema: `kafka_partition` (int), `min_offset` (long), `max_offset` (long), `record_count` (long), `checkpoint_id` (long), `window_start` (timestamp)

Each file is ~1.6 KB. A new set of files is produced every checkpoint interval (60s).

## Gap Detection

Query the offset index to detect dropped Kafka messages without scanning data files. The query uses `checkpoint_id` to deduplicate overlapping windows that can occur after Flink recovery:

```sql
-- Checkpoint-aware gap detection (deduplicated by checkpoint_id)
WITH deduped AS (
    SELECT kafka_partition, checkpoint_id,
           min(min_offset) AS min_offset,
           max(max_offset) AS max_offset,
           sum(record_count) AS record_count
    FROM read_parquet('offset-index/chk-*/*.parquet')
    GROUP BY kafka_partition, checkpoint_id
)
SELECT kafka_partition,
       min(min_offset) AS first_offset,
       max(max_offset) AS last_offset,
       sum(record_count) AS total_rows,
       max(max_offset) - min(min_offset) + 1 AS expected_rows,
       max(max_offset) - min(min_offset) + 1 - sum(record_count) AS gaps
FROM deduped
GROUP BY kafka_partition;
```

To run this locally after copying files from MinIO:

```bash
# Copy index files from the MinIO container to the host
docker compose exec minio mc cp --recursive local/flink-data/offset-index/ /tmp/oi/
docker compose cp minio:/tmp/oi /tmp/offset-index

# Run the checkpoint-aware gap detection query
duckdb -c "
WITH deduped AS (
    SELECT kafka_partition, checkpoint_id,
           min(min_offset) AS min_offset,
           max(max_offset) AS max_offset,
           sum(record_count) AS record_count
    FROM read_parquet('/tmp/offset-index/chk-*/*.parquet')
    GROUP BY kafka_partition, checkpoint_id
)
SELECT kafka_partition,
       min(min_offset) AS first_offset,
       max(max_offset) AS last_offset,
       sum(record_count) AS total_rows,
       max(max_offset) - min(min_offset) + 1 AS expected_rows,
       max(max_offset) - min(min_offset) + 1 - sum(record_count) AS gaps
FROM deduped
GROUP BY kafka_partition
ORDER BY kafka_partition;
"
```

A `gaps` value of 0 means no dropped messages for that partition.

### Duplicate Detection

After a recovery, the same offsets may appear under multiple checkpoint IDs. Use this query to find overlapping checkpoint windows:

```sql
SELECT a.kafka_partition,
       a.checkpoint_id AS chk_a,
       b.checkpoint_id AS chk_b,
       greatest(a.min_offset, b.min_offset) AS overlap_start,
       least(a.max_offset, b.max_offset) AS overlap_end
FROM read_parquet('/tmp/offset-index/chk-*/*.parquet') a
JOIN read_parquet('/tmp/offset-index/chk-*/*.parquet') b
  ON a.kafka_partition = b.kafka_partition
 AND a.checkpoint_id < b.checkpoint_id
 AND a.max_offset >= b.min_offset;
```

## Caveats

The CheckpointListener-based offset index trades operational simplicity (one fewer operator and sink) for some failure-mode trade-offs compared to the previous side-output approach.

### 1. S3 write failure kills the data path

If S3/MinIO is unreachable during `notifyCheckpointComplete()`, the exception propagates and **fails the task**, triggering a restart — even though the checkpoint and data writes succeeded. The old side-output approach had independent sinks, so index write failures couldn't affect data throughput.

**Severity**: High. A non-critical sidecar write can cause availability loss on the primary data path.

### 2. Orphaned index files after checkpoint rollback

Index files are written in `notifyCheckpointComplete()` outside of Flink's transactional sink protocol. If a downstream operator fails after the index is written but before the checkpoint fully commits, Flink rolls back to the previous checkpoint. The data sink discards uncommitted files, but the **index file persists on S3** — referencing offsets that no longer appear in any data file.

**Severity**: Medium. Gap detection may undercount gaps (false negatives) for the affected checkpoint window. **Mitigated**: the `checkpoint_id` column lets you identify and exclude orphaned index records by filtering to only checkpoint IDs present in the data files.

### 3. Duplicate offset counts after recovery

After a failure, Flink restores from the last successful checkpoint and replays Kafka records. The replayed window produces a new `chk-{id}` index with a new checkpoint ID but **overlapping offsets** with the orphaned index from the failed attempt. The gap detection query sums `record_count` across all files, so overlapping windows **double-count** offsets, potentially showing negative gap values.

**Severity**: Medium. **Resolved**: the `checkpoint_id` column in both data and index records lets the gap detection query group by checkpoint window, preventing double-counting of replayed offsets. See the checkpoint-aware gap detection and duplicate detection queries above.

### 4. Memory leak from abandoned checkpoints

If a checkpoint times out at the coordinator without sending a completion or abort notification, the corresponding snapshot stays in the `pendingSnapshots` ConcurrentHashMap indefinitely. With frequent timeouts, this slowly leaks memory.

**Severity**: Low. Unlikely under normal operation (Flink usually sends abort notifications), but possible under sustained checkpoint pressure.

### 5. Wider blast radius per subtask

The old approach used `keyBy(partition)` to isolate each partition's state. Now each subtask handles multiple partitions (e.g., 5 partitions per subtask with parallelism=2). If a subtask's index write fails mid-file, offset data for **all partitions on that subtask** is lost, not just one.

**Severity**: Low-Medium. Increases the scope of a single write failure.

### 6. No backpressure from index writes

The old FileSink participated in Flink's backpressure mechanism — slow S3 writes naturally slowed the pipeline. The direct `AvroParquetWriter` in `notifyCheckpointComplete()` runs outside backpressure. A slow write blocks the checkpoint completion callback, potentially delaying subsequent checkpoints and causing a timeout cascade.

**Severity**: Medium. Most visible during S3 latency spikes.

### Possible mitigations

- Wrap the index write in a try/catch to log-and-skip on failure, decoupling it from the data path (addresses #1)
- Write index files asynchronously in a background thread (addresses #1 and #6)
- ~~Include the checkpoint ID in index records and deduplicate in the gap detection query (addresses #3)~~ — **Done**: `checkpoint_id` is now included in both enriched data and offset index records
- Add a TTL or max-size bound to `pendingSnapshots` (addresses #4)

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
