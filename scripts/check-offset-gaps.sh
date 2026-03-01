#!/usr/bin/env bash
set -euo pipefail

# Check for offset gaps in parquet files referenced by the write-ahead commit log.
#
# Usage: ./check-offset-gaps.sh <start_checkpoint> <end_checkpoint>
#
# Reads commit log CSVs from MinIO (via docker compose exec), downloads the
# referenced parquet files, and uses DuckDB to verify there are no missing
# offsets per Kafka partition when output + dropped records are combined.

START_CHK="${1:-}"
END_CHK="${2:-}"

if [[ -z "${START_CHK}" || -z "${END_CHK}" ]]; then
  echo "Usage: $0 <start_checkpoint> <end_checkpoint>"
  echo "Example: $0 1 4"
  exit 1
fi

if ! command -v duckdb &>/dev/null; then
  echo "ERROR: duckdb is not installed. Install it with: brew install duckdb"
  exit 1
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT

MINIO_CONTAINER=$(docker compose ps -q minio 2>/dev/null)
if [[ -z "${MINIO_CONTAINER}" ]]; then
  echo "ERROR: MinIO container is not running. Start services with: docker compose up -d"
  exit 1
fi

WAL_PREFIX="local/flink-data/write-ahead-commit-log"

echo "Checking offset gaps for checkpoints ${START_CHK} to ${END_CHK}..."
echo ""

# Step 1: Collect all parquet file S3 paths from commit log CSVs
OUTPUT_FILES=""
DROPPED_FILES=""

for chk in $(seq "${START_CHK}" "${END_CHK}"); do
  for st in 0 1; do
    # Output files
    csv=$(docker compose exec -T minio mc cat "${WAL_PREFIX}/chk-${chk}/output-subtask-${st}.csv" 2>/dev/null || true)
    if [[ -n "${csv}" ]]; then
      paths=$(echo "${csv}" | tail -n +2 | cut -d',' -f2)
      for p in ${paths}; do
        OUTPUT_FILES="${OUTPUT_FILES} ${p}"
      done
    fi

    # Dropped files
    csv=$(docker compose exec -T minio mc cat "${WAL_PREFIX}/chk-${chk}/dropped-subtask-${st}.csv" 2>/dev/null || true)
    if [[ -n "${csv}" ]]; then
      paths=$(echo "${csv}" | tail -n +2 | cut -d',' -f2)
      for p in ${paths}; do
        DROPPED_FILES="${DROPPED_FILES} ${p}"
      done
    fi
  done
done

if [[ -z "${OUTPUT_FILES}" && -z "${DROPPED_FILES}" ]]; then
  echo "ERROR: No files found in commit log for checkpoints ${START_CHK}-${END_CHK}"
  exit 1
fi

# Step 2: Download parquet files from MinIO
OUTPUT_DIR="${WORK_DIR}/output"
DROPPED_DIR="${WORK_DIR}/dropped"
mkdir -p "${OUTPUT_DIR}" "${DROPPED_DIR}"

for s3_path in ${OUTPUT_FILES}; do
  # Convert s3://flink-data/output/... -> local/flink-data/output/...
  minio_path="local/${s3_path#s3://}"
  filename=$(basename "${s3_path}")
  docker compose exec -T minio mc cat "${minio_path}" > "${OUTPUT_DIR}/${filename}"
done

for s3_path in ${DROPPED_FILES}; do
  minio_path="local/${s3_path#s3://}"
  filename=$(basename "${s3_path}")
  docker compose exec -T minio mc cat "${minio_path}" > "${DROPPED_DIR}/${filename}"
done

output_count=$(find "${OUTPUT_DIR}" -type f | wc -l | tr -d ' ')
dropped_count=$(find "${DROPPED_DIR}" -type f | wc -l | tr -d ' ')
echo "Downloaded ${output_count} output files, ${dropped_count} dropped files"
echo ""

# Step 3: Build DuckDB file lists
OUTPUT_GLOB="${OUTPUT_DIR}/*"
DROPPED_GLOB="${DROPPED_DIR}/*"

# Step 4: Run gap analysis with DuckDB
RESULT=$(duckdb -csv -c "
WITH all_records AS (
    SELECT kafka_partition, kafka_offset
    FROM parquet_scan('${OUTPUT_GLOB}')
    UNION ALL
    SELECT kafka_partition, kafka_offset
    FROM parquet_scan('${DROPPED_GLOB}')
),
per_partition AS (
    SELECT kafka_partition,
           MIN(kafka_offset) AS min_offset,
           MAX(kafka_offset) AS max_offset,
           COUNT(*) AS record_count,
           COUNT(DISTINCT kafka_offset) AS distinct_offsets,
           MAX(kafka_offset) - MIN(kafka_offset) + 1 AS expected_count
    FROM all_records
    GROUP BY kafka_partition
),
summary AS (
    SELECT *,
           expected_count - distinct_offsets AS missing_offsets,
           record_count - distinct_offsets AS duplicate_offsets
    FROM per_partition
)
SELECT * FROM summary ORDER BY kafka_partition;
")

# Display results
echo "=== Offset Analysis (checkpoints ${START_CHK}-${END_CHK}) ==="
echo ""
duckdb -c "
WITH all_records AS (
    SELECT kafka_partition, kafka_offset
    FROM parquet_scan('${OUTPUT_GLOB}')
    UNION ALL
    SELECT kafka_partition, kafka_offset
    FROM parquet_scan('${DROPPED_GLOB}')
),
per_partition AS (
    SELECT kafka_partition,
           MIN(kafka_offset) AS min_offset,
           MAX(kafka_offset) AS max_offset,
           COUNT(*) AS record_count,
           COUNT(DISTINCT kafka_offset) AS distinct_offsets,
           MAX(kafka_offset) - MIN(kafka_offset) + 1 AS expected_count
    FROM all_records
    GROUP BY kafka_partition
),
summary AS (
    SELECT *,
           expected_count - distinct_offsets AS missing_offsets,
           record_count - distinct_offsets AS duplicate_offsets
    FROM per_partition
)
SELECT * FROM summary ORDER BY kafka_partition;
"

# Step 5: Check for failures
TOTAL_MISSING=$(echo "${RESULT}" | tail -n +2 | awk -F',' '{sum += $7} END {print sum+0}')
TOTAL_DUPLICATES=$(echo "${RESULT}" | tail -n +2 | awk -F',' '{sum += $8} END {print sum+0}')

echo ""
if [[ "${TOTAL_MISSING}" -gt 0 ]]; then
  echo "FAIL: ${TOTAL_MISSING} missing offset(s) detected"
  echo ""
  echo "Gap details:"
  duckdb -c "
  WITH all_records AS (
      SELECT kafka_partition, kafka_offset
      FROM parquet_scan('${OUTPUT_GLOB}')
      UNION ALL
      SELECT kafka_partition, kafka_offset
      FROM parquet_scan('${DROPPED_GLOB}')
  ),
  with_next AS (
      SELECT kafka_partition, kafka_offset,
             LEAD(kafka_offset) OVER (PARTITION BY kafka_partition ORDER BY kafka_offset) AS next_offset
      FROM (SELECT DISTINCT kafka_partition, kafka_offset FROM all_records)
  )
  SELECT kafka_partition,
         kafka_offset AS gap_after,
         next_offset AS resumes_at,
         next_offset - kafka_offset - 1 AS missing_count
  FROM with_next
  WHERE next_offset IS NOT NULL AND next_offset - kafka_offset > 1
  ORDER BY kafka_partition, kafka_offset
  LIMIT 50;
  "
  exit 1
elif [[ "${TOTAL_DUPLICATES}" -gt 0 ]]; then
  echo "WARN: ${TOTAL_DUPLICATES} duplicate offset(s) detected (at-least-once delivery)"
  echo "PASS: No missing offsets"
  exit 0
else
  echo "PASS: No missing offsets, no duplicates"
  exit 0
fi
