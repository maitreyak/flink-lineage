#!/usr/bin/env bash
set -euo pipefail

# Check for offset gaps in parquet files referenced by the write-ahead commit log.
#
# Usage: ./check-offset-gaps.sh <docker|kind|aws> <start_checkpoint> <end_checkpoint>
#
# Reads commit log CSVs, downloads the referenced parquet files, and uses
# DuckDB to verify there are no missing offsets per Kafka partition when
# output + dropped records are combined.
#
# Override WAL path with: WAL_S3_PATH=s3://my-bucket/wal ./check-offset-gaps.sh aws 1 4

ENV_TYPE="${1:-}"
START_CHK="${2:-}"
END_CHK="${3:-}"

if [[ -z "${ENV_TYPE}" || -z "${START_CHK}" || -z "${END_CHK}" ]]; then
  echo "Usage: $0 <docker|kind|aws> <start_checkpoint> <end_checkpoint>"
  echo "Example: $0 docker 1 4"
  exit 1
fi

if [[ "${ENV_TYPE}" != "docker" && "${ENV_TYPE}" != "kind" && "${ENV_TYPE}" != "aws" ]]; then
  echo "ERROR: Invalid environment '${ENV_TYPE}'. Must be one of: docker, kind, aws"
  exit 1
fi

if ! command -v duckdb &>/dev/null; then
  echo "ERROR: duckdb is not installed. Install it with: brew install duckdb"
  exit 1
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT

NAMESPACE="flink-lineage"

# Map "docker" argument to internal "compose" label
if [[ "${ENV_TYPE}" == "docker" ]]; then
  ENV_TYPE="compose"
fi

echo "Environment: ${ENV_TYPE}"

# Set WAL S3 path per environment (can be overridden via WAL_S3_PATH env var)
if [[ -z "${WAL_S3_PATH:-}" ]]; then
  case "${ENV_TYPE}" in
    compose|kind) WAL_S3_PATH="s3://flink-data/write-ahead-commit-log" ;;
    aws)          WAL_S3_PATH="s3://flink-commit-log/write-ahead-commit-log" ;;
  esac
fi

echo "WAL path: ${WAL_S3_PATH}"

# Helper: read a file from object storage to stdout
read_file() {
  local s3_path="$1"
  case "${ENV_TYPE}" in
    compose)
      local minio_path="local/${s3_path#s3://}"
      docker compose exec -T minio mc cat "${minio_path}" 2>/dev/null
      ;;
    kind)
      local minio_pod
      minio_pod=$(kubectl get pod -n "${NAMESPACE}" -l app.kubernetes.io/component=minio -o jsonpath='{.items[0].metadata.name}')
      local minio_path="local/${s3_path#s3://}"
      kubectl exec -n "${NAMESPACE}" "${minio_pod}" -- mc cat "${minio_path}" 2>/dev/null
      ;;
    aws)
      aws s3 cp "${s3_path}" - 2>/dev/null
      ;;
  esac
}

# Helper: download a file from object storage to a local path
download_file() {
  local s3_path="$1"
  local local_path="$2"
  case "${ENV_TYPE}" in
    compose)
      local minio_path="local/${s3_path#s3://}"
      docker compose exec -T minio mc cat "${minio_path}" > "${local_path}" 2>/dev/null
      ;;
    kind)
      local minio_pod
      minio_pod=$(kubectl get pod -n "${NAMESPACE}" -l app.kubernetes.io/component=minio -o jsonpath='{.items[0].metadata.name}')
      local minio_path="local/${s3_path#s3://}"
      kubectl exec -n "${NAMESPACE}" "${minio_pod}" -- mc cat "${minio_path}" > "${local_path}" 2>/dev/null
      ;;
    aws)
      aws s3 cp "${s3_path}" "${local_path}" --quiet 2>/dev/null
      ;;
  esac
}

# Ensure mc alias is configured for Kind
if [[ "${ENV_TYPE}" == "kind" ]]; then
  local_minio_pod=$(kubectl get pod -n "${NAMESPACE}" -l app.kubernetes.io/component=minio -o jsonpath='{.items[0].metadata.name}')
  kubectl exec -n "${NAMESPACE}" "${local_minio_pod}" -- mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 || true
fi

echo "Checking offset gaps for checkpoints ${START_CHK} to ${END_CHK}..."
echo ""

# Step 1: Collect all parquet file S3 paths from commit log CSVs
OUTPUT_FILES=""
DROPPED_FILES=""

for chk in $(seq "${START_CHK}" "${END_CHK}"); do
  for st in $(seq 0 15); do
    # Output files
    csv=$(read_file "${WAL_S3_PATH}/chk-${chk}/output-subtask-${st}.csv" || true)
    if [[ -n "${csv}" ]]; then
      paths=$(echo "${csv}" | tail -n +2 | cut -d',' -f2)
      for p in ${paths}; do
        OUTPUT_FILES="${OUTPUT_FILES} ${p}"
      done
    fi

    # Dropped files
    csv=$(read_file "${WAL_S3_PATH}/chk-${chk}/dropped-subtask-${st}.csv" || true)
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

# Step 2: Download parquet files
OUTPUT_DIR="${WORK_DIR}/output"
DROPPED_DIR="${WORK_DIR}/dropped"
mkdir -p "${OUTPUT_DIR}" "${DROPPED_DIR}"

for s3_path in ${OUTPUT_FILES}; do
  filename=$(basename "${s3_path}")
  download_file "${s3_path}" "${OUTPUT_DIR}/${filename}"
done

for s3_path in ${DROPPED_FILES}; do
  filename=$(basename "${s3_path}")
  download_file "${s3_path}" "${DROPPED_DIR}/${filename}"
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
