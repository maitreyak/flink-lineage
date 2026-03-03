#!/usr/bin/env bash
set -euo pipefail

# Check for offset gaps in parquet files referenced by the write-ahead commit log.
#
# Usage: ./check-offset-gaps.sh <docker|aws> <start_date> <end_date>
#   Date formats: 'MM-DD-YYYY' (full day) or 'MM-DD-YYYY HH:MM' (minute-level)
#
# Examples:
#   $0 aws '03-03-2026' '03-03-2026'
#   $0 aws '03-03-2026 21:37' '03-03-2026 21:39'
#   $0 docker '03-03-2026' '03-03-2026'
#
# Reads commit log CSVs, downloads the referenced parquet files, and uses
# DuckDB to verify there are no missing offsets per Kafka partition when
# output + dropped records are combined.
#
# AWS mode uses DuckDB's native S3 support (httpfs) to read files directly,
# avoiding slow sequential downloads. Docker Compose mode downloads via docker.
#
# Override WAL path with: WAL_S3_PATH=s3://my-bucket/wal ./check-offset-gaps.sh aws '03-03-2026' '03-03-2026'

ENV_TYPE="${1:-}"
START_DATE="${2:-}"
END_DATE="${3:-}"

if [[ -z "${ENV_TYPE}" || -z "${START_DATE}" || -z "${END_DATE}" ]]; then
  echo "Usage: $0 <docker|aws> <start_date> <end_date>"
  echo "  Date formats: 'MM-DD-YYYY' (full day) or 'MM-DD-YYYY HH:MM' (minute-level)"
  echo "Examples:"
  echo "  $0 aws '03-03-2026' '03-03-2026'"
  echo "  $0 aws '03-03-2026 21:37' '03-03-2026 21:39'"
  exit 1
fi

if [[ "${ENV_TYPE}" != "docker" && "${ENV_TYPE}" != "aws" ]]; then
  echo "ERROR: Invalid environment '${ENV_TYPE}'. Must be one of: docker, aws"
  exit 1
fi

if ! command -v duckdb &>/dev/null; then
  echo "ERROR: duckdb is not installed. Install it with: brew install duckdb"
  exit 1
fi

# Detect date-only vs date+time format
HAS_TIME=false
if [[ "${START_DATE}" =~ ^[0-9]{2}-[0-9]{2}-[0-9]{4}\ [0-9]{2}:[0-9]{2}$ ]]; then
  HAS_TIME=true
fi

# Convert dates to epoch milliseconds for commit_timestamp filtering (macOS compatible)
if [[ "${HAS_TIME}" == true ]]; then
  START_EPOCH=$(date -j -f '%m-%d-%Y %H:%M' "${START_DATE}" +%s)
  END_EPOCH=$(date -j -f '%m-%d-%Y %H:%M' "${END_DATE}" +%s)
  START_EPOCH_MS=$(( START_EPOCH * 1000 ))
  END_EPOCH_MS=$(( (END_EPOCH + 60) * 1000 - 1 ))
  START_DAY=$(date -j -f '%m-%d-%Y %H:%M' "${START_DATE}" +%m-%d-%Y)
  END_DAY=$(date -j -f '%m-%d-%Y %H:%M' "${END_DATE}" +%m-%d-%Y)
else
  START_DAY="${START_DATE}"
  END_DAY="${END_DATE}"
fi

# Enumerate each day in the range as MM-DD-YYYY strings (one per line)
build_days() {
  local start_epoch end_epoch current
  start_epoch=$(date -j -f '%m-%d-%Y' "${START_DAY}" +%s)
  end_epoch=$(date -j -f '%m-%d-%Y' "${END_DAY}" +%s)

  current=$start_epoch
  while [[ $current -le $end_epoch ]]; do
    date -j -r "$current" +%m-%d-%Y
    current=$((current + 86400))
  done
}

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
    compose) WAL_S3_PATH="s3://flink-data/write-ahead-commit-log" ;;
    aws)     WAL_S3_PATH="s3://flink-commit-log/write-ahead-commit-log" ;;
  esac
fi

echo "WAL path: ${WAL_S3_PATH}"
echo "Checking offset gaps for ${START_DATE} to ${END_DATE}..."
echo ""

DB_FILE="${WORK_DIR}/analysis.duckdb"

if [[ "${ENV_TYPE}" == "aws" ]]; then
  # ---------------------------------------------------------------------------
  # AWS: DuckDB reads commit log CSVs and parquet files directly from S3
  # ---------------------------------------------------------------------------

  # Build DuckDB glob list from per-day paths
  GLOB_LIST=""
  for day in $(build_days); do
    if [[ -n "${GLOB_LIST}" ]]; then GLOB_LIST+=", "; fi
    GLOB_LIST+="'${WAL_S3_PATH}/${day}/**/*.csv'"
  done

  # Add WHERE clause for time filtering when HH:MM is specified
  TIME_FILTER=""
  if [[ "${HAS_TIME}" == true ]]; then
    TIME_FILTER="WHERE commit_timestamp BETWEEN ${START_EPOCH_MS} AND ${END_EPOCH_MS}"
  fi

  duckdb "${DB_FILE}" <<EOSQL
.output /dev/null
INSTALL httpfs;
LOAD httpfs;
CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);

CREATE TABLE commit_log AS
SELECT
    checkpoint_id,
    s3_key,
    CASE WHEN filename LIKE '%/output-%' THEN 'output' ELSE 'dropped' END AS source
FROM read_csv_auto([${GLOB_LIST}], filename=true)
${TIME_FILTER};

SET VARIABLE parquet_files = (SELECT list(DISTINCT s3_key) FROM commit_log);

CREATE TABLE all_records AS
SELECT kafka_partition, kafka_offset
FROM read_parquet(getvariable('parquet_files'));

CREATE TABLE summary AS
WITH per_partition AS (
    SELECT kafka_partition,
           MIN(kafka_offset) AS min_offset,
           MAX(kafka_offset) AS max_offset,
           COUNT(*) AS record_count,
           COUNT(DISTINCT kafka_offset) AS distinct_offsets,
           MAX(kafka_offset) - MIN(kafka_offset) + 1 AS expected_count
    FROM all_records
    GROUP BY kafka_partition
)
SELECT *,
       expected_count - distinct_offsets AS missing_offsets,
       record_count - distinct_offsets AS duplicate_offsets
FROM per_partition;
EOSQL

  # Print file counts
  COUNTS=$(duckdb "${DB_FILE}" -csv -noheader -c "
    SELECT count(*) FILTER (WHERE source = 'output'),
           count(*) FILTER (WHERE source = 'dropped')
    FROM commit_log;
  ")
  output_count=$(echo "${COUNTS}" | cut -d',' -f1)
  dropped_count=$(echo "${COUNTS}" | cut -d',' -f2)
  echo "Found ${output_count} output files, ${dropped_count} dropped files"
  echo ""

else
  # ---------------------------------------------------------------------------
  # Docker Compose: download files via docker, then load into DuckDB
  # ---------------------------------------------------------------------------

  # Helper: list CSV paths under each day directory in the date range
  list_day_csvs() {
    local minio_base minio_path
    minio_base="local/${WAL_S3_PATH#s3://}"
    for day in $(build_days); do
      minio_path="${minio_base}/${day}"
      docker compose exec -T minio mc find "${minio_path}" --name '*.csv' 2>/dev/null | sed "s|^local/|s3://|" || true
    done
  }

  # Helper: read a file from object storage to stdout
  read_file() {
    local s3_path="$1"
    local minio_path="local/${s3_path#s3://}"
    docker compose exec -T minio mc cat "${minio_path}" 2>/dev/null
  }

  # Helper: download a file from object storage to a local path
  download_file() {
    local s3_path="$1"
    local local_path="$2"
    local minio_path="local/${s3_path#s3://}"
    docker compose exec -T minio mc cat "${minio_path}" > "${local_path}" 2>/dev/null
  }

  # Step 1: Collect parquet file S3 paths from commit log CSVs
  OUTPUT_FILES=""
  DROPPED_FILES=""

  for csv_s3_path in $(list_day_csvs); do
    csv=$(read_file "${csv_s3_path}" || true)
    if [[ -n "${csv}" ]]; then
      csv_name=$(basename "${csv_s3_path}")
      if [[ "${HAS_TIME}" == true ]]; then
        # Filter rows by commit_timestamp (field 3) within the time range
        paths=$(echo "${csv}" | tail -n +2 | awk -F',' -v start="${START_EPOCH_MS}" -v end="${END_EPOCH_MS}" '$3 >= start && $3 <= end {print $2}')
      else
        # Date-only: include all rows from matched CSVs
        paths=$(echo "${csv}" | tail -n +2 | awk -F',' '{print $2}')
      fi
      if [[ "${csv_name}" == output-* ]]; then
        for p in ${paths}; do
          OUTPUT_FILES="${OUTPUT_FILES} ${p}"
        done
      elif [[ "${csv_name}" == dropped-* ]]; then
        for p in ${paths}; do
          DROPPED_FILES="${DROPPED_FILES} ${p}"
        done
      fi
    fi
  done

  if [[ -z "${OUTPUT_FILES}" && -z "${DROPPED_FILES}" ]]; then
    echo "ERROR: No files found in commit log for ${START_DATE} to ${END_DATE}"
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

  # Step 3: Load into DuckDB
  OUTPUT_GLOB="${OUTPUT_DIR}/*"
  DROPPED_GLOB="${DROPPED_DIR}/*"

  duckdb "${DB_FILE}" <<EOSQL
CREATE TABLE all_records AS
SELECT kafka_partition, kafka_offset
FROM parquet_scan('${OUTPUT_GLOB}')
UNION ALL
SELECT kafka_partition, kafka_offset
FROM parquet_scan('${DROPPED_GLOB}');

CREATE TABLE summary AS
WITH per_partition AS (
    SELECT kafka_partition,
           MIN(kafka_offset) AS min_offset,
           MAX(kafka_offset) AS max_offset,
           COUNT(*) AS record_count,
           COUNT(DISTINCT kafka_offset) AS distinct_offsets,
           MAX(kafka_offset) - MIN(kafka_offset) + 1 AS expected_count
    FROM all_records
    GROUP BY kafka_partition
)
SELECT *,
       expected_count - distinct_offsets AS missing_offsets,
       record_count - distinct_offsets AS duplicate_offsets
FROM per_partition;
EOSQL
fi

# ---------------------------------------------------------------------------
# Display results (shared by both paths — queries the persistent DuckDB file)
# ---------------------------------------------------------------------------
echo "=== Offset Analysis (${START_DATE} to ${END_DATE}) ==="
echo ""
duckdb "${DB_FILE}" -c "SELECT * FROM summary ORDER BY kafka_partition;"

TOTAL_MISSING=$(duckdb "${DB_FILE}" -csv -noheader -c "SELECT coalesce(sum(missing_offsets), 0) FROM summary;")
TOTAL_DUPLICATES=$(duckdb "${DB_FILE}" -csv -noheader -c "SELECT coalesce(sum(duplicate_offsets), 0) FROM summary;")

echo ""
if [[ "${TOTAL_MISSING}" -gt 0 ]]; then
  echo "FAIL: ${TOTAL_MISSING} missing offset(s) detected"
  echo ""
  echo "Gap details:"
  duckdb "${DB_FILE}" -c "
  WITH with_next AS (
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
