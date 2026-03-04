#!/usr/bin/env bash
set -euo pipefail

# Check for offset gaps using offset-range metadata CSVs from the write-ahead commit log.
#
# Usage: ./check-offset-gaps.sh <docker|aws> <start_date> <end_date>
#   Date formats: 'MM-DD-YYYY' (full day) or 'MM-DD-YYYY HH:MM' (minute-level)
#
# Examples:
#   $0 aws '03-03-2026' '03-03-2026'
#   $0 aws '03-03-2026 21:37' '03-03-2026 21:39'
#   $0 docker '03-03-2026' '03-03-2026'
#
# Reads offset-range CSVs written by EnrichFunction at each checkpoint
# (columns: checkpoint_id, kafka_partition, min_offset, max_offset, record_count)
# and uses DuckDB to verify there are no missing offsets per Kafka partition.
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

# Convert dates to directory-level filtering info
if [[ "${HAS_TIME}" == true ]]; then
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
  # AWS: DuckDB reads offset-range CSVs directly from S3
  # ---------------------------------------------------------------------------

  # Build DuckDB glob list from per-day paths
  GLOB_LIST=""
  for day in $(build_days); do
    if [[ -n "${GLOB_LIST}" ]]; then GLOB_LIST+=", "; fi
    GLOB_LIST+="'${WAL_S3_PATH}/${day}/**/offsets-*.csv'"
  done

  duckdb "${DB_FILE}" <<EOSQL
.output /dev/null
INSTALL httpfs;
LOAD httpfs;
CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);

CREATE TABLE offset_ranges AS
SELECT checkpoint_id, kafka_partition, min_offset, max_offset, record_count
FROM read_csv_auto([${GLOB_LIST}]);

EOSQL

  FILE_COUNT=$(duckdb "${DB_FILE}" -csv -noheader -c "
    SELECT count(*) FROM (SELECT DISTINCT checkpoint_id, kafka_partition FROM offset_ranges);
  ")
  echo "Found ${FILE_COUNT} offset range entries"
  echo ""

else
  # ---------------------------------------------------------------------------
  # Docker Compose: download offset CSVs via docker, then load into DuckDB
  # ---------------------------------------------------------------------------

  CSV_DIR="${WORK_DIR}/offsets"
  mkdir -p "${CSV_DIR}"

  minio_base="local/${WAL_S3_PATH#s3://}"
  file_index=0
  for day in $(build_days); do
    minio_path="${minio_base}/${day}"
    csv_paths=$(docker compose exec -T minio mc find "${minio_path}" --name 'offsets-*.csv' 2>/dev/null | sed "s|^local/|s3://|" || true)
    for csv_s3_path in ${csv_paths}; do
      local_minio_path="local/${csv_s3_path#s3://}"
      docker compose exec -T minio mc cat "${local_minio_path}" > "${CSV_DIR}/offsets-${file_index}.csv" 2>/dev/null
      file_index=$((file_index + 1))
    done
  done

  if [[ ${file_index} -eq 0 ]]; then
    echo "ERROR: No offset range files found for ${START_DATE} to ${END_DATE}"
    exit 1
  fi

  echo "Found ${file_index} offset range files"
  echo ""

  duckdb "${DB_FILE}" <<EOSQL
CREATE TABLE offset_ranges AS
SELECT checkpoint_id, kafka_partition, min_offset, max_offset, record_count
FROM read_csv_auto('${CSV_DIR}/*.csv');
EOSQL
fi

# ---------------------------------------------------------------------------
# Gap analysis using running-max window function on offset ranges
# ---------------------------------------------------------------------------
duckdb "${DB_FILE}" <<'EOSQL'
CREATE TABLE gaps AS
WITH with_running_max AS (
    SELECT kafka_partition, min_offset, max_offset, record_count,
           MAX(max_offset) OVER (
               PARTITION BY kafka_partition ORDER BY min_offset
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS prev_running_max
    FROM offset_ranges
)
SELECT kafka_partition,
       prev_running_max AS gap_after,
       min_offset AS resumes_at,
       min_offset - prev_running_max - 1 AS missing_count
FROM with_running_max
WHERE prev_running_max IS NOT NULL
  AND min_offset > prev_running_max + 1;

CREATE TABLE summary AS
WITH per_partition AS (
    SELECT kafka_partition,
           MIN(min_offset) AS min_offset,
           MAX(max_offset) AS max_offset,
           SUM(record_count) AS record_count,
           MAX(max_offset) - MIN(min_offset) + 1 AS expected_count
    FROM offset_ranges
    GROUP BY kafka_partition
),
partition_gaps AS (
    SELECT kafka_partition,
           coalesce(SUM(missing_count), 0) AS missing_offsets
    FROM gaps
    GROUP BY kafka_partition
)
SELECT p.*,
       coalesce(g.missing_offsets, 0) AS missing_offsets,
       p.record_count - (p.expected_count - coalesce(g.missing_offsets, 0)) AS duplicate_offsets
FROM per_partition p
LEFT JOIN partition_gaps g USING (kafka_partition);
EOSQL

# ---------------------------------------------------------------------------
# Display results
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
  SELECT * FROM gaps
  ORDER BY kafka_partition, gap_after
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
