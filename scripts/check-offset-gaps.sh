#!/usr/bin/env bash
set -euo pipefail

# Check for offset gaps in parquet files referenced by the write-ahead commit log.
#
# Usage: ./check-offset-gaps.sh <docker|aws> <start_date> <end_date>
#        ./check-offset-gaps.sh <docker|aws> --paths <path1> [path2 ...]
#   Date formats: 'MM-DD-YYYY' (full day) or 'MM-DD-YYYY HH:MM' (minute-level)
#   Paths are relative to the WAL base path (e.g. 03-06-2026/15/40)
#
# Examples:
#   $0 aws '03-03-2026' '03-03-2026'
#   $0 aws '03-03-2026 21:37' '03-03-2026 21:39'
#   $0 docker '03-03-2026' '03-03-2026'
#   $0 aws --paths 03-06-2026/15/40 03-06-2026/15/41
#
# Reads commit log CSVs, downloads the referenced parquet files, and uses
# DuckDB to verify there are no missing offsets per Kafka partition when
# output + dropped records are combined.
#
# AWS mode downloads CSVs locally via `aws s3 sync` (fast parallel download),
# parses them with DuckDB, then reads parquet files from S3 in batches.
# Single-minute paths use a fast one-shot S3 query.
# Docker Compose mode downloads via docker.
#
# Override WAL path with: WAL_S3_PATH=s3://my-bucket/wal ./check-offset-gaps.sh aws '03-03-2026' '03-03-2026'

# Detect GNU vs BSD date
GNU_DATE=false
date --version &>/dev/null 2>&1 && GNU_DATE=true

parse_date() {  # "MM-DD-YYYY [HH:MM]" → output format
  local input="$1" fmt="$2"
  if [[ "$GNU_DATE" == true ]]; then
    local iso=$(echo "$input" | sed 's|\(..\)-\(..\)-\(....\)|\3-\1-\2|')
    date -d "$iso" +"$fmt"
  else
    # BSD: auto-detect format by presence of HH:MM
    [[ "$input" =~ [0-9]{2}:[0-9]{2}$ ]] \
      && date -j -f '%m-%d-%Y %H:%M' "$input" +"$fmt" \
      || date -j -f '%m-%d-%Y' "$input" +"$fmt"
  fi
}

epoch_to_date() {  # epoch seconds → output format
  local epoch="$1" fmt="$2"
  [[ "$GNU_DATE" == true ]] \
    && date -d "@$epoch" +"$fmt" \
    || date -j -r "$epoch" +"$fmt"
}

ENV_TYPE="${1:-}"

if [[ -z "${ENV_TYPE}" ]]; then
  echo "Usage: $0 <docker|aws> <start_date> <end_date>"
  echo "       $0 <docker|aws> --paths <path1> [path2 ...]"
  echo "  Date formats: 'MM-DD-YYYY' (full day) or 'MM-DD-YYYY HH:MM' (minute-level)"
  echo "  Paths are relative to the WAL base path (e.g. 03-06-2026/15/40)"
  echo "Examples:"
  echo "  $0 aws '03-03-2026' '03-03-2026'"
  echo "  $0 aws '03-03-2026 21:37' '03-03-2026 21:39'"
  echo "  $0 aws --paths 03-06-2026/15/40 03-06-2026/15/41"
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

if [[ "${ENV_TYPE}" == "aws" ]] && ! command -v aws &>/dev/null; then
  echo "ERROR: aws CLI is not installed. Install it from: https://aws.amazon.com/cli/"
  exit 1
fi

# Determine mode: --paths or date-range
PATHS_MODE=false
PATHS=()
START_DATE=""
END_DATE=""

if [[ "${2:-}" == "--paths" ]]; then
  PATHS_MODE=true
  shift 2
  if [[ $# -eq 0 ]]; then
    echo "ERROR: --paths requires at least one path argument"
    exit 1
  fi
  PATHS=("$@")
else
  START_DATE="${2:-}"
  END_DATE="${3:-}"
  if [[ -z "${START_DATE}" || -z "${END_DATE}" ]]; then
    echo "Usage: $0 <docker|aws> <start_date> <end_date>"
    echo "       $0 <docker|aws> --paths <path1> [path2 ...]"
    exit 1
  fi
fi

# Date-range mode: parse dates and build day list
HAS_TIME=false
if [[ "${PATHS_MODE}" == false ]]; then
  if [[ "${START_DATE}" =~ ^[0-9]{2}-[0-9]{2}-[0-9]{4}\ [0-9]{2}:[0-9]{2}$ ]]; then
    HAS_TIME=true
  fi

  if [[ "${HAS_TIME}" == true ]]; then
    START_EPOCH=$(parse_date "${START_DATE}" %s)
    END_EPOCH=$(parse_date "${END_DATE}" %s)
    START_EPOCH_MS=$(( START_EPOCH * 1000 ))
    END_EPOCH_MS=$(( (END_EPOCH + 60) * 1000 - 1 ))
    START_DAY=$(parse_date "${START_DATE}" %m-%d-%Y)
    END_DAY=$(parse_date "${END_DATE}" %m-%d-%Y)
  else
    START_DAY="${START_DATE}"
    END_DAY="${END_DATE}"
  fi
fi

# Enumerate each day in the range as MM-DD-YYYY strings (one per line)
build_days() {
  local start_epoch end_epoch current
  start_epoch=$(parse_date "${START_DAY}" %s)
  end_epoch=$(parse_date "${END_DAY}" %s)

  current=$start_epoch
  while [[ $current -le $end_epoch ]]; do
    epoch_to_date "$current" %m-%d-%Y
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
if [[ "${PATHS_MODE}" == true ]]; then
  echo "Checking offset gaps for paths: ${PATHS[*]}"
else
  echo "Checking offset gaps for ${START_DATE} to ${END_DATE}..."
fi
echo ""

DB_FILE="${WORK_DIR}/analysis.duckdb"

if [[ "${ENV_TYPE}" == "aws" ]]; then
  # ---------------------------------------------------------------------------
  # AWS: Download commit log CSVs locally via aws s3 sync, then parse with
  # DuckDB. Parquet files are read directly from S3 in batches.
  # Single-minute paths use a fast one-shot S3 query (no download needed).
  # ---------------------------------------------------------------------------

  # Add WHERE clause for time filtering when HH:MM is specified (date-range mode only)
  TIME_FILTER=""
  if [[ "${PATHS_MODE}" == false && "${HAS_TIME}" == true ]]; then
    TIME_FILTER="WHERE commit_timestamp BETWEEN ${START_EPOCH_MS} AND ${END_EPOCH_MS}"
  fi

  # Detect single-minute path for fast one-shot query
  IS_SINGLE_MINUTE=false
  SINGLE_MINUTE_GLOB=""
  if [[ "${PATHS_MODE}" == true && ${#PATHS[@]} -eq 1 ]]; then
    p="${PATHS[0]%/}"
    slashes="${p//[^\/]/}"
    if [[ ${#slashes} -ge 2 ]]; then
      IS_SINGLE_MINUTE=true
      SINGLE_MINUTE_GLOB="${WAL_S3_PATH}/${p}/*.csv"
    fi
  fi

  # Install httpfs once
  duckdb "${DB_FILE}" -c "INSTALL httpfs;" 2>/dev/null

  if [[ "${IS_SINGLE_MINUTE}" == true ]]; then
    # --- Fast path: single minute, one-shot S3 query ---
    duckdb "${DB_FILE}" <<EOSQL
.output /dev/null
LOAD httpfs;
SET http_retries = 3;
CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);

CREATE TABLE commit_log AS
SELECT
    checkpoint_id,
    s3_key,
    CASE WHEN filename LIKE '%/output-%' THEN 'output' ELSE 'dropped' END AS source
FROM read_csv_auto(['${SINGLE_MINUTE_GLOB}'], filename=true)
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
  else
    # --- Multi-path: download CSVs locally, then batch parquet reads from S3 ---

    # Step 1: Download CSVs via aws s3 sync
    LOCAL_CSV="${WORK_DIR}/csv"
    if [[ "${PATHS_MODE}" == true ]]; then
      for p in "${PATHS[@]}"; do
        p="${p%/}"
        echo "Downloading CSVs from ${p}..."
        mkdir -p "${LOCAL_CSV}/${p}"
        aws s3 sync "${WAL_S3_PATH}/${p}/" "${LOCAL_CSV}/${p}/" --quiet
      done
    else
      for day in $(build_days); do
        echo "Downloading CSVs from ${day}..."
        mkdir -p "${LOCAL_CSV}/${day}"
        aws s3 sync "${WAL_S3_PATH}/${day}/" "${LOCAL_CSV}/${day}/" --quiet
      done
    fi

    CSV_COUNT=$(find "${LOCAL_CSV}" -name '*.csv' | wc -l | tr -d ' ')
    echo "Downloaded ${CSV_COUNT} CSV files"

    if [[ "${CSV_COUNT}" -eq 0 ]]; then
      if [[ "${PATHS_MODE}" == true ]]; then
        echo "ERROR: No CSV files found for paths: ${PATHS[*]}"
      else
        echo "ERROR: No CSV files found for ${START_DATE} to ${END_DATE}"
      fi
      exit 1
    fi

    # Step 2: Parse CSVs locally with DuckDB (instant — no S3 overhead)
    duckdb "${DB_FILE}" <<EOSQL
CREATE TABLE commit_log AS
SELECT
    checkpoint_id,
    s3_key,
    CASE WHEN filename LIKE '%/output-%' THEN 'output' ELSE 'dropped' END AS source
FROM read_csv_auto('${LOCAL_CSV}/**/*.csv', filename=true)
${TIME_FILTER};

CREATE TABLE all_records (kafka_partition INTEGER, kafka_offset BIGINT);
EOSQL

    ROW_COUNT=$(duckdb "${DB_FILE}" -csv -noheader -c "SELECT count(*) FROM commit_log;")
    if [[ "${ROW_COUNT}" -eq 0 ]]; then
      if [[ "${PATHS_MODE}" == true ]]; then
        echo "ERROR: No files found in commit log for paths: ${PATHS[*]}"
      else
        echo "ERROR: No files found in commit log for ${START_DATE} to ${END_DATE}"
      fi
      exit 1
    fi

    # Step 3: Read parquet from S3 in batches
    PARQUET_BATCH_SIZE="${PARQUET_BATCH_SIZE:-2000}"
    PARQUET_PATHS=$(duckdb "${DB_FILE}" -csv -noheader -c "SELECT DISTINCT s3_key FROM commit_log;")
    PARQUET_COUNT=$(echo "${PARQUET_PATHS}" | grep -c . || true)
    echo "Reading ${PARQUET_COUNT} parquet files from S3 (batch size: ${PARQUET_BATCH_SIZE})..."

    SQL_FILE="${WORK_DIR}/parquet_batch.sql"
    {
      echo "LOAD httpfs;"
      echo "SET http_retries = 3;"
      echo "CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);"
      echo ".bail off"

      batch_list=""
      count=0
      while IFS= read -r path; do
        [[ -z "$path" ]] && continue
        if [[ -n "$batch_list" ]]; then
          batch_list="${batch_list}, '${path}'"
        else
          batch_list="'${path}'"
        fi
        count=$((count + 1))
        if [[ $count -ge $PARQUET_BATCH_SIZE ]]; then
          echo "INSERT INTO all_records SELECT kafka_partition, kafka_offset FROM read_parquet([${batch_list}]);"
          batch_list=""
          count=0
        fi
      done <<< "${PARQUET_PATHS}"

      if [[ $count -gt 0 ]]; then
        echo "INSERT INTO all_records SELECT kafka_partition, kafka_offset FROM read_parquet([${batch_list}]);"
      fi

      echo ".bail on"
    } > "${SQL_FILE}"

    duckdb "${DB_FILE}" < "${SQL_FILE}" 2>/dev/null || true

    # Step 4: Create summary
    duckdb "${DB_FILE}" <<EOSQL
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

  # Helper: list CSV paths under the relevant directories
  list_csvs() {
    local minio_base minio_path
    minio_base="local/${WAL_S3_PATH#s3://}"
    if [[ "${PATHS_MODE}" == true ]]; then
      for p in "${PATHS[@]}"; do
        minio_path="${minio_base}/${p}"
        docker compose exec -T minio mc find "${minio_path}" --name '*.csv' 2>/dev/null | sed "s|^local/|s3://|" || true
      done
    else
      for day in $(build_days); do
        minio_path="${minio_base}/${day}"
        docker compose exec -T minio mc find "${minio_path}" --name '*.csv' 2>/dev/null | sed "s|^local/|s3://|" || true
      done
    fi
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

  for csv_s3_path in $(list_csvs); do
    csv=$(read_file "${csv_s3_path}" || true)
    if [[ -n "${csv}" ]]; then
      csv_name=$(basename "${csv_s3_path}")
      if [[ "${PATHS_MODE}" == false && "${HAS_TIME}" == true ]]; then
        # Filter rows by commit_timestamp (field 3) within the time range
        paths=$(echo "${csv}" | tail -n +2 | awk -F',' -v start="${START_EPOCH_MS}" -v end="${END_EPOCH_MS}" '$3 >= start && $3 <= end {print $2}')
      else
        # Date-only or paths mode: include all rows from matched CSVs
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
    if [[ "${PATHS_MODE}" == true ]]; then
      echo "ERROR: No files found in commit log for paths: ${PATHS[*]}"
    else
      echo "ERROR: No files found in commit log for ${START_DATE} to ${END_DATE}"
    fi
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
if [[ "${PATHS_MODE}" == true ]]; then
  echo "=== Offset Analysis (paths: ${PATHS[*]}) ==="
else
  echo "=== Offset Analysis (${START_DATE} to ${END_DATE}) ==="
fi
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
