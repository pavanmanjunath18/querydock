#!/usr/bin/env bash
# Load one collected day (zstd Parquet) into the BigQuery backup table
# querydock:kettle.builds. The table is ingestion-time partitioned by DAY and each
# UTC day is loaded into its own partition (builds$YYYYMMDD) with --replace, so
# re-runs are idempotent. Load jobs are free; this script never runs a query.
set -euo pipefail
day="${1:?usage: bq_load_day.sh YYYY-MM-DD}"
repo="$(cd "$(dirname "$0")/../.." && pwd)"
file="$repo/data/raw/kettle-$day.parquet"
manifest="$repo/data/manifests/kettle-$day.json"
project=querydock dataset=kettle table=builds

[[ -f "$file" && -f "$manifest" ]] || { echo "missing $file or its manifest" >&2; exit 1; }
python3 -I -c 'import json,sys; m=json.load(open(sys.argv[1])); sys.exit(0 if m["complete"] and not m["provisional"] else "day is incomplete or provisional; not loading")' "$manifest"

# Free-storage guard: stay under the 10 GB BigQuery free tier (logical bytes).
used=$(bq --project_id="$project" show --format=json "$dataset.$table" 2>/dev/null \
  | python3 -I -c 'import json,sys; print(json.load(sys.stdin).get("numBytes",0))' 2>/dev/null || echo 0)
if (( used > 9000000000 )); then
  echo "backup table is at ${used} bytes (> 9 GB); stopping before the 10 GB free tier" >&2
  exit 1
fi

bq --project_id="$project" show "$dataset" >/dev/null 2>&1 \
  || bq --project_id="$project" mk --dataset --location=US \
       --description "Backup copy of QueryDock's Kettle-rule CI dataset" "$dataset"
bq --project_id="$project" show "$dataset.$table" >/dev/null 2>&1 \
  || bq --project_id="$project" mk --table --time_partitioning_type=DAY \
       --description "One partition per UTC build-start day; columns follow Kettle's schema (started/finished are epoch seconds)" \
       "$dataset.$table"

bq --project_id="$project" load --source_format=PARQUET --parquet_enable_list_inference \
   --replace "$dataset.$table\$${day//-/}" "$file"
echo "loaded $day into $project:$dataset.$table\$${day//-/}"
