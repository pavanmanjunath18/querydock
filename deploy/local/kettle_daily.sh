#!/usr/bin/env bash
# Daily export: collect every complete UTC day that is >= 2 days old and not yet
# collected (catches up after the laptop was asleep), then load it into BigQuery.
# Run by launchd (com.querydock.kettle-daily.plist). Safe to run repeatedly.
set -euo pipefail
repo="$(cd "$(dirname "$0")/../.." && pwd)"
bin="$repo/bin/querydock"
runs="$repo/data/runs"
mkdir -p "$runs"
oldest_kept_days=89   # the bucket keeps ~90 days; never ask for a day that is already gone
for back in $(seq "$oldest_kept_days" -1 2); do
  day=$(date -u -v-"${back}"d +%F)
  manifest="$repo/data/manifests/kettle-$day.json"
  if [[ -f "$manifest" ]] && python3 -I -c 'import json,sys; sys.exit(0 if json.load(open(sys.argv[1]))["complete"] else 1)' "$manifest"; then
    continue
  fi
  [[ -n "${BACKFILL:-}" || "$back" -le 9 ]] || continue  # daily mode only fills the last week
  echo "$(date -u +%FT%TZ) collecting $day" >> "$runs/daily.out"
  (cd "$repo/ingest" && "$bin" collect --date "$day" --data-dir "$repo/data" --rps 20 --concurrency 16) \
     >> "$runs/collect-$day.out" 2>&1 || { echo "collect $day failed" >> "$runs/daily.out"; continue; }
  "$repo/deploy/local/bq_load_day.sh" "$day" >> "$runs/daily.out" 2>&1 || echo "bq load $day failed" >> "$runs/daily.out"
done
