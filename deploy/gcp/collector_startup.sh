#!/usr/bin/env bash
# Startup script for the one-off collector VM (Debian 12, e2-standard-2, us-central1).
# It runs as root on every boot via the GCE metadata "startup-script", so it is
# written to be safe to re-run: finished runs exit immediately, finished days are skipped.
#
# What it does, in order:
#   1. installs git/jq and a pinned Go, clones the public repo at an exact commit, runs the tests, builds
#   2. collects UTC days newest-first with `querydock collect` (polite rate limit, set in metadata)
#   3. uploads each finished day's Parquet + manifest to the bucket, deletes the local copy
#   4. stops when the cumulative test-result count reaches the target (or MAX_DAYS)
#   5. writes DONE (or FAILED) + the log to the bucket and powers the VM off
# A hard stop (--max-run-duration, set by run_backfill.sh) deletes the VM after 24 h no matter what.
set -Eeuo pipefail

LOG=${LOG:-/var/log/qd-run.log}
exec > >(tee -a "$LOG") 2>&1

md() { # instance metadata attribute (or MD_<name> when LOCAL_TEST is set)
  if [[ -n "${LOCAL_TEST:-}" ]]; then local v="MD_${1//-/_}"; echo "${!v}"; return; fi
  curl -fsS -H 'Metadata-Flavor: Google' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"
}

BUCKET=$(md bucket)
RUN_ID=$(md run-id)
REPO_SHA=$(md repo-sha)
START_DAY=$(md start-day)           # newest day to collect (UTC, YYYY-MM-DD)
TARGET_TESTS=$(md target-tests)     # stop once this many test results are collected
MAX_DAYS=$(md max-days)             # ... or after this many days
RPS=$(md rps)
CONC=$(md concurrency)
WORKERS=$(md workers)

GO_VERSION=1.27.2
GO_SHA256=ecbadb99091a3f46e31f5f934b068b1864eafa7995211b39eaddf76996045fe5
WORK=${WORK:-/var/lib/qd}
GS="gs://$BUCKET"
RUN="$GS/runs/$RUN_ID"
QD=${QD:-/opt/querydock/bin/querydock}
mkdir -p "$WORK"

power_off() { [[ -n "${LOCAL_TEST:-}" ]] || shutdown -h now; }

finish() { # $1 = DONE | FAILED
  trap - ERR
  echo "$(date -u +%FT%TZ) $1" > "$WORK/$1"
  gcloud storage cp "$LOG" "$RUN/vm.log" || true
  gcloud storage cp "$WORK/$1" "$RUN/$1" || true
  power_off
}
trap 'echo "ERROR at line $LINENO"; finish FAILED' ERR

dec_day() { # previous calendar day; GNU date on the VM, BSD date for local tests
  date -u -d "$1 - 1 day" +%F 2>/dev/null || date -u -j -v-1d -f %F "$1" +%F
}

echo "== run $RUN_ID: start-day=$START_DAY target=$TARGET_TESTS max-days=$MAX_DAYS rps=$RPS conc=$CONC workers=$WORKERS repo=$REPO_SHA"

if gcloud storage ls "$RUN/DONE" >/dev/null 2>&1; then
  echo "run already finished; powering off"; power_off; exit 0
fi

if [[ -z "${LOCAL_TEST:-}" ]]; then
  export HOME=/root GOCACHE=/root/.cache/go-build PATH="$PATH:/usr/local/go/bin"
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git jq ca-certificates curl
  curl -fsSL -o /tmp/go.tgz "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"
  echo "$GO_SHA256  /tmp/go.tgz" | sha256sum -c -
  tar -C /usr/local -xzf /tmp/go.tgz
  rm -rf /opt/querydock
  git clone -q https://github.com/pavanmanjunath18/querydock /opt/querydock
  git -C /opt/querydock checkout -q "$REPO_SHA"
  (cd /opt/querydock/ingest && go test ./... && go build -o "$QD" ./cmd/querydock)
fi

day=$START_DAY; total=0; n=0
while (( n < MAX_DAYS && total < TARGET_TESTS )); do
  mf="$WORK/manifests/kettle-$day.json"
  if gcloud storage cp "$GS/manifests/kettle-$day.json" "$mf" >/dev/null 2>&1 && jq -e '.complete' "$mf" >/dev/null; then
    echo "== $day: already in the bucket, skipping"
  else
    ok=0
    for attempt in 1 2; do
      echo "== $day: collecting (attempt $attempt)"
      if "$QD" collect --date "$day" --data-dir "$WORK" --rps "$RPS" --concurrency "$CONC" --workers "$WORKERS" \
         && jq -e '.complete' "$mf" >/dev/null; then ok=1; break; fi
      echo "== $day: attempt $attempt failed or incomplete"
    done
    if (( ok == 0 )); then echo "== $day: giving up"; false; fi
    gcloud storage cp "$WORK/raw/kettle-$day.parquet" "$GS/raw/"
    gcloud storage cp "$mf" "$GS/manifests/"
    rm -f "$WORK/raw/kettle-$day.parquet"
  fi
  t=$(jq '.stats.Tests' "$mf")
  total=$((total + t)); n=$((n + 1))
  echo "== day=$day tests=$t cumulative=$total days=$n"
  day=$(dec_day "$day")
done

jq -n --arg run "$RUN_ID" --arg sha "$REPO_SHA" --argjson days "$n" --argjson tests "$total" --arg next "$day" \
  '{run:$run, repo_sha:$sha, days:$days, tests:$tests, next_day_not_collected:$next}' > "$WORK/summary.json"
gcloud storage cp "$WORK/summary.json" "$RUN/summary.json"
echo "== finished: $n days, $total test results"
finish DONE
