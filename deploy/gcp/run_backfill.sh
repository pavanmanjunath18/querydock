#!/usr/bin/env bash
# Operator script for the one-off Kettle-rule backfill on Google Cloud.
#
#   run_backfill.sh plan     print every command that `up` would run (creates nothing)
#   run_backfill.sh up       create bucket + service account + VM and start the run   (needs CONFIRM=yes)
#   run_backfill.sh status   VM state, days collected so far, tail of the VM's console output
#   run_backfill.sh pull     download finished Parquet + manifests into ./data
#   run_backfill.sh down     delete the VM (the bucket and its data are kept)
#
# Tunables (env): TARGET_TESTS=12000000 MAX_DAYS=14 RPS=150 CONCURRENCY=64 WORKERS=128
set -euo pipefail

PROJECT=querydock
REGION=us-central1
ZONE=us-central1-a
BUCKET=querydock-kettle-raw
SA_NAME=kettle-collector
SA="$SA_NAME@$PROJECT.iam.gserviceaccount.com"
VM=kettle-collector
TARGET_TESTS=${TARGET_TESTS:-12000000}
MAX_DAYS=${MAX_DAYS:-14}
RPS=${RPS:-150}
CONCURRENCY=${CONCURRENCY:-64}
WORKERS=${WORKERS:-128}

repo="$(cd "$(dirname "$0")/../.." && pwd)"
cmd=${1:-plan}
apply=0
[[ $cmd == up ]] && apply=1

# Every command that creates or changes something goes through run(): printed in plan mode, executed in up mode.
run() { echo "+ $*"; if (( apply )); then "$@"; fi; }   # a failing command aborts the script (set -e)

preflight() {
  [[ "$(gcloud config get project 2>/dev/null)" == "$PROJECT" ]] || { echo "gcloud project is not $PROJECT" >&2; exit 1; }
  git -C "$repo" fetch -q origin
  [[ -z "$(git -C "$repo" status --porcelain)" ]] || { echo "working tree has uncommitted changes; commit and push first (the VM clones from GitHub)" >&2; exit 1; }
  git -C "$repo" merge-base --is-ancestor HEAD origin/main || { echo "HEAD is not pushed to origin/main; push first" >&2; exit 1; }
}

case $cmd in
  plan|up)
    preflight
    if [[ $cmd == up && "${CONFIRM:-}" != yes ]]; then echo "refusing: set CONFIRM=yes to create resources" >&2; exit 1; fi
    REPO_SHA=$(git -C "$repo" rev-parse HEAD)
    RUN_ID=$(date -u +%Y%m%d-%H%M%S)
    START_DAY=$(date -u -v-3d +%F 2>/dev/null || date -u -d '3 days ago' +%F)   # newest day that is >= 48 h old
    echo "# project=$PROJECT region=$REGION zone=$ZONE bucket=gs://$BUCKET vm=$VM"
    echo "# repo commit=$REPO_SHA start-day=$START_DAY target=$TARGET_TESTS tests (max $MAX_DAYS days) rate=$RPS req/s"
    echo "# worst-case VM bill: 24 h x ~\$0.067/h = ~\$1.6 (hard stop below deletes the VM after 24 h)"

    gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1 && echo "# bucket exists" || \
      run gcloud storage buckets create "gs://$BUCKET" --project="$PROJECT" --location="$REGION" \
        --default-storage-class=STANDARD --uniform-bucket-level-access --public-access-prevention
    gcloud iam service-accounts describe "$SA" --project="$PROJECT" >/dev/null 2>&1 && echo "# service account exists" || \
      run gcloud iam service-accounts create "$SA_NAME" --project="$PROJECT" --display-name="Kettle-rule collector VM"
    # Only this bucket: no project-wide roles.
    run gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --project="$PROJECT" \
      --member="serviceAccount:$SA" --role=roles/storage.objectAdmin
    run gcloud compute instances create "$VM" --project="$PROJECT" --zone="$ZONE" \
      --machine-type=e2-standard-2 --image-family=debian-12 --image-project=debian-cloud \
      --boot-disk-size=30GB --boot-disk-type=pd-balanced \
      --service-account="$SA" --scopes=https://www.googleapis.com/auth/devstorage.read_write \
      --max-run-duration=24h --instance-termination-action=DELETE --no-restart-on-failure \
      --labels=app=querydock,purpose=kettle-backfill \
      --metadata="enable-oslogin=TRUE,bucket=$BUCKET,run-id=$RUN_ID,repo-sha=$REPO_SHA,start-day=$START_DAY,target-tests=$TARGET_TESTS,max-days=$MAX_DAYS,rps=$RPS,concurrency=$CONCURRENCY,workers=$WORKERS" \
      --metadata-from-file=startup-script="$repo/deploy/gcp/collector_startup.sh"
    [[ $cmd == plan ]] && echo "# plan only; nothing was created."
    ;;
  status)
    gcloud compute instances describe "$VM" --project="$PROJECT" --zone="$ZONE" --format='value(status)' 2>&1 | sed 's/^/vm: /'
    echo "days in bucket:"; gcloud storage ls "gs://$BUCKET/manifests/" 2>/dev/null | sed 's|.*/kettle-||' | tr '\n' ' '; echo
    for f in $(gcloud storage ls "gs://$BUCKET/runs/*/DONE" "gs://$BUCKET/runs/*/FAILED" 2>/dev/null); do echo "marker: $f"; done
    echo "--- last console output:"
    gcloud compute instances get-serial-port-output "$VM" --project="$PROJECT" --zone="$ZONE" 2>/dev/null | tail -n 15
    ;;
  pull)
    mkdir -p "$repo/data/raw" "$repo/data/manifests"
    gcloud storage rsync -r "gs://$BUCKET/raw" "$repo/data/raw"
    gcloud storage rsync -r "gs://$BUCKET/manifests" "$repo/data/manifests"
    ;;
  down)
    gcloud compute instances delete "$VM" --project="$PROJECT" --zone="$ZONE" --quiet
    ;;
  *) sed -n '2,10p' "$0"; exit 2 ;;
esac
