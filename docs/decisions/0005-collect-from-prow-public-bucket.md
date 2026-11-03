# ADR 0005: Collect CI results from Prow's public bucket, using Kettle's rules

- Status: accepted (2026-10-14); supersedes the BigQuery-export plan in SPEC §2

## Context
SPEC §2 planned to export days from the Kettle BigQuery dataset `kubernetes-public:k8s_infra_kettle`. Its README says
the dataset is publicly accessible, but queries fail with "User does not have permission to query table"
(verified 2026-10-14 and 2026-11-03 from a billing-enabled project, in `bq` and in the console). The dataset's IAM
policy in kubernetes/k8s.io (`infra/gcp/terraform/kubernetes-public/k8s-kettle.tf`) names only the Prow on-call group and
Kettle's service account. The raw artifacts Kettle reads, `gs://kubernetes-ci-logs`, are public and keep 90 days.

## Decision
Collect from the bucket with a Go collector (`ingest/`) that ports Kettle's build discovery and row rules
(`make_db.py`, `make_json.py` at a pinned commit, with Kettle's own test cases and a conformance check against its
Python parser). Output is zstd Parquet per UTC day plus a manifest; `querydock.kettle.builds` in BigQuery is a backup
copy loaded from the Parquet. Skipped tests are not counted.

## Consequences
- The data is the same facts Kettle would have produced, parsed by the same rules, but we own the pipeline.
- History is limited to what the bucket retains (90 days), so days are exported as they age in and frozen.
- A full-speed collection runs on a VM in the bucket's region (free reads, no sleeping laptop); a polite 20 req/s limit
  is kept for laptop runs.
- BigQuery bills logical bytes (~1.4 GB per 10M test results), so the free 10 GB covers only a few days; the Parquet
  archive in Cloud Storage is the complete copy.
