# Data collection: first two complete days (measured 2026-11-03)

Source: Prow's public CI artifacts, `gs://kubernetes-ci-logs` (`logs/` and `pr-logs/`), parsed with the
rules of Kubernetes test-infra's Kettle (`make_db.py` + `make_json.py` @ `528e0fbf0b38`).
**Skipped tests are not counted** (Kettle drops them), so "test results" means tests that actually ran.
See [ADR 0005](../decisions/0005-collect-from-prow-public-bucket.md).

## How it was run
- `deploy/gcp/run_backfill.sh up` (commit `41b97c0`): one `e2-standard-2` VM in `us-central1` (same region as the
  bucket), Go 1.27.2, 150 requests/s shared limit, 64 in-flight requests, 128 build workers.
- Days collected newest-first until the cumulative count reached the 12,000,000 target (checked after each day).
- Output: zstd Parquet + a JSON manifest per day in `gs://querydock-kettle-raw`; VM deleted when finished.

## Results

| | Fri 2026-10-30 (UTC) | Sat 2026-10-31 (UTC) |
|---|---|---|
| Builds collected | 20,764 | 11,714 |
| **Test results (executed)** | **12,846,079** | **7,152,461** |
| Failed test results | 1,538 | 1,553 |
| Builds with 0 test results | 11,534 (55%) | 4,876 (42%) |
| Presubmit (`pr:`) / periodic+postsubmit builds | 12,368 / 8,396 | 3,392 / 8,322 |
| Distinct jobs with a build that day | 2,335 | 2,081 |
| Max test results in one build | 150,173 | 150,329 |
| Top-5 jobs' share of test results | 44.4% | 31.2% |
| JUnit files read / decoded size | 23,405 / 28.3 GB | 14,520 / 10.9 GB |
| Downloaded (wire, same-region) | 28.5 GB | 11 GB |
| Parquet size (zstd) | 104 MB | 60 MB |
| Collection time | 36 min | 17 min |
| Requests / retries / throttled (HTTP 429/503) | 113,627 / 0 / 0 | 67,226 / 0 / 0 |
| Malformed JUnit files (skipped, as Kettle does) | 17 | 20 |
| Builds dropped / fetch failures | 0 / 0 | 0 / 0 |

Total: **19,998,540 test results in 32,478 builds**; 164 MB of Parquet.

## Verification
- Parquet SHA-256 and size match the manifests (both days).
- `bench/conformance/check_day_parquet.py` (PyArrow, an independent reader): **12/12 checks pass on both days**
  (no duplicate builds, per-build counts sum to the manifest totals, every failed test has failure text,
  all builds start inside the UTC day, no negative or missing times, no successful `#` repeats).
- The same files loaded into BigQuery (`querydock.kettle.builds`, one partition per day, 2.83 GB logical) reproduce
  every count exactly (`SUM(tests_run)`, `SUM(ARRAY_LENGTH(test))`, `COUNT(DISTINCT path)`).

## Observations about the data
- Volume is skewed: on Friday two presubmit unit-test jobs of one sub-project (kueue) are ~30% of all results.
  These are Go unit-test subtests, not e2e tests. Daily totals therefore say little about "how many tests exist".
- The `version` column is null for every build; the Kubernetes version is in `metadata` (`job-version`).
- `result` values: `SUCCESS`, `FAILURE`, `ABORTED`, and a lowercase `aborted` (8 builds over the two days).
- The 6-hour ID lookback is far wider than needed: the slowest build started 389 s after its ID was minted.
- Only two days were collected (one Friday, one Saturday). They are not a long-run average.

## Cost
VM ~56 minutes at the ~$0.067/h list price of `e2-standard-2` plus disk and an external IP: about $0.07
(estimate; the billing report lags). Reads from the bucket in the same region are not charged to us. The VM deleted
itself after power-off; no instances, disks or addresses remain.

## Reproduce
```bash
deploy/gcp/run_backfill.sh plan                       # prints every command; creates nothing
CONFIRM=yes deploy/gcp/run_backfill.sh up             # needs the HEAD commit pushed
uv run --with pyarrow python -I bench/conformance/check_day_parquet.py data/raw/kettle-2026-10-30.parquet data/manifests/kettle-2026-10-30.json
```
