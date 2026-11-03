# Build log

## 2026-10-13 — Session 0 (Phase 0: spec)

- Wrote `docs/SPEC.md`; the spec was approved the same day. Decisions are in SPEC §9.
- Versions checked against Homebrew, PyPI and GitHub releases.
- Kettle findings: skipped tests are dropped upstream; the `day`/`week` tables are rolling windows and `all` uses a 30-day window, so a backfill and a daily export are planned (SPEC §2.5).
- Benchmark models fixed before any results: `claude-sonnet-5-5` and `claude-haiku-5-5`. Cost estimate ≈ $58 (SPEC §5.6), not yet approved for spending.
- Daily volume isn't known yet; it needs BigQuery access.
- Environment: macOS arm64, 16 GB RAM / 8 cores, 41 GB free disk. Docker daemon isn't running; kind, Helm, Go, k6 and gcloud aren't installed.

## 2026-10-13 — Session 1 (setup, Phase 1 day 1)

- Public repo created: github.com/pavanmanjunath18/querydock (`docs/HANDOFF.md` gitignored).
- Installed via Homebrew: kind 0.33.0, Helm 4.3.0, Go 1.27.2, k6 2.3.0, Google Cloud SDK 589.0.0 (bq 2.1.40).
- Docker Desktop started (currently 8 GB / 8 CPUs; the target of 10 GB / 6 CPUs is set by hand in Docker Desktop settings).
- Benchmark models revised before any results (SPEC §5.5): `qwen3.5:9b` (Ollama ID 56671c2ab938) primary, Gemini Flash on Vertex AI, optional `claude-haiku-5-5` batch.
- Ollama tool-call smoke test (not the benchmark): 3/3 calls well-formed, 2/3 picked the right tool, 5.8 GB resident at 8K context.
- Spec additions: `$jsonSchema` validation, per-tenant RBAC, `/v1` + `/v2`, Terraform EC2 demo, disk plan, new cut order.
- ADRs 0001–0004 written.
- Waiting on the maintainer: Google Cloud free-trial activation, `gcloud auth login` in the browser, Docker resources, disk cleanup choices.

## 2026-11-03 — Session 2 (first data)

- Confirmed again that `kubernetes-public:k8s_infra_kettle` is not readable (CLI dry-run and console); ADR 0005 records the change of source.
- Go collector (Kettle make_db/make_json rules) built and tested; Compute Engine API enabled; `deploy/gcp/` scripts tested locally with fakes.
- VM run (e2-standard-2, us-central1, 150 req/s): two complete days collected in ~56 min for ~$0.07; VM self-deleted, nothing billable left.
- Result: 32,478 builds, 19,998,540 executed test results (Fri 12.85M, Sat 7.15M); 0 retries/throttles. Details: docs/results/data-collection-2026-11-03.md.
- Verification: Parquet SHA-256 vs manifest; PyArrow checker 12/12 on both days; BigQuery backup (`querydock.kettle.builds`, 2.83 GB) reproduces all counts.
- Mistakes caught: gcloud's multi-part download silently failed and my first checksum loop printed a false "OK" on empty values; redone with a plain download and a check that fails on missing files.
- Findings: heavy skew (two kueue unit-test jobs ≈30% of Friday), 42-55% of builds have 0 test results, `version` is null (use metadata `job-version`), `result` has a lowercase `aborted`.
- Open: tenant mapping (ADR 0004) must be tested against real job names; free disk is 17 GB (plan needs ~12 GB).
