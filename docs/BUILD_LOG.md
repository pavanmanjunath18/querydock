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
