# Build log

## 2026-10-13 — Session 0 (Phase 0: spec)

- Wrote `docs/SPEC.md`; the spec was approved the same day. Decisions are in SPEC §9.
- Versions checked against Homebrew, PyPI and GitHub releases.
- Kettle findings: skipped tests are dropped upstream; the `day`/`week` tables are rolling windows and `all` uses a 30-day window, so a backfill and a daily export are planned (SPEC §2.5).
- Benchmark models fixed before any results: `claude-sonnet-5-5` and `claude-haiku-5-5`. Cost estimate ≈ $58 (SPEC §5.6), not yet approved for spending.
- Daily volume isn't known yet; it needs BigQuery access.
- Environment: macOS arm64, 16 GB RAM / 8 cores, 41 GB free disk. Docker daemon isn't running; kind, Helm, Go, k6 and gcloud aren't installed.
