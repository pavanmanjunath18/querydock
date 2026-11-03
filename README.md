# QueryDock

A Kubernetes-native platform for CI test-result data:
- a Go ingest service writes real Kubernetes CI results (Kettle/BigQuery) into sharded MongoDB;
- a multi-tenant FastAPI query API, a Python SDK and a Go CLI expose them;
- an MCP server lets an LLM answer aggregation questions through a validated, read-only query layer.

**Status:** Phase 1 (data + ingest). Design: [docs/SPEC.md](docs/SPEC.md). Decisions: [docs/decisions/](docs/decisions/). Progress: [docs/BUILD_LOG.md](docs/BUILD_LOG.md).

Every number this README will report comes from a committed script under `bench/`, with its date, versions and hardware recorded in `docs/results/`.

## Data

All data is real Kubernetes CI output. It comes from Prow's **public artifacts** in `gs://kubernetes-ci-logs`
(each build's `started.json`, `finished.json` and JUnit XML), **parsed with the rules of Kubernetes test-infra's
Kettle** (ported to Go and tested against Kettle's own test cases). **Skipped tests are not counted**: a "test result"
here is a test that actually ran. The BigQuery dataset Kettle publishes to is not readable by outside accounts, which is
why the artifacts are read directly ([ADR 0005](docs/decisions/0005-collect-from-prow-public-bucket.md)).

First measured days (Fri 30 and Sat 31 Oct 2026, UTC): 32,478 builds and 19,998,540 test results; details and
verification in [docs/results/data-collection-2026-11-03.md](docs/results/data-collection-2026-11-03.md).
