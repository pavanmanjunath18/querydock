# QueryDock

A Kubernetes-native platform for CI test-result data:
- a Go ingest service writes real Kubernetes CI results (Kettle/BigQuery) into sharded MongoDB;
- a multi-tenant FastAPI query API, a Python SDK and a Go CLI expose them;
- an MCP server lets an LLM answer aggregation questions through a validated, read-only query layer.

**Status:** Phase 1 (data + ingest). Design: [docs/SPEC.md](docs/SPEC.md). Decisions: [docs/decisions/](docs/decisions/). Progress: [docs/BUILD_LOG.md](docs/BUILD_LOG.md).

Every number this README will report comes from a committed script under `bench/`, with its date, versions and hardware recorded in `docs/results/`.
