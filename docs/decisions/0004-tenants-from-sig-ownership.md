# ADR 0004: Tenants are Kubernetes SIGs, derived from job-config ownership

- Status: accepted (2026-10-13)

## Context
Multi-tenancy should follow real boundaries in the data. Kettle rows carry the Prow `job` name but no owner.

## Decision
Derive `job → tenant` from the directory layout of `config/jobs/kubernetes/sig-*/` in kubernetes/test-infra at a pinned commit, giving `tenants/job_to_tenant.json`. Unmapped jobs go to `unassigned`. Every document stores `tenant_map_version`.

## Consequences
- Tenants are real SIGs.
- Because `tenant` is a shard-key prefix and `_id` is unique only per shard, the map must not change for already-ingested data. A map change means a full re-ingest into a new collection.
