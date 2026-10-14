# ADR 0001: Deploy sharded MongoDB with our own Helm chart

- Status: accepted (2026-10-13)

## Context
QueryDock needs a sharded MongoDB cluster (config servers, 2 shards, mongos) on kind, on a 16 GB laptop. The options were:
- the Bitnami `mongodb-sharded` chart: images frozen in `bitnamilegacy`, no MongoDB 8 images;
- the Percona Operator 1.23.1: heavier, needs `unsafeFlags` for single-member replica sets, and runs Percona's server build;
- a small chart of our own on the official images.

## Decision
`deploy/helm/mongodb-sharded/` on `mongo:8.0.32`:
- StatefulSets for the config-server replica set and each shard replica set (one member each);
- a mongos Deployment;
- an init Job that does `rs.initiate`, `sh.addShard`, `shardCollection`, users/roles and `$jsonSchema` validators.

## Consequences
- Smallest memory footprint, and every part of the topology is explicit and reviewable.
- No HA (single-member replica sets); this is documented in the README.
- No backups or upgrades from an operator. Out of scope for a laptop deployment.
