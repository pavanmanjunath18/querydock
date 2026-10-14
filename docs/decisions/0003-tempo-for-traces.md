# ADR 0003: Grafana Tempo for trace storage

- Status: accepted (2026-10-13)

## Context
Services emit OpenTelemetry traces (CLI → ingest → MongoDB, API → MongoDB). Prometheus, Loki and Grafana don't store traces.

## Decision
Run Tempo 3.1.0 in single-binary mode with 48 h retention. The OTel Collector exports traces to it, and Grafana links traces ↔ logs ↔ metrics. Head sampling is 100% normally and 10% during load tests.

## Consequences
About 512 MB of memory and ~1 GB of disk, in exchange for end-to-end trace views.
