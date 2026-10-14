# QueryDock: specification

Status: **Approved 2026-10-13.** Decisions are recorded in §9. Any change to this spec goes in an ADR under `docs/decisions/`.

QueryDock is a Kubernetes-native platform for CI test-result data. It ingests real CI results into sharded MongoDB, serves them through a multi-tenant query API, a Python SDK and a CLI, and lets an LLM answer aggregation questions through an MCP server. Every query passes the same validator.

Rule for every number in this repo: it's produced by a committed script, measured on stated hardware and versions, and reported exactly as measured. The benchmark question set and the load-test setup are frozen before any tuning.

Items marked **[VERIFY IN P1]** can only be confirmed once BigQuery access exists. Each has a concrete first step below.

---

## 0. Summary

- **Data:** Kettle (`kubernetes-public:k8s_infra_kettle`) is public, and its schema includes per-test name, duration and failure. Two properties of the source code shape the plan:
  1. Kettle **drops skipped tests**, and passing tests carry no `failed` field, so "test results" means *executed* tests (passed + failed).
  2. `day` and `week` are **rolling windows** that get rebuilt, and `all` is loaded with a 30-day window. Older days can't be fetched again later, so every available day gets exported and frozen as soon as access works, and a daily export keeps the archive growing (§2.5).
- **Real daily volume is unknown** until one cheap BigQuery query runs (§2.4).
- **MongoDB 8.0.32** on the official images, deployed with our own Helm chart. **Go 1.27.2**, **Python 3.13**.
- **Traces** are stored in Grafana Tempo, next to Prometheus, Loki and Grafana.
- **The guarantee is layered:** read-only database role → validator → `explain()` gate → server-side limits.
- **Benchmark models:** a local open model on Ollama, **`qwen3.5:9b`** (primary, 3 runs); **Gemini Flash on Vertex AI** (1 run, paid from Google Cloud trial credit); optionally **`claude-haiku-5-5`** through the Message Batches API (1 run, ≈$1, only with approval). All scores go in one table with the model named next to each (§5.5).
- **Laptop:** 16 GB RAM / 8 cores, arm64. The stack fits in a ~10 GB Docker allocation using single-member replica sets (§7).

---

## 1. Versions (verified 2026-10-13)

Checked against Homebrew, PyPI and GitHub releases. Exact patch versions are re-checked at install time and recorded in `docs/versions.md`.

| Component | Version | Reason |
|---|---|---|
| Go | **1.27.2** | Current stable |
| MongoDB server | **8.0.32** (`mongo:8.0.32` official image) | Mature major release line. 9.0.x first shipped 2026-09-28 and its own docs still call Community availability "coming soon"; three patches landed on day one, including a sharding-metadata fix. 8.3 is a rapid release with limited Community support. Upgrading later will get its own ADR |
| MongoDB Go driver | **v2.9.2** | Includes the fixes for CVE-2026-88031 (GridFS) and CVE-2026-81521 (BulkWrite). We use BulkWrite, so a version before 2.8.2 is ruled out |
| mongosh | 2.13.0 | Init jobs, debugging |
| Python | **3.13** | Every dependency supports ≥3.10; 3.13 has the widest wheel coverage |
| FastAPI / Uvicorn / Pydantic | **0.143.0** / 0.54.0 / 2.14.1 | |
| PyMongo | **4.18.3**, async API (`AsyncMongoClient`) | Motor is deprecated; PyMongo Async replaces it |
| MCP spec | **2026-07-28** | Current revision (no sessions or `initialize` handshake) |
| MCP Python SDK | **mcp 2.3.0** | The v2 line implements 2026-07-28; v1 only gets security fixes |
| Anthropic Python SDK | 1.13.0 | Benchmark harness |
| google-cloud-bigquery | 3.46.1 | Export job |
| kind | **0.33.0** | |
| kubectl | 1.34.1 | Match it to kind's node image (skew ≤ ±1) |
| Helm | **4.3.0** | |
| OTel Go SDK / contrib | v1.47.0 / v1.47.0 | |
| OTel Python SDK | 1.45.1 | |
| OTel Collector (contrib) | **v0.162.0** | |
| Prometheus | **3.15.0** | kube-prometheus-stack chart 92.3.1, slimmed down (§7) |
| Loki | **3.7.8**, single binary | Chart maintained by grafana-community (`loki-18.15.1`) |
| Log shipper | **Grafana Alloy 1.20.1** (chart `alloy-1.13.1`) | Promtail reached EOL on 2026-03-02 |
| Tempo | **3.1.0**, single binary | Trace storage |
| Grafana | **13.2.4** | Grafana 12 is out of support |
| k6 | **2.3.0** | Scripts written against v2 from the start |

Not used: the Bitnami `mongodb-sharded` chart. Its images moved to an unmaintained `bitnamilegacy` registry in Aug 2025, and there are no MongoDB 8 images.

---

## 2. The Kettle dataset

### 2.1 Schema (`kettle/schema.json`, test-infra master)

One row per **build**, with tests nested inside:

| Field | Type | Notes |
|---|---|---|
| `path` | STRING | GCS path of the build, e.g. `gs://kubernetes-ci-logs/logs/<job>/<number>`. **Unique per build, so it's the natural key** |
| `job` | STRING | Prow job name |
| `number` | INTEGER | Build number |
| `started`, `finished` | TIMESTAMP | |
| `elapsed` | FLOAT | Seconds |
| `passed` | BOOLEAN | |
| `result` | STRING | `SUCCESS` / `FAILURE` / … |
| `version` | STRING | Kubernetes version under test |
| `repos`, `repo_commit` | STRING | |
| `executor` | STRING | |
| `tests_run`, `tests_failed` | INTEGER | Cheap counters; used for the volume query |
| `metadata` | REPEATED RECORD {key, value} | |
| `test` | REPEATED RECORD {`name` STRING, `time` FLOAT, `failed` BOOLEAN, `failure_text` STRING} | One entry per executed test case |

What `make_json.py` does that matters here:
- `parse_junit` **ignores skipped tests**.
- A passing test has no `failed` key, so it reads as NULL. Rule: `failed IS NULL OR failed = false` → passed.
- `time` can be missing; it's stored as null, never as 0.
- A test name can appear more than once in one build (several JUnit files, retries). The ID scheme in §3.2 handles this.

### 2.2 How the tables are maintained (`kettle/update.py`)

| Table | Window | Load mode |
|---|---|---|
| `day` | builds from the last 1 day | rebuilt |
| `week` | last 7 days | rebuilt |
| `all` | loaded with a 30-day window (temporary limit per code comment, issue #20024) | appended |

The Kettle README also notes that it "occasionally stops updating", so the table's last-modified time is checked before a day is treated as complete.

### 2.3 Access and cost

- Project `querydock` is linked to the Google Cloud free-trial billing account, so it is **not** limited to the sandbox. The BigQuery free tier still applies: **1 TB of queries and 10 GB of storage per month**, then $6.25/TB queried, paid from trial credit. Safety nets: a custom quota of ~500 GB/day, budget alerts at $10/$25, and the 500 GB/month stop-and-ask rule.
- Exporting to GCS needs a billed bucket, so instead **query results are paged straight to local NDJSON** with the Python client. Reading results is not billed as a query. Fallback: write a sandbox table one day at a time and page it with `tabledata.list`.
- **Bytes scanned are unknown [VERIFY IN P1].** If a table isn't partitioned on `started`, a date filter doesn't reduce the bytes billed: each query costs the size of the referenced columns across the whole table.
- **Guardrails:**
  - Every query gets a `--dry_run` first and runs with `--maximum_bytes_billed`.
  - A running ledger is kept in `docs/results/bq_usage.md`.
  - **Work stops for maintainer approval if planned usage in a calendar month would pass 500 GB.**

### 2.4 First queries (Phase 1, in order)

```bash
# 1. Free: metadata only. Size, row count, partitioning, last modified.
bq show --format=prettyjson kubernetes-public:k8s_infra_kettle.all
bq show --format=prettyjson kubernetes-public:k8s_infra_kettle.week
bq show --format=prettyjson kubernetes-public:k8s_infra_kettle.day
```

```sql
-- 2. Daily volume, the cheap way: reads only `started` and `tests_run`.
SELECT DATE(started) AS day, COUNT(*) AS builds, SUM(tests_run) AS tests_run, SUM(tests_failed) AS tests_failed
FROM `kubernetes-public.k8s_infra_kettle.all`
GROUP BY day ORDER BY day;

-- 3. Check that tests_run matches the nested array, for one day only (dry run first).
SELECT SUM(tests_run) AS counter, SUM(ARRAY_LENGTH(test)) AS actual
FROM `kubernetes-public.k8s_infra_kettle.week`
WHERE started >= TIMESTAMP('2026-10-12') AND started < TIMESTAMP('2026-10-13');

-- 4. Full export columns.
SELECT path, job, number, started, finished, elapsed, result, passed, version,
       repos, repo_commit, executor, tests_run, tests_failed, metadata, test
FROM `kubernetes-public.k8s_infra_kettle.<table>`
WHERE started >= @start AND started < @end;
```

**Real daily volume** = number of `test` entries across builds whose `started` falls on that UTC day, counted from the frozen export (the actual ingest input). Query 2 is a cross-check.

### 2.5 Export plan

**Backfill (once, as soon as access works):**
1. Run `bq show` (free), then dry-run a `CREATE TABLE querydock.kettle.all_snapshot_<date> AS SELECT <export columns> FROM kubernetes-public.k8s_infra_kettle.all`. That is **one scan** of the public table, and it leaves a copy in our own project. Show the byte count and wait for approval if it would pass the 500 GB stop.
2. Download our copy into **zstd-compressed Parquet**, one file per UTC day: `data/raw/kettle-YYYY-MM-DD.parquet`. Reading our own table doesn't scan the public one again. Read-path cost is checked before running.
3. Commit a manifest per day: builds, tests, SHA-256, source table + `lastModifiedTime`, query text, export time. `data/raw/` is gitignored.
4. The first and last days are partial; they're marked `partial: true` and excluded from per-day volume figures.

**Daily export (ongoing, so no day is lost):**
- A launchd job on the dev Mac (`deploy/local/com.querydock.kettle-export.plist`) runs `bench/data/export_daily.py` once a day. It's idempotent.
- **Primary path:** append the whole `day` table (the smallest table) to the project's BigQuery snapshot table and write it locally as Parquet under `data/raw/snapshots/`. Calendar days are assembled from overlapping snapshots, deduplicated on `path`.
- **Catch-up path:** when a gap is detected (laptop asleep, Kettle stalled), it pulls the missing range from `week`. That covers up to 7 days of downtime.
- **Freezing:** a calendar day is frozen only once it's ≥ 2 days old, because Kettle backfills late builds. Before that the day is provisional.
- Every run enforces `--maximum_bytes_billed` and the monthly ledger. If projected monthly usage (bytes per day-snapshot × 30, plus backfill) is over 500 GB, the job doesn't run and the maintainer is asked first.
- Each run logs a line to `data/raw/export_log.ndjson`. A missed run shows up as a gap in the log.

**Fixture for tests:** a slice of one real day, about 50 builds over 3 tenants, with hand-checked answers. It's committed (small, real, from public CI logs).

### 2.6 Tenants

Tenant = **the Kubernetes SIG that owns the job**. It's derived from the directory layout of `config/jobs/kubernetes/sig-*/` in test-infra at a pinned commit, giving `tenants/job_to_tenant.json`. Unmapped jobs go to `unassigned`. The map version is stored on every document, because the tenant is part of the shard key (§3.3).

### 2.7 Fallback source

If Kettle access fails or the data is stale: JUnit artifacts from GitHub Actions on large open-source repos. Switching needs maintainer approval.

---

## 3. MongoDB design

### 3.1 Collections (database `querydock`)

**`test_results`** — one document per executed test case.
```js
{
  _id: "<hex sha256(path \x00 name \x00 occurrence)>",   // deterministic
  tenant: "sig-node",
  job: "ci-kubernetes-node-e2e-containerd",
  build_id: "<sha256(path)>",
  build_number: 12345,
  started: ISODate(...),          // build start (UTC); time axis for all queries
  name: "[sig-node] Pods should ...",
  status: "passed" | "failed",
  duration_s: 12.3 | null,
  failure_text: "<first 4 KB>" | null,  failure_truncated: bool,
  version: "v1.36.0-alpha...", repo_commit: "abc123",
  tenant_map_version: "test-infra@<sha>",
  ingested_at: ISODate(...)
}
```
**`builds`** — one per build, `_id = sha256(path)`. Build-level fields plus `tests_run`/`tests_failed`.
**`rollup_test_daily`** — one per (tenant, day, job, name): `runs, fails, sum_duration_s, max_duration_s`. Maintained by a scheduled internal job using `$merge`, under a separate write role and outside the validator path.
**`ingest_checkpoints`**, **`dead_letters`** — ingest bookkeeping.
Control data (`tenants`, `api_keys`) lives in a separate database, `querydock_control`, which the query role can't read.

**Schema enforcement:** `test_results`, `builds` and `rollup_test_daily` are created with a `$jsonSchema` validator (`validationLevel: "strict"`, `validationAction: "error"`): required fields, BSON types, `status` enum, `duration_s` ≥ 0 or null, and `tenant` matching `^[a-z0-9-]+$`. The ingest service sends documents that fail it to `dead_letters` instead of dropping them. Tests insert bad documents and assert they're rejected. Schemas live in `deploy/mongo/schemas/*.json` and are applied by the init Job.

`occurrence` = the 0-based count of earlier entries with the same name in that build's `test` array. It's stable because export files are frozen.

### 3.2 Idempotent writes

- Deterministic `_id`. Writes are unordered `InsertMany`; duplicate-key errors (E11000) mean "already written", not failure. Replays carry identical content, so this is correct, and it's cheaper than full-document upserts.
- The collector stores its checkpoint (file, line offset) **after** the ingest service acknowledges a batch. A restart replays from the checkpoint, and partially written batches are harmless.
- **Sharding gotcha:** in a sharded cluster, `_id` is unique only *per shard*. Uniqueness across the cluster holds only because a given document always routes to the same shard, which requires `tenant` to be deterministic. That's why `tenant_map_version` is pinned.

### 3.3 Shard key candidates (`test_results`, compared by measurement)

| # | Key | For | Against |
|---|---|---|---|
| A | `{tenant: 1, _id: "hashed"}` **(lead candidate)** | Every query has a forced tenant filter (§4), so it's **targeted**. The hashed suffix spreads one tenant's writes across shards, with no monotonic hot spot | Time ranges within a tenant still touch all of that tenant's chunks |
| B | `{tenant: 1, started: 1}` | Tight, targeted time ranges | Monotonic writes per tenant mean a hot chunk on one shard; jumbo-chunk risk for large tenants |
| C | `{_id: "hashed"}` | Most even writes | **Every query is scatter-gather.** The baseline that shows the tenant prefix's value |

Comparison metrics (`docs/perf/shard-key.md`): ingest docs/s, data distribution (`$shardedDataDistribution`), shards targeted per benchmark query (`explain`), and p95 under k6. Rebuilding the collection three times at full size is slow, so the comparison runs on **2 real days**, and only the winner is loaded at full size. The doc says so.

### 3.4 Deployment on Kubernetes (decided: own chart)

`deploy/helm/mongodb-sharded/`, our own chart using the official `mongo:8.0.32` image:
- StatefulSets for 1 config-server replica set and 2 shard replica sets.
- A mongos Deployment.
- An init Job: `rs.initiate`, `sh.addShard`, `shardCollection`, roles and users.

Each replica set has **one member**: 2 shards, no HA. The README says so.

### 3.5 Index plan to test (ESR: equality, sort, range)

| Index | Serves |
|---|---|
| `{tenant: 1, started: -1}` | Pass rate and failure trends over a window |
| `{tenant: 1, name: 1, started: -1}` | Per-test history, flaky detection, slowest tests for a named test |
| `{tenant: 1, job: 1, started: -1}` | Pass rate by job, "what broke since build X" |
| `{tenant: 1, started: -1, name: 1}` partial on `{status: "failed"}` | Failure-only queries, with a much smaller index |
| `builds: {tenant: 1, job: 1, number: -1}` | Resolving "since build X" to a start time |

Each index is justified by an `explain()` diff in `docs/perf/`. Indexes that no query uses (per `$indexStats`) get dropped.

**Load-test baseline** = shard-key index + `_id` only: no query indexes and no rollups, with the same data, hardware and k6 script.

---

## 4. Validator (`shared/validator`, one implementation used by both the API and MCP)

Input: `(tenant_from_api_key, collection, pipeline, options)`. Output: an approved pipeline with execution options, or a list of rejection reasons. It's pure Python with no database access, and fully unit-tested. The `explain()` gate runs in the API after validation.

1. **Collection allowlist:** `test_results`, `builds`, `rollup_test_daily`.
2. **Stage allowlist:** `$match, $project, $addFields/$set, $unset, $group, $sort, $limit, $skip, $count, $unwind, $bucket, $bucketAuto, $sortByCount, $facet` (sub-pipelines validated recursively), `$replaceRoot/$replaceWith, $setWindowFields`.
   **Rejected:** `$out, $merge` (writes); `$lookup, $graphLookup, $unionWith` (they read other collections); `$collStats, $indexStats, $currentOp, $listSessions, $planCacheStats, $documents, $changeStream`; and **anything not listed**.
3. **Expression operator allowlist**, walked recursively through every value. `$function`, `$accumulator`, `$where` (server-side JavaScript) and unknown operators are rejected. Field paths must match the schema.
4. **Forced tenant filter:** the server **prepends** `{$match: {tenant: <key's tenant>}}`. The tenant never comes from the request body. Combined with rule 2, no stage can reach another tenant's documents.
5. **Limits:** at most 20 stages and nesting depth 10. The server **appends** `$limit` = min(requested, 1000). `maxTimeMS` = 5000. `allowDiskUse: false`. Request body ≤ 64 KB. Results ≤ 1000 documents / 4 MB.
6. **`explain()` gate:** `queryPlanner` verbosity. Reject a winning plan with `COLLSCAN` on `test_results`, or one that targets more shards than hold the tenant's chunks. The rejection names a usable index, so an LLM caller can fix its query.
7. **Defense in depth:** the API's MongoDB user has only `read` on `querydock`. A test checks that writes fail even when validation is bypassed.

Red-team suite (`api/tests/redteam/`):
- `$out`/`$merge` at the top level and inside `$facet`
- `$lookup` to `querydock_control.api_keys`, and `$unionWith`
- `$function`/`$where`
- `{$match: {tenant: {$ne: ...}}}` and `$or` tricks
- unknown operators, huge `$limit`, deep nesting, an unindexed `$group` over everything
- reader-role keys calling admin endpoints
- tenant A's key fetching tenant B's IDs

**Pass = every attempt rejected, or zero foreign documents returned.**

---

## 5. Benchmark (150 questions)

### 5.1 Frozen dataset
Questions run against a fixed snapshot of 2 real days, identified by manifest SHA-256. Expected answers never change.

### 5.2 Writing the set
- **Six categories, about 25 each:**
  - template-shaped (flaky tests, failure trends, slowest tests, since-build-X, pass rate by job)
  - parameterized variants
  - multi-step (group, then rank)
  - time-window reasoning
  - per-test drill-down
  - phrasing variety
- **Each item has:** `id`, question, tenant, **required output fields** (scoring compares values, not names), `order_matters`, a hand-written reference pipeline, and an expected result computed by running that pipeline on the snapshot. Items also record an `author` field (human or model-drafted).
- **Who writes them:** the maintainer writes part of the set and reviews every item. The rest are drafted by Claude.
- **Freeze before any tuning:** commit, tag `bench-v1`, and record the file's SHA-256 in the README **before any MCP prompt or tool-description work**.
- **Tuning happens only on a separate 30-question dev set**, never on the 150.
- **Changes create versions:** any later change creates `bench-v2`, both scores are reported, and no question is ever dropped.

### 5.3 Scoring, in code (`bench/score.py`)
- A question is correct when the returned rows equal the expected rows on the required fields after normalization.
- Rows are sorted unless `order_matters`. Numbers match within relative 1e-6 / absolute 0.01 for durations.
- Top-k ties: any order that respects the ties is accepted.
- Refusals and validator rejections count as wrong.

### 5.4 Conditions
- **(a) QueryDock:** the model works through the MCP tools in a fixed order:
  1. **Pick a template and fill in its parameters first.**
  2. **Write a raw pipeline only when no template fits.**
  3. Every pipeline, from a template or written by hand, goes through the validator and the `explain()` gate. Rejections come back to the model as feedback.
- **(b) Baseline:** same model and schema description; writes one raw pipeline that runs read-only, with no templates and no retry loop.

### 5.5 Models and run settings (fixed 2026-10-13, before any results)

| Role | Model (pinned) | Served by | Runs | Paid from |
|---|---|---|---|---|
| Primary | **`qwen3.5:9b`**, Ollama ID `56671c2ab938` (9.0B, Q4_K_M), Ollama 0.35.1 | Ollama on the dev Mac | **3** × 150 per condition | Free (local) |
| Second | **Gemini Flash**: exact model ID pinned from the Vertex AI model list on the day the project is created, and recorded here before any run | Vertex AI | **1** × 150 per condition | Google Cloud trial credit |
| Optional reference | **`claude-haiku-5-5`** | Anthropic Message Batches API | **1** × 150 per condition | ≈$1, **only after approval** |

- **Why `qwen3.5:9b`:** the newest small Qwen model, with native tool calling and a reported BFCL-V4 score of 66.1 (Qwen's own figure). At 5.8 GB resident with an 8K context, it fits next to the benchmark backend. A 3-question smoke test on 2026-10-13 produced well-formed calls on all 3 and picked the right tool on 2. That test is not part of the benchmark, and the model was not chosen by tuning against it.
- **Benchmark backend:** the benchmark runs against a single-node MongoDB holding the frozen 2-day snapshot, plus the API and MCP server. The kind cluster is stopped during local-model runs, which leaves ~8 GB for the model. Latency numbers are never taken from those runs.
- **Local settings:** `think: false` for speed (recorded), `num_ctx: 32768`, Ollama default sampling. Variance is captured by the 3 runs.
- **Gemini settings:** the provider's default sampling. Thinking and effort settings are recorded with the results.
- **Haiku settings:** effort pinned to `medium`, the model's default. Each tool-loop turn is one batch round across all questions (about 4–6 rounds). Batch prompt caching is best-effort, so the cost estimate assumes no cache hits.
- **Every model:** the same MCP tools, system prompt and schema description. Refusals and malformed tool calls count as wrong and are reported separately. **No model fallback.**
- **Reporting:** one table, one row per model × condition, with the **model name and pinned version next to every score**, the run count, mean, min–max, invalid-call rate and date. Every measured score is reported, including the weak ones.

### 5.6 Cost estimate (2026-10-13)

Per-question token assumptions (corrected after a 5-question pilot): QueryDock ≈ 45K input tokens over about 4 calls + 3.2K output; baseline ≈ 6.2K input + 1.5K output.

| Model | Prices per MTok (in / out) | QueryDock 150 q | Baseline 150 q | Pilot + dev tuning | Total |
|---|---|---|---|---|---|
| `qwen3.5:9b` (3 runs) | local | $0 | $0 | $0 | **$0**, about 6–10 h of laptop time |
| Gemini Flash (1 run) | $0.75 / $3.75 (introductory price until 2026-12-31, global endpoint) | ≈$6.90 | ≈$1.50 | ≈$1.50 | **≈$10 of trial credit** |
| `claude-haiku-5-5` batch (1 run) | $0.05 / $0.25 (batch, 50% off) | ≈$0.46 | ≈$0.08 | — | **≈$0.55** (≤$1) |

Prompt tuning happens only on the dev set and only with `qwen3.5:9b`. The tuned prompts are frozen before any test run on any model. Before Vertex AI or the Anthropic API is called, the estimate is reissued for approval.

---

## 6. Measurement plan

Every result file goes to `docs/results/<topic>-<date>.md` and states the script, git SHA, versions, hardware and dataset manifest. The README reports these numbers exactly as measured.

| # | What's measured | Script / method | Reported as |
|---|---|---|---|
| 1 | Real daily CI volume and ingest capacity | `bench/volume/count_real_days.py`: executed tests per complete UTC day from the frozen exports, cross-checked against post-ingest counts. `bench/volume/throughput.sh`: replays one real day at full speed for sustained docs/s, **labelled as replay** | Mean, min and max per day over all complete days, noting that skipped tests aren't recorded upstream; capacity in docs/s and docs/day |
| 2 | Multi-tenant REST API, SDK, CLI, API-key auth | pytest per endpoint; isolation test (A's key on B's data → 0 rows / 404); SDK and CLI tests | Test pass/fail in CI |
| 3 | Idempotent ingest under restarts | `bench/chaos/restart_150.sh`: 150 seeded random force-kills of the ingest pod mid-batch, then duplicates on the natural key (path, name, occurrence) **and** missing documents against the source count | Exact duplicate and missing counts. Zero duplicates is expected by construction, so "missing" is the main signal |
| 4 | Aggregation templates | Unit tests against hand-computed fixture answers | Test pass/fail |
| 5 | MCP query builder | Works from Claude Desktop and Claude Code; recorded demo | Demo video |
| 6 | Guardrails | Red-team suite (§4) | Pass/fail. A failure is a bug to fix, never a test to drop |
| 7 | NL query accuracy | §5 | Mean (min–max) per model × condition, with model IDs and date |
| 8 | Index, rollup and shard-key effect | `docs/perf/*.md`: before/after `explain`, shard-key comparison | Plans and tables |
| 9 | Query latency under load | `bench/k6/query_mix.js`: constant-arrival-rate **150 req/s**, 2 min warm-up + 10 min measured, fixed weighted template mix across tenants, seeded parameters, k6 outside the cluster; median of 3 runs. Baseline per §3.5. Dataset: every real day available, up to ~12M documents, **no synthetic padding** | Baseline and optimized p95, % change, exact document count, "single laptop" |
| 11 | API versioning | `/v1` and `/v2` run side by side; **one real breaking change** in `/v2`: cursor pagination replaces offset, and `duration_s` (float) becomes `duration_ms` (int). Contract tests pin both versions; `/v1` responses carry a `Deprecation` header | Both test suites green |
| 12 | Per-tenant RBAC | Roles per tenant: `admin` (create, rotate and revoke that tenant's keys; query) and `reader` (query only). Tests: a reader can't manage keys (403); an admin of A can't touch B's keys (404); revoked keys fail (401) | Test pass/fail in CI |
| 13 | Schema enforcement | `$jsonSchema` tests (§3.1) | Test pass/fail |
| 14 | Live demo on AWS | `deploy/terraform/`: one EC2 instance running k3s with the stack (Helm), for a 1–2 day demo, then `terraform destroy`. `terraform plan` + a cost estimate are shown first; **nothing is created without approval** | Demo URL while up; destroy confirmed in `docs/results/` |
| 10 | Observability | OTel traces CLI → ingest → MongoDB and API → MongoDB in Tempo; Prometheus metrics; Loki logs via Alloy; Grafana dashboards as JSON; alerts: ingest dead-letter rate, API p95 > 300 ms for 5 min, no ingest for 15 min | Screenshots + JSON in the repo |

---

## 7. Laptop resource plan (16 GB, 8 cores, arm64)

Docker Desktop: **10 GB RAM, 6 CPUs, 60 GB disk image** (41 GB free on the host today; the plan needs ~30 GB). One kind node.

| Workload | Memory limit | Notes |
|---|---|---|
| kind control plane + system pods | ~1.2 GB | |
| Config server (1-member RS) | 512 MB | |
| Shard 0, shard 1 (1-member RS each) | 1.75 GB each | `wiredTigerCacheSizeGB: 1` |
| mongos | 256 MB | |
| ingest (Go) | 128 MB | |
| query API ×2 | 256 MB each | |
| MCP server | 128 MB | Usually local over stdio |
| Prometheus (2-day retention) + Alertmanager + kube-state-metrics | ~900 MB | node-exporter and default rules off |
| Loki single binary | 512 MB | |
| Tempo single binary | 512 MB | |
| Alloy | 256 MB | |
| OTel collector | 256 MB | |
| Grafana | 256 MB | |
| **Total** | **~9.2 GB** | |

Data estimate: ~350 B per test document → ~4.2 GB uncompressed for 12M documents, ~1.5–2 GB on disk compressed, plus 1–2 GB of indexes. The working set is bigger than the 2 GB of total cache.

**Disk plan** (host free space 2026-10-13: 38 GB). Exports are **zstd-compressed Parquet only** (no raw JSON on disk). Prometheus, Loki and Tempo keep **48 hours**.

| Component | Estimate | Basis / control |
|---|---|---|
| Kettle Parquet archive (backfill ~30 days) | 3–9 GB | ~0.1–0.3 GB/day compressed (test names dictionary-encode well); measured on day 1 and this row updated |
| Daily export growth | ~0.1–0.3 GB/day | Same; `day` snapshots are merged into frozen days, then deleted |
| MongoDB, 2 shards, ~12M docs | 3–4 GB | ~1.5–2 GB of zstd-compressed data (`blockCompressor: zstd`) + 1–2 GB of indexes |
| Shard-key comparison (2 days, rebuilt 3×) | ~1 GB transient | Dropped after each variant |
| Benchmark single-node MongoDB (2-day snapshot) | ~0.5 GB | |
| Prometheus, 48 h | ~0.5 GB | Small cluster, 15 s scrape |
| Loki, 48 h | ~0.5–1 GB | Info-level logs, no per-document lines |
| Tempo, 48 h | ~1 GB | 100% sampling normally, 10% head sampling during k6 runs |
| Container images (Docker + copies inside the kind node) | ~6–7 GB | kind node, mongo 8.0.32, observability, 3 service images |
| `qwen3.5:9b` | 6.6 GB | Already pulled |
| Go module + Python caches | ~1.5 GB | |
| **Total new** | **≈ 24–33 GB** | Tight against 38 GB free, so clearing some space first is recommended |

**Scale-down order if it doesn't fit:**
1. Drop the second API replica.
2. Turn off Alertmanager (use Grafana alerting).
3. Lower Loki and Tempo to 256 MB.
4. Shard cache to 0.75 GB.
5. Pause observability during bulk loads only, never during measured runs.

---

## 8. Plan: 15 working days × ~5 h (≈75 h over 3 weeks)

Each day's exit check has to pass before moving on. The schedule slips rather than skipping an exit check. If time runs short, cut in this order: **rollup collections first, then the Gemini column**. `/v1`+`/v2`, RBAC, schema validation, the Terraform/EC2 demo and model-named scores are required and are not cut.

**Week 1: data and ingest (P1–P2)**

| Day | Work | Exit check |
|---|---|---|
| D1 | Repo + CI skeleton, `.gitignore`, ADRs 0001–0004 (own Mongo chart, MongoDB 8.0, Tempo, SIG tenancy). Tool installs. GCP sandbox auth | `bq ls kubernetes-public:k8s_infra_kettle` works |
| D2 | `bq show` + dry runs; budget check; **backfill export of `all`** (§2.5); manifests | Every available day frozen; bytes logged |
| D3 | Volume script → **real daily volume recorded**; launchd daily export; tenant map; fixture slice | `docs/results/volume-*.md` committed; first scheduled run logged |
| D4 | Go collector: NDJSON reader, batching, checkpoints. Ingest service: validation, deterministic IDs, unordered insert, retries/backoff, dead-letter. Single-node Mongo in Docker | Unit tests green |
| D5 | Idempotency tests (replay, partial batch, kill mid-batch locally); ingest 1 real day | Source count = DB count; 0 natural-key duplicates |

**Week 2: API, benchmark, Kubernetes (P3–P5)**

| Day | Work | Exit check |
|---|---|---|
| D6 | FastAPI: tenants, API keys (hashed), RBAC (admin/reader), `/v1`; isolation tests | Isolation test passes |
| D7 | 5 templates + fixture-checked tests; Python SDK; CLI `push`/`query` | Templates match hand-computed answers |
| D8 | **Benchmark authoring:** freeze the 2-day snapshot; draft questions; maintainer writes and reviews; reference pipelines + expected results; 30-question dev set; **tag `bench-v1`** | Tag + SHA-256 in the README, before any MCP work |
| D9 | `shared/validator` + red-team suite + `explain()` gate; MCP server (templates + validated builder) | All red-team attempts rejected; works from Claude Code |
| D10 | Benchmark harness + scorer; **pilot (with cost approval)**, then dev-set tuning and full runs within the approved budget. kind cluster + own MongoDB chart (2 shards) | Scores recorded per model × condition; sharded cluster up |

**Week 3: scale, reliability, polish (P5–P8)**

| Day | Work | Exit check |
|---|---|---|
| D11 | All services on kind via Helm; full flow CLI → ingest → mongos → API → MCP | End-to-end smoke test in CI (kind) |
| D12 | Shard-key comparison (2 days); load every real day; **baseline k6** | `docs/perf/shard-key.md` + baseline results committed |
| D13 | Indexes + rollups from `explain`; re-run k6 ×3 | Before/after p95 + plans committed |
| D14 | Chaos ×150; `$jsonSchema` validators + tests; OTel/Tempo, Prometheus, Loki/Alloy, Grafana dashboards + 3 alerts | Duplicate/missing counts committed; dashboards JSON in the repo |
| D15 | `/v2` breaking change (cursor pagination, `duration_ms`); Terraform EC2 + k3s (plan + cost estimate shown first, created only after approval, destroyed after the demo); README with measured numbers, diagram, quickstart; demo video | Every README number links to a results file |

---

## 9. Decisions (2026-10-13)

1. MongoDB on Kubernetes: **our own Helm chart, official `mongo:8.0.32` images.**
2. **Grafana Tempo** added for traces.
3. Capacity: **~5 h/day for 2–3 weeks**; plan in §8.
4. Benchmark: the maintainer **writes part of the 150 and reviews all of them** in week 2.
5. Models (revised 2026-10-13, before any results): **`qwen3.5:9b` on Ollama, primary, 3 runs; Gemini Flash on Vertex AI, 1 run (trial credit); optional `claude-haiku-5-5` via Message Batches, 1 run (≈$1, approval needed)**. Template-first MCP flow. All scores in one table, with the model named next to each.
6. Repo: **public on GitHub**, pushed as work proceeds; `docs/HANDOFF.md` is gitignored.
7. Data: **back-fill everything in `all` to zstd Parquet, keep a copy as a BigQuery table in the `querydock` project, then export daily**; stop for approval above 500 GB/month.
8. Required features (never cut): `/v1` + `/v2` with one breaking change; per-tenant RBAC with tests; `$jsonSchema` validation; Terraform EC2 + k3s demo (approval before creation, destroyed after); model named next to every score. Cut order if behind: rollups, then the Gemini column.
9. Google Cloud: project `querydock`, BigQuery + Vertex AI APIs, budget alerts at $10 and $25, custom BigQuery query quota of ~500 GB/day. The maintainer does the free-trial activation (payment, identity, terms) personally.

---

## Sources

- Kettle: [README](https://github.com/kubernetes/test-infra/blob/master/kettle/README.md), [schema.json](https://github.com/kubernetes/test-infra/blob/master/kettle/schema.json), [update.py](https://github.com/kubernetes/test-infra/blob/master/kettle/update.py), [make_json.py](https://github.com/kubernetes/test-infra/blob/master/kettle/make_json.py)
- [BigQuery sandbox](https://cloud.google.com/bigquery/docs/sandbox)
- [Claude API pricing](https://platform.claude.com/docs/en/about-claude/pricing), [prompt caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)
- [MCP spec 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28), [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk/releases)
- [MongoDB 9.0 release notes](https://www.mongodb.com/docs/manual/release-notes/9.0/), [MongoDB 8.0 release notes](https://www.mongodb.com/docs/v8.0/release-notes/8.0/), [Go driver release notes](https://www.mongodb.com/docs/drivers/go/upcoming/reference/release-notes/)
- [Bitnami catalog changes](https://northflank.com/blog/bitnami-deprecates-free-images-migration-steps-and-alternatives), [Promtail EOL](https://grafana.com/docs/loki/latest/send-data/promtail/)
- Version numbers: Homebrew, PyPI and GitHub releases, checked 2026-10-13
