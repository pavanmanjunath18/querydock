"""Independent integrity check of one collected day (Parquet) against its manifest.

Reads the file with PyArrow (a different implementation from the Go writer), in
row-group batches, and compares what is actually in the file with what the collector's
manifest claims. Exits non-zero on any mismatch.

  uv run --with pyarrow python -I bench/conformance/check_day_parquet.py \
      data/raw/kettle-2026-10-31.parquet data/manifests/kettle-2026-10-31.json
"""
import collections
import datetime as dt
import json
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def main(parquet_path, manifest_path):
    m = json.load(open(manifest_path))
    day = dt.datetime.strptime(m["day"], "%Y-%m-%d").replace(tzinfo=dt.UTC)
    lo_ok, hi_ok = int(day.timestamp()), int((day + dt.timedelta(days=1)).timestamp())

    pf = pq.ParquetFile(parquet_path)
    rows = 0
    paths = set()
    sum_run = sum_failed_col = n_tests = n_failed_entries = 0
    failed_without_text = passed_with_text = hash_names_passed = neg_time = null_time = 0
    started_min, started_max = None, None
    out_of_day = zero_test_builds = max_tests = pr_builds = 0
    jobs = set()
    top = collections.Counter()
    results = collections.Counter()
    nulls = collections.Counter()
    cols = ["path", "job", "number", "started", "finished", "result", "passed", "version", "repos",
            "metadata", "test", "tests_run", "tests_failed"]

    for batch in pf.iter_batches(batch_size=500, columns=cols):
        rows += batch.num_rows
        paths.update(batch.column("path").to_pylist())
        for j, r, k in zip(batch.column("job").to_pylist(), batch.column("result").to_pylist(),
                           batch.column("tests_run").to_pylist()):
            jobs.add(j)
            top[j] += k
            results[r] += 1
            pr_builds += j.startswith("pr:")
            zero_test_builds += k == 0
            max_tests = max(max_tests, k)
        for c in ("number", "started", "finished", "result", "version", "repos", "metadata"):
            nulls[c] += batch.column(c).null_count
        sum_run += pc.sum(batch.column("tests_run")).as_py() or 0
        sum_failed_col += pc.sum(batch.column("tests_failed")).as_py() or 0
        st = batch.column("started").to_pylist()
        for s in st:
            if s is None:
                continue
            started_min = s if started_min is None else min(started_min, s)
            started_max = s if started_max is None else max(started_max, s)
            out_of_day += not (lo_ok <= s < hi_ok)
        flat = pc.list_flatten(batch.column("test"))
        n_tests += len(flat)
        if len(flat):
            failed = pc.fill_null(flat.field("failed"), False)
            text = flat.field("failure_text")
            n_failed_entries += pc.sum(failed.cast(pa.int64())).as_py() or 0
            failed_without_text += pc.sum(pc.and_(failed, pc.is_null(text)).cast(pa.int64())).as_py() or 0
            passed_with_text += pc.sum(pc.and_(pc.invert(failed), pc.is_valid(text)).cast(pa.int64())).as_py() or 0
            names = flat.field("name")
            hash_names_passed += pc.sum(pc.and_(pc.match_substring(names, "#"), pc.invert(failed)).cast(pa.int64())).as_py() or 0
            tm = flat.field("time")
            null_time += tm.null_count
            neg_time += pc.sum(pc.less(tm, 0).cast(pa.int64())).as_py() or 0

    exp = m["stats"]
    checks = [
        ("rows == manifest parquet.rows", rows, m["parquet"]["rows"]),
        ("distinct paths == rows (no duplicate builds)", len(paths), rows),
        ("sum(tests_run) == manifest stats.Tests", sum_run, exp["Tests"]),
        ("sum(len(test[])) == sum(tests_run)", n_tests, sum_run),
        ("sum(tests_failed) == manifest stats.TestsFailed", sum_failed_col, exp["TestsFailed"]),
        ("failed entries in test[] == sum(tests_failed)", n_failed_entries, sum_failed_col),
        ("failed tests without failure_text", failed_without_text, 0),
        ("passed tests with failure_text", passed_with_text, 0),
        ("passed tests with '#' in name (Kettle drops these)", hash_names_passed, 0),
        ("builds started outside the UTC day", out_of_day, 0),
        ("negative test times", neg_time, 0),
        ("tests with no time", null_time, 0),
    ]
    bad = 0
    print(f"file: {parquet_path}  day: {m['day']}")
    for label, got, want in checks:
        ok = got == want
        bad += not ok
        print(f"  {'OK  ' if ok else 'FAIL'} {label}: {got:,}" + ("" if ok else f"  (expected {want:,})"))
    ts = lambda s: dt.datetime.fromtimestamp(s, dt.UTC).strftime("%F %T")
    print(f"  info builds: {rows:,} | presubmit (pr:) {pr_builds:,} | periodic/postsubmit {rows - pr_builds:,} | distinct jobs {len(jobs):,}")
    print(f"  info started range UTC: {ts(started_min)} -> {ts(started_max)}")
    print(f"  info results: {dict(results)} | builds with 0 tests: {zero_test_builds:,} | max tests in one build: {max_tests:,}")
    print(f"  info null counts: {dict(nulls)}")
    print("  info top 5 jobs by test results:")
    for j, k in top.most_common(5):
        print(f"    {k:>10,}  {j}")
    print("RESULT:", "ALL CHECKS PASSED" if not bad else f"{bad} CHECK(S) FAILED")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
