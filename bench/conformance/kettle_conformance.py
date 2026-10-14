"""Conformance check: QueryDock's Go JUnit parser vs Kettle's own Python parser.

Runs kubernetes/test-infra kettle/make_json.py's `remove_system_out` (make_db.py)
and `parse_junit` at the pinned commit on real JUnit files from a sampled day, runs
`querydock parse-junit` on the same bytes, and compares results field by field.

Usage (from repo root):
  uv run --with pyarrow python bench/conformance/kettle_conformance.py \
      --parquet data/raw/kettle-2026-10-08.parquet --builds 300 --seed 1
"""
import argparse
import ast
import json
import os
import random
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date

import pyarrow.parquet as pq

PIN = "528e0fbf0b38876ffe6b1e9e746550f2c4fc6893"
RAW = f"https://raw.githubusercontent.com/kubernetes/test-infra/{PIN}/kettle/"
API = "https://storage.googleapis.com/storage/v1/b/kubernetes-ci-logs/o"
UA = "querydock-conformance/0.1 (+https://github.com/pavanmanjunath18/querydock)"
_last = [0.0]


def polite_get(url, rps=5.0):
    wait = _last[0] + 1.0 / rps - time.time()
    if wait > 0:
        time.sleep(wait)
    _last[0] = time.time()
    for attempt in range(6):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code not in (429, 500, 502, 503, 504):
                raise
        except OSError:
            pass
        time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"giving up on {url}")


def load_kettle_functions(cache_dir):
    """Extract parse_junit / remove_system_out source from the pinned Kettle files."""
    ns = {"ET": ET, "logging": __import__("logging")}
    for fname, func in (("make_json.py", "parse_junit"), ("make_db.py", "remove_system_out")):
        path = os.path.join(cache_dir, f"{PIN[:12]}-{fname}")
        if not os.path.exists(path):
            src = polite_get(RAW + fname).decode()
            with open(path, "w") as f:
                f.write(src)
        src = open(path).read()
        node = next(n for n in ast.parse(src).body if isinstance(n, ast.FunctionDef) and n.name == func)
        exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"), ns)
    return ns["remove_system_out"], ns["parse_junit"]


def kettle_parse(remove_system_out, parse_junit, data):
    text = data.decode("utf-8")  # see report: Kettle decodes via requests' resp.text
    text = remove_system_out(text)
    try:
        ET.fromstring(text)
    except ET.ParseError:
        return {"malformed": True, "bad_time": False, "tests": []}
    try:
        return {"malformed": False, "bad_time": False, "tests": list(parse_junit(text))}
    except ValueError:
        return {"malformed": False, "bad_time": True, "tests": []}


def norm(t):
    out = {"name": t["name"], "time": float(t.get("time", 0.0))}
    if t.get("failed"):
        out["failed"] = True
        out["failure_text"] = t.get("failure_text")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--builds", type=int, default=300)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--work", default="data/raw/conformance")
    ap.add_argument("--bin", default="bin/querydock-dev")
    ap.add_argument("--report", default=f"docs/results/kettle-conformance-{date.today()}.md")
    a = ap.parse_args()
    os.makedirs(a.work, exist_ok=True)

    tbl = pq.read_table(a.parquet, columns=["path", "tests_run"]).to_pylist()
    rng = random.Random(a.seed)
    with_tests = [r["path"] for r in tbl if r["tests_run"] > 0]
    sample = rng.sample(with_tests, min(a.builds, len(with_tests)))

    files = []
    for i, path in enumerate(sample):
        prefix = path.removeprefix("gs://kubernetes-ci-logs/") + "/artifacts/"
        q = urllib.parse.urlencode({"prefix": prefix, "delimiter": "/", "fields": "items(name)"})
        listing = json.loads(polite_get(f"{API}?{q}") or b"{}")
        for item in listing.get("items", []):
            name = item["name"]
            base = name.rsplit("/", 1)[-1]
            if not (base.startswith("junit") and base.endswith(".xml")):
                continue
            local = os.path.join(a.work, name.replace("/", "__"))
            if not os.path.exists(local):
                data = polite_get(f"{API}/{urllib.parse.quote(name, safe='')}?alt=media")
                if data is None:
                    continue
                with open(local, "wb") as f:
                    f.write(data)
            files.append(local)
        if i % 50 == 0:
            print(f"{i}/{len(sample)} builds, {len(files)} files", file=sys.stderr)

    remove_system_out, parse_junit = load_kettle_functions(a.work)
    go = {}
    for chunk in range(0, len(files), 200):
        out = subprocess.run([a.bin, "parse-junit", *files[chunk:chunk + 200]],
                             check=True, capture_output=True, text=True).stdout
        for line in out.splitlines():
            r = json.loads(line)
            go[r["file"]] = r

    same = mismatch = 0
    tests_total = 0
    examples = []
    undecodable = 0
    for f in files:
        data = open(f, "rb").read()
        try:
            py = kettle_parse(remove_system_out, parse_junit, data)
        except UnicodeDecodeError:
            undecodable += 1
            py = {"malformed": True, "bad_time": False, "tests": []}
        g = go[f]
        pt = [norm(t) for t in py["tests"]]
        gt = [norm(t) for t in (g["tests"] or [])]
        tests_total += len(pt)
        if (py["malformed"], py["bad_time"], pt) == (g["malformed"], g["bad_time"], gt):
            same += 1
        else:
            mismatch += 1
            if len(examples) < 10:
                diff = next((i for i, (x, y) in enumerate(zip(pt, gt)) if x != y), min(len(pt), len(gt)))
                examples.append((f, py["malformed"], g["malformed"], len(pt), len(gt),
                                 pt[diff] if diff < len(pt) else None, gt[diff] if diff < len(gt) else None))

    os.makedirs(os.path.dirname(a.report), exist_ok=True)
    with open(a.report, "w") as r:
        r.write(f"# Kettle parser conformance ({date.today()})\n\n")
        r.write(f"- Kettle source: kubernetes/test-infra `{PIN}` (`parse_junit`, `remove_system_out`), Python {sys.version.split()[0]} stdlib ElementTree\n")
        r.write(f"- Sample: {len(sample)} builds with tests (seed {a.seed}) from `{os.path.basename(a.parquet)}`, {len(files)} JUnit files, {tests_total} tests (Kettle count)\n")
        r.write(f"- Script: `bench/conformance/kettle_conformance.py`\n\n")
        r.write(f"| Files identical | Files differing | Not UTF-8 |\n|---|---|---|\n| {same} | {mismatch} | {undecodable} |\n\n")
        if examples:
            r.write("## First differences\n\n")
            for e in examples:
                r.write(f"- `{os.path.basename(e[0])}`: malformed py={e[1]} go={e[2]}, tests py={e[3]} go={e[4]}\n  - py: `{json.dumps(e[5])[:300]}`\n  - go: `{json.dumps(e[6])[:300]}`\n")
        r.write("\nKnown, untested difference: Kettle decodes bytes with `requests`' `resp.text`; this check decodes UTF-8. Only non-ASCII text could differ.\n")
    print(f"identical={same} differing={mismatch} undecodable={undecodable} files={len(files)} tests={tests_total} -> {a.report}")
    return 0 if mismatch == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
