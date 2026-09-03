#!/usr/bin/env python3
"""Build a compact health digest of the extract pipeline's landed data.

Reads only the data directory, the source ymls, and source_types.json —
no Airflow access needed, so it runs under any Python 3.10+ with stdlib.
Output (stdout) is one JSON object per source plus a summary line, compact
enough to paste into a small model's context.

Usage: digest.py [--window-hours N] [--json]
"""
import argparse
import json
import os
import pathlib
import statistics
import sys
from datetime import datetime, timedelta, timezone

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent
SOURCES_DIR = REPO / "pipelines" / "extract" / "sources"
DATA_ROOT = pathlib.Path(os.environ.get("EXTRACT_DATA_ROOT", "~/dev/data/extract")).expanduser()
TYPES = json.loads((HERE / "source_types.json").read_text())


def read_yml(path):
    """Minimal flat 'key: value' parser — the source ymls use nothing deeper."""
    cfg = {}
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].rstrip() if not line.lstrip().startswith("#") else ""
        if ":" in line:
            k, v = line.split(":", 1)
            cfg[k.strip()] = v.strip().strip('"')
    return cfg


def load_ids(path, limit=20000):
    ids, recs = set(), {}
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= limit or not line.strip():
                break
            r = json.loads(line)
            ids.add(r.get("id"))
            recs[r.get("id")] = r
    return ids, recs


def check_source(name, cfg, window_start):
    meta = TYPES.get(name, {})
    src_dir = DATA_ROOT / "raw" / f"source={name}"
    manifests = sorted(src_dir.glob("dt=*/*.meta.json"))
    out = {
        "source": name,
        "type": meta.get("type", "unknown"),
        "enabled": cfg.get("enabled", "true") != "false",
        "runs_in_window": 0,
        "zero_record_runs": 0,
        "last_run_age_min": None,
        "expected_gap_min": meta.get("expected_gap_minutes"),
        "stale": None,
        "records_last": None,
        "records_median": None,
        "bytes_last": None,
    }
    if meta.get("notes"):
        out["notes"] = meta["notes"]
    if not manifests:
        # Never produced anything — either it hasn't reached its first cron
        # tick yet (fine) or it has been failing since creation. The manifest
        # only proves success, so distinguish via judgment, not staleness math.
        out["no_data_yet"] = True
        out["stale"] = None
        return out

    window_records = []
    for m in manifests:
        info = json.loads(m.read_text())
        started = datetime.fromisoformat(info["started_at"])
        if started >= window_start:
            out["runs_in_window"] += 1
            window_records.append(info["records"])
            if info["records"] == 0:
                out["zero_record_runs"] += 1
    last = json.loads(manifests[-1].read_text())
    last_started = datetime.fromisoformat(last["started_at"])
    age_min = (datetime.now(timezone.utc) - last_started).total_seconds() / 60
    out["last_run_age_min"] = round(age_min, 1)
    out["records_last"] = last["records"]
    out["bytes_last"] = last["bytes"]
    if window_records:
        out["records_median"] = statistics.median(window_records)
    gap = meta.get("expected_gap_minutes")
    if gap and out["enabled"]:
        out["stale"] = age_min > 2 * gap + 10

    # Compare the two most recent data files for novelty / value drift.
    files = sorted(src_dir.glob("dt=*/*.ndjson"))
    if len(files) >= 2:
        prev_ids, prev_recs = load_ids(files[-2])
        cur_ids, cur_recs = load_ids(files[-1])
        if cur_ids:
            out["id_novelty"] = round(len(cur_ids - prev_ids) / len(cur_ids), 3)
        common = prev_ids & cur_ids
        keys = meta.get("value_keys")
        if keys and common:
            changed = sum(
                1 for i in common
                if any(prev_recs[i].get(k) != cur_recs[i].get(k) for k in keys)
            )
            out["value_change_fraction"] = round(changed / len(common), 3)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-hours", type=int, default=24)
    ap.add_argument("--json", action="store_true", help="one JSON array instead of lines")
    args = ap.parse_args()
    window_start = datetime.now(timezone.utc) - timedelta(hours=args.window_hours)

    results = []
    for yml in sorted(SOURCES_DIR.glob("*.yml")):
        cfg = read_yml(yml)
        results.append(check_source(cfg.get("name", yml.stem), cfg, window_start))

    if args.json:
        print(json.dumps(results, indent=1))
    else:
        for r in results:
            print(json.dumps(r, separators=(",", ":")))
    flags = [r["source"] for r in results if r.get("stale") or r.get("zero_record_runs", 0) > 2]
    print(f"# {len(results)} sources, window={args.window_hours}h, "
          f"generated={datetime.now(timezone.utc).isoformat(timespec='seconds')}, "
          f"flagged={flags or 'none'}", file=sys.stdout)


if __name__ == "__main__":
    main()
