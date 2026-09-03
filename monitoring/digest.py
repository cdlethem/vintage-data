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
    # Merge success + failure events on a timeline so we can tell "flaky but
    # currently up" (failures scattered among successes) from "down now"
    # (failures trailing the last success). The latter is the real alarm.
    events = []
    for m in manifests:
        events.append((json.loads(m.read_text())["started_at"], "ok"))
    out["failed_runs"] = 0
    for m in sorted(src_dir.glob("dt=*/*.fail.json")):
        info = json.loads(m.read_text())
        events.append((info["started_at"], "fail"))
        if datetime.fromisoformat(info["started_at"]) >= window_start:
            out["failed_runs"] += 1
            err = (info.get("error") or "").strip().splitlines()
            out["last_error"] = err[-1][:160] if err else f"exit {info.get('exit_code')}"
    events.sort()
    consecutive = 0
    for _, kind in reversed(events):
        if kind == "fail":
            consecutive += 1
        else:
            break
    out["consecutive_failures"] = consecutive

    if not manifests:
        # Never succeeded — either it hasn't reached its first cron tick yet
        # (fine, failed_runs 0) or it has been failing since creation (visible
        # in failed_runs). Staleness math needs a last success, so leave null.
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
            sample = cur_recs[next(iter(common))]
            present = [k for k in keys if k in sample]
            if not present:
                # Guard against stale config: comparing keys the records don't
                # carry reads as "never changes" and fakes a frozen source.
                out["value_keys_missing"] = keys
            else:
                changed = sum(
                    1 for i in common
                    if any(prev_recs[i].get(k) != cur_recs[i].get(k) for k in present)
                )
                out["value_change_fraction"] = round(changed / len(common), 3)
    return out


def classify(r):
    """Deterministic health status. Reliable failure signals (staleness,
    repeated crashes, config drift) are PROBLEM; softer signals (isolated
    failures, frozen novelty/values) are WATCH because several sources have
    legitimate quiet periods that only the notes/model can dismiss. Everything
    else is OK — so a healthy pipeline needs no model call at all."""
    if not r.get("enabled", True):
        return "OK"  # intentionally paused; not a health signal
    if r.get("value_keys_missing"):
        return "PROBLEM"  # monitoring misconfig — the drift check is blind
    if r.get("stale"):
        return "PROBLEM"
    gap = r.get("expected_gap_min") or 0
    if r.get("no_data_yet"):
        # Only a problem for a fast source that should have produced by now.
        return "PROBLEM" if gap and gap < 1440 else "WATCH"
    if r.get("consecutive_failures", 0) >= 3:
        return "PROBLEM"  # trailing failures since last success — down now
    if r.get("failed_runs", 0) >= 1:
        return "WATCH"  # flaky but recovered — worth a note, not an alarm
    if r["type"] == "feed" and r.get("id_novelty") == 0.0 and gap and gap <= 60:
        return "WATCH"  # a fast feed with no new ids — often quiet hours, check notes
    if r["type"] == "status" and r.get("value_change_fraction") == 0.0 and gap and gap <= 30:
        return "WATCH"  # frozen values on a source that should be drifting
    return "OK"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-hours", type=int, default=24)
    ap.add_argument("--json", action="store_true", help="one JSON array instead of lines")
    ap.add_argument("--triage", action="store_true",
                    help="print STATUS: line + only WATCH/PROBLEM source objects")
    args = ap.parse_args()
    window_start = datetime.now(timezone.utc) - timedelta(hours=args.window_hours)

    results = []
    for yml in sorted(SOURCES_DIR.glob("*.yml")):
        cfg = read_yml(yml)
        r = check_source(cfg.get("name", yml.stem), cfg, window_start)
        r["status"] = classify(r)
        results.append(r)

    problems = [r["source"] for r in results if r["status"] == "PROBLEM"]
    watches = [r["source"] for r in results if r["status"] == "WATCH"]
    gen = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if args.triage:
        # Compact: a verdict line plus only the sources needing a look. This is
        # what a scheduled model call consumes — small input, small output.
        flagged = [r for r in results if r["status"] != "OK"]
        print(f"STATUS: {'PROBLEM' if problems else 'WATCH' if watches else 'OK'} "
              f"| {len(results)} sources, window={args.window_hours}h, generated={gen}")
        print(f"PROBLEM: {problems or 'none'}")
        print(f"WATCH: {watches or 'none'}")
        for r in flagged:
            print(json.dumps(r, separators=(",", ":")))
        return

    if args.json:
        print(json.dumps(results, indent=1))
    else:
        for r in results:
            print(json.dumps(r, separators=(",", ":")))
    print(f"# {len(results)} sources, window={args.window_hours}h, generated={gen}, "
          f"PROBLEM={problems or 'none'}, WATCH={watches or 'none'}", file=sys.stdout)


if __name__ == "__main__":
    main()
