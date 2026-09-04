#!/usr/bin/env python3
"""Backfill a source's history into the sink, one unit at a time.

The scheduled pipeline only ever fetches "now". Where an upstream also serves
history, this walks that history and lands it as ordinary sink files, so the
load layer ingests it with no special case: a backfill unit is
``raw/source=<name>/dt=<unit date>/<name>_backfill_<unit>.ndjson`` plus the
usual ``.meta.json``, and the loader's ledger diff picks it up like any run.

What a unit is, and how to build its CLI args, comes from an optional
``backfill:`` block in the source's yml — the same config that already
describes the scheduled instance, so there is no second place to register a
source::

    backfill:
      unit: month                     # day | hour | month | year | values | single
      args: ["--month", "{yyyy}-{mm}"]
      pace_seconds: 3                 # sleep between units (politeness floor)
      timeout_minutes: 20             # per unit, not for the whole walk

Design rules that matter operationally:

* **Resumable by construction.** A unit whose ``.meta.json`` already exists is
  skipped, so an interrupted walk continues where it stopped and a completed
  one is a no-op. The manifest is written even for a zero-record unit, so
  "upstream genuinely has nothing for 2019-03" is remembered rather than
  re-fetched forever.
* **One request stream per source.** Units within a source are strictly
  sequential with a pacing sleep. Different sources may run concurrently
  because they are different hosts; ``--concurrency`` bounds that.
* **A dead upstream stops its own source, not the run.** Each unit retries
  with backoff; after ``--max-consecutive-failures`` units fail in a row that
  source is abandoned (the endpoint is probably gone or throttling us) while
  every other source carries on.
* **Historical dt, not today's.** Each unit lands in the partition it belongs
  to, so ``_dt`` in the warehouse means "when the data is from".
"""
from __future__ import annotations

import argparse
import calendar
import json
import pathlib
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "orchestration" / "include"))

import yaml  # noqa: E402  (after sys.path setup)

from sinks import get_sink  # noqa: E402

SCRIPTS_DIR = REPO_ROOT / "pipelines" / "extract" / "scripts"
SOURCES_DIR = REPO_ROOT / "pipelines" / "extract" / "sources"
ENVELOPE = ("source", "fetched_at", "id")


def _month_range(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield date(y, m, 1)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)




def plan_units(spec: dict, today: date | None = None) -> list[dict]:
    """Expand a ``backfill:`` block into concrete units, oldest first.

    Each unit is ``{"id": <filename-safe token>, "dt": "YYYY-MM-DD",
    "fields": {...}}``; ``fields`` is what ``args`` templates interpolate.
    """
    today = today or datetime.now(timezone.utc).date()
    kind = spec.get("unit", "single")
    end = spec.get("end")
    if end and len(end) == 7:
        end_date = date.fromisoformat(end + "-01")
    else:
        end_date = date.fromisoformat(end) if end else today

    if kind == "single":
        return [{"id": "full", "dt": today.isoformat(), "fields": {}}]

    if kind == "values":
        values = spec.get("values") or []
        if not values:
            raise ValueError("backfill unit 'values' needs a non-empty values list")
        return [{"id": str(v).replace("/", "_").replace(" ", "_"),
                 "dt": spec.get("dt", today.isoformat()),
                 "fields": {"value": str(v)}} for v in values]

    start = spec.get("start")
    if not start:
        raise ValueError(f"backfill unit {kind!r} needs a start")

    units: list[dict] = []
    if kind == "year":
        for y in range(int(str(start)[:4]), end_date.year + 1):
            units.append({"id": f"{y}", "dt": f"{y}-01-01", "fields": {"yyyy": f"{y}"}})
        return units

    if kind == "month":
        first = date.fromisoformat(start + "-01" if len(start) == 7 else start)
        for d in _month_range(first, end_date):
            last_day = calendar.monthrange(d.year, d.month)[1]
            units.append({
                "id": f"{d.year}-{d.month:02d}",
                "dt": d.isoformat(),
                "fields": {"yyyy": f"{d.year}", "mm": f"{d.month:02d}",
                           "date": d.isoformat(),
                           "month_end": d.replace(day=last_day).isoformat()},
            })
        return units

    if kind in ("day", "hour"):
        d = date.fromisoformat(start)
        while d <= end_date:
            fields = {"date": d.isoformat(), "yyyy": f"{d.year}",
                      "mm": f"{d.month:02d}", "dd": f"{d.day:02d}"}
            if kind == "day":
                units.append({"id": d.isoformat(), "dt": d.isoformat(), "fields": fields})
            else:
                for hour in range(24):
                    units.append({"id": f"{d.isoformat()}-{hour:02d}", "dt": d.isoformat(),
                                  "fields": {**fields, "hour": str(hour)}})
            d += timedelta(days=1)
        return units

    raise ValueError(f"unknown backfill unit {kind!r}")


def render_args(template: list, fields: dict) -> list[str]:
    return [str(a).format(**fields) for a in template]


# ------------------------------------------------------------------ unit runner

def unit_paths(root: pathlib.Path, name: str, unit: dict) -> tuple[pathlib.Path, pathlib.Path]:
    stem = f"{name}_backfill_{unit['id']}"
    data = root / "raw" / f"source={name}" / f"dt={unit['dt']}" / f"{stem}.ndjson"
    return data, data.with_name(data.name + ".meta.json")


def run_unit(cfg: dict, unit: dict, timeout_s: int) -> dict:
    """Fetch one unit and land it through the sink. Returns the manifest."""
    name = cfg["name"]
    script = SCRIPTS_DIR / cfg["script"]
    spec = cfg["backfill"]
    args = render_args(spec.get("args", []), unit["fields"])
    started = datetime.now(timezone.utc)
    sink = get_sink(cfg.get("sink", "local"))
    filename = f"{name}_backfill_{unit['id']}.ndjson"

    records = 0
    bytes_written = 0
    with tempfile.NamedTemporaryFile(mode="w+", prefix=f".backfill-{name}-",
                                     dir=sink.root, encoding="utf-8",
                                     errors="replace", delete=True) as err:
        proc = subprocess.Popen([sys.executable, str(script), *args], cwd=SCRIPTS_DIR,
                                stdout=subprocess.PIPE, stderr=err, text=True)
        try:
            with sink.writer(name, unit["dt"], filename) as out:
                for line in proc.stdout:
                    if not line.strip():
                        continue
                    if records == 0:
                        _check_envelope(line)
                    out.write(line if line.endswith("\n") else line + "\n")
                    records += 1
                    bytes_written += len(line.encode("utf-8"))
            returncode = proc.wait(timeout=timeout_s)
        except BaseException:
            proc.kill()
            proc.wait()
            sink.discard()
            raise
        err.seek(0)
        stderr = err.read().strip()

    meta = {
        "source": name,
        "script": cfg["script"],
        "args": args,
        "backfill_unit": unit["id"],
        "started_at": started.isoformat(),
        "duration_s": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
        "exit_code": returncode,
        "records": records,
    }
    if returncode != 0:
        meta["error"] = stderr[-500:] if stderr else None
        sink.fail(meta)
        raise RuntimeError(f"{cfg['script']} {' '.join(args)} exited {returncode}")
    sink.commit(meta)
    return meta


def _check_envelope(line: str):
    try:
        first = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"first stdout line is not JSON: {line[:200]!r}") from exc
    missing = [k for k in ENVELOPE if k not in first]
    if missing:
        raise ValueError(f"first record is missing envelope keys {missing}")


# ------------------------------------------------------------------- source walk

def walk_source(cfg: dict, *, limit: int | None, retries: int,
                max_consecutive_failures: int, log=print) -> dict:
    name = cfg["name"]
    spec = cfg["backfill"]
    pace = float(spec.get("pace_seconds", 3))
    timeout_s = int(spec.get("timeout_minutes", cfg.get("timeout_minutes", 20))) * 60
    sink_root = get_sink(cfg.get("sink", "local")).root

    units = plan_units(spec)
    if spec.get("newest_first"):
        units.reverse()
    todo = []
    for u in units:
        _, manifest = unit_paths(sink_root, name, u)
        if not manifest.exists():
            todo.append(u)
    skipped = len(units) - len(todo)
    if limit:
        todo = todo[:limit]

    stats = {"source": name, "units_total": len(units), "already_done": skipped,
             "attempted": 0, "ok": 0, "empty": 0, "failed": 0,
             "records": 0, "bytes": 0, "aborted": False}
    consecutive = 0
    for i, unit in enumerate(todo):
        for attempt in range(retries + 1):
            try:
                meta = run_unit(cfg, unit, timeout_s)
                stats["attempted"] += 1
                stats["records"] += meta["records"]
                stats["bytes"] += meta["bytes"]
                stats["ok" if meta["records"] else "empty"] += 1
                consecutive = 0
                break
            except Exception as exc:
                if attempt < retries:
                    time.sleep(30 * (attempt + 1) ** 2)  # 30s, 120s: back off, don't hammer
                    continue
                stats["attempted"] += 1
                stats["failed"] += 1
                consecutive += 1
                log(f"[{name}] unit {unit['id']} failed: {str(exc)[:160]}")
        if consecutive >= max_consecutive_failures:
            stats["aborted"] = True
            log(f"[{name}] abandoned after {consecutive} consecutive failures")
            break
        if i + 1 < len(todo):
            time.sleep(pace)
    return stats


def load_configs(names: list[str] | None) -> list[dict]:
    out = []
    for path in sorted(SOURCES_DIR.glob("*.yml")):
        cfg = yaml.safe_load(path.read_text())
        if not cfg.get("backfill"):
            continue
        if names and cfg["name"] not in names:
            continue
        out.append(cfg)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sources", nargs="*", help="source names; default every source with a backfill block")
    parser.add_argument("--plan", action="store_true", help="print the unit plan and exit")
    parser.add_argument("--limit-units", type=int, help="cap units per source this run (sampling/estimating)")
    parser.add_argument("--concurrency", type=int, default=4, help="sources in parallel (never units)")
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--max-consecutive-failures", type=int, default=5)
    args = parser.parse_args()

    configs = load_configs(args.sources or None)
    if not configs:
        print("no sources with a backfill block matched", file=sys.stderr)
        return 1

    if args.plan:
        grand = 0
        for cfg in configs:
            units = plan_units(cfg["backfill"])
            root = get_sink(cfg.get("sink", "local")).root
            done = sum(1 for u in units if unit_paths(root, cfg["name"], u)[1].exists())
            grand += len(units) - done
            print(f"{cfg['name']:32s} {cfg['backfill'].get('unit','single'):7s} "
                  f"units={len(units):6d} done={done:6d} todo={len(units)-done:6d} "
                  f"first={units[0]['id']} last={units[-1]['id']}")
        print(f"\ntotal units to fetch: {grand}")
        return 0

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        results = list(pool.map(
            lambda cfg: walk_source(cfg, limit=args.limit_units, retries=args.retries,
                                    max_consecutive_failures=args.max_consecutive_failures),
            configs))
    print(f"\n{'source':32s} {'units':>6s} {'ok':>6s} {'empty':>6s} {'fail':>5s} {'records':>12s} {'MB':>9s}")
    for r in sorted(results, key=lambda r: -r["bytes"]):
        print(f"{r['source']:32s} {r['attempted']:6d} {r['ok']:6d} {r['empty']:6d} "
              f"{r['failed']:5d} {r['records']:12,d} {r['bytes']/1e6:9.1f}"
              + ("  ABORTED" if r["aborted"] else ""))
    print(f"\ntotal: {sum(r['records'] for r in results):,} records, "
          f"{sum(r['bytes'] for r in results)/1e9:.2f} GB, {(time.time()-t0)/60:.1f} min")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
