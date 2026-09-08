#!/usr/bin/env python3
"""Stream-validate an InfoDengue sink artifact without materializing it.

The 4.8M-record / 3.8 GB national-backfill NDJSON is judged line by line: one
pass, bounded per-key state, no temporary file, no write to the artifact. The
acceptance caps come from ``extract/sources/<source>.yml::acceptance`` with a
built-in default that mirrors the committed block.

Checks, mapped to the ``acceptance`` block:

  shape          every line is a JSON object.
  stable key     ``id`` must be exactly ``<geocode>|<disease>|<SE>`` using the
                 record's own components (the promised identity); ``SE`` is the
                 epidemiological week ``YYYYWW``.
  duplicates     records that repeat an already-seen stable key; for each
                 repeat the value fields are compared with the first sighting.
  coverage       distinct geocodes, per-geocode record and week spread, the
                 week / year histogram.
  caps           ``--mode year``: one historical unit is bounded by
                 ``single_year.max_records`` / ``.max_bytes`` and must span a
                 single year. ``--mode rolling``: the scheduled sweep is bounded
                 by ``rolling_window.*`` with at most ``.weeks`` distinct weeks.
                 ``--mode full``: a multi-year artifact is checked year by year
                 (per-year cap) plus any year outside the requested range.
  source         record / byte count compared with the ``.meta.json`` sidecar.
  baseline       ``--baseline`` streams a second artifact and reports
                 overlapping / novel / missing stable keys.

Exit status: 0 = report written; non-zero only for operational errors (bad
artifact path). The ``verdict`` field in the report is the machine-readable
judgment; a failing verdict never destroys evidence.

Usage:
    validate_infodengue.py <artifact.ndjson> [--source infodengue]
                           [--mode year|rolling|full]
                           [--requested-ymin N --requested-ymax N]
                           [--baseline other.ndjson]
                           [--report-out DIR-or-FILE]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def repo_root_hint() -> Path:
    env = os.environ.get("EXTRACT_REPO_ROOT")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[1]


def load_acceptance(source: str, repo_root: Path | None = None) -> dict:
    """Read ``extract/sources/<source>.yml`` ``acceptance`` block ({} if absent)."""
    import yaml  # local import: tools are stdlib-first and can opt out of PyYAML
    root = repo_root or repo_root_hint()
    path = root / "extract" / "sources" / f"{source}.yml"
    if not path.exists():
        return {}
    cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return cfg.get("acceptance") or {}


DEFAULT_ACCEPTANCE = {
    "identity": ["geocode", "disease", "se"],
    "rolling_window": {"weeks": 10, "max_records": 75_000, "max_bytes": 64 * 2**20},
    "single_year": {"max_records": 350_000, "max_bytes": 320 * 2**20},
    "value_fields": ["casos", "casos_est", "Rt", "nivel", "nivel_inc",
                     "p_rt1", "versao_modelo"],
}


def merge_acceptance(acceptance: dict) -> dict:
    """Built-in defaults updated by whatever the source YAML provides."""
    m = json.loads(json.dumps(DEFAULT_ACCEPTANCE))
    for key, value in (acceptance or {}).items():
        if isinstance(value, dict) and isinstance(m.get(key), dict):
            m[key].update(value)
        else:
            m[key] = value
    return m


def week_of(se: str) -> tuple[int, int] | None:
    """``SE`` -> (year, week); the epidemiological week is ``YYYYWW``."""
    if not isinstance(se, str) or len(se) != 6 or not se.isdigit():
        return None
    year, week = int(se[:4]), int(se[4:])
    if year < 1990 or year > 2100 or not 1 <= week <= 53:
        return None
    return year, week


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(4 * 2**20), b""):
            h.update(chunk)
    return h.hexdigest()


def open_lines(artifact: Path):
    with open(artifact, "rb", buffering=1024 * 1024) as fh:
        yield from fh


def validate_one(artifact: Path, acc: dict, *, mode: str,
                 requested_ymin: int | None, requested_ymax: int | None) -> dict:
    value_fields = list(acc["value_fields"])
    per_year_cap = acc["single_year"]["max_records"]
    year_byte_cap = acc["single_year"]["max_bytes"]
    rolling_weeks = acc["rolling_window"]["weeks"]
    rolling_cap = acc["rolling_window"]["max_records"]

    total_lines = malformed = 0
    bytes_seen = 0
    malformed_samples: list[dict] = []
    bad_envelope = 0
    duplicates = 0                 # repeat occurrences beyond the first
    repeat_keys: set[str] = set()  # distinct keys that repeated
    value_changes = 0
    value_change_samples: list[dict] = []

    geocode_records: Counter = Counter()
    geocode_weeks: dict[str, set[str]] = {}
    weeks: Counter = Counter()
    years: Counter = Counter()
    seen: set[str] = set()
    first_values: dict[str, dict] = {}

    for raw in open_lines(artifact):
        total_lines += 1
        bytes_seen += len(raw)
        try:
            rec = json.loads(raw)
            if not isinstance(rec, dict):
                raise ValueError("not an object")
        except (ValueError, UnicodeDecodeError) as exc:
            malformed += 1
            if len(malformed_samples) < 5:
                malformed_samples.append({"line": total_lines, "error": str(exc),
                                          "sample": raw[:200].decode("utf-8", "replace")})
            continue

        geo = str(rec.get("geocode"))
        disease = str(rec.get("disease"))
        se = str(rec.get("SE"))
        rid = str(rec.get("id"))
        if rid != "|".join((geo, disease, se)):
            bad_envelope += 1
        parsed = week_of(se)
        if parsed is None:
            bad_envelope += 1
        else:
            years[parsed[0]] += 1
            weeks[se] += 1
            geocode_weeks.setdefault(geo, set()).add(se)
        geocode_records[geo] += 1

        if rid in seen:
            duplicates += 1
            repeat_keys.add(rid)
            prev = first_values.get(rid)
            if prev is not None:
                changed = {k: (prev.get(k), rec.get(k)) for k in value_fields
                           if prev.get(k) != rec.get(k)}
                if changed:
                    value_changes += 1
                    if len(value_change_samples) < 5:
                        value_change_samples.append({"id": rid, "changes": changed})
        else:
            seen.add(rid)
            first_values[rid] = {k: rec.get(k) for k in value_fields}

    records = total_lines - malformed
    year_counts = dict(sorted(years.items()))
    per_year_breach = {y: {"records": n, "max_records": per_year_cap}
                       for y, n in year_counts.items() if n > per_year_cap}
    unexpected_years = sorted(y for y in year_counts
                              if (requested_ymin is not None and y < requested_ymin)
                              or (requested_ymax is not None and y > requested_ymax))

    verdict = "ok"
    if malformed:
        verdict = "malformed_records"
    elif mode == "year":
        if records > per_year_cap:
            verdict = "record_cap_breached"
        elif bytes_seen > year_byte_cap:
            verdict = "byte_cap_breached"
        elif len(years) > 1:
            verdict = "year_unit_multiple_years"
    elif mode == "rolling":
        if len(weeks) > rolling_weeks:
            verdict = "rolling_window_exceeded"
        elif records > rolling_cap:
            verdict = "record_cap_breached"
    elif mode == "full" and per_year_breach:
        verdict = "per_year_cap_breached"
    if verdict == "ok" and bad_envelope:
        verdict = "bad_envelope"

    record_spread = sorted(geocode_records.values())
    week_spread = sorted(len(s) for s in geocode_weeks.values())
    return {
        "artifact": str(artifact),
        "mode": mode,
        "records": records,
        "total_lines": total_lines,
        "bytes": bytes_seen,
        "malformed": malformed,
        "malformed_samples": malformed_samples,
        "stable_key_mismatches": bad_envelope,
        "distinct_keys": len(seen),
        "duplicate_records": duplicates,
        "distinct_repeat_keys": len(repeat_keys),
        "value_changes": value_changes,
        "value_change_samples": value_change_samples,
        "distinct_geocodes": len(geocode_records),
        "distinct_weeks": len(weeks),
        "min_week": min(weeks) if weeks else None,
        "max_week": max(weeks) if weeks else None,
        "year_counts": year_counts,
        "per_year_cap_breaches": per_year_breach,
        "unexpected_years": unexpected_years,
        "geocode_record_distribution": {
            "min": record_spread[0] if record_spread else 0,
            "max": record_spread[-1] if record_spread else 0,
            "median": median(record_spread)},
        "geocode_week_distribution": {
            "min": week_spread[0] if week_spread else 0,
            "max": week_spread[-1] if week_spread else 0,
            "median": median(week_spread)},
        "verdict": verdict,
    }


def median(values: list[int]) -> float:
    if not values:
        return 0
    vals = sorted(values)
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2


def read_meta_sidecar(artifact: Path) -> dict | None:
    meta = artifact.with_name(artifact.name + ".meta.json")
    if not meta.exists():
        return None
    try:
        data = json.loads(meta.read_text(encoding="utf-8"))
    except ValueError:
        return None
    return {"path": str(meta), "sha256": sha256_file(meta),
            "records": data.get("records"), "bytes": data.get("bytes"),
            "started_at": data.get("started_at"), "finished_at": data.get("finished_at"),
            "duration_s": data.get("duration_s"), "exit_code": data.get("exit_code")}


def keyset_of(artifact: Path) -> tuple[set[tuple[str, str, str]], int]:
    """Stream (geocode, disease, SE) identity tuples; returns (set, line count)."""
    keys: set[tuple[str, str, str]] = set()
    n = 0
    for raw in open_lines(artifact):
        n += 1
        try:
            rec = json.loads(raw)
        except ValueError:
            continue
        if isinstance(rec, dict):
            keys.add((str(rec.get("geocode")), str(rec.get("disease")), str(rec.get("SE"))))
    return keys, n


def compare(a: Path, b: Path) -> dict:
    ka, na = keyset_of(a)
    kb, nb = keyset_of(b)
    common = ka & kb
    return {"a": str(a), "b": str(b),
            "a_lines": na, "b_lines": nb,
            "a_distinct_keys": len(ka), "b_distinct_keys": len(kb),
            "overlapping": len(common),
            "in_a_not_b": len(ka - kb),
            "in_b_not_a": len(kb - ka)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("artifact", help="the .ndjson artifact to validate (read-only)")
    ap.add_argument("--source", default="infodengue",
                    help="source name whose extract/sources/<name>.yml supplies the acceptance block")
    ap.add_argument("--mode", choices=["rolling", "year", "full"], default="full",
                    help="rolling = scheduled 10-week sweep; year = one historical unit; "
                         "full = multi-year artifact, checked year by year")
    ap.add_argument("--requested-ymin", type=int, default=None)
    ap.add_argument("--requested-ymax", type=int, default=None)
    ap.add_argument("--baseline", default=None,
                    help="a second artifact to compare stable keys against (one extra pass)")
    ap.add_argument("--report-out", default=None,
                    help="directory (or exact .json) for the report; default "
                         "<artifact>.validate.json next to the artifact")
    args = ap.parse_args(argv)

    artifact = Path(args.artifact).expanduser().resolve()
    if not artifact.exists():
        print(f"artifact not found: {artifact}", file=sys.stderr)
        return 2

    acc = merge_acceptance(load_acceptance(args.source))
    started = datetime.now(timezone.utc)
    report = validate_one(artifact, acc, mode=args.mode,
                          requested_ymin=args.requested_ymin,
                          requested_ymax=args.requested_ymax)
    report["artifact_sha256"] = sha256_file(artifact)
    meta = read_meta_sidecar(artifact)
    report["sidecar_meta"] = meta
    if meta is not None:
        report["sidecar_consistency"] = {
            "records_match": meta.get("records") in (None, report["records"]),
            "bytes_match": meta.get("bytes") in (None, report["bytes"]),
        }
    if args.baseline:
        report["baseline"] = compare(artifact, Path(args.baseline).expanduser().resolve())
    report["validated_at"] = datetime.now(timezone.utc).isoformat()
    report["elapsed_s"] = round((datetime.now(timezone.utc) - started).total_seconds(), 3)

    out = Path(args.report_out) if args.report_out else artifact.parent / f"{artifact.name}.validate.json"
    if out.suffix == ".json":
        out_file = out
    else:
        out_file = out / f"{artifact.name}.validate.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                        encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nreport: {out_file}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())