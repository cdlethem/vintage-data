#!/usr/bin/env python3
"""Fetch CelesTrak SOCRATES conjunctions.

The official filtered HTML endpoint timed out during the 2026-09-06 remediation
check, so production deliberately remains on the verified CSV export. Table mode
is retained only as an explicit manual recovery path, never silently selected.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://celestrak.org/SOCRATES"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
REQUIRED = {"NORAD_CAT_ID_1", "NORAD_CAT_ID_2", "OBJECT_NAME_1", "OBJECT_NAME_2", "TCA", "TCA_RANGE", "TCA_RELATIVE_SPEED", "MAX_PROB", "DILUTION"}
LAST_REQUEST: dict[str, object] = {}



class TableParser(HTMLParser):
    def __init__(self):
        super().__init__(); self.rows = []; self._row = None; self._cell = None
    def handle_starttag(self, tag, attrs):
        if tag == "tr": self._row = []
        elif tag in {"th", "td"} and self._row is not None: self._cell = []
    def handle_data(self, data):
        if self._cell is not None: self._cell.append(data)
    def handle_endtag(self, tag):
        if tag in {"th", "td"} and self._cell is not None:
            self._row.append("".join(self._cell).strip()); self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row: self.rows.append(self._row)
            self._row = None


def _get(url: str, timeout: int = 60) -> tuple[str, int, int, int, float]:
    started = time.monotonic(); last = None
    for attempt in range(1, 4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                LAST_REQUEST.update({"status": getattr(resp, "status", 200), "attempts": attempt,
                                     "bytes": len(raw), "elapsed_s": round(time.monotonic() - started, 3)})
                return raw.decode("utf-8", errors="replace"), getattr(resp, "status", 200), attempt, len(raw), time.monotonic() - started
        except (urllib.error.URLError, TimeoutError) as exc:
            last = exc
            if attempt < 3: time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"SOCRATES request failed after 3 attempts: {last}")


def _record(row: dict[str, str], fetched_at: str) -> dict:
    missing = REQUIRED - row.keys()
    if missing: raise ValueError(f"SOCRATES row missing columns: {sorted(missing)}")
    return {"source": "celestrak_socrates", "fetched_at": fetched_at,
            "id": f"{row['NORAD_CAT_ID_1']}:{row['NORAD_CAT_ID_2']}:{row['TCA']}",
            "norad_id_1": row["NORAD_CAT_ID_1"], "object_name_1": row["OBJECT_NAME_1"],
            "norad_id_2": row["NORAD_CAT_ID_2"], "object_name_2": row["OBJECT_NAME_2"], "tca": row["TCA"],
            "tca_range_km": float(row["TCA_RANGE"]), "tca_relative_speed_km_s": float(row["TCA_RELATIVE_SPEED"]),
            "max_prob": float(row["MAX_PROB"]), "dilution_km": float(row["DILUTION"]) if row["DILUTION"] else None}


def fetch_conjunctions(sort: str = "maxProb", limit: int | None = None, mode: str = "csv"):
    fetched_at = datetime.now(timezone.utc).isoformat()
    if mode == "csv":
        text, status, attempts, byte_count, elapsed = _get(f"{BASE}/sort-{sort}.csv")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames or not REQUIRED.issubset(reader.fieldnames):
            raise ValueError("SOCRATES CSV missing documented headers")
        for i, row in enumerate(reader):
            if limit is not None and i >= limit: break
            yield _record(row, fetched_at)
        return
    if mode != "table": raise ValueError(f"unknown mode {mode!r}")
    text, status, attempts, byte_count, elapsed = _get(f"{BASE}/table-socrates.php?NAME=,&ORDER=MAXPROB&MAX={limit or 100}")
    parser = TableParser(); parser.feed(text)
    header_index = next((i for i, row in enumerate(parser.rows) if REQUIRED.issubset(row)), None)
    if header_index is None: raise ValueError("SOCRATES table missing documented headers")
    headers = parser.rows[header_index]
    for cells in parser.rows[header_index + 1:]:
        if len(cells) == len(headers): yield _record(dict(zip(headers, cells)), fetched_at)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--mode", choices=("csv", "table"), default="csv")
    parser.add_argument("--sort", choices=("maxProb", "minRange"), default="maxProb"); parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv); rows = []; started = time.monotonic()
    try:
        for row in fetch_conjunctions(args.sort, args.limit, args.mode): rows.append(row); print(json.dumps(row, ensure_ascii=False))
    except Exception as exc:
        print(SUMMARY_PREFIX + json.dumps({"health":"failed", "completeness":"failed", "records":len(rows), "error":str(exc), "metrics":{"endpoint":args.mode}}), file=sys.stderr)
        return 1
    tcas = [r["tca"] for r in rows]
    request = {**LAST_REQUEST, "endpoint": args.mode, "mode": args.mode}
    print(SUMMARY_PREFIX + json.dumps({"health":"degraded" if args.mode == "csv" else "healthy", "completeness":"complete", "records":len(rows), "requests":{"attempted":1}, "metrics":{**request, "parsed_rows":len(rows), "pair_count":len({(r['norad_id_1'], r['norad_id_2']) for r in rows}), "tca_min":min(tcas) if tcas else None, "tca_max":max(tcas) if tcas else None, "parse_complete":True}}), file=sys.stderr)
    return 0

if __name__ == "__main__": raise SystemExit(main())
