#!/usr/bin/env python3
"""Fetch CelesTrak SOCRATES conjunctions.

The official filtered HTML endpoint timed out during the 2026-09-06 remediation
check, so production deliberately remains on the verified CSV export. Table mode
is retained only as an explicit manual recovery path, never silently selected.
"""
from __future__ import annotations

import argparse
import csv
import http.client
import io
import json
import os
import re
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
CHUNK_BYTES = 64 * 1024
MAX_ERROR_CHARS = 240
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


def _rounded(seconds: float) -> float:
    return round(seconds, 3)


def _safe_error(exc: BaseException) -> str:
    """Return a bounded diagnostic without URL credentials or query secrets."""
    message = f"{type(exc).__name__}: {exc}"
    message = re.sub(r"(?i)(authorization|api[_-]?key|token|password|secret)=([^&\s]+)", r"\1=[REDACTED]", message)
    message = re.sub(r"//([^/@\s:]+):[^/@\s]+@", r"//\1:[REDACTED]@", message)
    return message[:MAX_ERROR_CHARS]


def _content_length(response) -> int | None:
    headers = getattr(response, "headers", None)
    value = headers.get("Content-Length") if headers is not None else None
    if value is None and hasattr(response, "getheader"):
        value = response.getheader("Content-Length")
    try:
        length = int(value)
    except (TypeError, ValueError):
        return None
    return length if length >= 0 else None


def _set_request_metrics(attempts: list[dict[str, object]], started: float, phase: str | None = None) -> None:
    received_bytes = sum(int(attempt["received_bytes"]) for attempt in attempts)
    received_chunks = sum(int(attempt["received_chunks"]) for attempt in attempts)
    LAST_REQUEST.clear()
    total_elapsed = _rounded(time.monotonic() - started)
    LAST_REQUEST.update({
        "attempts": len(attempts),
        "attempt_metrics": attempts,
        "received_bytes": received_bytes,
        "received_chunks": received_chunks,
        "elapsed_s": total_elapsed,
        "total_elapsed_s": total_elapsed,
    })
    if attempts and attempts[-1]["status"] is not None:
        LAST_REQUEST["status"] = attempts[-1]["status"]
    if phase is not None:
        LAST_REQUEST["failure_phase"] = phase


def _get(url: str, timeout: int = 60) -> tuple[str, int, int, int, float]:
    started = time.monotonic()
    attempts: list[dict[str, object]] = []
    last: BaseException | None = None
    last_phase = "before_headers"
    for attempt_number in range(1, 4):
        attempt_started = time.monotonic()
        attempt_phase = "before_headers"
        attempt = {"attempt": attempt_number, "status": None, "elapsed_to_headers_s": None,
                   "received_bytes": 0, "received_chunks": 0, "elapsed_s": None}
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                attempt["status"] = getattr(resp, "status", 200)
                attempt["elapsed_to_headers_s"] = _rounded(time.monotonic() - attempt_started)
                content_length = _content_length(resp)
                payload = bytearray()
                while True:
                    chunk = resp.read(CHUNK_BYTES)
                    if not chunk:
                        break
                    if len(chunk) > CHUNK_BYTES:
                        raise OSError("response returned an oversized read chunk")
                    payload.extend(chunk)
                    attempt["received_bytes"] = int(attempt["received_bytes"]) + len(chunk)
                    attempt["received_chunks"] = int(attempt["received_chunks"]) + 1
                if content_length is not None and int(attempt["received_bytes"]) != content_length:
                    attempt_phase = "incomplete_transfer"
                    raise OSError("response body did not match Content-Length")
                attempt["elapsed_s"] = _rounded(time.monotonic() - attempt_started)
                attempts.append(attempt)
                _set_request_metrics(attempts, started)
                return payload.decode("utf-8", errors="replace"), int(attempt["status"]), attempt_number, int(attempt["received_bytes"]), time.monotonic() - started
        except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
            if isinstance(exc, urllib.error.HTTPError):
                attempt["status"] = exc.code
                attempt["elapsed_to_headers_s"] = _rounded(time.monotonic() - attempt_started)
                attempt_phase = "http_response"
            if isinstance(exc, http.client.IncompleteRead) and exc.partial:
                partial = exc.partial
                if len(partial) <= CHUNK_BYTES:
                    attempt["received_bytes"] = int(attempt["received_bytes"]) + len(partial)
                    attempt["received_chunks"] = int(attempt["received_chunks"]) + 1
            if attempt["elapsed_to_headers_s"] is not None and attempt_phase == "before_headers":
                attempt_phase = "body_transfer"
            last_phase = attempt_phase
            attempt["elapsed_s"] = _rounded(time.monotonic() - attempt_started)
            attempt["failure_phase"] = attempt_phase
            attempt["error"] = _safe_error(exc)
            attempts.append(attempt)
            last = exc
            _set_request_metrics(attempts, started, last_phase)
            if attempt_number < 3:
                time.sleep(2 ** (attempt_number - 1))
    raise RuntimeError(f"SOCRATES request failed after 3 attempts during {last_phase}: {_safe_error(last or RuntimeError())}")


def _record(row: dict[str, str], fetched_at: str) -> dict:
    missing = REQUIRED - row.keys()
    if missing: raise ValueError(f"SOCRATES row missing columns: {sorted(missing)}")
    return {"source": "celestrak_socrates", "fetched_at": fetched_at,
            "id": f"{row['NORAD_CAT_ID_1']}:{row['NORAD_CAT_ID_2']}:{row['TCA']}",
            "norad_id_1": row["NORAD_CAT_ID_1"], "object_name_1": row["OBJECT_NAME_1"],
            "norad_id_2": row["NORAD_CAT_ID_2"], "object_name_2": row["OBJECT_NAME_2"], "tca": row["TCA"],
            "tca_range_km": float(row["TCA_RANGE"]), "tca_relative_speed_km_s": float(row["TCA_RELATIVE_SPEED"]),
            "max_prob": float(row["MAX_PROB"]), "dilution_km": float(row["DILUTION"]) if row["DILUTION"] else None}


def _mark_parse_failure(phase: str) -> None:
    LAST_REQUEST["failure_phase"] = phase


def fetch_conjunctions(sort: str = "maxProb", limit: int | None = None, mode: str = "csv"):
    fetched_at = datetime.now(timezone.utc).isoformat()
    if mode == "csv":
        text, status, attempts, byte_count, elapsed = _get(f"{BASE}/sort-{sort}.csv")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames or not REQUIRED.issubset(reader.fieldnames):
            _mark_parse_failure("csv_header_validation")
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
        request = {**LAST_REQUEST, "endpoint": args.mode, "mode": args.mode,
                   "run_elapsed_s": _rounded(time.monotonic() - started)}
        print(SUMMARY_PREFIX + json.dumps({"health":"failed", "completeness":"failed", "records":len(rows), "error":_safe_error(exc), "metrics":request}), file=sys.stderr)
        return 1
    tcas = [r["tca"] for r in rows]
    request = {**LAST_REQUEST, "endpoint": args.mode, "mode": args.mode,
               "run_elapsed_s": _rounded(time.monotonic() - started)}
    print(SUMMARY_PREFIX + json.dumps({"health":"degraded" if args.mode == "csv" else "healthy", "completeness":"complete", "records":len(rows), "requests":{"attempted":1}, "metrics":{**request, "parsed_rows":len(rows), "pair_count":len({(r['norad_id_1'], r['norad_id_2']) for r in rows}), "tca_min":min(tcas) if tcas else None, "tca_max":max(tcas) if tcas else None, "parse_complete":True}}), file=sys.stderr)
    return 0

if __name__ == "__main__": raise SystemExit(main())
