#!/usr/bin/env python3
"""Fetch CelesTrak SOCRATES conjunctions.

The filtered HTML endpoint is unverified in production. CSV remains the scheduled
default and the explicit rollback mode; table retrieval requires --mode table.
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


PRIMARY_HEADING = (
    "data&graphs", "noradcatalognumber", "name[opsstatus]", "dayssinceepoch",
    "tca(utc)", "minrange(km)", "relativespeed(km/sec)",
)
SECONDARY_HEADING = ("maxprobability", "dilutionthreshold(km)")
FOOTER_FIELDS = set(PRIMARY_HEADING + SECONDARY_HEADING)


def _heading(cells: list[str], expected: tuple[str, ...]) -> bool:
    return tuple(re.sub(r"\s+", "", cell).lower() for cell in cells) == expected


def _utc_tca(value: str) -> str:
    # The official display labels TCA as UTC but omits the time-zone suffix.
    try:
        match = re.fullmatch(r"(\d{4} [A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?", value)
        if match is None:
            raise ValueError("invalid display time")
        instant = datetime.strptime(match[1], "%Y %b %d %H:%M:%S")
        fraction = match[2]
        if fraction:
            instant = instant.replace(microsecond=int(fraction.ljust(6, "0")))
        return instant.strftime("%Y-%m-%dT%H:%M:%S") + (f".{fraction}" if fraction else "") + "Z"
    except ValueError as exc:
        raise ValueError("SOCRATES table has invalid UTC TCA") from exc


def _footer_description(cell: str) -> bool:
    field, separator, description = cell.partition(":")
    return (bool(separator and description.strip()) and
            re.sub(r"\s+", "", field).lower() in FOOTER_FIELDS)


def _table_records(parser: TableParser, maximum: int, fetched_at: str) -> list[dict]:
    rows = parser.rows
    start = next((index for index, (kind, cells) in enumerate(rows)
                  if kind == "th" and len(cells) == 7), None)
    if (start is None or not _heading(rows[start][1], PRIMARY_HEADING)
            or start + 1 >= len(rows) or rows[start + 1][0] != "th"
            or not _heading(rows[start + 1][1], SECONDARY_HEADING)):
        _mark_parse_failure("table_header_validation")
        raise ValueError("SOCRATES table missing two display heading rows")

    records = []
    seen = set()
    pending: list[str] | str | None = None
    footer = False
    for kind, cells in rows[start + 2:]:
        if footer:
            if len(cells) != 1 or not _footer_description(cells[0]):
                _mark_parse_failure("table_row_validation")
                raise ValueError("SOCRATES table has extra event data after footer")
            continue
        if pending == "heading":
            if kind != "th" or not _heading(cells, SECONDARY_HEADING):
                _mark_parse_failure("table_header_validation")
                raise ValueError("SOCRATES table has changed display headings")
            pending = None
        elif pending is None and len(cells) == 1 and cells[0].startswith("Data Fields:"):
            footer = True
        elif kind == "th":
            if pending is not None:
                _mark_parse_failure("table_row_validation")
                raise ValueError("SOCRATES table has an incomplete event")
            if not _heading(cells, PRIMARY_HEADING):
                _mark_parse_failure("table_header_validation")
                raise ValueError("SOCRATES table has changed display headings")
            pending = "heading"
        elif kind == "td" and len(cells) == 7 and pending is None:
            pending = cells
        elif kind == "td" and len(cells) == 6 and isinstance(pending, list):
            values = {
                "NORAD_CAT_ID_1": pending[1], "OBJECT_NAME_1": pending[2],
                "TCA_RANGE": pending[5], "TCA_RELATIVE_SPEED": pending[6],
                "NORAD_CAT_ID_2": cells[1], "OBJECT_NAME_2": cells[2],
                "MAX_PROB": cells[4], "DILUTION": cells[5],
            }
            try:
                if any(not cell for cell in pending + cells) or not values["NORAD_CAT_ID_1"].isascii() or not values["NORAD_CAT_ID_1"].isdecimal() or not values["NORAD_CAT_ID_2"].isascii() or not values["NORAD_CAT_ID_2"].isdecimal():
                    raise ValueError("SOCRATES table has incomplete data or invalid object IDs")
                values["TCA"] = _utc_tca(pending[4])
                record = _record(values, fetched_at)
            except (KeyError, TypeError, ValueError) as exc:
                _mark_parse_failure("table_row_validation")
                raise ValueError(f"SOCRATES table event {len(records) + 1}: {_safe_error(exc)}") from exc
            if record["id"] in seen:
                _mark_parse_failure("table_row_validation")
                raise ValueError("SOCRATES table has a duplicate event")
            seen.add(record["id"])
            records.append(record)
            pending = None
        else:
            _mark_parse_failure("table_row_validation")
            raise ValueError("SOCRATES table has an incomplete event")
    if pending is not None or not footer or not records or len(records) > maximum:
        _mark_parse_failure("table_row_validation")
        raise ValueError(f"SOCRATES table expected 1 to {maximum} complete events and a footer")
    return records


class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows: list[tuple[str | None, list[str]]] = []
        self._row: list[str] | None = None
        self._kind: str | None = None
        self._cell: list[str] | None = None
        self._cell_kind: str | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            if self._row is not None:
                raise ValueError("SOCRATES table has an unfinished row")
            self._row = []
            self._kind = None
        elif tag in {"th", "td"} and self._row is not None:
            if self._cell is not None:
                raise ValueError("SOCRATES table has an unfinished cell")
            self._cell = []
            self._cell_kind = tag

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag):
        if tag in {"th", "td"} and self._cell is not None:
            if tag != self._cell_kind or self._kind not in (None, tag):
                raise ValueError("SOCRATES table has mixed or unfinished cells")
            self._kind = tag
            self._row.append("".join(self._cell).strip())
            self._cell = None
            self._cell_kind = None
        elif tag == "tr" and self._row is not None:
            if self._cell is not None:
                raise ValueError("SOCRATES table has an unfinished cell")
            self.rows.append((self._kind, self._row))
            self._row = None
            self._kind = None

    def close(self):
        super().close()
        if self._row is not None or self._cell is not None:
            raise ValueError("SOCRATES table has an unfinished row")


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
                time.sleep(2 ** attempt_number)
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
        records = []
        for i, row in enumerate(reader):
            if limit is not None and i >= limit: break
            try:
                records.append(_record(row, fetched_at))
            except (KeyError, TypeError, ValueError):
                _mark_parse_failure("csv_row_validation")
                raise
        yield from records
        return
    if mode != "table":
        raise ValueError(f"unknown mode {mode!r}")
    if sort != "maxProb" or limit is not None and not 1 <= limit <= 100:
        _mark_parse_failure("table_parameters")
        raise ValueError("SOCRATES table requires maxProb sort and a limit from 1 to 100")
    maximum = 100 if limit is None else limit
    text, status, attempts, byte_count, elapsed = _get(
        f"{BASE}/table-socrates.php?NAME=,&ORDER=MAXPROB&MAX={maximum}")
    if status != 200:
        _mark_parse_failure("http_response")
        raise ValueError(f"SOCRATES table returned HTTP {status}")
    parser = TableParser()
    try:
        parser.feed(text)
        parser.close()
    except ValueError:
        _mark_parse_failure("table_row_validation")
        raise
    records = _table_records(parser, maximum, fetched_at)
    yield from records


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
