#!/usr/bin/env python3
"""Read-only check of the official SOCRATES table against the production CSV export.

This is a manual validation command, not a replacement for the CSV extractor.
A passing result requires both fixed CelesTrak views to agree on the top 100
conjunctions. It does not establish scheduled-run reliability.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import re
import sys
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

TABLE_URL = "https://celestrak.org/SOCRATES/table-socrates.php?NAME=,&ORDER=MAXPROB&MAX=100"
# Intended-source baseline: the production extractor's official probability-sorted CSV.
BASELINE_URL = "https://celestrak.org/SOCRATES/sort-maxProb.csv"
ROW_COUNT = 100
REQUIRED = {
    "NORAD_CAT_ID_1", "NORAD_CAT_ID_2", "OBJECT_NAME_1", "OBJECT_NAME_2",
    "TCA", "TCA_RANGE", "TCA_RELATIVE_SPEED", "MAX_PROB", "DILUTION",
}
USER_AGENT = "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
MAX_TABLE_BYTES = 2 * 1024 * 1024
MAX_CSV_BYTES = 128 * 1024 * 1024


class ValidationError(ValueError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


class TableParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows = []
        self._row = None
        self._cell = None
        self._kind = None
        self._row_kind = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            if self._row is not None:
                raise ValidationError("table: nested or unfinished row")
            self._row = []
            self._row_kind = None
        elif tag in ("th", "td") and self._row is not None:
            if self._cell is not None:
                raise ValidationError("table: nested or unfinished cell")
            self._kind = tag
            self._cell = []

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("th", "td") and self._cell is not None and self._kind == tag:
            if self._row_kind is not None and self._row_kind != tag:
                raise ValidationError("table: mixed heading and data cells")
            self._row_kind = tag
            self._row.append("".join(self._cell).strip())
            self._cell = None
            self._kind = None
        elif tag in ("th", "td") and self._cell is not None:
            raise ValidationError("table: mismatched cell closing tag")
        elif tag == "tr" and self._row is not None:
            if self._cell is not None:
                raise ValidationError("table: incomplete cell")
            self.rows.append((self._row_kind, self._row))
            self._row = None
            self._row_kind = None

    def close(self):
        super().close()
        if self._row is not None or self._cell is not None:
            raise ValidationError("table: incomplete row")


def _heading(cells: list[str], expected: tuple[str, ...]) -> bool:
    return tuple(re.sub(r"\s+", "", cell).lower() for cell in cells) == expected


PRIMARY_HEADING = (
    "data&graphs", "noradcatalognumber", "name[opsstatus]", "dayssinceepoch",
    "tca(utc)", "minrange(km)", "relativespeed(km/sec)",
)
SECONDARY_HEADING = ("maxprobability", "dilutionthreshold(km)")


def _utc_time(value: str, label: str) -> str:
    # Retain subsecond precision while comparing display and export UTC instants.
    try:
        if re.fullmatch(r"\d{4} [A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2}(?:\.\d+)?", value):
            date, clock = value.rsplit(" ", 1)
            base, _, fraction = clock.partition(".")
            if len(fraction) > 6:
                raise ValueError("excess precision")
            instant = datetime.strptime(f"{date} {base}", "%Y %b %d %H:%M:%S")
            instant = instant.replace(microsecond=int(fraction.ljust(6, "0")) if fraction else 0)
        else:
            if re.search(r"\.\d{7,}", value):
                raise ValueError("excess precision")
            instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
            # The official table labels this column TCA (UTC) and displays no suffix.
            if instant.tzinfo is not None and instant.utcoffset().total_seconds() != 0:
                raise ValueError("not UTC")
    except (ValueError, OverflowError) as exc:
        raise ValidationError(f"{label}: invalid TCA") from exc
    return instant.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def _download(opener, url: str, media_types: set[str], max_bytes: int) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT}, method="GET")
    with opener.open(request, timeout=60) as response:
        if response.status != 200:
            raise ValidationError(f"{url}: HTTP status is not 200")
        if response.geturl() != url:
            raise ValidationError(f"{url}: response was redirected")
        content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type not in media_types:
            raise ValidationError(f"{url}: unexpected Content-Type")
        length_header = response.headers.get("Content-Length")
        if length_header is not None:
            try:
                length = int(length_header)
            except ValueError as exc:
                raise ValidationError(f"{url}: invalid Content-Length") from exc
            if length <= 0 or length > max_bytes:
                raise ValidationError(f"{url}: invalid Content-Length")
        else:
            length = None
            if response.headers.get("Transfer-Encoding", "").lower() != "chunked":
                raise ValidationError(f"{url}: missing Content-Length or chunked encoding")
        payload = response.read(max_bytes + 1)
        if not payload or len(payload) > max_bytes or (length is not None and len(payload) != length):
            raise ValidationError(f"{url}: empty, oversized or incomplete response")
    try:
        return payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError(f"{url}: invalid UTF-8") from exc


def _headers(headers, label: str) -> None:
    if not headers or len(headers) != len(set(headers)) or not REQUIRED.issubset(headers):
        raise ValidationError(f"{label}: missing or duplicate required headers")


def _identity(row: dict[str, str], label: str) -> tuple[str, str, str]:
    if any(row.get(field) is None for field in REQUIRED):
        raise ValidationError(f"{label}: malformed row")
    first, second, tca = (row[field].strip() for field in ("NORAD_CAT_ID_1", "NORAD_CAT_ID_2", "TCA"))
    if not first.isascii() or not first.isdecimal() or not second.isascii() or not second.isdecimal() or not tca:
        raise ValidationError(f"{label}: invalid NORAD ID or TCA")
    if not row["OBJECT_NAME_1"].strip() or not row["OBJECT_NAME_2"].strip():
        raise ValidationError(f"{label}: missing object name")
    for field in ("TCA_RANGE", "TCA_RELATIVE_SPEED", "MAX_PROB", "DILUTION"):
        value = row[field].strip()
        if field == "DILUTION" and not value:
            continue
        try:
            number = float(value)
        except ValueError as exc:
            raise ValidationError(f"{label}: invalid {field}") from exc
        if not math.isfinite(number) or number < 0 or (field == "MAX_PROB" and number > 1):
            raise ValidationError(f"{label}: invalid {field}")
    return first, second, _utc_time(tca, label)


def _table(text: str) -> list[tuple[str, str, str]]:
    parser = TableParser()
    parser.feed(text)
    parser.close()
    rows = parser.rows
    start = next((index for index, (kind, cells) in enumerate(rows)
                  if kind == "th" and _heading(cells, PRIMARY_HEADING)), None)
    if (start is None or start + 1 >= len(rows) or rows[start + 1][0] != "th" or
            not _heading(rows[start + 1][1], SECONDARY_HEADING)):
        raise ValidationError("table: required display headings not found")
    headings = rows[start:start + 2]
    # The first object's seven display cells precede six second-object cells:
    # display link, NORAD ID, name, DSE, probability, dilution threshold.
    records = []
    pending = None
    footer = False
    for kind, cells in rows[start + 2:]:
        if footer:
            if (kind == "td" and len(cells) in (6, 7)) or (kind == "th" and (kind, cells) in headings):
                raise ValidationError("table: extra event data after footer")
            continue
        if pending == "heading":
            if (kind, cells) != headings[1]:
                raise ValidationError("table: malformed repeated headings")
            pending = None
        elif kind == "th":
            if pending is not None:
                raise ValidationError("table: incomplete event")
            if (kind, cells) == headings[0]:
                pending = "heading"
            elif len(records) == ROW_COUNT and len(cells) == 1 and cells[0].startswith("Data Fields:"):
                footer = True
            else:
                raise ValidationError("table: malformed repeated headings")
        elif kind == "td" and len(cells) == 7 and pending is None:
            if any(not cell for cell in cells):
                raise ValidationError("table: incomplete data row")
            pending = cells
        elif kind == "td" and len(cells) == 6 and isinstance(pending, list):
            if any(not cell for cell in cells):
                raise ValidationError("table: incomplete data row")
            first, second = pending[1], cells[1]
            if any(not value.isascii() or not value.isdecimal() for value in (first, second)):
                raise ValidationError("table: invalid NORAD ID")
            try:
                probability = float(cells[4])
            except ValueError as exc:
                raise ValidationError("table: invalid MAX_PROB") from exc
            if not math.isfinite(probability) or not 0 <= probability <= 1:
                raise ValidationError("table: invalid MAX_PROB")
            records.append((first, second, _utc_time(pending[4], "table")))
            pending = None
        else:
            raise ValidationError("table: malformed data row")
    if pending is not None:
        raise ValidationError("table: incomplete event or headings")
    if len(records) != ROW_COUNT:
        raise ValidationError(f"table: expected {ROW_COUNT} rows, received {len(records)}")
    return records


def _baseline(text: str) -> tuple[list[tuple[str, str, str]], int]:
    reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
    _headers(reader.fieldnames, "baseline CSV")
    first_rows = []
    count = 0
    for row in reader:
        if None in row:
            raise ValidationError("baseline CSV: malformed row")
        identity = _identity(row, "baseline CSV")
        if count < ROW_COUNT:
            first_rows.append(identity)
        count += 1
    if count < ROW_COUNT:
        raise ValidationError(f"baseline CSV: fewer than {ROW_COUNT} rows")
    return first_rows, count


def _coverage(rows: list[tuple[str, str, str]], label: str):
    conjunctions = set(rows)
    if len(conjunctions) != len(rows):
        raise ValidationError(f"{label}: duplicate conjunction ID")
    return ({object_id for first, second, _ in rows for object_id in (first, second)},
            {(first, second) for first, second, _ in rows},
            {tca for _, _, tca in rows}, conjunctions)


def validate() -> dict:
    # No proxies or redirects: a candidate cannot select or redirect a destination.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    table = _table(_download(opener, TABLE_URL, {"text/html"}, MAX_TABLE_BYTES))
    baseline, baseline_count = _baseline(_download(
        opener, BASELINE_URL, {"text/csv", "text/plain", "application/octet-stream"}, MAX_CSV_BYTES,
    ))
    table_ids, table_pairs, table_tcas, table_conjunctions = _coverage(table, "table")
    baseline_ids, baseline_pairs, baseline_tcas, baseline_conjunctions = _coverage(baseline, "baseline CSV")
    if (table_ids != baseline_ids or table_pairs != baseline_pairs or
            table_tcas != baseline_tcas or table_conjunctions != baseline_conjunctions):
        raise ValidationError(
            "table/CSV top-100 reconciliation failed: "
            f"IDs {len(table_ids & baseline_ids)}/{len(baseline_ids)}, "
            f"object pairs {len(table_pairs & baseline_pairs)}/{len(baseline_pairs)}, "
            f"TCAs {len(table_tcas & baseline_tcas)}/{len(baseline_tcas)}, "
            f"conjunctions {len(table_conjunctions & baseline_conjunctions)}/{ROW_COUNT}"
        )
    return {
        "result": "passed", "endpoint": TABLE_URL, "intended_source_baseline": BASELINE_URL,
        "table_rows": len(table), "baseline_rows": baseline_count,
        "matched_ids": len(table_ids), "matched_object_pairs": len(table_pairs),
        "matched_tcas": len(table_tcas), "tca_min": min(table_tcas), "tca_max": max(table_tcas),
    }


def main(argv: list[str] | None = None) -> int:
    # A fixed command only: no URL, proxy, script, or arbitrary executable arguments.
    argparse.ArgumentParser(description=__doc__).parse_args(argv)
    try:
        result = validate()
    except (ValidationError, OSError, csv.Error, UnicodeError) as exc:
        # Do not emit untrusted response bodies, redirected URLs, or network error details.
        if isinstance(exc, ValidationError):
            print(f"validation failed: {exc}", file=sys.stderr)
        else:
            print(f"validation failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
