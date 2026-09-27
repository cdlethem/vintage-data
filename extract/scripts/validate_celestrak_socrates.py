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
import sys
import urllib.request
from html.parser import HTMLParser
from datetime import datetime


TABLE_URL = "https://celestrak.org/SOCRATES/table-socrates.php?NAME=,&ORDER=MAXPROB&MAX=100"
# Intended-source baseline: the production extractor's official probability-sorted CSV.
BASELINE_URL = "https://celestrak.org/SOCRATES/sort-maxProb.csv"
ROW_COUNT = 100
REQUIRED = {
    "NORAD_CAT_ID_1", "OBJECT_NAME_1", "DSE_1", "NORAD_CAT_ID_2", "OBJECT_NAME_2", "DSE_2",
    "TCA", "TCA_RANGE", "TCA_RELATIVE_SPEED", "MAX_PROB", "DILUTION",
}
# Official display headings, repeated every ten conjunctions (not CSV headers).
TABLE_FIRST_HEADER = ("Data&Graphs", "NORADCatalogNumber", "Name [Ops Status]",
                      "DaysSinceEpoch", "TCA(UTC)", "MinRange(km)", "RelativeSpeed(km/sec)")
TABLE_SECOND_HEADER = ("MaxProbability", "DilutionThreshold(km)")

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
        self.rows: list[tuple[int, list[str]]] = []
        self.unclosed_rows: set[int] = set()
        self._table = 0
        self._table_depth = 0
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            if self._table_depth == 0:
                self._table += 1
            self._table_depth += 1
        elif tag == "tr" and self._table_depth == 1:
            if self._row is not None:
                self.unclosed_rows.add(self._table)
            self._row = []
        elif tag in ("th", "td") and self._row is not None:
            self._cell = []

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("th", "td") and self._cell is not None:
            self._row.append("".join(self._cell).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append((self._table, self._row))
            self._row = None
            self._cell = None
        elif tag == "table" and self._table_depth:
            if self._table_depth == 1 and self._row is not None:
                self.unclosed_rows.add(self._table)
                self._row = None
                self._cell = None
            self._table_depth -= 1


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
    try:
        instant = datetime.fromisoformat(tca.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError(f"{label}: invalid TCA") from exc
    if instant.tzinfo is not None and instant.utcoffset().total_seconds() != 0:
        raise ValidationError(f"{label}: non-UTC TCA")
    tca = instant.replace(tzinfo=None).isoformat(sep=" ")
    if not row["OBJECT_NAME_1"].strip() or not row["OBJECT_NAME_2"].strip():
        raise ValidationError(f"{label}: missing object name")
    for field in ("DSE_1", "DSE_2", "TCA_RANGE", "TCA_RELATIVE_SPEED", "MAX_PROB", "DILUTION"):
        value = row[field].strip()
        if field == "DILUTION" and not value:
            continue
        try:
            number = float(value)
        except ValueError as exc:
            raise ValidationError(f"{label}: invalid {field}") from exc
        if not math.isfinite(number) or number < 0 or (field == "MAX_PROB" and number > 1):
            raise ValidationError(f"{label}: invalid {field}")
    return first, second, tca


def _table(text: str) -> list[tuple[str, str, str]]:
    parser = TableParser()
    parser.feed(text)
    parser.close()
    header_index = next((i for i, (_, cells) in enumerate(parser.rows)
                         if tuple(cells) == TABLE_FIRST_HEADER), None)
    if header_index is None:
        raise ValidationError("table: required display headers not found")
    table_number = parser.rows[header_index][0]
    if any(number == table_number and len(cells) in (6, 7)
           for number, cells in parser.rows[:header_index]):
        raise ValidationError("table: event-shaped rows before first display header")
    if any(number != table_number and tuple(cells) == TABLE_FIRST_HEADER
           for number, cells in parser.rows):
        raise ValidationError("table: ambiguous display sections")
    if table_number in parser.unclosed_rows or parser._table_depth:
        raise ValidationError("table: unfinished display table row")
    rows = []
    for number, cells in parser.rows[header_index:]:
        if number != table_number:
            break
        rows.append(cells)
    # The official display has a final prose legend inside the result table.
    if rows and len(rows[-1]) == 1 and rows[-1][0].startswith("Data Fields:"):
        rows.pop()
    records = []
    index = 0
    while index < len(rows):
        if tuple(rows[index]) != TABLE_FIRST_HEADER or index + 1 >= len(rows) or tuple(rows[index + 1]) != TABLE_SECOND_HEADER:
            raise ValidationError("table: malformed display headers")
        index += 2
        pairs_in_section = 0
        while index < len(rows) and tuple(rows[index]) != TABLE_FIRST_HEADER:
            if index + 1 >= len(rows) or len(rows[index]) != 7 or len(rows[index + 1]) != 6:
                raise ValidationError("table: malformed data row or incomplete conjunction pair")
            first, second = rows[index:index + 2]
            if not first[0] or not second[0]:
                raise ValidationError("table: malformed data row")
            row = {
                "NORAD_CAT_ID_1": first[1], "NORAD_CAT_ID_2": second[1],
                "DSE_1": first[3], "DSE_2": second[3],
                "OBJECT_NAME_1": first[2], "OBJECT_NAME_2": second[2],
                "TCA": first[4], "TCA_RANGE": first[5], "TCA_RELATIVE_SPEED": first[6],
                "MAX_PROB": second[4], "DILUTION": second[5],
            }
            records.append(_identity(row, "table"))
            pairs_in_section += 1
            index += 2
        if not pairs_in_section:
            raise ValidationError("table: empty display section")
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
