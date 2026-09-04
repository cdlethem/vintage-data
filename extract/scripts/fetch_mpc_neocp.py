#!/usr/bin/env python3
"""Minor Planet Center — current NEO Confirmation Page candidates.

Temporary candidates appear, accumulate observations, then confirm or disappear.
Verified live 2026-09-03 at the MPC tabular NEOCP page. Its HTML uses malformed
``<tr>`` openers where some row closers should be and hidden sort values inside cells;
the parser deliberately handles both. Rows moved to the comet confirmation page remain
in the table with blank scores and are excluded. MPC asks clients to stay at or below
five requests per second; this client makes one request per run.

Stdlib only.
"""
import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "mpc_neocp_candidates"
URL = "https://minorplanetcenter.net/iau/NEO/toconfirm_tabular.html"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self._row = None
        self._cell = None
        self._hidden_depth = 0

    def _finish_row(self):
        if self._cell is not None and self._row is not None:
            self._row.append(" ".join("".join(self._cell).split()))
        self._cell = None
        if self._row and any(self._row):
            self.rows.append(self._row)
        self._row = None

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "span" and "display:none" in attributes.get("style", "").replace(" ", ""):
            self._hidden_depth += 1
        elif tag == "tr":
            # The live page sometimes starts the next row instead of closing this one.
            if self._row is not None:
                self._finish_row()
            self._row = []
        elif tag in ("th", "td") and self._row is not None:
            if self._cell is not None:
                self._row.append(" ".join("".join(self._cell).split()))
            self._cell = []

    def handle_data(self, data):
        if self._cell is not None and not self._hidden_depth:
            self._cell.append(data)

    def handle_endtag(self, tag):
        if tag == "span" and self._hidden_depth:
            self._hidden_depth -= 1
        elif tag in ("th", "td") and self._cell is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self._finish_row()

    def close(self):
        super().close()
        if self._row is not None:
            self._finish_row()


def fetch_candidates(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("iso-8859-1", "replace")

    parser = TableParser()
    parser.feed(text)
    parser.close()
    header_index = next(
        (index for index, row in enumerate(parser.rows) if "Temp Desig" in row),
        None,
    )
    if header_index is None:
        raise ValueError("NEOCP page is missing the expected table header")
    headers = parser.rows[header_index]

    emitted = 0
    for cells in parser.rows[header_index + 1:]:
        row = {header: cells[index] if index < len(cells) else "" for index, header in enumerate(headers)}
        designation = row.get("Temp Desig", "")
        if not designation or not row.get("Score"):
            continue
        row.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": designation,
        })
        yield row
        emitted += 1
        if emitted >= limit:
            break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_candidates(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
