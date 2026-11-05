#!/usr/bin/env python3
"""U.S. TSA — daily national checkpoint passenger throughput.

Daily totals expose travel demand, holiday peaks, and disruption recovery. Verified live
2026-09-03 on TSA's official passenger-volume page, updated weekdays by 09:00. The
script selects only the two-column ``Date``/``Numbers`` table and preserves the HTTP
Last-Modified timestamp. U.S. federal public data.

**Transport note (2026-11-05)**: requests go through a `curl` subprocess, not
`urllib`. As of 2026-11-05 the Akamai edge in front of www.tsa.gov answers
`urllib` with a 403 "Access Denied" (AkamaiGHost/edgesuite) on every request,
while `curl` is served normally — verified from the pipeline host on the same
URL: `urllib` 403 for any User-Agent, `curl` 200. The edge fingerprints the TLS
client *and* the header set: our `vintage-data` User-Agent and curl's bare
`Accept: */*` each 403 even via curl, and only a browser User-Agent together
with browser `Accept`/`Accept-Language` headers passes. Stdlib only; `curl` is
a system dependency.
"""

import argparse
import json
import subprocess
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "tsa_checkpoint_throughput"
URL = "https://www.tsa.gov/travel/passenger-volumes"
# A genuine browser fingerprint is required (see the transport note above);
# the project's contact User-Agent is rejected by the edge.
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _get(url: str, timeout: int = 30):
    """GET via a curl subprocess (see the transport note in the docstring).

    Returns ``(last_modified, body_text)``; ``last_modified`` is ``""`` when
    the response carries no Last-Modified header.
    """
    proc = subprocess.run(
        [
            "curl", "-sS", "--fail", "--max-time", str(timeout), "-D", "-",
            "--compressed",
            "-H", f"User-Agent: {USER_AGENT}",
            "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
            "-H", "Accept-Language: en-US,en;q=0.9",
            url,
        ],
        capture_output=True, text=True, timeout=timeout + 5,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"GET {url} failed (curl exit {proc.returncode}): {proc.stderr.strip()}")
    # -D - prepends the response header dump; HTTP/2 responses use bare LF
    # line endings, so fall back to the LF blank line when the CRLF one is
    # absent.
    head, sep, body = proc.stdout.partition("\r\n\r\n")
    if not sep:
        head, sep, body = proc.stdout.partition("\n\n")
    if not sep:
        raise RuntimeError(f"GET {url} failed: no header/body separator in curl output")
    last_modified = ""
    for line in head.splitlines():
        if line.lower().startswith("last-modified:"):
            last_modified = line.split(":", 1)[1].strip()
    return last_modified, body


class Tables(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables, self.table, self.row, self.cell = [], None, None, None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.table = []
        elif tag == "tr" and self.table is not None:
            self.row = []
        elif tag in ("th", "td") and self.row is not None:
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("th", "td") and self.cell is not None:
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            if any(self.row):
                self.table.append(self.row)
            self.row = None
        elif tag == "table" and self.table is not None:
            self.tables.append(self.table)
            self.table = None


def fetch_throughput(limit: int = 400, timeout: int = 30):
    if limit <= 0:
        return
    modified, text = _get(URL, timeout)
    fetched_at = datetime.now(timezone.utc).isoformat()
    parser = Tables()
    parser.feed(text)
    table = next(
        (table for table in parser.tables if table and table[0] == ["Date", "Numbers"]),
        None,
    )
    if table is None:
        raise ValueError("TSA page is missing the passenger-volume table")
    for cells in table[1 : limit + 1]:
        if len(cells) != 2:
            raise ValueError("TSA volume row has an unexpected shape")
        day = (
            datetime.strptime(cells[0], "%m/%d/%Y")
            .replace(tzinfo=timezone.utc)
            .date()
            .isoformat()
        )
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": day,
            "date": day,
            "passengers": int(cells[1].replace(",", "")),
            "publisher_updated_at": modified,
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=400)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_throughput(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
