#!/usr/bin/env python3
"""US Treasury daily yield curve — business-day rates for 26 maturities (keyless, HTML).

The Treasury publishes the interpolated "Daily Treasury Yield Curve" once per
business day (after the close): one row per day with rates from the 1-month
bill to the 30-year bond, plus the T-bill bank-discount/coupon pairs. The
file is served as a static HTML table at
``/resource-center/data-chart-center/interest-rates/TextView`` per calendar
year; there is no machine-readable endpoint (the legacy CSV/XML URLs now
404), so the table is parsed from the page.

'Current' per run = business-day rows dated after the stored watermark
(``last_date``), nothing else. One request per calendar year, from the
watermark's year (or the current year on a cold start) through the current
year, so a stopped pipeline re-walks only the years it missed. State is
written only after every record is printed, so an aborted run re-fetches
rather than skipping (at-least-once).

Values are quoted percentages; ``N/A`` (e.g. the unused extrapolated
20/30-year columns) becomes null.

Verified live 2026-11-11: the 2026 page returned 217 rows through 11/10 and
the 2025 page 250 rows (01/02..12/31), confirming daily (weekdays-only)
updates.

U.S. Government work: public domain, no attribution required.

Stdlib only.
"""

import argparse
import html.parser
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "treasury_yield_curve"
BASE = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "TextView?type=daily_treasury_yield_curve&field_tdr_date_value={year}"
)
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

#: Exact column order of the Daily Treasury Yield Curve table (verified 2026-11-11).
EXPECTED_HEADER = [
    "Date", "20 YR", "30 YR", "Extrapolation Factor",
    "6 WEEKS BANK DISCOUNT", "COUPON EQUIVALENT",
    "8 WEEKS BANK DISCOUNT", "COUPON EQUIVALENT",
    "17 WEEKS BANK DISCOUNT", "COUPON EQUIVALENT",
    "52 WEEKS BANK DISCOUNT", "COUPON EQUIVALENT",
    "1 Mo", "1.5 Mo", "2 Mo", "3 Mo", "4 Mo", "6 Mo",
    "1 Yr", "2 Yr", "3 Yr", "5 Yr", "7 Yr", "10 Yr", "20 Yr", "30 Yr",
]
FIELD_NAMES = [
    "date", "y20_extrapolated", "y30_extrapolated", "extrapolation_factor",
    "t6w_bank_discount", "t6w_coupon_equivalent",
    "t8w_bank_discount", "t8w_coupon_equivalent",
    "t17w_bank_discount", "t17w_coupon_equivalent",
    "t52w_bank_discount", "t52w_coupon_equivalent",
    "y1m", "y1_5m", "y2m", "y3m", "y4m", "y6m",
    "y1y", "y2y", "y3y", "y5y", "y7y", "y10y", "y20y", "y30y",
]


class YieldCurveTableParser(html.parser.HTMLParser):
    """Collect every ``<table>`` on the page as a list of rows of cell text."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag == "table" and self._table is not None:
            self.tables.append(self._table)
            self._table = None
            self._row = None
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self._table.append(self._row)
            self._row = None
        elif tag in ("td", "th") and self._cell is not None:
            text = " ".join("".join(self._cell).split())
            self._row.append(text)
            self._cell = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "treasury_yield_curve.json"


def load_state(path: pathlib.Path) -> dict:
    """Return {"last_date": "YYYY-MM-DD"} or {} when the watermark is absent."""
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict):
        raise ValueError(f"malformed watermark state in {path}")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def fetch_year(year: int, timeout: int = 30, retries: int = 2,
               retry_delay: float = 10.0) -> tuple[str, dict]:
    """GET one calendar-year page; return (body text, request stats).

    Retries transient HTTP statuses and connection-level failures with a fixed
    delay; anything else raises immediately.
    """
    url = BASE.format(year=year)
    stats = {"attempted": 0, "succeeded": 0, "failed": 0,
             "retries": 0, "backoff_s": 0.0}
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        stats["attempted"] += 1
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": USER_AGENT, "Accept": "text/html"}
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
            stats["succeeded"] += 1
            return body.decode("utf-8", "replace"), stats
        except urllib.error.HTTPError as exc:
            if exc.code in RETRIABLE_STATUSES and attempt < retries:
                last_error = exc
                stats["failed"] += 1
                stats["retries"] += 1
                time.sleep(retry_delay)
                stats["backoff_s"] += retry_delay
                continue
            stats["failed"] += 1
            raise RuntimeError(
                f"HTTP {exc.code} fetching the {year} yield curve page"
            ) from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            if attempt < retries:
                last_error = exc
                stats["failed"] += 1
                stats["retries"] += 1
                time.sleep(retry_delay)
                stats["backoff_s"] += retry_delay
                continue
            stats["failed"] += 1
            raise RuntimeError(
                f"connection failed fetching the {year} yield curve page: {last_error}"
            ) from exc
    raise RuntimeError(f"unreachable retry loop for {year}: {last_error}")


def parse_year_table(body: str, year: int) -> list[dict[str, object]]:
    """Parse one calendar-year page into row dicts (rate columns nullable).

    Fails loudly on layout drift: a header that no longer matches, a row with
    the wrong cell count, an unparsable date, or a date outside ``year``.
    """
    parser = YieldCurveTableParser()
    parser.feed(body)
    table = None
    for candidate in parser.tables:
        if candidate and [c for c in candidate[0]] == EXPECTED_HEADER:
            table = candidate
            break
    if table is None:
        observed = [
            " | ".join(row[:6])
            for t in parser.tables[:3]
            for row in t[:1]
        ]
        raise RuntimeError(
            "daily_treasury_yield_curve table not recognized on the "
            f"{year} page; page tables start with: {observed!r}"
        )

    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for line_no, cells in enumerate(table[1:], start=2):
        if len(cells) != len(EXPECTED_HEADER):
            raise RuntimeError(
                f"row {line_no} of the {year} table has {len(cells)} cells, "
                f"expected {len(EXPECTED_HEADER)}"
            )
        try:
            month, day, row_year = (int(part) for part in cells[0].split("/"))
        except ValueError:
            raise RuntimeError(
                f"row {line_no} of the {year} table has an unparsable date "
                f"{cells[0]!r}"
            ) from None
        if row_year != year:
            raise RuntimeError(
                f"row {line_no} of the {year} table is dated {cells[0]!r}"
            )
        date_iso = f"{row_year:04d}-{month:02d}-{day:02d}"
        if date_iso in seen:
            raise RuntimeError(f"duplicate date {date_iso} in the {year} table")
        seen.add(date_iso)
        record: dict[str, object] = {"date": date_iso}
        for name, value in zip(FIELD_NAMES[1:], cells[1:]):
            record[name] = None if value in ("", "N/A") else float(value)
        rows.append(record)
    return rows


def main(argv: list[str] | None = None) -> None:
    current_year = datetime.now(timezone.utc).year
    parser = argparse.ArgumentParser(
        description=(
            "Fetch US Treasury daily yield-curve rows newer than the watermark; "
            "one request per calendar year."
        )
    )
    parser.add_argument(
        "--start-year", type=int,
        help="first calendar year, inclusive (defaults to the watermark's year "
             "or the current year)",
    )
    parser.add_argument(
        "--end-year", type=int,
        help=f"last calendar year, inclusive (defaults to the current UTC year, {current_year})",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=2,
                        help="retries per year page after a transient failure")
    parser.add_argument("--retry-delay", type=float, default=10.0,
                        help="seconds to wait between retries")
    parser.add_argument("--state-file",
                        help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off pull)")
    args = parser.parse_args(argv)

    if isinstance(args.timeout, bool) or args.timeout <= 0:
        parser.error("timeout must be a positive integer")
    if args.retries < 0:
        parser.error("retries must be non-negative")

    path = state_path(args.state_file)
    state = {} if args.no_state else load_state(path)
    watermark = state.get("last_date", "")
    if watermark:
        try:
            datetime.strptime(watermark, "%Y-%m-%d")
        except ValueError:
            parser.error(f"watermark is not a YYYY-MM-DD date: {watermark!r}")

    end_year = args.end_year if args.end_year is not None else current_year
    if end_year < 1900 or end_year > current_year:
        parser.error(f"end-year must be between 1900 and the current year ({current_year})")
    start_year = args.start_year
    if start_year is None:
        start_year = int(watermark[:4]) if watermark else end_year
    if start_year < 1900 or start_year > end_year:
        parser.error(f"start-year must be between 1900 and end-year ({end_year})")

    fetched_at = datetime.now(timezone.utc).isoformat()
    stats = {"attempted": 0, "succeeded": 0, "failed": 0,
             "retries": 0, "backoff_s": 0.0}
    rows: list[dict[str, object]] = []
    try:
        for year in range(start_year, end_year + 1):
            body, year_stats = fetch_year(
                year, timeout=args.timeout, retries=args.retries,
                retry_delay=args.retry_delay,
            )
            for key in stats:
                stats[key] += year_stats[key]  # type: ignore[operator]
            rows.extend(parse_year_table(body, year))
    except (RuntimeError, ValueError) as error:
        print(f"fetch_treasury_yield_curve: {error}", file=sys.stderr)
        sys.exit(1)

    emitted = 0
    newest = watermark
    for row in sorted(rows, key=lambda r: r["date"]):
        if watermark and row["date"] <= watermark:
            continue
        record = {"source": SOURCE, "fetched_at": fetched_at,
                  "id": row["date"], **row}
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
        emitted += 1
        if row["date"] > newest:
            newest = row["date"]

    if not args.no_state and emitted:
        save_state(path, {"last_date": newest})

    health = "healthy" if stats["failed"] == 0 else "degraded"
    summary = {
        "health": health,
        "completeness": "complete",
        "requests": {
            "attempted": stats["attempted"],
            "succeeded": stats["succeeded"],
            "failed": stats["failed"],
            "retries": stats["retries"],
            "backoff_s": stats["backoff_s"],
        },
        "metrics": {
            "rows_in_source": len(rows),
            "rows_emitted": emitted,
            "latest_source_date": max((r["date"] for r in rows), default=None),
            "years": list(range(start_year, end_year + 1)),
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")),
          file=sys.stderr)


if __name__ == "__main__":
    main()
