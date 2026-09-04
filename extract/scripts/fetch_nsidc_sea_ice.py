#!/usr/bin/env python3
"""NSIDC Sea Ice Index — daily Arctic/Antarctic extent, plain CSV, no key.

Lead #47: "one of the cleanest, most consequential long time series in existence, and
trivially easy to ingest" — and it mostly lives up to that, with one real CSV-parsing
trap documented below.

**Verified live 2026-09-03.** The URL pattern in the catalog blurb assumed a
`_v3.0.csv` filename, which **404'd** — the directory listing showed the file has
since moved to `_v4.0.csv`, updated `03-Sep-2026 10:52`, confirming this is actively
maintained, not an abandoned mirror. Fetched
`noaadata.apps.nsidc.org/NOAA/G02135/north/daily/data/N_seaice_extent_daily_v4.0.csv`
— **200, 1,893,402 bytes, 15,830 rows**, daily records from 1978-10-26 through
**2026-09-02** (yesterday relative to the probe), extent 4.674 million km².

Endpoints (base `https://noaadata.apps.nsidc.org/NOAA/G02135`):
    /north/daily/data/N_seaice_extent_daily_v4.0.csv     Arctic daily series
    /south/daily/data/S_seaice_extent_daily_v4.0.csv     Antarctic daily series
    (check the parent `data/` directory listing before hardcoding a version number —
    the v3.0 filename this catalog originally pointed at is already gone)

Quirks that will cost someone an afternoon:
    * **The version number in the filename changes over time and old ones 404.**
      Confirmed live: `_v3.0.csv` no longer exists, `_v4.0.csv` does. List the
      directory (`.../data/`) rather than hardcoding a version, or expect to update
      this URL again eventually.
    * **Two header rows, not one** — a column-name row, then a units row
      (`YYYY, MM, DD, 10^6 sq km, ...`). Skip both before reading data.
    * **The `Source Data` column is a real CSV-quoting trap.** On days where more than
      one satellite source contributed, the cell holds a Python-list-repr string with
      internal commas, and the CSV writer correctly quotes the whole field — but a
      naive `line.split(",")` still shreds it: a verified real row split into 8 pieces
      on a comma-split instead of the correct 6 fields. Use the stdlib `csv` module
      (verified correct here), never manual comma-splitting.
    * `Year`/`Month`/`Day`/`Extent`/`Missing` fields all carry leading whitespace
      inside the CSV values (`"    09"`, not `"09"`) — `.strip()` before casting.
    * `Missing` (km² not observed) is usually `0.000` but can be nonzero on
      instrument-gap days; don't discard those rows, the `Extent` value is still the
      best available estimate for that day.

Etiquette: keyless, static file on a public NOAA/NSIDC mirror. It's the whole series
in one file, refreshed roughly daily — fetch once a day and diff against your last
seen date rather than re-downloading and re-processing 15,000+ rows for one new point.

Stdlib only.
"""
import csv
import io
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://noaadata.apps.nsidc.org/NOAA/G02135"
FILES = {
    "north": f"{BASE}/north/daily/data/N_seaice_extent_daily_v4.0.csv",
    "south": f"{BASE}/south/daily/data/S_seaice_extent_daily_v4.0.csv",
}


def _get_text(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8")


def fetch_extent(hemisphere: str = "north", since_year: int | None = None):
    """Daily extent for one hemisphere. Use csv.reader -- the Source Data column can
    contain internal commas inside a quoted field, which a naive split breaks on."""
    now = datetime.now(timezone.utc).isoformat()
    text = _get_text(FILES[hemisphere])
    reader = csv.reader(io.StringIO(text))
    next(reader)  # column-name header row
    next(reader)  # units header row
    for row in reader:
        if len(row) < 5:
            continue
        year, month, day, extent, missing = (c.strip() for c in row[:5])
        year_i = int(year)
        if since_year and year_i < since_year:
            continue
        date = f"{year_i:04d}-{int(month):02d}-{int(day):02d}"
        yield {
            "source": f"nsidc_sea_ice_{hemisphere}",
            "fetched_at": now,
            "id": f"{hemisphere}:{date}",
            "date": date,
            "extent_million_km2": float(extent),
            "missing_million_km2": float(missing),
        }


if __name__ == "__main__":
    hemisphere = sys.argv[1] if len(sys.argv) > 1 else "north"
    since = int(sys.argv[2]) if len(sys.argv) > 2 else datetime.now().year
    for rec in fetch_extent(hemisphere, since_year=since):
        print(json.dumps(rec, ensure_ascii=False))
