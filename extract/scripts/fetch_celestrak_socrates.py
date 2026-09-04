#!/usr/bin/env python3
"""CelesTrak SOCRATES — predicted satellite conjunctions, keyless CSV.

Lead #107: space traffic control as open data. Three times a day CelesTrak runs every
active payload against the full public catalog of unclassified orbital element sets,
looking seven days ahead for close approaches. Which objects are about to pass
dangerously close, updated continuously.

**Verified live 2026-09-03.** The catalog blurb's implied endpoint didn't exist; the
real one was found from the SOCRATES landing page's own links, not guessed:
`celestrak.org/SOCRATES/table-socrates.php?NAME=,&ORDER=MAXPROB&MAX=25` — an HTML
results page confirming the run is current (**"Data current as of 2026 Sep 02
20:55:30 UTC"**, considering **16,510 primaries, 32,348 secondaries, 144,373
conjunctions**). The same site also publishes the full result set as plain CSV:
`sort-maxProb.csv` — **200, 16,237,272 bytes**, header matching the documented schema
exactly, first row a real Starlink-Starlink conjunction 21 meters apart. Deeper in the
file: two COSMOS satellites (2581/2582) at 535m range with `MAX_PROB: 8.810E-02`.

Endpoints (base `https://celestrak.org/SOCRATES`):
    /sort-maxProb.csv        the full current run, sorted by collision probability
    /sort-minRange.csv       same run, sorted by closest approach distance
    /table-socrates.php?NAME=<filter>&ORDER=MAXPROB|MINRANGE|SSC&MAX=N   filtered HTML

Columns (documented at `/socrates-format.php`, verified against the real CSV header):
    NORAD_CAT_ID_1/2, OBJECT_NAME_1/2, DSE_1/2 (days since GP epoch — accuracy proxy),
    TCA (time of closest approach), TCA_RANGE (km), TCA_RELATIVE_SPEED (km/s),
    MAX_PROB, DILUTION (dilution threshold, km).

Quirks that will cost someone an afternoon:
    * **The catalog's implied URL pattern doesn't exist; find the real one from the
      page's own links.** CelesTrak's site has been restructured (SOCRATES ->
      "SOCRATES Plus") and old direct-CSV URL guesses will 404. `table-socrates.php`
      and the `sort-*.csv` exports are what the current site actually serves.
    * `OBJECT_NAME_1`/`_2` embed an operational-status suffix in brackets —
      `[+]` operational, `[P]` partially operational, observed live in both forms.
      Strip it if you want a clean name, or parse it as its own status field.
    * **The same object pair can appear multiple times** with slightly different DSE
      and TCA values — verified live (COSMOS 2581/2582 appear three times with TCA 22
      minutes apart). This is CelesTrak reporting successive close-approach windows
      within the 7-day computation, not a duplicate; keep all rows, don't dedupe by
      object-pair alone.
    * `MAX_PROB` is scientific notation as text (`"8.810E-02"`) — cast with `float()`,
      which handles it fine, but don't assume a fixed-decimal format if you're
      validating the column with a regex.
    * A run takes over 12 hours to compute (`"Computation run time: 12h 32m 02.576s"`
      observed live) — the file updates a few times a day, not continuously; polling
      faster than that just re-fetches 16 MB of the same data.

Etiquette: keyless, but this is a large file (16+ MB) computed at real compute cost by
a small operation CelesTrak funds partly by donation. Fetch at most a few times a day
and cache; never poll this in a tight loop.

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
BASE = "https://celestrak.org/SOCRATES"


def _get_text(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_conjunctions(sort: str = "maxProb", limit: int | None = None):
    """The full current SOCRATES run. sort is 'maxProb' or 'minRange'."""
    now = datetime.now(timezone.utc).isoformat()
    text = _get_text(f"{BASE}/sort-{sort}.csv")
    reader = csv.DictReader(io.StringIO(text))
    for i, row in enumerate(reader):
        if limit and i >= limit:
            return
        yield {
            "source": "celestrak_socrates",
            "fetched_at": now,
            "id": f"{row['NORAD_CAT_ID_1']}:{row['NORAD_CAT_ID_2']}:{row['TCA']}",
            "norad_id_1": row["NORAD_CAT_ID_1"],
            "object_name_1": row["OBJECT_NAME_1"],
            "norad_id_2": row["NORAD_CAT_ID_2"],
            "object_name_2": row["OBJECT_NAME_2"],
            "tca": row["TCA"],
            "tca_range_km": float(row["TCA_RANGE"]),
            "tca_relative_speed_km_s": float(row["TCA_RELATIVE_SPEED"]),
            "max_prob": float(row["MAX_PROB"]),
            "dilution_km": float(row["DILUTION"]) if row["DILUTION"] else None,
        }


if __name__ == "__main__":
    sort = sys.argv[1] if len(sys.argv) > 1 else "maxProb"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    for rec in fetch_conjunctions(sort, limit=limit):
        print(json.dumps(rec, ensure_ascii=False))
