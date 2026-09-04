#!/usr/bin/env python3
"""InfoDengue — weekly Brazilian municipality alert estimates, full national sweep.

Reported and estimated cases, intervals, Rt, alert level, and model versions are
revised retrospectively, making ``fetched_at`` a revision axis. Verified live
2026-09-03 against ``https://info.dengue.mat.br/api/alertcity``. The API requires a
municipality geocode, disease, and epidemiological-year range and returns CSV.

**Rebuilt 2026-09-04**: the prior config tracked one municipality (Rio de Janeiro).
IBGE's own public municipalities directory
(``servicodados.ibge.gov.br/api/v1/localidades/municipios``, keyless) lists **all
5,571 real Brazilian municipalities live** with their official 7-digit geocodes.
Verified live 2026-09-04 against a random sample of 10 non-capital municipalities:
all 10 returned real InfoDengue data (the national arbovirus surveillance system,
SINAN, covers essentially the whole country, not just big cities). This now
discovers the full municipality list at runtime and sweeps every one -- thousands
of tracked entities' weekly epidemiological trend, aggregated, instead of one city.

Run daily to capture revisions to this weekly series. Follow InfoDengue attribution
terms.

Stdlib only.
"""
import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "infodengue_city_alerts"
URL = "https://info.dengue.mat.br/api/alertcity"
IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
REQUEST_DELAY_S = 0.3


def fetch_municipality_geocodes(timeout: int = 30):
    """Every real Brazilian municipality's IBGE geocode, live -- no hardcoded,
    staleness-prone list (municipalities are occasionally created/renamed).
    IBGE gzips this response regardless of Accept-Encoding, so decompress
    explicitly rather than relying on urllib to do it (it doesn't)."""
    request = urllib.request.Request(IBGE_MUNICIPIOS_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
        if response.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
    rows = json.loads(raw.decode("utf-8"))
    return [str(row["id"]) for row in rows if row.get("id")]


def fetch_alerts(
    geocode: str = "3304557",
    disease: str = "dengue",
    year: int | None = None,
    limit: int | None = 100,
    timeout: int = 30,
    ey_start: int | None = None,
    ey_end: int | None = None,
):
    if ey_start is None:
        ey_start = year or datetime.now(timezone.utc).year
    if ey_end is None:
        ey_end = year if year is not None else ey_start
    if ey_start > ey_end:
        raise ValueError(f"start year {ey_start} is after end year {ey_end}")
    query = urllib.parse.urlencode({
        "geocode": geocode,
        "disease": disease,
        "format": "csv",
        "ew_start": 1,
        "ew_end": 53,
        "ey_start": ey_start,
        "ey_end": ey_end,
    })
    request = urllib.request.Request(f"{URL}?{query}", headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(text))
    expected = {"SE", "municipio_nome"}
    if not reader.fieldnames or not expected.issubset(reader.fieldnames):
        raise ValueError(f"InfoDengue CSV is missing columns: {sorted(expected)}")
    for index, row in enumerate(reader):
        if limit is not None and limit > 0 and index >= limit:
            break
        record = dict(row)
        record.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"{geocode}|{disease}|{row['SE']}",
            "geocode": geocode,
            "disease": disease,
        })
        yield record


def fetch_all_municipalities(disease: str = "dengue", year: int | None = None,
                             limit_per_municipality: int | None = 10, timeout: int = 30,
                             ey_start: int | None = None, ey_end: int | None = None):
    """Full national sweep: every real Brazilian municipality IBGE lists. One
    municipality erroring (timeout, no surveillance data yet) is logged and
    skipped rather than aborting the other 5,570."""
    geocodes = fetch_municipality_geocodes(timeout=timeout)
    for geocode in geocodes:
        try:
            yield from fetch_alerts(
                geocode,
                disease=disease,
                year=year,
                limit=limit_per_municipality,
                timeout=timeout,
                ey_start=ey_start,
                ey_end=ey_end,
            )
        except Exception as exc:  # noqa: BLE001 - keep the national sweep going
            print(f"infodengue: skipping geocode {geocode!r}: {exc!r}", file=sys.stderr)
        time.sleep(REQUEST_DELAY_S)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--geocode", default=None,
                        help="single-municipality mode; omit for the full national sweep")
    parser.add_argument("--disease", choices=("dengue", "chikungunya", "zika"), default="dengue")
    parser.add_argument("--year", type=int,
                        help="single epidemiological year (shorthand for --start-year/--end-year)")
    parser.add_argument("--start-year", type=int,
                        help="first epidemiological year to fetch (use with --end-year)")
    parser.add_argument("--end-year", type=int,
                        help="last epidemiological year to fetch (use with --start-year)")
    parser.add_argument("--limit", type=int, default=10,
                        help="per-municipality row cap (default 10 most recent weeks; 0 means all)")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.year is not None and (args.start_year is not None or args.end_year is not None):
        parser.error("--year cannot be combined with --start-year/--end-year")
    if (args.start_year is None) != (args.end_year is None):
        parser.error("--start-year and --end-year must be provided together")
    limit = max(0, args.limit)
    if args.geocode:
        gen = fetch_alerts(
            args.geocode,
            disease=args.disease,
            year=args.year,
            limit=limit,
            timeout=args.timeout,
            ey_start=args.start_year,
            ey_end=args.end_year,
        )
    else:
        gen = fetch_all_municipalities(
            disease=args.disease,
            year=args.year,
            limit_per_municipality=limit,
            timeout=args.timeout,
            ey_start=args.start_year,
            ey_end=args.end_year,
        )
    for record in gen:
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
