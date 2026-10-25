#!/usr/bin/env python3
"""EFFIS / Copernicus EMS — daily cumulative wildland fire area statistics.

The European Forest Fire Information System (EFFIS, run by JRC inside the
Copernicus Emergency Management Service) publishes satellite-based
(Rapid Damage Assessment) fire statistics for Europe. The keyless JSON API
`GET /rda-stats?year=YYYY` returns, per region (EU, EU_non, ME_AF, total,
and a small unattributed "null" bucket), the year-to-date cumulative burned
forest area:

  * ``estSumTotal``    estimated total area burned (ha), year to date
  * ``sumRDATotal``    RDA-detected area burned (ha), year to date
  * ``sumPerForest%``  burned area as a percentage of the region's forest cover

The numbers are a running cumulative: each day's run snapshots the current
year-to-date total, so the collected rows form a daily time series of the
cumulative burned area (daily increments are available by differencing).
The value resets at the start of each calendar year; completed years are
frozen (e.g. 2025 final total is stable), so historical years can be
retrieved once with ``--year`` and do not need to be re-pulled.

**Verified live 2026-10-25:** ``/rda-stats?year=2026`` returned 200 with
``total.estSumTotal=2065149.47`` (EU 796964.21, EU_non 838327.37,
ME_AF 429268.42) and ``/rda-stats?year=2025`` the frozen full-year value
``total.estSumTotal=2360769.47``. One ~450-byte JSON document per request.

Quirks (verified live 2026-10-25):

  * The documented ``country`` query parameter returns **500 Internal
    Server Error** (single- and multi-value). It is unusable; this fetcher
    does not send it. The API only exposes the regional roll-up.
  * ``/status`` (satellite feed health) is static content from 2022 —
    not collected.
  * The API is a small FastAPI service with an OpenAPI document at
    ``/openapi.json`` (paths: /rda-stats, /geocoder, /healtz, /status).

Licence: EU public sector information, CC BY 4.0 — EFFIS data license
(https://forest-fire.emergency.copernicus.eu/about-effis/data-license)
implements the Commission reuse policy; reuse is allowed with appropriate
credit. Attribution: EFFIS / Copernicus EMS, https://effis.jrc.ec.europa.eu/

Etiquette: keyless, one bounded request per run, daily cadence. Transient
5xx/429/connection failures are retried in a bounded way before the run is
allowed to fail.

Stdlib only.
"""
import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "effis_fire_stats"
BASE = "https://api2.effis.emergency.copernicus.eu"
ENDPOINT = "/rda-stats"
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
HEADERS = {"Accept": "application/json", "User-Agent": USER_AGENT}

# A single ~450-byte document; a brief upstream flap must not lose a run.
MAX_ATTEMPTS = 4
RETRY_BASE_DELAY = 5.0
MAX_RETRY_DELAY = 30.0
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"


def _retry_delay(error: urllib.error.HTTPError, fallback: float) -> float:
    """Return a courteous bounded delay from an HTTP Retry-After header."""
    value = error.headers.get("Retry-After") if error.headers is not None else None
    if value is not None:
        try:
            delay = float(value)
        except (TypeError, ValueError):
            delay = None
        if delay is not None and delay >= 0:
            return min(delay, MAX_RETRY_DELAY)
    return min(fallback, MAX_RETRY_DELAY)


def _get(year: int, timeout: int):
    """Fetch one rda-stats document, with bounded retries on transient failures.

    Transient failures -- 5xx, 429, and connection errors -- retry with
    5s/10s/20s backoff (at most ~35 seconds of sleeping, well inside the
    task timeout). A persistent outage still fails the run after the
    bounded attempts; other HTTP errors (e.g. 404) fail immediately. A
    200 response that is not a JSON object fails the run too: this endpoint
    serves a single JSON document, so an HTML error page means the service
    changed shape, and an empty result must not be silently written.
    """
    url = f"{BASE}{ENDPOINT}?{urllib.parse.urlencode({'year': year})}"
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt == MAX_ATTEMPTS - 1:
                raise
            reason = f"HTTP {exc.code}"
            delay = _retry_delay(exc, RETRY_BASE_DELAY * (2 ** attempt))
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            reason = str(exc.reason if isinstance(exc, urllib.error.URLError) else exc)
            delay = min(RETRY_BASE_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
        else:
            try:
                document = json.loads(body)
            except json.JSONDecodeError as exc:
                raise ValueError(f"non-JSON body from {url}") from exc
            if not isinstance(document, dict):
                raise ValueError(f"expected a JSON object from {url}")
            return document
        print(f"effis_fire_stats: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)


def _check_number(row: dict, field: str, region: str) -> float:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)) \
            or not math.isfinite(value) or value < 0:
        raise ValueError(f"region {region!r}: field {field!r} is not a finite non-negative number")
    return value


def normalize_document(document: dict, year: int, fetched_at: str):
    """Validate the rda-stats document and yield one record per region.

    The top-level keys are EFFIS's own region codes (total, EU, EU_non,
    ME_AF, null); they are used verbatim as the stable record id component.
    """
    if "total" not in document:
        raise ValueError("document is missing the 'total' aggregate")
    for region in sorted(document):
        row = document[region]
        if not isinstance(row, dict):
            raise ValueError(f"region {region!r} is not an object")
        area = row.get("area")
        if area is not None and not isinstance(area, str):
            raise ValueError(f"region {region!r}: field 'area' is not a string")
        est_total = _check_number(row, "estSumTotal", region)
        rda_total = _check_number(row, "sumRDATotal", region)
        pct = row.get("sumPerForest%")
        if pct is not None and (
            isinstance(pct, bool) or not isinstance(pct, (int, float))
            or not math.isfinite(pct) or not 0 <= pct <= 100
        ):
            raise ValueError(f"region {region!r}: field 'sumPerForest%' is not a finite percentage")
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"{year}_{region}",
            "year": year,
            "region": region,
            "area": area,
            "estSumTotal": est_total,
            "sumRDATotal": rda_total,
            "sumPerForest%": pct,
        }


def fetch_rda_stats(year: int, timeout: int = DEFAULT_TIMEOUT):
    """One bounded request for one year's cumulative fire-area statistics."""
    if not isinstance(year, int) or isinstance(year, bool) or not 1900 <= year <= 9999:
        raise ValueError("--year must be a four-digit calendar year")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or not 1 <= timeout <= MAX_TIMEOUT:
        raise ValueError(f"--timeout must be an integer between 1 and {MAX_TIMEOUT}")
    document = _get(year, timeout)
    # One UTC timestamp for every record in the run.
    fetched_at = datetime.now(timezone.utc).isoformat()
    yield from normalize_document(document, year, fetched_at)


def main(argv=None):
    parser = argparse.ArgumentParser(description="EFFIS daily cumulative burned-forest-area statistics (keyless)")
    parser.add_argument("--year", type=int, default=None,
                        help="calendar year to snapshot (default: current UTC year)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help=f"per-request timeout in seconds (1..{MAX_TIMEOUT})")
    args = parser.parse_args(argv)
    year = args.year if args.year is not None else datetime.now(timezone.utc).year
    records = list(fetch_rda_stats(year, timeout=args.timeout))
    for record in records:
        print(json.dumps(record, ensure_ascii=False))
    print(SUMMARY_PREFIX + json.dumps(
        {"source": SOURCE, "year": year, "records": len(records)},
        separators=(",", ":")), file=sys.stderr)


if __name__ == "__main__":
    main()
