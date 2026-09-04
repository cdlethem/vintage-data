#!/usr/bin/env python3
"""Queensland Open Hospitals — live emergency-department pressure for every facility.

Facility pages expose publisher update time, median and non-critical waits, patients
waiting, treatment spaces, closure, and a live-data-availability flag, refreshed every
15–30 minutes. The publisher's sitemap enumerated 40 facility pages when verified live
2026-09-04, so a bare run covers the whole state; explicit slugs narrow it.

Queensland Health terms apply. This is situational data, not medical advice. A facility
whose page omits the live payload is reported on stderr and skipped, so one broken page
cannot drop the other 39. Stdlib only.
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "queensland_emergency_pressure"
BASE_URL = "https://openhospitals.health.qld.gov.au/facility/"
SITEMAP = "https://openhospitals.health.qld.gov.au/sitemap.xml"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _get(url: str, timeout: int) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", "replace")


def discover_slugs(timeout: int = 30):
    slugs = dict.fromkeys(re.findall(r"/facility/([^<\s/]+)</loc>", _get(SITEMAP, timeout)))
    if not slugs:
        raise ValueError("sitemap exposed no facility pages; structure changed")
    return list(slugs)


FLIGHT_CHUNK = re.compile(r'self\.__next_f\.push\(\[\d+,("(?:[^"\\]|\\.)*")\]\)')
DECODER = json.JSONDecoder()


def _payload(text: str) -> str:
    """Reassemble the page's React flight payload into ordinary JSON text."""
    chunks = FLIGHT_CHUNK.findall(text)
    if not chunks:
        raise ValueError("page carries no flight payload; structure changed")
    return "".join(json.loads(chunk) for chunk in chunks)


def _object(payload: str, key: str):
    """Return the smallest JSON object in ``payload`` that carries ``key``.

    The payload interleaves the facility's own card with nearby-facility cards that
    repeat keys like ``name``, so a document-wide key match would mix facilities.
    """
    marker = payload.find(f'"{key}":')
    if marker < 0:
        return None
    start = payload.rfind("{", 0, marker)
    while start >= 0:
        try:
            candidate, _ = DECODER.raw_decode(payload, start)
        except ValueError:
            candidate = None
        if isinstance(candidate, dict) and key in candidate:
            return candidate
        start = payload.rfind("{", 0, start)
    return None


def fetch_pressure(slugs=(), timeout: int = 30):
    fetched_at = datetime.now(timezone.utc).isoformat()
    slugs = list(slugs) or discover_slugs(timeout)
    failures = 0
    for slug in slugs:
        try:
            payload = _payload(_get(BASE_URL + slug, timeout))
            header = _object(payload, "data_update_time") or {}
            live = _object(payload, "api_code") or {}
            if header.get("slug") != slug:
                raise ValueError("facility card is missing or belongs to another facility")
            fields = {
                "facility_name": header.get("name"),
                "facility_address": header.get("address"),
                "opening_hours": header.get("open"),
                "publisher_updated_at": header.get("data_update_time"),
                "wait_time_all_patients": header.get("wait_time_all_patients"),
                "currently_closed": header.get("currently_closed"),
                "live_data_availability": header.get("live_data_availability"),
                "api_code": live.get("api_code"),
                "wait_time_non_critical": live.get("wait_time_non_critical"),
                "patients_waiting": live.get("patients_waiting"),
                "treatment_spaces": live.get("treatment_spaces"),
                "treatment_spaces_updated_date": live.get("treatment_spaces_updated_date"),
            }
            if any(fields[name] is None for name in ("facility_name", "publisher_updated_at", "live_data_availability")):
                raise ValueError("page is missing its live facility payload")
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, TimeoutError, OSError) as error:
            failures += 1
            print(f"{slug}: {type(error).__name__}: {error}", file=sys.stderr)
            continue
        fields.update({"source": SOURCE, "fetched_at": fetched_at, "id": slug, "facility_slug": slug})
        yield fields
    if failures == len(slugs):
        raise RuntimeError(f"all {failures} facility pages failed")
    if failures:
        print(f"{failures} of {len(slugs)} facility pages failed", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("facility_slugs", nargs="*", help="omit to cover every facility in the sitemap")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_pressure(args.facility_slugs, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
