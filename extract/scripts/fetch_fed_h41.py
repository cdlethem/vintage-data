#!/usr/bin/env python3
"""Federal Reserve H.4.1 — Reserve Balances of Depository Institutions (keyless).

Weekly release (Fridays, ~16:15 US Eastern) of every depository-institution
reserve series: required/excess reserves, bank and public funds, borrowing,
assets, and the components in between. The Board publishes the complete
history since 2002 as one XML zip on the release page:

    https://www.federalreserve.gov/releases/h41/data/FRB_h41_xml.zip

**Verified live 2026-11-07**: the zip (~9 MB) downloads in under a second and
contains `H41_data.xml` (~125 MB): 1,332 weekly series, 1,661,004
observations, all with a latest period of 2026-11-04 (the Wednesday the
report covers; the release publishes Wednesday levels).

Design: stateless bounded window. Each run re-emits the observations of the
last N weekly periods (default 13, i.e. one quarter) for every series that
has data in that window. The window is anchored to the newest period present
in the file, not the wall clock, so a given payload always yields the same
records. A failed or missed run loses nothing: the next run re-covers the
whole window, and the Board's revisions of recent weeks are picked up because
they land inside it.

XML shape (SDMX-flavoured, FRB compact). Note the three namespaces:
Series in .../structure/compact/H41_H41 (prefix kf), Annotations and Obs
in .../structure/compact/common (prefix frb), but Annotation, AnnotationType
and AnnotationText in the SDMX standard namespace
http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common (prefix common):
    kf:Series  (ns .../structure/compact/H41_H41)
          common:AnnotationText: <text>   (identical for both, verified 2026-11-07)
      frb:Obs  (ns .../structure/compact/common)
          OBS_STATUS="A"|"NA", OBS_VALUE="10450" ("-999999" when NA),
          TIME_PERIOD="2026-11-04"

Quirks:
  * OBS_STATUS "NA" carries the sentinel -999999; emitted as value: null.
  * All values are in millions of dollars (UNIT_MULT 1000000, UNIT
    "Currency", CURRENCY "USD", FREQ "19" = weekly) — kept verbatim per
    record so a future unit change is visible in raw.
  * The Data Download Program (datadownload/Choose.aspx?rel=h41) is being
    retired by the Fed; this file on the release page is the stable path.

Stdlib only.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any, Iterator, Sequence

SOURCE = "fed_h41"
URL = "https://www.federalreserve.gov/releases/h41/data/FRB_h41_xml.zip"
DATA_MEMBER = "H41_data.xml"
DEFAULT_WEEKS = 13
MAX_WEEKS = 52
DEFAULT_TIMEOUT = 120
MAX_TIMEOUT = 300
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

NS_COMMON = "http://www.federalreserve.gov/structure/compact/common"
NS_SDMX_COMMON = "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common"
NS_H41 = "http://www.federalreserve.gov/structure/compact/H41_H41"
TAG_SERIES = f"{{{NS_H41}}}Series"
TAG_OBS = f"{{{NS_COMMON}}}Obs"
TAG_ANN_TYPE = f"{{{NS_SDMX_COMMON}}}AnnotationType"
TAG_ANN_TEXT = f"{{{NS_SDMX_COMMON}}}AnnotationText"
SERIES_KEYS = (
    ("series_name", "SERIES_NAME"),
    ("frequency", "FREQ"),
    ("category", "CATEGORY"),
    ("subcategory", "SUBCATEGORY"),
    ("component", "COMPONENT"),
    ("distribution", "DISTRIBUTION"),
    ("series_type", "SERIESTYPE"),
    ("unit", "UNIT"),
    ("unit_multiplier", "UNIT_MULT"),
    ("currency", "CURRENCY"),
)


def download_zip(timeout: float = DEFAULT_TIMEOUT) -> bytes:
    """Fetch the H.4.1 release zip in one bounded request."""
    req = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _read_data_member(zip_bytes: bytes) -> bytes:
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        return zf.read(DATA_MEMBER)


def _iter_series(xml_bytes: bytes) -> Iterator[dict[str, Any]]:
    """Yield one dict per kf:Series: meta fields, description, and its Obs list."""
    current: dict[str, Any] | None = None
    for _event, el in ET.iterparse(BytesIO(xml_bytes), events=("start", "end")):
        if _event == "start" and el.tag == TAG_SERIES:
            current = {"meta": dict(el.attrib), "description": "", "obs": []}
        elif _event == "end" and el.tag == TAG_ANN_TYPE and current is not None:
            if el.text and el.text.strip() == "Short Description":
                # The Long Description is identical; the next AnnotationText
                # after this marker is the one we keep.
                current["pending_ann"] = True
        elif _event == "end" and el.tag == TAG_ANN_TEXT and current is not None:
            if current.get("pending_ann"):
                current["description"] = (el.text or "").strip()
                current["pending_ann"] = False
        elif _event == "end" and el.tag == TAG_OBS and current is not None:
            current["obs"].append(dict(el.attrib))
        elif _event == "end" and el.tag == TAG_SERIES and current is not None:
            yield current
            current = None
            el.clear()


def _max_period(xml_bytes: bytes) -> str:
    """Newest TIME_PERIOD in the file; the window anchor."""
    latest = ""
    for series in _iter_series(xml_bytes):
        for obs in series["obs"]:
            period = obs.get("TIME_PERIOD", "")
            if period > latest:
                latest = period
    return latest


def _parse_value(raw: str) -> int | float | None:
    try:
        num = float(raw)
    except ValueError:
        return None
    return int(num) if num.is_integer() else num


def _parse_multiplier(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return raw


def normalize_observation(
    series: dict[str, Any], obs: dict[str, str], fetched_at: str
) -> dict[str, Any]:
    """Build one raw record from a series meta block and one Obs element."""
    period = obs.get("TIME_PERIOD", "")
    status = obs.get("OBS_STATUS", "")
    value = _parse_value(obs.get("OBS_VALUE", "")) if status == "A" else None
    record: dict[str, Any] = {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": f"{series['meta'].get('SERIES_NAME', '')}:{period}",
        "series_name": series["meta"].get("SERIES_NAME"),
        "description": series["description"],
        "date": period,
        "value": value,
        "obs_status": status,
    }
    for key, attr in SERIES_KEYS[1:]:
        raw = series["meta"].get(attr)
        record[key] = _parse_multiplier(raw) if key == "unit_multiplier" else raw
    return record


def parse_h41_xml(
    xml_bytes: bytes,
    weeks: int = DEFAULT_WEEKS,
    fetched_at: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield the observations of the last `weeks` weekly periods, newest data first.

    The window is anchored to the newest period present in the payload, so the
    same file always yields the same records regardless of the wall clock.
    """
    if not 1 <= weeks <= MAX_WEEKS:
        raise ValueError(f"weeks must be 1..{MAX_WEEKS}, got {weeks}")
    latest = _max_period(xml_bytes)
    if not latest:
        raise ValueError("no observations found in H.4.1 payload")
    cutoff = (datetime.strptime(latest, "%Y-%m-%d").date()
              - timedelta(weeks=weeks - 1)).isoformat()
    fetched_at = fetched_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Collect, then sort by period desc then series, so each run's output is
    # deterministic for a given payload.
    kept: list[dict[str, Any]] = []
    for series in _iter_series(xml_bytes):
        obs_list = [o for o in series["obs"] if o.get("TIME_PERIOD", "") >= cutoff]
        if not obs_list:
            continue
        for obs in obs_list:
            kept.append(normalize_observation(series, obs, fetched_at))
    kept.sort(key=lambda r: (r["date"], r["series_name"]), reverse=True)
    yield from kept


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weeks", type=int, default=DEFAULT_WEEKS,
                        help="how many trailing weekly periods to emit (1-52)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                        help=f"HTTP timeout seconds (max {MAX_TIMEOUT})")
    args = parser.parse_args(argv)
    timeout = min(max(args.timeout, 5.0), MAX_TIMEOUT)

    try:
        zip_bytes = download_zip(timeout)
        xml_bytes = _read_data_member(zip_bytes)
    except (urllib.error.URLError, TimeoutError, OSError, zipfile.BadZipFile, KeyError) as exc:
        print(f"fed_h41: download failed: {exc}", file=sys.stderr)
        raise SystemExit(2)

    try:
        records = list(parse_h41_xml(xml_bytes, weeks=args.weeks))
    except (ValueError, ET.ParseError) as exc:
        print(f"fed_h41: parse failed: {exc}", file=sys.stderr)
        raise SystemExit(2)

    if not records:
        print("fed_h41: no records in window (unexpected; upstream may have changed)",
              file=sys.stderr)
        raise SystemExit(2)

    latest = max(r["date"] for r in records)
    series_count = len({r["series_name"] for r in records})
    print(f"fed_h41: {len(records)} observations across {series_count} series, "
          f"latest period {latest}, fetched_at {records[0]['fetched_at']}", file=sys.stderr)
    for rec in records:
        print(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
