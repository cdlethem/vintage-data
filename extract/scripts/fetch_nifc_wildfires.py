#!/usr/bin/env python3
"""Snapshot NIFC WFIGS current US wildfire incidents as NDJSON.

The National Interagency Fire Center (NIFC) publishes the "Incidents" layer of
its WFIGS (Wildfire Information Geospatial Information System) ArcGIS service.
It is a public, keyless FeatureServer holding every currently active US
wildland fire (category WF), prescribed fire (RX) and incident complex (CX),
with size, containment, cause, discovery time, jurisdiction and point
geometry. NIFC refreshes the layer continuously; a daily snapshot records how
each incident's size/containment evolves and captures new ignitions, so the
series is a running US wildfire log (incidents drop out of the layer once
closed — the same snapshot semantics as noaa_nhc_current_storms).

Live-verified 2026-10-15:
  * GET {LAYER}/query?where=1=1&f=json returned 430 features (259 WF, 170 RX,
    1 CX); every feature carried a unique IrwinID GUID; newest
    ModifiedOnDateTime_dt was within ~2 hours.
  * The service maxRecordCount is 1000 per query and the layer allows at most
    the first 2000 features (resultOffset + resultRecordCount), so the
    fetcher pages in 1000-row steps and reports completeness=partial if the
    layer ever exceeds 2000 incidents.

ArcGIS epoch values are milliseconds since the Unix epoch; they are converted
to UTC ISO-8601. The stable incident identity is IrwinID; OBJECTID is a
fallback only.
"""
import argparse
from datetime import datetime, timezone
import json
import math
import os
import sys
from typing import Any, Iterator, Sequence
import urllib.parse
import urllib.request

SOURCE = "nifc_wildfires"
QUERY_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "WFIGS_Incident_Locations_Current/FeatureServer/0/query"
)
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 300
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

PAGE_SIZE = 1000          # service maxRecordCount
MAX_FEATURES = 2000       # layer limit: resultOffset + resultRecordCount

# Source attribute name -> record field. Values are passed through verbatim
# (None stays None).
TEXT_FIELDS = (
    ("IncidentName", "incident_name"),
    ("UniqueFireIdentifier", "unique_fire_identifier"),
    ("IncidentTypeCategory", "incident_type_category"),
    ("IncidentTypeKind", "incident_type_kind"),
    ("IncidentComplexityLevel", "incident_complexity_level"),
    ("FireCause", "fire_cause"),
    ("POOState", "poo_state"),
    ("POOCounty", "poo_county"),
    ("POOCity", "poo_city"),
    ("POOProtectingAgency", "poo_protecting_agency"),
    ("DispatchCenterID", "dispatch_center_id"),
    ("GACC", "gacc"),
)
NUMBER_FIELDS = (
    ("IncidentSize", "incident_size_acres"),
    ("DiscoveryAcres", "discovery_acres"),
    ("PercentContained", "percent_contained"),
    ("InitialLatitude", "initial_latitude"),
    ("InitialLongitude", "initial_longitude"),
)
EPOCH_MS_FIELDS = (
    ("FireDiscoveryDateTime", "fire_discovery_date_time"),
    ("ModifiedOnDateTime_dt", "modified_date_time"),
)


def _iso_utc(epoch_ms: Any, field: str) -> str | None:
    """Convert an ArcGIS epoch-milliseconds value to ISO-8601 UTC seconds."""
    if epoch_ms is None:
        return None
    if isinstance(epoch_ms, bool) or not isinstance(epoch_ms, (int, float)):
        raise ValueError(f"{field} must be epoch milliseconds, got {epoch_ms!r}")
    if not math.isfinite(epoch_ms) or epoch_ms < 0:
        raise ValueError(f"{field} must be a non-negative finite number, got {epoch_ms!r}")
    parsed = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return parsed.isoformat(timespec="seconds")


def _number(value: Any, field: str) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{field} must be a finite number, got {value!r}")
    return value


def _text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string, got {value!r}")
    return value


def normalize_feature(feature: Any, fetched_at: str, feature_index: int) -> dict[str, Any]:
    """Validate one WFIGS feature and normalize it without altering source values."""
    if not isinstance(feature, dict):
        raise ValueError(f"feature #{feature_index} is not an object: {feature!r}")
    attributes = feature.get("attributes")
    if not isinstance(attributes, dict):
        raise ValueError(f"feature #{feature_index} is missing attributes: {feature!r}")
    geometry = feature.get("geometry")
    if not isinstance(geometry, dict):
        raise ValueError(f"feature #{feature_index} is missing point geometry: {feature!r}")

    record: dict[str, Any] = {"source": SOURCE, "fetched_at": fetched_at}

    irwin_id = _text(attributes.get("IrwinID"), "IrwinID")
    object_id = attributes.get("OBJECTID")
    if irwin_id is None:
        if isinstance(object_id, bool) or not isinstance(object_id, int):
            raise ValueError(
                f"feature #{feature_index} has neither IrwinID nor a numeric OBJECTID"
            )
        record["id"] = f"oid-{object_id}"
    else:
        record["id"] = irwin_id

    for attr, field in TEXT_FIELDS:
        record[field] = _text(attributes.get(attr), attr)
    for attr, field in NUMBER_FIELDS:
        record[field] = _number(attributes.get(attr), attr)
    for attr, field in EPOCH_MS_FIELDS:
        record[field] = _iso_utc(attributes.get(attr), attr)

    for axis, field in (("x", "lon"), ("y", "lat")):
        value = geometry.get(axis)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"feature #{feature_index} geometry {axis} is not finite: {value!r}")
        record[field] = value

    return record


def _query_params(page_offset: int, total_count: bool) -> dict[str, str]:
    params: dict[str, str] = {
        "where": "1=1",
        "f": "json",
        "outFields": "*",
        "returnGeometry": "true",
        "orderByFields": "OBJECTID",
    }
    if total_count:
        params["returnCountOnly"] = "true"
    else:
        params["resultOffset"] = str(page_offset)
        params["resultRecordCount"] = str(PAGE_SIZE)
    return params


def _query(url: str, params: dict[str, str], timeout: int) -> tuple[Any, int]:
    """Run one query; returns (document, response_bytes)."""
    request = urllib.request.Request(
        f"{url}?" + urllib.parse.urlencode(params),
        headers={"User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read()
    return json.loads(payload), len(payload)


class FetchStats:
    """Counters for the VINTAGE_RUN_SUMMARY line."""

    def __init__(self) -> None:
        self.requested = 0
        self.succeeded = 0
        self.payload_bytes = 0


def fetch_wildfires(timeout: int = DEFAULT_TIMEOUT) -> tuple[Iterator[dict[str, Any]], dict[str, Any]]:
    """Fetch the full current-incident snapshot.

    Returns (records, summary_dict). Records are deduplicated on the incident
    id; the summary carries completeness=partial when the layer exceeds the
    2000-feature ArcGIS query cap.
    """
    stats = FetchStats()

    def query(params: dict[str, str]) -> Any:
        stats.requested += 1
        document, size = _query(QUERY_URL, params, timeout)
        stats.succeeded += 1
        stats.payload_bytes += size
        return document

    count_document = query(_query_params(0, total_count=True))
    total = count_document.get("count")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise ValueError(f"layer reported an invalid count: {total!r}")

    truncated = total > MAX_FEATURES
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    seen: set[str] = set()
    duplicates = 0
    records: list[dict[str, Any]] = []

    offset = 0
    remaining = min(total, MAX_FEATURES)
    while remaining > 0:
        document = query(_query_params(offset, total_count=False))
        features = document.get("features")
        if not isinstance(features, list):
            raise ValueError(f"query page at offset {offset} is missing features")
        for index, feature in enumerate(features):
            record = normalize_feature(feature, fetched_at, offset + index)
            if record["id"] in seen:
                duplicates += 1
                continue
            seen.add(record["id"])
            records.append(record)
        fetched_now = len(features)
        if fetched_now == 0:
            break
        offset += fetched_now
        remaining -= fetched_now

    summary = {
        "health": "degraded" if truncated else "healthy",
        "completeness": "partial" if truncated else "complete",
        "requests": {
            "attempted": stats.requested,
            "succeeded": stats.succeeded,
            "payload_bytes": stats.payload_bytes,
        },
        "metrics": {
            "layer_total": total,
            "incidents": len(records),
            "duplicates_skipped": duplicates,
        },
    }
    return iter(records), summary


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help=f"per-request timeout in seconds (1..{MAX_TIMEOUT})")
    args = parser.parse_args(argv)
    if not 1 <= args.timeout <= MAX_TIMEOUT:
        raise ValueError(f"--timeout must be between 1 and {MAX_TIMEOUT}, got {args.timeout}")

    records, summary = fetch_wildfires(timeout=args.timeout)
    for record in records:
        print(json.dumps(record, ensure_ascii=False))
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":"), sort_keys=True),
          file=sys.stderr)


if __name__ == "__main__":
    main()
