#!/usr/bin/env python3
"""Snapshot active National Weather Service alerts as NDJSON.

The NWS alerts endpoint returns a GeoJSON FeatureCollection and may advertise the next
collection page in ``pagination.next``. A run follows those links, gives every emitted
alert the same UTC fetch timestamp, and preserves both useful top-level fields and the
original feature payload.

Stdlib only.
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator

SOURCE = "nws_active_alerts"
URL = "https://api.weather.gov/alerts/active"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)


def normalize_feature(feature: Any, fetched_at: str) -> dict[str, Any]:
    """Validate and normalize one NWS GeoJSON alert feature."""
    if not isinstance(feature, dict):
        raise ValueError("NWS alert feature is not an object")

    alert_id = feature.get("id")
    if not isinstance(alert_id, str) or not alert_id:
        raise ValueError("NWS alert feature is missing its source id")

    properties = feature.get("properties")
    if not isinstance(properties, dict):
        raise ValueError(f"NWS alert {alert_id!r} has invalid properties")

    geometry = feature.get("geometry")
    if geometry is not None and not isinstance(geometry, dict):
        raise ValueError(f"NWS alert {alert_id!r} has invalid geometry")

    return {
        "source": SOURCE,
        "id": alert_id,
        "fetched_at": fetched_at,
        "properties": properties,
        "geometry": geometry,
        "feature": feature,
    }


def _next_url(document: dict[str, Any]) -> str | None:
    pagination = document.get("pagination")
    if pagination is None:
        return None
    if not isinstance(pagination, dict):
        raise ValueError("NWS alert collection has invalid pagination metadata")

    next_url = pagination.get("next")
    if next_url is None:
        return None
    if not isinstance(next_url, str) or not next_url:
        raise ValueError("NWS alert collection has an invalid next-page URL")

    parsed = urllib.parse.urlsplit(next_url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "api.weather.gov"
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("NWS alert next-page URL is outside api.weather.gov")
    return next_url


def _get_collection(url: str, timeout: int) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/geo+json",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    if not isinstance(document, dict):
        raise ValueError("NWS response is not an object")
    if document.get("type") != "FeatureCollection":
        raise ValueError("NWS response is not a GeoJSON FeatureCollection")
    if not isinstance(document.get("features"), list):
        raise ValueError("NWS alert collection is missing features")
    return document


def fetch_alerts(timeout: int = 60, max_pages: int = 100) -> Iterator[dict[str, Any]]:
    """Fetch all advertised active-alert pages within the explicit request bound."""
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if max_pages <= 0:
        raise ValueError("max_pages must be positive")

    fetched_at = datetime.now(timezone.utc).isoformat()
    url: str | None = URL
    seen_urls: set[str] = set()

    for page_number in range(max_pages):
        if url in seen_urls:
            raise ValueError("NWS alert pagination contains a cycle")
        seen_urls.add(url)

        document = _get_collection(url, timeout)
        for feature in document["features"]:
            yield normalize_feature(feature, fetched_at)

        url = _next_url(document)
        if url is None:
            return
        if page_number + 1 == max_pages:
            raise RuntimeError(f"NWS alert pagination exceeded max_pages={max_pages}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args()

    for record in fetch_alerts(timeout=args.timeout, max_pages=args.max_pages):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
