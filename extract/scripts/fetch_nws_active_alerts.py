#!/usr/bin/env python3
"""National Weather Service active-alert GeoJSON snapshots.

The API returns a GeoJSON FeatureCollection. Alert feature IDs are the publisher's
canonical identifiers; retain the complete feature because both alert properties and
geometries can change while an alert remains active. Pagination links are supplied by
the collection response and followed only within the official API origin.

Stdlib only.
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "nws_active_alerts"
URL = "https://api.weather.gov/alerts/active"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
MAX_PAGES = 100


def _page_url(next_url: str, current_url: str) -> str:
    """Resolve and constrain a publisher-supplied pagination URL."""
    url = urllib.parse.urljoin(current_url, next_url)
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "api.weather.gov"
        or parsed.port not in (None, 443)
    ):
        raise ValueError("NWS pagination URL is outside https://api.weather.gov")
    return url


def _next_page(document: dict, current_url: str) -> str | None:
    pagination = document.get("pagination")
    if pagination is None:
        return None
    if not isinstance(pagination, dict):
        raise TypeError("NWS response has malformed pagination")
    next_url = pagination.get("next")
    if next_url is None:
        return None
    if not isinstance(next_url, str) or not next_url:
        raise TypeError("NWS response has malformed pagination.next")
    return _page_url(next_url, current_url)


def fetch_alerts(limit: int = 1000, timeout: int = 30):
    """Yield up to ``limit`` active-alert snapshot records."""
    if limit <= 0:
        return

    fetched_at = datetime.now(timezone.utc).isoformat()
    page_url = URL
    seen_urls = set()
    emitted = 0

    for _ in range(MAX_PAGES):
        if page_url in seen_urls:
            raise ValueError("NWS pagination repeated a page URL")
        seen_urls.add(page_url)

        request = urllib.request.Request(page_url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = json.load(response)
        if not isinstance(document, dict):
            raise TypeError("NWS response is not a GeoJSON FeatureCollection")
        features = document.get("features")
        if document.get("type") != "FeatureCollection" or not isinstance(features, list):
            raise ValueError("NWS response is not a GeoJSON FeatureCollection")

        for feature in features:
            if not isinstance(feature, dict):
                raise TypeError("NWS response contains a malformed feature")
            alert_id = feature.get("id")
            if not isinstance(alert_id, str) or not alert_id:
                raise ValueError("NWS alert feature is missing its ID")
            yield {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": alert_id,
                "properties": feature.get("properties"),
                "geometry": feature.get("geometry"),
                "feature": feature,
            }
            emitted += 1
            if emitted >= limit:
                return

        page_url = _next_page(document, page_url)
        if page_url is None:
            return

    raise ValueError(f"NWS pagination exceeded {MAX_PAGES} pages")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_alerts(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
