#!/usr/bin/env python3
"""Polymarket — prediction-market events and implied probabilities (keyless).

The world's largest prediction market: every market is a tradable question
with outcome prices between 0 and 1 that move continuously as people trade.
Each market eventually RESOLVES to a real-world outcome, so daily sampling
yields a self-labeling probability time series (implied probability vs.
actual result) that no other source in this catalog provides at this scale.

Two slices per run, both bounded:
  1. active  — open events with 24h trading volume >= --min-volume, ordered
     by 24h volume descending (the questions people are trading right now,
     with live outcome prices and top of book).
  2. closed  — events with lifetime volume >= --min-volume whose end date
     falls within a 48h window around now (the resolutions: which outcome
     won, final price, final volume). The 48h window tolerates late
     resolutions (sports games resolve hours after the final whistle); a
     closed event can therefore appear in two consecutive runs. RAW is
     insert-only, so the duplicate rows carry identical final values.

The long tail (thousands of tiny daily sports games) is deliberately excluded
by the volume floor: it is ~95% of all events, churns daily, and adds little
analytical value.

Verified live 2026-10-06 against the Gamma API:
  * GET /events/keyset?limit=100&closed=false&order=volume24hr&ascending=false
    — active events ordered by 24h volume; ~350 events above $10k 24h volume.
  * GET /events/keyset?limit=100&closed=true&volume_min=10000&end_date_min=..&end_date_max=..
    — ~700 resolutions per 24h above the $10k lifetime-volume floor.
  * Response envelope: {"events": [...], "next_cursor": "..."}; page size is
    capped at 100 by the API; the legacy offset endpoint (/events) returns
    `Warning: 299 use /events/keyset`, so only the keyset endpoint is used.

Quirks:
  * `outcomes` and `outcomePrices` arrive as JSON-encoded strings
    ('"[\"Yes\", \"No\"]"') — parsed to real lists here.
  * `volume_min`/`volume_max` filter LIFETIME volume, not 24h volume; the
    active slice's 24h floor is applied client-side after the volume24hr sort.
  * No documented rate limit for the public Gamma API (Cloudflare-fronted);
    a daily run makes ~40-60 requests in under a minute.
  * No explicit data licence is published; the public API is keyless and the
    market data mirrors on-chain (Polygon) trading records.

Stdlib only.
"""

import argparse
import json
import os
import sys
import time as _time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

SOURCE = "polymarket_events"
BASE = "https://gamma-api.polymarket.com"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
PAGE_SIZE = 100  # Gamma caps `limit` at 100


def _get_json(url: str, timeout: int, retries: int = 2, retry_delay: float = 15.0) -> dict:
    """GET one page, retrying only on transient (429/5xx) HTTP failures."""
    for attempt in range(retries + 1):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code in RETRIABLE_STATUSES and attempt < retries:
                _time.sleep(retry_delay)
                continue
            raise


def _to_float(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _parse_json_list(value):
    """`outcomes`/`outcomePrices` arrive JSON-encoded inside strings."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, list) else None
    return None


def normalize_market(market) -> dict:
    """Slim a market to the analytically useful fields."""
    if not isinstance(market, dict):
        raise TypeError("Polymarket market is not an object")
    if market.get("id") is None:
        raise ValueError("Polymarket market is missing id")
    prices = _parse_json_list(market.get("outcomePrices"))
    if prices is not None:
        prices = [_to_float(p) for p in prices]
    return {
        "id": str(market["id"]),
        "question": market.get("question"),
        "slug": market.get("slug"),
        "conditionId": market.get("conditionId"),
        "outcomes": _parse_json_list(market.get("outcomes")),
        "outcomePrices": prices,
        "bestBid": _to_float(market.get("bestBid")),
        "bestAsk": _to_float(market.get("bestAsk")),
        "spread": _to_float(market.get("spread")),
        "lastTradePrice": _to_float(market.get("lastTradePrice")),
        "liquidity": _to_float(market.get("liquidity")),
        "volume": _to_float(market.get("volume")),
        "oneDayPriceChange": _to_float(market.get("oneDayPriceChange")),
        "oneWeekPriceChange": _to_float(market.get("oneWeekPriceChange")),
        "closed": market.get("closed"),
        "closedTime": market.get("closedTime"),
        "umaResolutionStatus": market.get("umaResolutionStatus"),
        "startDate": market.get("startDate"),
        "endDate": market.get("endDate"),
    }


def normalize_event(event, fetched_at: str, slice_name: str) -> dict:
    """Validate and slim one event, stamping the shared run timestamp."""
    if not isinstance(event, dict):
        raise TypeError("Polymarket event is not an object")
    if event.get("id") is None:
        raise ValueError("Polymarket event is missing id")
    markets = event.get("markets")
    if not isinstance(markets, list):
        raise TypeError("Polymarket event is missing markets list")
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": str(event["id"]),
        "slice": slice_name,
        "ticker": event.get("ticker"),
        "slug": event.get("slug"),
        "title": event.get("title"),
        "startDate": event.get("startDate"),
        "endDate": event.get("endDate"),
        "creationDate": event.get("creationDate"),
        "updatedAt": event.get("updatedAt"),
        "closed": event.get("closed"),
        "negRisk": event.get("negRisk"),
        "volume24hr": _to_float(event.get("volume24hr")),
        "volume1wk": _to_float(event.get("volume1wk")),
        "volume1mo": _to_float(event.get("volume1mo")),
        "liquidity": _to_float(event.get("liquidity")),
        "markets": [normalize_market(m) for m in markets],
    }


def _page_url(params: dict) -> str:
    return f"{BASE}/events/keyset?{urllib.parse.urlencode(params)}"


def _iter_slice(params: dict, min_volume: float, max_pages: int, fetch_page,
                fetched_at: str, slice_name: str):
    """Walk one keyset-paginated slice, yielding normalized events."""
    cursor: str | None = None
    seen_cursors: set[str] = set()
    for page in range(1, max_pages + 1):
        document = fetch_page(_page_url(params))
        events = document.get("events")
        if not isinstance(events, list):
            raise TypeError("Polymarket response is missing events list")
        for event in events:
            record = normalize_event(event, fetched_at, slice_name)
            # The active slice is ordered by volume24hr descending, so the
            # first sub-floor event ends the slice (null means no 24h volume).
            if slice_name == "active" and (record["volume24hr"] or 0.0) < min_volume:
                return
            yield record
        cursor = document.get("next_cursor")
        if not cursor:
            return
        if cursor in seen_cursors:
            raise RuntimeError("Polymarket keyset pagination repeated a cursor")
        seen_cursors.add(cursor)
        params = {**params, "after_cursor": cursor}
    raise RuntimeError(f"Polymarket {slice_name} pagination exceeded max_pages={max_pages}")


def fetch_events(min_volume: float = 10000.0, window_hours: int = 24, max_pages: int = 50,
                 timeout: int = 30, retries: int = 2, retry_delay: float = 15.0,
                 now: datetime | None = None, fetch_page=None):
    """Yield active (trading now) then recently closed (resolved) events."""
    if max_pages <= 0:
        raise ValueError("max_pages must be positive")
    if min_volume < 0:
        raise ValueError("min_volume must be non-negative")
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if fetch_page is None:
        fetch_page = lambda url: _get_json(url, timeout, retries, retry_delay)
    if now is None:
        now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()
    window = timedelta(hours=window_hours)
    active_params = {
        "limit": PAGE_SIZE,
        "closed": "false",
        "order": "volume24hr",
        "ascending": "false",
    }
    closed_params = {
        "limit": PAGE_SIZE,
        "closed": "true",
        "volume_min": min_volume,
        # 48h window: tolerate resolutions that land hours after the end date
        # without double-counting events more than once per run.
        "end_date_min": (now - window).isoformat().replace("+00:00", "Z"),
        "end_date_max": (now + window).isoformat().replace("+00:00", "Z"),
        "order": "endDate",
        "ascending": "true",
    }
    yield from _iter_slice(active_params, min_volume, max_pages, fetch_page,
                           fetched_at, "active")
    yield from _iter_slice(closed_params, min_volume, max_pages, fetch_page,
                           fetched_at, "closed")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-volume", type=float, default=10000.0,
                        help="USD volume floor: 24h volume for active events, lifetime volume for closed events")
    parser.add_argument("--window-hours", type=int, default=24,
                        help="half-width of the closed-events end-date window")
    parser.add_argument("--max-pages", type=int, default=50,
                        help="per-slice page bound (100 events per page)")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=2,
                        help="extra attempts per request on 429/5xx")
    parser.add_argument("--retry-delay", type=float, default=15.0)
    args = parser.parse_args(argv)

    counts = {"active": 0, "closed": 0, "markets": 0}
    requests = 0

    def fetch_page(url: str) -> dict:
        nonlocal requests
        requests += 1
        return _get_json(url, args.timeout, args.retries, args.retry_delay)

    for record in fetch_events(
        min_volume=args.min_volume,
        window_hours=args.window_hours,
        max_pages=args.max_pages,
        fetch_page=fetch_page,
    ):
        counts[record["slice"]] += 1
        counts["markets"] += len(record["markets"])
        print(json.dumps(record, ensure_ascii=False))

    summary = {
        "events": counts["active"] + counts["closed"],
        "events_active": counts["active"],
        "events_closed": counts["closed"],
        "markets": counts["markets"],
        "requests": requests,
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)


if __name__ == "__main__":
    main()
