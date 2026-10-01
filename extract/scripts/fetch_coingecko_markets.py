#!/usr/bin/env python3
"""Snapshot CoinGecko's top coins by market cap as NDJSON.

Each run makes one bounded request for the top ``per_page`` coins ranked by
market cap, in the configured quote currency. Records carry the stable
CoinGecko coin id plus the market fields the snapshot returns (price,
market cap, volume, 24h change, supply, all-time high/low). Free-tier rate
limits make one call per poll the right size; the free tier also caps total
monthly calls, so poll at most hourly.

Stdlib only.
"""

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence
import urllib.error
import urllib.parse
import urllib.request


SOURCE = "coingecko_markets"
BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
DEFAULT_PER_PAGE = 250
MAX_PER_PAGE = 250
DEFAULT_RETRIES = 2
MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 20
MAX_RETRY_DELAY = 300
CURRENCY_PATTERN = re.compile(r"^[a-z0-9]{2,15}$")
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


def _is_finite_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    return math.isfinite(value)


def normalize_coin(coin: Any, fetched_at: str, vs_currency: str) -> dict[str, Any]:
    """Validate one CoinGecko market row without changing source values."""
    if not isinstance(coin, dict):
        raise ValueError("CoinGecko response contains a non-object market row")

    coin_id = coin.get("id")
    if not isinstance(coin_id, str) or not coin_id:
        raise ValueError("CoinGecko market row is missing its stable coin id")

    price = coin.get("current_price")
    if not _is_finite_number(price):
        raise ValueError(
            f"CoinGecko coin {coin_id!r} has a missing or non-numeric price"
        )

    for field in (
        "market_cap",
        "total_volume",
        "price_change_24h",
        "price_change_percentage_24h",
        "circulating_supply",
        "total_supply",
        "max_supply",
        "ath",
        "ath_change_percentage",
        "atl",
        "atl_change_percentage",
    ):
        value = coin.get(field)
        if value is not None and not _is_finite_number(value):
            raise ValueError(
                f"CoinGecko coin {coin_id!r} has a non-numeric {field!r}"
            )

    return {
        "source": SOURCE,
        "id": coin_id,
        "fetched_at": fetched_at,
        "symbol": coin.get("symbol"),
        "name": coin.get("name"),
        "vs_currency": vs_currency,
        "market_cap_rank": coin.get("market_cap_rank"),
        "price": price,
        "market_cap": coin.get("market_cap"),
        "total_volume": coin.get("total_volume"),
        "price_change_24h": coin.get("price_change_24h"),
        "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
        "circulating_supply": coin.get("circulating_supply"),
        "total_supply": coin.get("total_supply"),
        "max_supply": coin.get("max_supply"),
        "ath": coin.get("ath"),
        "ath_date": coin.get("ath_date"),
        "ath_change_percentage": coin.get("ath_change_percentage"),
        "atl": coin.get("atl"),
        "atl_date": coin.get("atl_date"),
        "atl_change_percentage": coin.get("atl_change_percentage"),
        "last_updated": coin.get("last_updated"),
    }


def build_url(vs_currency: str, per_page: int) -> str:
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": str(per_page),
        "page": "1",
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    return f"{BASE_URL}?{urllib.parse.urlencode(params)}"


def fetch_markets(
    vs_currency: str = "usd",
    per_page: int = DEFAULT_PER_PAGE,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> Iterator[dict[str, Any]]:
    """Fetch one top-coins snapshot, retrying only transient HTTP failures."""
    if not isinstance(vs_currency, str) or not CURRENCY_PATTERN.match(vs_currency):
        raise ValueError(
            f"vs_currency must be 2-15 lowercase alphanumerics, got {vs_currency!r}"
        )
    if (
        isinstance(per_page, bool)
        or not isinstance(per_page, int)
        or not 1 <= per_page <= MAX_PER_PAGE
    ):
        raise ValueError(f"per_page must be between 1 and {MAX_PER_PAGE}")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or not 1 <= timeout <= MAX_TIMEOUT
    ):
        raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT} seconds")
    if (
        isinstance(retries, bool)
        or not isinstance(retries, int)
        or not 0 <= retries <= MAX_RETRIES
    ):
        raise ValueError(f"retries must be between 0 and {MAX_RETRIES}")
    if (
        isinstance(retry_delay, bool)
        or not isinstance(retry_delay, (int, float))
        or not math.isfinite(retry_delay)
        or not 0 <= retry_delay <= MAX_RETRY_DELAY
    ):
        raise ValueError(
            f"retry_delay must be a finite number between 0 and {MAX_RETRY_DELAY}"
        )

    url = build_url(vs_currency, per_page)
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    fetched_at = datetime.now(timezone.utc).isoformat()

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                document = json.load(response)
            last_error = None
            break
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in RETRIABLE_STATUSES or attempt == retries:
                break
            print(
                f"CoinGecko request failed with HTTP {exc.code}; "
                f"retry {attempt + 1}/{retries} in {retry_delay:g}s",
                file=sys.stderr,
            )
            time.sleep(retry_delay)

    if last_error is not None:
        raise RuntimeError(
            f"CoinGecko markets request failed: HTTP {last_error.code} "
            f"{last_error.reason!r} after {retries + 1} attempt(s)"
        ) from last_error

    if not isinstance(document, list):
        raise ValueError("CoinGecko response is not a list of market rows")

    for coin in document:
        yield normalize_coin(coin, fetched_at, vs_currency)


def _run_summary(records: list[dict[str, Any]], vs_currency: str) -> None:
    payload = {
        "rows": len(records),
        "vs_currency": vs_currency,
        "first_id": records[0]["id"] if records else None,
        "last_id": records[-1]["id"] if records else None,
    }
    print(SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")), file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vs-currency", default="usd")
    parser.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY)
    args = parser.parse_args(argv)

    records = list(
        fetch_markets(
            vs_currency=args.vs_currency,
            per_page=args.per_page,
            timeout=args.timeout,
            retries=args.retries,
            retry_delay=args.retry_delay,
        )
    )
    for record in records:
        print(json.dumps(record, ensure_ascii=False))
    _run_summary(records, args.vs_currency)


if __name__ == "__main__":
    main()
