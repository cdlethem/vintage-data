#!/usr/bin/env python3
"""Snapshot Frankfurter's ECB daily foreign-exchange reference rates as NDJSON.

Each run makes one bounded request to the public, keyless Frankfurter API
scoped to the European Central Bank provider: USD reference rates for the
current 25-currency ECB basket over the seven complete UTC calendar dates
before the run. The API snaps from/to to the last ECB business day on or
before each bound, and ECB publishes one rate set per business day, so the
window covers weekends and late publication without ever making more than
one request. Records carry the stable ``date:base:quote`` id plus the rate,
so revisions appear as new raw rows.

Stdlib only.
"""

import argparse
import json
import math
import os
from datetime import date, datetime, timedelta, timezone
import re
import sys
import time
from typing import Any, Iterator, Sequence
import urllib.request
import urllib.error
import urllib.parse


SOURCE = "frankfurter_fx"
BASE_URL = "https://api.frankfurter.dev/v2/providers/ecb/rates"
DEFAULT_BASE = "USD"
DEFAULT_QUOTES = (
    "AUD,BRL,CAD,CHF,CNY,CZK,EUR,GBP,HUF,IDR,INR,JPY,KRW,MXN,MYR,NOK,NZD,"
    "PHP,PLN,RON,SEK,SGD,THB,TRY,ZAR"
)
DEFAULT_LOOKBACK_DAYS = 7
MAX_LOOKBACK_DAYS = 31
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 120
DEFAULT_RETRIES = 2
MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 10
MAX_RETRY_DELAY = 300
CURRENCY_CODE_PATTERN = re.compile(r"^[A-Z]{3}$")
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


def parse_quotes(value: str) -> tuple[str, ...]:
    """Split a comma-separated quote list, rejecting empty and malformed codes."""
    quotes: list[str] = []
    for part in value.split(","):
        code = part.strip()
        if not CURRENCY_CODE_PATTERN.match(code):
            raise ValueError(
                f"quote code {code!r} is not a three-letter uppercase currency code"
            )
        if code in quotes:
            raise ValueError(f"quote code {code!r} is repeated")
        quotes.append(code)
    if not quotes:
        raise ValueError("quotes must name at least one currency")
    return tuple(quotes)


def rate_window(today: date, lookback_days: int) -> tuple[date, date]:
    """Return the inclusive (start, end) window of complete UTC dates before today."""
    if (
        isinstance(lookback_days, bool)
        or not isinstance(lookback_days, int)
        or not 1 <= lookback_days <= MAX_LOOKBACK_DAYS
    ):
        raise ValueError(f"lookback_days must be between 1 and {MAX_LOOKBACK_DAYS}")
    end = today - timedelta(days=1)
    start = end - timedelta(days=lookback_days - 1)
    return start, end


def build_url(base: str, quotes: tuple[str, ...], start: date, end: date) -> str:
    params = {
        "base": base,
        "quotes": ",".join(quotes),
        "from": start.isoformat(),
        "to": end.isoformat(),
    }
    return f"{BASE_URL}?{urllib.parse.urlencode(params)}"


def normalize_rate(row: Any, fetched_at: str) -> dict[str, Any]:
    """Validate one Frankfurter rate row without changing source values."""
    if not isinstance(row, dict):
        raise ValueError("Frankfurter response contains a non-object rate row")

    row_date = row.get("date")
    if not isinstance(row_date, str) or not re.match(r"^\d{4}-\d{2}-\d{2}$", row_date):
        raise ValueError("Frankfurter rate row is missing its ISO date")

    base = row.get("base")
    if not isinstance(base, str) or not CURRENCY_CODE_PATTERN.match(base):
        raise ValueError(f"Frankfurter rate row has an invalid base currency {base!r}")

    quote = row.get("quote")
    if not isinstance(quote, str) or not CURRENCY_CODE_PATTERN.match(quote):
        raise ValueError(
            f"Frankfurter rate row has an invalid quote currency {quote!r}"
        )

    rate = row.get("rate")
    if not _is_finite_number(rate) or rate <= 0:
        raise ValueError(
            f"Frankfurter rate row {row_date}:{base}:{quote} has a missing "
            "or non-positive rate"
        )

    return {
        "source": SOURCE,
        "id": f"{row_date}:{base}:{quote}",
        "fetched_at": fetched_at,
        "date": row_date,
        "base": base,
        "quote": quote,
        "rate": rate,
    }


def fetch_rates(
    base: str = DEFAULT_BASE,
    quotes: tuple[str, ...] = parse_quotes(DEFAULT_QUOTES),
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    today: date | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
) -> Iterator[dict[str, Any]]:
    """Fetch one bounded ECB rate window, retrying only transient HTTP failures."""
    if not isinstance(base, str) or not CURRENCY_CODE_PATTERN.match(base):
        raise ValueError(
            f"base must be a three-letter uppercase currency code, got {base!r}"
        )
    if not isinstance(quotes, tuple) or not quotes:
        raise ValueError("quotes must be a non-empty tuple of currency codes")
    for code in quotes:
        if not isinstance(code, str) or not CURRENCY_CODE_PATTERN.match(code):
            raise ValueError(f"quote code {code!r} is not a three-letter uppercase code")
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

    reference = today or date.today()
    start, end = rate_window(reference, lookback_days)
    url = build_url(base, quotes, start, end)
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
                f"Frankfurter request failed with HTTP {exc.code}; "
                f"retry {attempt + 1}/{retries} in {retry_delay:g}s",
                file=sys.stderr,
            )
            time.sleep(retry_delay)

    if last_error is not None:
        raise RuntimeError(
            f"Frankfurter FX request failed: HTTP {last_error.code} "
            f"{last_error.reason!r} after {retries + 1} attempt(s)"
        ) from last_error

    if not isinstance(document, list):
        raise ValueError("Frankfurter response is not a list of rate rows")

    for row in document:
        yield normalize_rate(row, fetched_at)




def _run_summary(
    records: list[dict[str, Any]], base: str, quotes: tuple[str, ...]
) -> None:
    payload = {
        "rows": len(records),
        "base": base,
        "quotes": list(quotes),
        "first_id": records[0]["id"] if records else None,
        "last_id": records[-1]["id"] if records else None,
    }
    print(SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")), file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default=DEFAULT_BASE)
    parser.add_argument("--quotes", default=DEFAULT_QUOTES)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY)
    args = parser.parse_args(argv)

    quotes = parse_quotes(args.quotes)
    records = list(
        fetch_rates(
            base=args.base,
            quotes=quotes,
            lookback_days=args.lookback_days,
            timeout=args.timeout,
            retries=args.retries,
            retry_delay=args.retry_delay,
        )
    )
    for record in records:
        print(json.dumps(record, ensure_ascii=False))
    _run_summary(records, args.base, quotes)


if __name__ == "__main__":
    main()
