#!/usr/bin/env python3
"""US EIA RTO / Form EIA-930 — hourly net generation by balancing authority and fuel.

Each row is one (hour, balancing authority, fuel type) with the reported net
generation in megawatthours — how much of each fuel (natural gas, wind, solar,
coal, nuclear, ...) every one of the ~110 US balancing authorities burned or
produced each hour. The fuel-mix time series is the headline series; per-BA
rows give the regional resolution.

Endpoint: `GET https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/`
with `api_key=DEMO_KEY` — EIA's published, registration-free public key.

**Verified live 2026-10-03 (UTC)**:
  * Latest-period probe (sort period desc, length 1) — **200**, latest period
    `2026-10-02T06`, i.e. the series trails real time by ~23.5 hours. EIA
    publishes EIA-930 operational data once per day with a 1-2 day reporting
    lag, so the newest finalized hour advances ~24h per day — a daily poll is
    the honest cadence, and each poll collects exactly the hours published
    since the last one.
  * Range fetch `start=2026-10-01T00&end=2026-10-01T02` — **200, total 1431
    rows** (~477/hour across all BAs), rows carry
    `period, respondent, respondent-name, fueltype, type-name, value,
    value-units`; `value` is a numeric **string** in JSON.
  * Full-history total is ~27.5 million hourly rows (2022-01 onward), so the
    fetcher is strictly incremental: a watermark on the last collected hour
    plus a lookback of completed hours for the first run.
  * **The DEMO_KEY quota is tiny and shared**: responses carry
    `x-ratelimit-limit: 10` with a 429 `retry-after` that runs to the next
    UTC midnight — i.e. ~10 requests per UTC day, shared with every other
    DEMO_KEY user. A normal day needs 4 (one probe + 3 pages of a day's
    ~11.5k rows).

Quirks that will cost someone an afternoon:
  * **JSON responses are capped at 5000 rows** — the response carries an
    "incomplete return" warning, not an error, so the fetcher paginates with
    `offset` and cross-checks `total` on every page.
  * **`data[]` only accepts `value`** — requesting `data[]=period` and friends
    is a 400; facet fields ride along in each row unconditionally.
  * **Values and `total` are strings** ("375", "10695") in JSON, and values
    can be revised in place later (a balancing authority resubmits). The
    watermark therefore never re-fetches an already-collected hour: this
    source records the series as published at first observation, not the
    latest revision.
  * `frequency` is `hourly` for this route; `daily`/`quarterly`/`annual` are
    rejected with a 400 naming the valid values.

Rate-limit behaviour (matches the shared quota above): a 429 is terminal for
the run — in-process retries would only burn more of the shared budget and
the server's retry-after runs to hours, so 5xx/network errors are the only
retried class (bounded backoff). When a 429 lands mid-catch-up, after at
least one page succeeded, the watermark advances to the last completed page
and the run completes (exit 0, health "degraded", completeness "partial") so
the next scheduled run resumes where this one stopped; a multi-day backlog
therefore drains a few pages per day instead of failing forever.

Stdlib only.
"""
import argparse
import json
import os
import pathlib
import re
import sys
import time as _time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

SOURCE = "eia_electricity_rto"
ENDPOINT = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
API_KEY = "DEMO_KEY"  # EIA's published registration-free public key
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
DEFAULT_LOOKBACK_DAYS = 2  # first run = 1 probe + 6 pages = 7 of the ~10 shared daily requests
MAX_LOOKBACK_DAYS = 14  # longer backlogs drain a few pages per run via the watermark
DEFAULT_PAGE_SIZE = 5000
MAX_PAGE_SIZE = 5000  # EIA's JSON response cap
MAX_PAGES_PER_RUN = 60
MAX_RETRIES = 3
RETRY_BACKOFF_S = (2.0, 5.0, 10.0)
MIN_REQUEST_INTERVAL = 1.5  # modest courtesy pacing; the daily quota, not burst rate, is the binding limit
DEFAULT_TIMEOUT = 60
STATE_VERSION = 1
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
PERIOD_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}\Z")
NUMBER_PATTERN = re.compile(r"[+-]?\d+(\.\d+)?")


def parse_period(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not PERIOD_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a YYYY-MM-DDTHH hour, got {value!r}")
    return datetime.strptime(value, "%Y-%m-%dT%H")


def period_text(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H")


def to_number(value: object, field: str) -> int | float:
    """EIA JSON carries numerics as strings; accept the plain numeric forms."""
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric, got {value!r}")
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str) and NUMBER_PATTERN.fullmatch(value.strip()):
        text = value.strip()
        return float(text) if "." in text else int(text)
    raise ValueError(f"{field} must be numeric, got {value!r}")


def default_state() -> dict:
    return {"version": STATE_VERSION, "source": SOURCE, "last_collected_period": None}


def state_path(explicit: str | None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def validate_state(document: object, path: pathlib.Path) -> dict:
    if not isinstance(document, dict):
        raise ValueError(f"state {path} must be a JSON object")
    state = default_state()
    state.update(document)
    if state.get("version") != STATE_VERSION:
        raise ValueError(f"state {path} has unsupported version {state.get('version')!r}")
    raw = state.get("last_collected_period")
    if raw is not None:
        parse_period(raw, f"state last_collected_period in {path}")
    return state


def load_state(path: pathlib.Path) -> dict:
    try:
        with open(path, encoding="utf-8") as handle:
            document = json.load(handle)
    except FileNotFoundError:
        return default_state()
    except json.JSONDecodeError as error:
        raise ValueError(f"state {path} is not valid JSON: {error}") from error
    return validate_state(document, path)


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish state after stdout has accepted the complete output."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_suffix(path.suffix + ".tmp")
    with open(staged, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
    try:
        os.replace(staged, path)
    except BaseException:
        try:
            staged.unlink()
        except OSError:
            pass
        raise


def build_latest_url() -> str:
    params = {
        "api_key": API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 1,
    }
    return f"{ENDPOINT}?{urllib.parse.urlencode(params)}"


def build_range_url(start: datetime, end: datetime, page_size: int, offset: int) -> str:
    params = {
        "api_key": API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "start": period_text(start),
        "end": period_text(end),
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": page_size,
        "offset": offset,
    }
    return f"{ENDPOINT}?{urllib.parse.urlencode(params)}"


class RateLimited(RuntimeError):
    """EIA's shared rate limit was reached; the quota recovers with time.

    Retrying in-process would only burn more of the shared DEMO_KEY budget,
    so this is terminal for the current run.
    """

    def __init__(self, retry_after: float | None):
        self.retry_after = retry_after
        hint = f"; server asks for {int(retry_after)}s" if retry_after else ""
        super().__init__(f"EIA rate limit hit{hint}; will recover — do not retry faster")


def request_json(url: str, timeout: int, sleep=_time.sleep) -> object:
    """One GET; 429 is terminal (shared quota), 5xx/network retry with backoff."""
    retry = 0
    while True:
        request = urllib.request.Request(
            url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 429:
                raise RateLimited(_retry_after_seconds(error)) from error
            retryable = 500 <= error.code < 600
            if not retryable or retry >= MAX_RETRIES:
                raise
            retry += 1
            sleep(RETRY_BACKOFF_S[retry - 1])
        except urllib.error.URLError:
            if retry >= MAX_RETRIES:
                raise
            retry += 1
            sleep(RETRY_BACKOFF_S[retry - 1])


def _retry_after_seconds(error: urllib.error.HTTPError) -> float:
    raw = error.headers.get("Retry-After") if error.headers else None
    if raw is None:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


def validate_latest(document: object) -> datetime:
    response = document.get("response") if isinstance(document, dict) else None
    rows = response.get("data") if isinstance(response, dict) else None
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise ValueError("EIA latest-period probe did not return exactly one row")
    return parse_period(rows[0].get("period"), "latest period")


def validate_range_page(document: object, start: datetime, end: datetime) -> tuple[list[dict], int]:
    if not isinstance(document, dict) or not isinstance(document.get("response"), dict):
        raise ValueError("EIA range response missing response object")
    response = document["response"]
    total = to_number(response.get("total"), "response total")
    if not float(total).is_integer() or total < 0:
        raise ValueError(f"EIA range response total is not a non-negative integer")
    total = int(total)
    rows = response.get("data")
    if not isinstance(rows, list):
        raise ValueError("EIA range response missing data list")
    parsed = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"EIA row {index} is not an object")
        period = parse_period(row.get("period"), f"row {index} period")
        if not (start <= period <= end):
            raise ValueError(f"row {index} period {period_text(period)} outside requested range")
        respondent = row.get("respondent")
        fueltype = row.get("fueltype")
        if not isinstance(respondent, str) or not respondent:
            raise ValueError(f"row {index} has missing respondent")
        if not isinstance(fueltype, str) or not fueltype:
            raise ValueError(f"row {index} has missing fueltype")
        parsed.append({
            "period": period_text(period),
            "respondent": respondent,
            "respondent_name": row.get("respondent-name"),
            "fueltype": fueltype,
            "fueltype_name": row.get("type-name"),
            "value_mwh": to_number(row.get("value"), f"row {index} value"),
            "value_units": row.get("value-units"),
        })
    return parsed, total


def collect_range(
    start: datetime,
    end: datetime,
    *,
    page_size: int,
    max_pages: int,
    timeout: int,
    transport,
    counters: dict,
) -> Iterator[dict]:
    """Yield rows page by page so a mid-range failure keeps completed pages."""
    seen: set[str] = set()
    total: int | None = None
    offset = 0
    for _ in range(max_pages):
        url = build_range_url(start, end, page_size, offset)
        counters["attempted"] += 1
        document = transport(url, timeout)
        counters["succeeded"] += 1
        page_rows, page_total = validate_range_page(document, start, end)
        if total is None:
            total = page_total
        elif page_total != total:
            raise ValueError(f"EIA total changed mid-pagination: {total} -> {page_total}")
        if not page_rows:
            return
        for row in page_rows:
            row_id = f"{row['period']}|{row['respondent']}|{row['fueltype']}"
            if row_id in seen:
                raise ValueError(f"duplicate row {row_id} in EIA response")
            seen.add(row_id)
            yield row
        offset += len(page_rows)
        if offset >= total:
            return
    raise ValueError(f"EIA range {period_text(start)}..{period_text(end)} exceeds the {max_pages}-page cap")


def run(
    *,
    path: pathlib.Path | None,
    output,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    page_size: int = DEFAULT_PAGE_SIZE,
    timeout: int = DEFAULT_TIMEOUT,
    transport=request_json,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = _time.sleep,
    monotonic: Callable[[], float] = _time.monotonic,
) -> int:
    if (
        lookback_days <= 0
        or lookback_days > MAX_LOOKBACK_DAYS
        or page_size <= 0
        or page_size > MAX_PAGE_SIZE
        or timeout <= 0
    ):
        raise ValueError("lookback, page size, or timeout is outside its allowed bound")
    fetched_at = now()
    if fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must include a UTC offset")
    fetched_at = fetched_at.astimezone(timezone.utc)

    state = default_state() if path is None else load_state(path)
    counters = {"attempted": 0, "succeeded": 0}

    # The binding limit is the daily shared quota, not burst rate; this is
    # only courtesy spacing between the handful of requests per run.
    monotonic_clock = monotonic
    next_allowed = [0.0]

    def paced_transport(url: str, timeout_: int) -> object:
        wait = next_allowed[0] - monotonic_clock()
        if wait > 0:
            sleep(wait)
        next_allowed[0] = monotonic_clock() + MIN_REQUEST_INTERVAL
        return transport(url, timeout_)

    counters["attempted"] += 1
    latest = validate_latest(paced_transport(build_latest_url(), timeout))
    counters["succeeded"] += 1

    last_raw = state.get("last_collected_period")
    last = parse_period(last_raw, "state last_collected_period") if last_raw else None
    if last is not None:
        start = last + timedelta(hours=1)
    else:
        start = latest - timedelta(days=lookback_days) + timedelta(hours=1)

    records: list[dict] = []
    watermark: datetime | None = None
    health = "healthy"
    completeness = "complete"
    if start <= latest:
        try:
            for row in collect_range(
                start,
                latest,
                page_size=page_size,
                max_pages=MAX_PAGES_PER_RUN,
                timeout=timeout,
                transport=paced_transport,
                counters=counters,
            ):
                records.append({
                    "source": SOURCE,
                    "fetched_at": fetched_at.isoformat().replace("+00:00", "Z"),
                    "id": f"{row['period']}|{row['respondent']}|{row['fueltype']}",
                    **row,
                })
                watermark = parse_period(row["period"], "row period")
        except RateLimited:
            if not records:
                raise
            # Quota exhausted mid-catch-up: keep the completed pages; the next
            # run resumes from the saved watermark.
            health = "degraded"
            completeness = "partial"

    payload = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records
    )
    if payload:
        written = output.write(payload)
        if written is not None and written != len(payload):
            raise OSError("short write to NDJSON output")
    output.flush()

    state_change: dict = {}
    if path is not None and watermark is not None:
        state["last_collected_period"] = period_text(watermark)
        state_change["last_collected_period"] = period_text(watermark)
        save_state(path, state)

    summary = {
        "health": health,
        "completeness": completeness,
        "requests": counters,
        "state_change": state_change,
        "metrics": {
            "records": len(records),
            "period_start": period_text(start) if records else None,
            "period_end": period_text(watermark) if watermark is not None else None,
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    return len(records)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="do not read or update persistent state (one-off pull)")
    parser.add_argument(
        "--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
        help=f"initial-run lookback in hours, 1-{MAX_LOOKBACK_DAYS} days (default {DEFAULT_LOOKBACK_DAYS})",
    )
    parser.add_argument(
        "--page-size", type=int, default=DEFAULT_PAGE_SIZE,
        help=f"page size, 1-{MAX_PAGE_SIZE} (default {DEFAULT_PAGE_SIZE}; EIA caps JSON at 5000)",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    arguments = parser.parse_args()
    if arguments.no_state and arguments.state_file:
        parser.error("--no-state and --state-file are mutually exclusive")
    try:
        count = run(
            path=None if arguments.no_state else state_path(arguments.state_file),
            output=sys.stdout,
            lookback_days=arguments.lookback_days,
            page_size=arguments.page_size,
            timeout=arguments.timeout,
        )
    except ValueError as error:
        parser.error(str(error))
    except RateLimited as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(f"Collected {count} EIA RTO generation rows", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
