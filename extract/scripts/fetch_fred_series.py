#!/usr/bin/env python3
"""St. Louis Fed FRED — curated keyless panel of US macro series as NDJSON.

One row per (series, observation date): interest rates, Treasury yields,
inflation, labor, growth, commodities, FX, and equity indices. FRED's
fredgraph.csv endpoint is public and keyless; values are stamped as the Fed
publishes them (daily series appear 20:30-22:00 UTC, monthly releases land
mid-month, so the panel changes continuously at daily cadence).

Endpoint per series:
    https://fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES>&cosd=YYYY-MM-DD&cefd=YYYY-MM-DD
Header line is `observation_date,<SERIES>`; missing values are a literal `.`; not-yet-published periods appear as empty value cells, which FRED may include outside the requested window (observed live 2026-10-11).

Verified live 2026-10-11:
  * DFF (effective fed funds) last obs 2026-10-08; T10Y2Y 2026-10-09 — daily
    series lag real time by ~1 day, as expected.
  * UNRATE / FEDFUNDS last obs 2026-09-01 — monthly, mid-month release lag.
  * The endpoint hard-throttles sustained keyless traffic (a burst of ~12
    requests in a minute degraded to 0-byte hung reads for ~15 minutes), so
    runs are paced (default 3 s between series) and retries are bounded.

State: per-series watermark (last published, non-null observation_date) under
$EXTRACT_DATA_ROOT/state/fred_series.json. Each run re-requests at most
`--lookback-days` per series, so a failed series is retried on the next
scheduled run without re-emitting anything already delivered (RAW is
insert-only with no cross-run deduplication). A single bad series degrades
the run to partial; only a run in which every requested series fails exits
non-zero.

Stdlib only.
"""

import argparse
import json
import os
import pathlib
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Sequence

SOURCE = "fred_series"
BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

# Curated panel: (series_id, cadence). Keep this list small: the keyless
# endpoint throttles, and every entry costs one request per run.
SERIES: tuple[tuple[str, str], ...] = (
    # Daily: policy and money-market rates
    ("DFF", "daily"),          # effective federal funds rate
    ("SOFR", "daily"),         # secured overnight financing rate
    # Daily: Treasury constant maturities and the 10y-2y spread
    ("DGS3MO", "daily"),
    ("DGS2", "daily"),
    ("DGS10", "daily"),
    ("T10Y2Y", "daily"),
    # Daily: money, FX, equities, commodities, volatility
    ("M2SL", "daily"),         # M2 money stock (seasonally adjusted)
    ("DTWEXBGS", "daily"),     # trade-weighted dollar index (broad)
    ("DEXUSEU", "weekly"),     # euro/USD (BIS trade-weighted, Friday)
    ("VIXCLS", "daily"),       # CBOE VIX closing value
    ("DCOILWTICO", "daily"),   # WTI crude, $/bbl
    ("DCOILBRENTEU", "daily"),  # Brent crude, $/bbl
    ("DHHNGSP", "daily"),      # Henry Hub natural gas, $/mmBtu
    ("SP500", "daily"),
    ("NASDAQCOM", "daily"),    # Nasdaq Composite
    # Weekly: mortgages
    ("MORTGAGE30US", "weekly"),  # Freddie Mac 30-yr fixed, %
    # Monthly: inflation, labor, growth, housing, sentiment
    ("CPIAUCSL", "monthly"),
    ("CPIAUCNS", "monthly"),  # core CPI
    ("PCEPI", "monthly"),
    ("UNRATE", "monthly"),
    ("PAYEMS", "monthly"),
    ("FEDFUNDS", "monthly"),
    ("UMCSENT", "monthly"),
    ("HOUST", "monthly"),
    ("INDPRO", "monthly"),
)

DEFAULT_BOOTSTRAP_DAYS = 365
MAX_BOOTSTRAP_DAYS = 730
DEFAULT_LOOKBACK_DAYS = 14
MAX_LOOKBACK_DAYS = 90
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
DEFAULT_RETRIES = 3
MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 5.0
MAX_RETRY_DELAY = 60.0
DEFAULT_REQUEST_INTERVAL = 3.0
MAX_REQUEST_INTERVAL = 60.0

DATE_FORMAT = "%Y-%m-%d"
SleepFn = Callable[[float], None]


class FredError(RuntimeError):
    """A fatal, run-level contract violation (bad state, bad arguments)."""


class SeriesFetchError(FredError):
    """One series failed (bounded retries exhausted, bad response body)."""


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path | None) -> dict:
    if path is None or not path.exists():
        return {"version": STATE_VERSION, "watermarks": {}}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise FredError(f"unreadable state file {path}: {exc}") from exc
    watermarks = raw.get("watermarks") if isinstance(raw, dict) else None
    if not isinstance(watermarks, dict):
        raise FredError(f"state file {path} has no watermarks object")
    clean: dict[str, str] = {}
    for series_id, value in watermarks.items():
        if parse_observation_date(value) is None:
            raise FredError(
                f"state watermark for {series_id!r} is not a YYYY-MM-DD date: {value!r}"
            )
        clean[str(series_id)] = value
    return {"version": STATE_VERSION, "watermarks": clean}


def save_state(path: pathlib.Path | None, state: dict) -> None:
    """Publish watermarks atomically after output is complete."""
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".staged")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def parse_observation_date(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) != 10:
        return None
    try:
        return datetime.strptime(value, DATE_FORMAT).date().isoformat()
    except ValueError:
        return None


def parse_value(series_id: str, raw: str) -> float | None:
    raw = raw.strip()
    if raw == ".":
        return None  # FRED's missing-value placeholder
    try:
        value = float(raw)
    except ValueError as exc:
        raise SeriesFetchError(f"{series_id}: unparseable value {raw!r}") from exc
    if not (value == value and value not in (float("inf"), float("-inf"))):
        raise SeriesFetchError(f"{series_id}: non-finite value {raw!r}") from exc
    return value


def series_url(series_id: str, start: date, end: date) -> str:
    params = {
        "id": series_id,
        "cosd": start.strftime(DATE_FORMAT),
        "cefd": end.strftime(DATE_FORMAT),
    }
    return f"{BASE_URL}?{urllib.parse.urlencode(params)}"


def request_series(
    series_id: str,
    start: date,
    end: date,
    timeout: int,
    retries: int,
    retry_delay: float,
    sleep: SleepFn | None = None,
) -> str:
    """Fetch one bounded window, retrying only transient failures.

    Any failure that exhausts its bounded retries is a per-series failure,
    never a run-level one: one dead series must not lose the other 24.
    """
    sleep = sleep or time.sleep
    url = series_url(series_id, start, end)
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        if attempt:
            delay = min(retry_delay * (2 ** (attempt - 1)), MAX_RETRY_DELAY)
            sleep(delay * (0.8 + 0.4 * random.random()))
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
            return body.decode("utf-8")
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUSES:
                raise SeriesFetchError(
                    f"{series_id}: HTTP {exc.code} from {url}"
                ) from exc
            last_error = exc
        except (TimeoutError, OSError) as exc:
            last_error = exc
    raise SeriesFetchError(
        f"{series_id}: {retries + 1} attempts failed; last error: {last_error}"
    ) from last_error


def parse_series_csv(body: str, series_id: str) -> list[tuple[str, float | None]]:
    """Validate the window response; return (date, value) pairs ascending.

    Rows with empty value cells are not-yet-published periods (FRED returns them,
    sometimes outside the requested window, until the data publishes); they are
    dropped so no row is emitted and the watermark never advances past the last
    published observation. Raises SeriesFetchError on a mismatched header,
    malformed rows, or non-ascending dates.
    """
    lines = [line for line in body.splitlines() if line.strip()]
    if not lines:
        raise SeriesFetchError(f"{series_id}: empty response body")
    header = lines[0].strip()
    if header != f"observation_date,{series_id}":
        raise SeriesFetchError(f"{series_id}: unexpected header {header!r}")
    observations: list[tuple[str, float | None]] = []
    previous: str | None = None
    rows = lines[1:]
    for line in rows:
        cells = line.split(",")
        if len(cells) != 2:
            raise SeriesFetchError(f"{series_id}: malformed row {line!r}")
        observation_date = parse_observation_date(cells[0])
        if observation_date is None:
            raise SeriesFetchError(f"{series_id}: malformed observation date {cells[0]!r}")
        if previous is not None and observation_date <= previous:
            raise SeriesFetchError(
                f"{series_id}: observations not strictly ascending at {observation_date}"
            )
        previous = observation_date
        raw = cells[1].strip()
        if raw == "":
            continue # not-yet-published period; no row, no watermark advance
        observations.append((observation_date, parse_value(series_id, raw)))
    return observations


def fetch_series(
    series_ids: Sequence[str],
    today: date,
    bootstrap_days: int,
    lookback_days: int,
    state: dict,
    timeout: int,
    retries: int,
    retry_delay: float,
    request_interval: float,
    sleep: SleepFn | None = None,
    now: Callable[[], datetime] | None = None,
) -> tuple[list[dict], dict, int, list[dict]]:
    """Fetch new observations for every series; never re-emit a delivered row.

    Returns (records, updated_state, requests_attempted, series_failures).
    """
    sleep = sleep or time.sleep
    watermarks = dict(state["watermarks"])
    records: list[dict] = []
    failures: list[dict] = []
    current = now or (lambda: datetime.now(timezone.utc))
    if current().tzinfo is None:
        raise FredError("now must return a timezone-aware datetime")
    fetched_at = current().isoformat()
    requested = 0
    for index, series_id in enumerate(series_ids):
        if index:
            sleep(request_interval)
        watermark = watermarks.get(series_id)
        if watermark is None:
            start = today - timedelta(days=bootstrap_days)
        else:
            # Watermark is exclusive: re-request from the day after the last
            # delivered observation, bounded so a long outage does not turn
            # the next run into a bulk re-download.
            start = max(
                date.fromisoformat(watermark) + timedelta(days=1),
                today - timedelta(days=lookback_days),
            )
        if start > today:
            continue  # watermark already at today: nothing new is possible
        requested += 1
        try:
            body = request_series(
                series_id, start, today, timeout, retries, retry_delay, sleep
            )
            observations = parse_series_csv(body, series_id)
        except SeriesFetchError as exc:
            failures.append({"series": series_id, "error": str(exc)})
            continue
        start_iso, end_iso = start.isoformat(), today.isoformat()
        for observation_date, value in observations:
            if not start_iso <= observation_date <= end_iso:
                continue  # FRED pads monthly responses with out-of-window rows
            if watermark is not None and observation_date <= watermark:
                continue
            records.append(
                {
                    "source": SOURCE,
                    "id": f"{series_id}:{observation_date}",
                    "series_id": series_id,
                    "observation_date": observation_date,
                    "value": value,
                    "fetched_at": fetched_at,
                }
            )
        # Advance only to the last published (non-null) observation, so a
        # not-yet-published trailing period is re-requested next run.
        published = [
            observation_date
            for observation_date, value in observations
            if value is not None and start_iso <= observation_date <= end_iso
        ]
        if published:
            last = published[-1]
            if watermark is None or last > watermark:
                watermarks[series_id] = last
    updated_state = {"version": STATE_VERSION, "watermarks": watermarks}
    return records, updated_state, requested, failures


def run_summary(series_ids: Sequence[str], records: list[dict],
                failures: list[dict], requested: int, today: date) -> None:
    failed = len(failures)
    if requested and failed == requested:
        health, completeness = "failed", "failed"
    elif failed:
        health, completeness = "degraded", "partial"
    else:
        health, completeness = "healthy", "complete"
    payload = {
        "health": health,
        "completeness": completeness,
        "requests": {
            "attempted": requested,
            "succeeded": requested - failed,
            "failed": failed,
        },
        "metrics": {
            "series_total": len(series_ids),
            "records": len(records),
            "as_of": today.isoformat(),
            "failures": failures[:10],
        },
    }
    print(SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")), file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-days", type=int, default=DEFAULT_BOOTSTRAP_DAYS,
                        help="history depth for series with no watermark yet")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help="re-request window per series on stateful runs")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY)
    parser.add_argument("--request-interval", type=float, default=DEFAULT_REQUEST_INTERVAL,
                        help="pace between series requests (the endpoint throttles)")
    parser.add_argument("--state-file",
                        help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write watermarks (one-off pull)")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if args.no_state and args.state_file:
        raise SystemExit("error: --no-state and --state-file are mutually exclusive")
    if not 1 <= args.bootstrap_days <= MAX_BOOTSTRAP_DAYS:
        raise SystemExit(f"error: --bootstrap-days must be 1..{MAX_BOOTSTRAP_DAYS}")
    if not 1 <= args.lookback_days <= MAX_LOOKBACK_DAYS:
        raise SystemExit(f"error: --lookback-days must be 1..{MAX_LOOKBACK_DAYS}")
    if not 1 <= args.timeout <= MAX_TIMEOUT:
        raise SystemExit(f"error: --timeout must be 1..{MAX_TIMEOUT}")
    if not 0 <= args.retries <= MAX_RETRIES:
        raise SystemExit(f"error: --retries must be 0..{MAX_RETRIES}")
    if not 0 <= args.retry_delay <= MAX_RETRY_DELAY:
        raise SystemExit(f"error: --retry-delay must be 0..{MAX_RETRY_DELAY}")
    if not 0 <= args.request_interval <= MAX_REQUEST_INTERVAL:
        raise SystemExit(f"error: --request-interval must be 0..{MAX_REQUEST_INTERVAL}")

    try:
        path = None if args.no_state else state_path(args.state_file)
        state = load_state(path)
        today = datetime.now(timezone.utc).date()
        series_ids = tuple(series_id for series_id, _ in SERIES)
        records, updated_state, requested, failures = fetch_series(
            series_ids,
            today,
            args.bootstrap_days,
            args.lookback_days,
            state,
            args.timeout,
            args.retries,
            args.retry_delay,
            args.request_interval,
        )
        for record in records:
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
        save_state(path, updated_state)
        run_summary(series_ids, records, failures, requested, today)
        if requested and len(failures) == requested:
            raise SystemExit(1)  # every requested series failed
    except FredError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
