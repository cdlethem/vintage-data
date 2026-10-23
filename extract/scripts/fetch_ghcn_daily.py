#!/usr/bin/env python3
"""Fetch GHCN-Daily climate summaries for a curated set of major US stations.

Source: NCEI data access service, dataset ``daily-summaries``
(``https://www.ncei.noaa.gov/access/services/data/v1``). Keyless. Each run
makes ONE bounded request for the whole station list over a date window and
emits one NDJSON record per (station, date) observation row.

The service publishes an observation date's row once the reporting network
has closed it; measured live 2026-10-23 the newest row available was
2026-10-18 (about a five-day lag) and a new date appears roughly daily
thereafter. The watermark is the newest DATE emitted, so a run that finds no
new data keeps the old watermark and the next run retries the same window.

Values arrive as fixed-width GHCN-D scaled integers; they are normalized to
SI units: temperatures /10 to Celsius, PRCP /10 to millimetres, SNOW and
SNWD /10 to centimetres (GHCN-D scale factors; temperatures and PRCP
verified against known 2026-10 station values). A missing element is absent
from the row and is emitted as null.

Station list: 6 long-running ASOS airport stations plus 11 CoCoRaHS
community stations, verified live 2026-10-23 to be returning rows within
the last two weeks. Coordinates/names from the NCEI ghcnd-stations.txt
metadata file.

Stdlib only.
"""
from __future__ import annotations
import argparse, json, os, pathlib, re, sys, urllib.parse, urllib.request
from datetime import date, datetime, timedelta, timezone

SOURCE = "ghcn_daily"
API = "https://www.ncei.noaa.gov/access/services/data/v1"
DATASET = "daily-summaries"
DATA_TYPES = "TMP,TMAX,TMIN,PRCP,SNOW,SNWD"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DEFAULT_LOOKBACK_DAYS = 31
MAX_LOOKBACK_DAYS = 365
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 300

# station id -> (name, latitude, longitude); see module docstring.
STATIONS = {
    "USW00012839": ("FL MIAMI INTL AP", 25.7881, -80.31),
    "USW00013874": ("GA ATLANTA HARTSFIELD-JACKSON INT", 33.6297, -84.44),
    "USW00023174": ("CA LOS ANGELES INTL AP", 33.9381, -118.38),
    "USW00023234": ("CA SAN FRANCISCO INTL AP", 37.6197, -122.36),
    "USW00024233": ("WA SEATTLE TACOMA AP", 47.4447, -122.31),
    "USW00094846": ("IL CHICAGO OHARE INTL AP", 41.9603, -87.93),
    "US10lanc001": ("NE LINCOLN 2.7 SSW", 40.7630, -96.68),
    "US1ARPY0002": ("AR HOUSTON 5.0 S", 34.9631, -92.70),
    "US1AZGL0016": ("AZ MIAMI 1.1 W", 33.3918, -110.87),
    "US1HIHI0035": ("HI HAWAIIAN OCEAN VIEW 4.7 NNE", 19.1350, -155.74),
    "US1IDAD0002": ("ID BOISE 5.3 NW", 43.6695, -116.26),
    "US1MOCE0017": ("MO EUGENE 4.8 NE", 38.4077, -92.34),
    "US1MOFSA199": ("MO MEMPHIS 0.1 S", 40.4594, -92.16),
    "US1MOSS0055": ("MO LAKE ST LOUIS 1.7 WSW", 38.7816, -90.81),
    "US1MSNS0002": ("MS PHILADELPHIA 4.5 SW", 32.7339, -89.16),
    "US1NJUN0010": ("NJ NEW PROVIDENCE 0.8 ESE", 40.6945, -74.38),
    "US1OKCV0091": ("OK OKLAHOMA CITY 10.9 S", 35.3100, -97.52),
}

# GHCN-D element -> (output field, unit label); all scaled by 10.
ELEMENTS = {
    "TMP": "temp_c",
    "TMAX": "tmax_c",
    "TMIN": "tmin_c",
    "PRCP": "precip_mm",
    "SNOW": "snow_cm",
    "SNWD": "snow_depth_cm",
}
# GHCN-D missing-value sentinels, per the GHCN-D format specification.
MISSING_SENTINELS = {-999, -9999}


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    return pathlib.Path(os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT).expanduser() / "state" / f"{SOURCE}.json"


def load_watermark(path: pathlib.Path) -> str:
    """Return the newest DATE emitted by a previous run ("" when absent)."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return ""
    value = data.get("last_date", "")
    if not isinstance(value, str) or not (not value or DATE_RE.fullmatch(value)):
        raise ValueError(f"malformed watermark state in {path}")
    return value


def save_watermark(path: pathlib.Path, watermark: str) -> None:
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps({"last_date": watermark}) + "\n", encoding="utf-8")
    os.replace(staged, path)


def _scaled(value: object, element: str) -> float | None:
    """Parse one fixed-width GHCN-D scaled value; None for missing/sentinel."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError(f"{element} is not a string: {value!r}")
    text = value.strip()
    if not text:
        return None
    try:
        raw = int(text)
    except ValueError:
        raise ValueError(f"{element} is not an integer: {value!r}")
    if raw in MISSING_SENTINELS:
        return None
    return raw / 10.0


def normalize_row(row: dict, fetched_at: str, window_start: date, window_end: date) -> dict | None:
    """Validate one service row and normalize it; None when it carries no data.

    Raises on any contract violation (unknown station, bad/out-of-window
    date, unparseable value) so a bad response fails the run rather than
    being silently dropped.
    """
    station = row.get("STATION")
    if not isinstance(station, str) or station not in STATIONS:
        raise ValueError(f"row references unknown station: {station!r}")
    obs_date = row.get("DATE")
    if not isinstance(obs_date, str) or not DATE_RE.fullmatch(obs_date):
        raise ValueError(f"row has invalid DATE: {obs_date!r}")
    try:
        parsed = date.fromisoformat(obs_date)
    except ValueError:
        raise ValueError(f"row has unparseable DATE: {obs_date!r}") from None
    if not (window_start <= parsed <= window_end):
        raise ValueError(f"row DATE {obs_date} outside requested window {window_start}..{window_end}")

    values = {field: _scaled(row.get(element), element) for element, field in ELEMENTS.items()}
    if all(v is None for v in values.values()):
        return None  # row exists but carries no requested element: nothing to store

    name, latitude, longitude = STATIONS[station]
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": f"{station}-{obs_date}",
        "station": station,
        "station_name": name,
        "latitude": latitude,
        "longitude": longitude,
        "date": obs_date,
        **values,
    }


def fetch_daily(window_start: date, window_end: date, fetched_at: str,
                timeout: int = DEFAULT_TIMEOUT) -> tuple[list[dict], int]:
    """One bounded request; return (records, skipped_empty_rows)."""
    params = {
        "dataset": DATASET,
        "stations": ",".join(STATIONS),
        "dataTypes": DATA_TYPES,
        "startDate": window_start.isoformat(),
        "endDate": window_end.isoformat(),
        "format": "json",
    }
    url = f"{API}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        document = json.load(response)
    if not isinstance(document, list):
        raise ValueError("NCEI data service response is not a JSON list")

    records, skipped = [], 0
    seen: set[tuple[str, str]] = set()
    for row in document:
        if not isinstance(row, dict):
            raise ValueError(f"malformed row in NCEI response: {row!r}")
        record = normalize_row(row, fetched_at, window_start, window_end)
        if record is None:
            skipped += 1
            continue
        key = (record["station"], record["date"])
        if key in seen:
            raise ValueError(f"duplicate row for {key[0]} {key[1]} in one response")
        seen.add(key)
        records.append(record)
    records.sort(key=lambda r: (r["station"], r["date"]))
    return records, skipped


def window_for(watermark: str, lookback_days: int, today: date) -> tuple[date, date]:
    start = today - timedelta(days=lookback_days)
    if watermark:
        after = date.fromisoformat(watermark) + timedelta(days=1)
        if after > start:
            start = after
    return start, today


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off pull)")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help=f"maximum window when no watermark, or one behind it (1..{MAX_LOOKBACK_DAYS})")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help=f"request timeout in seconds (1..{MAX_TIMEOUT})")
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if not 1 <= args.lookback_days <= MAX_LOOKBACK_DAYS:
        parser.error(f"--lookback-days must be within 1..{MAX_LOOKBACK_DAYS}")
    if not 1 <= args.timeout <= MAX_TIMEOUT:
        parser.error(f"--timeout must be within 1..{MAX_TIMEOUT}")

    records_emitted = 0
    try:
        path = state_path(args.state_file)
        watermark = "" if args.no_state else load_watermark(path)
        today = datetime.now(timezone.utc).date()
        window_start, window_end = window_for(watermark, args.lookback_days, today)
        if window_start > window_end:
            raise ValueError(f"empty window {window_start}..{window_end}")
        fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        records, skipped = fetch_daily(window_start, window_end, fetched_at, args.timeout)

        fresh = [r for r in records if not watermark or r["date"] > watermark]
        for record in fresh:
            print(json.dumps(record, ensure_ascii=False))
            records_emitted += 1
        if fresh and not args.no_state:
            save_watermark(path, max(r["date"] for r in fresh))

        summary = {
            "health": "healthy",
            "completeness": "complete",
            "records": records_emitted,
            "coverage": {
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "watermark_before": watermark or None,
                "watermark_after": (max(r["date"] for r in fresh) if fresh else watermark) or None,
                "skipped_empty_rows": skipped,
            },
            "requests": {"attempted": 1},
        }
        print(SUMMARY_PREFIX + json.dumps(summary), file=sys.stderr)
        return 0
    except Exception as exc:
        print(SUMMARY_PREFIX + json.dumps({
            "health": "failed",
            "completeness": "failed",
            "records": records_emitted,
            "error": str(exc),
        }), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
