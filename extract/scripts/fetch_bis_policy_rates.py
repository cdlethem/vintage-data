#!/usr/bin/env python3
"""BIS Data Portal — central bank policy rates (flow WS_CBPOL), keyless SDMX-JSON.

One request fetches the whole policy-rate table: 38 reporting areas (including
the XM euro-area aggregate) in both frequencies, D (business-day policy rate in
force) and M (month-end policy rate). Values are per cent per year.

**Verified live 2026-11-07**:
  * `GET /api/v1/data/WS_CBPOL/all/all?startPeriod=2026-01-01&endPeriod=2026-11-07`
    with `Accept: application/vnd.sdmx.data+json;version=1.0.0` — **200,
    323,780 bytes** for ten months: 76 series (38 areas x 2 frequencies), latest
    daily observation 2026-11-03 (the Monday release), latest monthly period
    2026-10, real values (e.g. US 4.5 -> 3.25 across 2026).
  * Data changes weekly: the daily series gains one business-day observation per
    release (Mondays) and the monthly series gains one period per month-end.

Quirks that will cost someone an afternoon:
  * The v1 payload is wrapped in a `data` object (SDMX-JSON 1.0.0 message:
    `meta` + `data.dataSets` + `data.structure`), unlike the ECB data API's
    top-level working-draft layout — code written against the ECB shape breaks
    here.
  * `TIME_PERIOD` is the sole observation-level dimension, and it is ONE shared
    list of period codes mixing monthly ("2026-10") and daily ("2026-11-03")
    codes; every series' observation indexes point into that combined list
  * Observation values are JSON *strings* ("4.25"), not numbers.
  * Unavailable values are the string "NaN" with OBS_STATUS "M" (a full 365-day
    pull contains ~1,700 of them on daily series); they are dropped and counted
    as missing, not treated as schema errors.
  * The v2 `/dataflow` endpoint answers 501 ("dataflow resource not supported");
    flow discovery works via v1 `GET /dataflow/BIS/all/latest` (SDMX-ML XML).
  * The `all/all` key selects every series; a 365-day window is about 0.4 MB.

Watermarking: state maps each `{frequency}:{ref_area}` series to the last
period emitted. Scheduled runs emit strictly newer observations plus each
series' latest period from the response, so a corrected latest value surfaces
on the next run without re-emitting the whole window. `--no-state` performs a
one-off pull of the full window.

Etiquette: keyless, no published rate limit, one small request per daily run.
BIS statistical data may be reproduced and distributed free of charge with the
BIS cited as the source (https://www.bis.org/terms_conditions.htm).

Stdlib only.
"""

import argparse
import hashlib
import io
import json
import math
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

SOURCE = "bis_policy_rates"
FLOW = "WS_CBPOL"
API_BASE = "https://stats.bis.org/api/v1/data"
ACCEPT = "application/vnd.sdmx.data+json;version=1.0.0"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
ATTRIBUTION = "Bank for International Settlements (BIS), Data Portal, https://data.bis.org/"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
DEFAULT_LOOKBACK_DAYS = 365
MAX_LOOKBACK_DAYS = 365
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
UNIT_MEASURE_PERCENT = "368"
UNIT_MEASURE_NAME = "per cent per year"

_PERIOD_RE = re.compile(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$")
_INDEX_RE = re.compile(r"0|[1-9][0-9]*")
_CODE_RE = re.compile(r"[A-Z0-9]{2,4}\Z")
MAX_ERROR_CHARS = 240


def period_sort_key(period: str) -> tuple[int, int, int]:
    """Chronological key for a BIS period code; month-only keys to the 1st."""
    match = _PERIOD_RE.fullmatch(period)
    if match is None:
        raise ValueError(f"BIS period {period!r} is not a YYYY, YYYY-MM, or YYYY-MM-DD code")
    year = int(match.group(1))
    month = int(match.group(2) or 1)
    day = int(match.group(3) or 1)
    if not 1 <= month <= 12:
        raise ValueError(f"BIS period {period!r} has an invalid month")
    if not 1 <= day <= 31:
        raise ValueError(f"BIS period {period!r} has an invalid day")
    return (year, month, day)


def _period_in_window(period: str, start: date, end: date) -> bool:
    year, month, day = period_sort_key(period)
    if period.count("-") == 1:
        # Month-only period: included when the month overlaps the window.
        return start.year * 12 + start.month <= year * 12 + month <= end.year * 12 + end.month
    return start <= date(year, month, day) <= end


def default_window(
    now: datetime | None = None, lookback_days: int = DEFAULT_LOOKBACK_DAYS
) -> tuple[str, str]:
    """Return (start, end) as ISO dates for the rolling request window."""
    today = (now or datetime.now(timezone.utc)).date()
    return (today - timedelta(days=lookback_days)).isoformat(), today.isoformat()


def state_path(explicit: str | None = None) -> Path:
    """Where the per-series watermarks live: outside the repo, beside the raw data."""
    if explicit:
        return Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return Path(root).expanduser() / "state" / f"{SOURCE}.json"

def _empty_state() -> dict:
    return {"version": STATE_VERSION, "source": SOURCE, "last_period": {}}


def load_state(path: Path) -> dict:
    """Read the watermark file; a missing or corrupt file re-baselines the source."""
    try:
        state = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return _empty_state()
    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        return _empty_state()
    last_period = state.get("last_period")
    if not isinstance(last_period, dict):
        return _empty_state()
    for key, period in last_period.items():
        if not isinstance(key, str) or not isinstance(period, str):
            return _empty_state()
    return {"version": STATE_VERSION, "source": SOURCE, "last_period": last_period}


def save_state(path: Path, state: dict) -> None:
    """Publish the watermark atomically so a crash cannot leave it half-written."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".staged")
    staged.write_text(
        json.dumps(state, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )
    os.replace(staged, path)


def build_url(start_period: str, end_period: str) -> str:
    """Build the single all-series WS_CBPOL request URL for a date window."""
    return (
        f"{API_BASE}/{FLOW}/all/all?"
        + urllib.parse.urlencode({"startPeriod": start_period, "endPeriod": end_period})
    )


def load_response(response: Any) -> Any:
    """Decode JSON while rejecting duplicate object keys and non-finite constants."""
    def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"BIS response contains duplicate object key {key!r}")
            result[key] = value
        return result

    def _reject_constant(value: str) -> Any:
        raise ValueError(f"BIS response contains invalid JSON number {value}")

    return json.load(
        response,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_constant,
    )


def _required_object(parent: Mapping[str, Any], field: str, context: str) -> Mapping[str, Any]:
    value = parent.get(field)
    if not isinstance(value, Mapping):
        raise ValueError(f"BIS {context} is missing its {field} object")
    return value


def _definition_values(raw_dimension: Any, context: str) -> tuple[str, list[tuple[str, str]]]:
    if not isinstance(raw_dimension, Mapping):
        raise ValueError(f"BIS {context} must be an object")
    identifier = raw_dimension.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise ValueError(f"BIS {context} is missing its id")
    raw_values = raw_dimension.get("values")
    if not isinstance(raw_values, list):
        raise ValueError(f"BIS {context} values must be an array")
    values: list[tuple[str, str]] = []
    for raw in raw_values:
        if not isinstance(raw, Mapping):
            raise ValueError(f"BIS {context} values must be objects")
        code = raw.get("id")
        if not isinstance(code, str) or not code:
            raise ValueError(f"BIS {context} contains a value without an id")
        name = raw.get("name")
        values.append((code, str(name) if name is not None else code))
    return identifier, values


def _attribute_definitions(raw: Any, context: str) -> list[tuple[str, list[tuple[str, str]]]]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(f"BIS {context} definitions must be an array")
    definitions = []
    for raw_dimension in raw:
        identifier, values = _definition_values(raw_dimension, f"BIS {context} definition")
        definitions.append((identifier, values))
    return definitions


def _canonical_index(value: Any, size: int, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"BIS {context} index {value!r} must be a non-negative integer")
    if isinstance(value, str) and not _INDEX_RE.fullmatch(value):
        raise ValueError(f"BIS {context} index {value!r} must be a non-negative integer")
    index = int(value)
    if index < 0:
        raise ValueError(f"BIS {context} index {value!r} must be a non-negative integer")
    if index >= size:
        raise ValueError(f"BIS {context} index {index} is out of range for {size} values")
    return index


def _selected_attribute_values(
    raw_indexes: Any,
    definitions: list[tuple[str, list[tuple[str, str]]]],
    context: str,
) -> dict[str, str]:
    if raw_indexes is None:
        raw_indexes = []
    if not isinstance(raw_indexes, list):
        raise ValueError(f"BIS {context} attributes must be an array")
    if len(raw_indexes) != len(definitions):
        raise ValueError(f"BIS {context} attribute count does not match its structure")
    selected: dict[str, str] = {}
    for (identifier, values), raw_index in zip(definitions, raw_indexes):
        if raw_index is None:
            continue
        index = _canonical_index(raw_index, len(values), f"{context} attribute {identifier}")
        selected[identifier] = values[index][0]
    return selected


def stable_observation_id(frequency: str, ref_area: str, period: str) -> str:
    """Derive revision-stable identity from the source-natural observation key."""
    canonical = f"bis:{FLOW}:{frequency}:{ref_area}:{period}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_response(
    document: Any, start_period: str, end_period: str, fetched_at: str
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Validate a complete BIS data message and return sorted observation records."""
    start = date.fromisoformat(start_period)
    end = date.fromisoformat(end_period)
    if start > end:
        raise ValueError("BIS request window starts after it ends")
    if not isinstance(document, Mapping):
        raise ValueError("BIS response must be a JSON object")
    meta = _required_object(document, "meta", "response")
    prepared = meta.get("prepared")
    if prepared is not None and not isinstance(prepared, str):
        raise ValueError("BIS meta prepared must be a string")
    data = _required_object(document, "data", "response")
    structure = _required_object(data, "structure", "data object")
    dimensions = _required_object(structure, "dimensions", "structure")

    raw_series_dimensions = dimensions.get("series")
    if not isinstance(raw_series_dimensions, list) or len(raw_series_dimensions) != 2:
        raise ValueError("BIS series dimensions must be exactly FREQ, REF_AREA")
    if raw_series_dimensions[0].get("id") != "FREQ" or raw_series_dimensions[1].get("id") != "REF_AREA":
        raise ValueError("BIS series dimensions must be exactly FREQ, REF_AREA")
    _freq_id, freq_values = _definition_values(raw_series_dimensions[0], "FREQ dimension")
    _area_id, area_values = _definition_values(raw_series_dimensions[1], "REF_AREA dimension")
    if not freq_values or any(code not in {"M", "D"} for code, _ in freq_values):
        raise ValueError("BIS FREQ dimension must contain only monthly and daily codes")
    if not area_values:
        raise ValueError("BIS REF_AREA dimension has no values")
    for code, _ in area_values:
        if not _CODE_RE.fullmatch(code):
            raise ValueError(f"BIS REF_AREA code {code!r} is not a compact code")

    raw_observation_dimensions = dimensions.get("observation")
    if not isinstance(raw_observation_dimensions, list) or len(raw_observation_dimensions) != 1:
        raise ValueError("BIS observation dimensions must contain exactly one dimension")
    if raw_observation_dimensions[0].get("id") != "TIME_PERIOD":
        raise ValueError("BIS sole observation dimension must be TIME_PERIOD")
    _time_id, period_values = _definition_values(
        raw_observation_dimensions[0], "TIME_PERIOD dimension"
    )
    for period, _ in period_values:
        period_sort_key(period)
        if not _period_in_window(period, start, end):
            raise ValueError(f"BIS period {period!r} falls outside the requested window")
    periods = [period for period, _ in period_values]

    attributes = _required_object(structure, "attributes", "structure")
    dataset_definitions = _attribute_definitions(attributes.get("dataset"), "dataset")
    series_definitions = _attribute_definitions(attributes.get("series"), "series")
    observation_definitions = _attribute_definitions(attributes.get("observation"), "observation")

    datasets = data.get("dataSets")
    if not isinstance(datasets, list) or len(datasets) != 1:
        raise ValueError("BIS response must contain exactly one dataset")
    dataset = datasets[0]
    if not isinstance(dataset, Mapping):
        raise ValueError("BIS dataset must be an object")
    dataset_attributes = _selected_attribute_values(
        dataset.get("attributes"), dataset_definitions, "dataset"
    )
    if dataset_attributes.get("UNIT_MEASURE") != UNIT_MEASURE_PERCENT:
        raise ValueError(
            f"BIS WS_CBPOL must be reported in per cent per year (unit measure {UNIT_MEASURE_PERCENT})"
        )
    action = dataset.get("action")
    if action is not None and not isinstance(action, str):
        raise ValueError("BIS dataset action must be a string")
    reporting_begin = dataset.get("reportingBegin")
    reporting_end = dataset.get("reportingEnd")
    for field, value in (("reportingBegin", reporting_begin), ("reportingEnd", reporting_end)):
        if value is not None and not isinstance(value, str):
            raise ValueError(f"BIS dataset {field} must be a string")

    raw_series = dataset.get("series")
    if not isinstance(raw_series, Mapping):
        raise ValueError("BIS dataset series must be an object")

    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    missing = 0
    for encoded, series in raw_series.items():
        if not isinstance(series, Mapping):
            raise ValueError("BIS series payload must be an object")
        indexes = encoded.split(":")
        if len(indexes) != 2:
            raise ValueError(f"BIS series identifier {encoded!r} must have two indexes")
        frequency = freq_values[_canonical_index(indexes[0], len(freq_values), "series FREQ")][0]
        ref_area, ref_area_name = area_values[
            _canonical_index(indexes[1], len(area_values), "series REF_AREA")
        ]
        series_attributes = _selected_attribute_values(
            series.get("attributes"), series_definitions, "series"
        )
        observations = series.get("observations")
        if not isinstance(observations, Mapping):
            raise ValueError("BIS series observations must be an object")
        for raw_period_index, raw_observation in observations.items():
            period_index = _canonical_index(
                raw_period_index, len(periods), "observation index"
            )
            period = periods[period_index]
            if not isinstance(raw_observation, list) or not raw_observation:
                raise ValueError("BIS observation must be a non-empty array")
            value = raw_observation[0]
            _selected_attribute_values(
                raw_observation[1:], observation_definitions, "observation"
            )
            if value is None or value == "NaN":
                # BIS encodes unavailable values as the string "NaN" (OBS_STATUS M).
                missing += 1
                continue
            if isinstance(value, str):
                try:
                    rate = float(value)
                except ValueError:
                    raise ValueError(f"BIS observation value {value!r} is not a number")
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                rate = float(value)
            else:
                raise ValueError(f"BIS observation value {value!r} has an unsupported type")
            if not math.isfinite(rate):
                raise ValueError("BIS observation rate must be a finite number")
            observation_id = stable_observation_id(frequency, ref_area, period)
            if observation_id in seen_ids:
                raise ValueError("BIS response contains duplicate observation identifiers")
            seen_ids.add(observation_id)
            records.append(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": observation_id,
                    "flow": FLOW,
                    "frequency": frequency,
                    "ref_area": ref_area,
                    "ref_area_name": ref_area_name,
                    "period": period,
                    "rate": rate,
                    "unit": UNIT_MEASURE_NAME,
                    "series_attributes": series_attributes,
                    "source_metadata": {
                        "provider": "Bank for International Settlements",
                        "attribution": ATTRIBUTION,
                        "prepared": prepared,
                        "dataset_action": action,
                        "reporting_begin": reporting_begin,
                        "reporting_end": reporting_end,
                    },
                }
            )

    records.sort(
        key=lambda record: (
            record["ref_area"],
            record["frequency"],
            period_sort_key(record["period"]),
        )
    )
    info = {
        "prepared": prepared,
        "reporting_end": reporting_end,
        "missing_observations_dropped": missing,
        "series": len(raw_series),
    }
    return records, info


def fetch_policy_rates(
    start_period: str, end_period: str, timeout: int = 60, fetched_at: str = ""
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Make the single all-series request and fully validate its response."""
    url = build_url(start_period, end_period)
    request = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": ACCEPT}
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        status = getattr(response, "status", None)
        if status is None:
            status = response.getcode()
        payload = response.read()
    document = load_response(io.BytesIO(payload))
    records, info = parse_response(document, start_period, end_period, fetched_at)
    diagnostics = {"status": status, "bytes": len(payload)}
    diagnostics.update(info)
    return records, diagnostics


def _series_key(record: Mapping[str, Any]) -> str:
    return f"{record['frequency']}:{record['ref_area']}"


def select_new(
    records: list[dict[str, Any]], last_period: Mapping[str, str]
) -> tuple[list[dict[str, Any]], dict[str, str], int]:
    """Pick the records to emit: strictly newer than the watermark, plus each
    series' latest period so corrections to the newest value surface."""
    by_series: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        by_series.setdefault(_series_key(record), []).append(record)
    selected: list[dict[str, Any]] = []
    repeated = 0
    updated: dict[str, str] = dict(last_period)
    for key, series_records in sorted(by_series.items()):
        watermark = last_period.get(key)
        watermark_key = period_sort_key(watermark) if watermark is not None else None
        latest_key = max(period_sort_key(r["period"]) for r in series_records)
        latest_period = max(
            (r["period"] for r in series_records), key=period_sort_key
        )
        for record in series_records:
            record_key = period_sort_key(record["period"])
            if watermark_key is not None and record_key <= watermark_key:
                if record_key != latest_key:
                    continue
                repeated += 1
            selected.append(record)
        if watermark_key is None or latest_key > watermark_key:
            updated[key] = latest_period
    selected.sort(
        key=lambda record: (
            record["ref_area"],
            record["frequency"],
            period_sort_key(record["period"]),
        )
    )
    return selected, updated, repeated


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch BIS central bank policy rates (WS_CBPOL) as NDJSON."
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"rolling request window in days, 1-{MAX_LOOKBACK_DAYS} "
        f"(default {DEFAULT_LOOKBACK_DAYS})",
    )
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument(
        "--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state"
    )
    parser.add_argument(
        "--no-state", action="store_true",
        help="ignore and do not write the watermark (one-off pull)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if not 1 <= args.lookback_days <= MAX_LOOKBACK_DAYS:
        parser.error(f"--lookback-days must be between 1 and {MAX_LOOKBACK_DAYS}")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    start_period, end_period = default_window(lookback_days=args.lookback_days)
    path = None if args.no_state else state_path(args.state_file)
    if path is None:
        last_period: dict[str, str] = {}
    else:
        last_period = load_state(path)["last_period"]
    window = [start_period, end_period]
    started = time.monotonic()
    try:
        records, diagnostics = fetch_policy_rates(
            start_period, end_period, args.timeout, fetched_at
        )
    except Exception as error:
        summary = {
            "health": "failed",
            "completeness": "failed",
            "records": 0,
            "bytes": 0,
            "requests": {"attempted": 1, "maximum": 1},
            "metrics": {
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "status": None,
                "window": window,
            },
            "error": str(error)[:MAX_ERROR_CHARS],
        }
        print(SUMMARY_PREFIX + json.dumps(summary, sort_keys=True), file=sys.stderr)
        return 1

    selected, updated, repeated = select_new(records, last_period)
    if path is not None:
        save_state(
            path,
            {"version": STATE_VERSION, "source": SOURCE, "last_period": updated},
        )
    for record in selected:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    summary = {
        "health": "healthy",
        "completeness": "complete",
        "records": len(selected),
        "bytes": diagnostics["bytes"],
        "requests": {"attempted": 1, "maximum": 1},
        "metrics": {
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "status": diagnostics["status"],
            "window": window,
            "series": diagnostics["series"],
            "new_records": len(selected) - repeated,
            "repeated_records": repeated,
            "missing_observations_dropped": diagnostics["missing_observations_dropped"],
            "reporting_end": diagnostics["reporting_end"],
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, sort_keys=True), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
