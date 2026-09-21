#!/usr/bin/env python3
"""Fetch one bounded ECB SDMX-JSON exchange-rate series as NDJSON.

The request always names one dataflow and one complete series key.  The default
window is the seven completed UTC dates before today, and every explicit window
is limited to 31 inclusive dates.  The complete response is validated before
anything is written so malformed data cannot produce a partial artifact.

Stdlib only; the ECB data API does not require credentials.
"""

import argparse
import hashlib
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

SOURCE = "ecb_sdmx_statistics"
DEFAULT_FLOW = "EXR"
DEFAULT_SERIES_KEY = "D.USD.EUR.SP00.A"
API_BASE = "https://data-api.ecb.europa.eu/service/data"
ACCEPT = "application/vnd.sdmx.data+json;version=1.0.0-wd"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
MAX_WINDOW_DAYS = 31
ATTRIBUTION = "European Central Bank (ECB), Statistical Data Warehouse"

_CODE_RE = re.compile(r"[A-Za-z0-9_-]+")
_INDEX_RE = re.compile(r"0|[1-9][0-9]*")


def default_date_window(now: datetime | date | None = None) -> tuple[str, str]:
    """Return the seven complete UTC dates immediately before today."""
    if now is None:
        today = datetime.now(timezone.utc).date()
    elif isinstance(now, datetime):
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        today = now.astimezone(timezone.utc).date()
    else:
        today = now
    return (today - timedelta(days=7)).isoformat(), (today - timedelta(days=1)).isoformat()


def _parse_date(value: str, field: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO date in YYYY-MM-DD form") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field} must be an ISO date in YYYY-MM-DD form")
    return parsed


def validate_request(
    flow: str, series_key: str, start_period: str, end_period: str, timeout: int
) -> tuple[date, date, tuple[str, ...]]:
    """Validate an exact, bounded request before any network access."""
    if not isinstance(flow, str) or not _CODE_RE.fullmatch(flow):
        raise ValueError("flow must be a non-empty ECB code")
    if not isinstance(series_key, str):
        raise ValueError("series_key must be a complete dot-separated key")
    key_parts = tuple(series_key.split("."))
    if not key_parts or any(not _CODE_RE.fullmatch(part) for part in key_parts):
        raise ValueError("series_key must be a complete dot-separated key")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("timeout must be a positive integer")

    start = _parse_date(start_period, "start_period")
    end = _parse_date(end_period, "end_period")
    if start > end:
        raise ValueError("start_period must not be after end_period")
    if (end - start).days + 1 > MAX_WINDOW_DAYS:
        raise ValueError(f"date window must not exceed {MAX_WINDOW_DAYS} days")
    return start, end, key_parts


def build_url(flow: str, series_key: str, start_period: str, end_period: str) -> str:
    """Build the sole ECB request URL for an exact series and date window."""
    encoded_flow = urllib.parse.quote(flow, safe="")
    encoded_key = urllib.parse.quote(series_key, safe=".")
    query = urllib.parse.urlencode(
        {
            "startPeriod": start_period,
            "endPeriod": end_period,
            "format": "jsondata",
        }
    )
    return f"{API_BASE}/{encoded_flow}/{encoded_key}?{query}"


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"ECB response contains duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"ECB response contains invalid JSON number {value}")


def load_response(response: Any) -> Any:
    """Decode JSON while rejecting duplicate object keys and non-finite constants."""
    return json.load(
        response,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_json_constant,
    )


def _required_object(parent: Mapping[str, Any], field: str, context: str) -> Mapping[str, Any]:
    value = parent.get(field)
    if not isinstance(value, Mapping):
        raise ValueError(f"ECB {context} {field} must be an object")
    return value


def _required_list(parent: Mapping[str, Any], field: str, context: str) -> list[Any]:
    value = parent.get(field)
    if not isinstance(value, list):
        raise ValueError(f"ECB {context} {field} must be an array")
    return value


def _definitions(
    raw_definitions: Any, context: str, *, allow_empty_values: bool = False
) -> list[tuple[str, list[Mapping[str, Any]], Mapping[str, Any]]]:
    if not isinstance(raw_definitions, list):
        raise ValueError(f"ECB {context} definitions must be an array")
    result: list[tuple[str, list[Mapping[str, Any]], Mapping[str, Any]]] = []
    seen_ids: set[str] = set()
    for position, raw in enumerate(raw_definitions):
        if not isinstance(raw, Mapping):
            raise ValueError(f"ECB {context} definition {position} must be an object")
        identifier = raw.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in seen_ids:
            raise ValueError(f"ECB {context} identifiers must be non-empty and unique")
        seen_ids.add(identifier)
        name = raw.get("name", identifier)
        if not isinstance(name, str):
            raise ValueError(f"ECB {context} {identifier!r} name must be a string")
        values = raw.get("values")
        if not isinstance(values, list) or (not values and not allow_empty_values):
            raise ValueError(f"ECB {context} {identifier!r} values must be an array")
        normalized_values: list[Mapping[str, Any]] = []
        seen_value_ids: set[str] = set()
        for raw_value in values:
            if not isinstance(raw_value, Mapping):
                raise ValueError(f"ECB {context} {identifier!r} value must be an object")
            value_id = raw_value.get("id")
            if (
                not isinstance(value_id, str)
                or not value_id
                or value_id in seen_value_ids
            ):
                raise ValueError(
                    f"ECB {context} {identifier!r} value identifiers must be non-empty and unique"
                )
            value_name = raw_value.get("name", value_id)
            if not isinstance(value_name, str):
                raise ValueError(
                    f"ECB {context} {identifier!r} value name must be a string"
                )
            seen_value_ids.add(value_id)
            normalized_values.append(raw_value)
        result.append((identifier, normalized_values, raw))
    return result


def _canonical_index(value: Any, size: int, context: str) -> int:
    if not isinstance(value, str) or not _INDEX_RE.fullmatch(value):
        raise ValueError(f"ECB {context} has invalid index {value!r}")
    index = int(value)
    if index >= size:
        raise ValueError(f"ECB {context} index {value!r} is outside [0, {size})")
    return index


def _selected_attributes(
    raw_indexes: Any,
    definitions: Sequence[tuple[str, list[Mapping[str, Any]], Mapping[str, Any]]],
    context: str,
) -> dict[str, Mapping[str, Any] | None]:
    if raw_indexes is None:
        raw_indexes = []
    if not isinstance(raw_indexes, list) or len(raw_indexes) > len(definitions):
        raise ValueError(f"ECB {context} attributes must be a bounded index array")
    selected: dict[str, Mapping[str, Any] | None] = {}
    for position, (identifier, values, _definition) in enumerate(definitions):
        raw_index = raw_indexes[position] if position < len(raw_indexes) else None
        if raw_index is None:
            selected[identifier] = None
            continue
        if (
            not isinstance(raw_index, int)
            or isinstance(raw_index, bool)
            or raw_index < 0
            or raw_index >= len(values)
        ):
            raise ValueError(
                f"ECB {context} attribute {identifier!r} has invalid value index"
            )
        selected[identifier] = dict(values[raw_index])
    return selected


def stable_observation_id(flow: str, series_key: str, period: str) -> str:
    """Derive revision-stable identity from the source-natural observation key."""
    canonical = json.dumps(
        [SOURCE, flow, series_key, period], ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _header_metadata(document: Mapping[str, Any]) -> dict[str, Any]:
    header = _required_object(document, "header", "response")
    response_id = header.get("id")
    prepared_at = header.get("prepared")
    sender = header.get("sender")
    if not isinstance(response_id, str) or not response_id:
        raise ValueError("ECB response header id must be a non-empty string")
    if not isinstance(prepared_at, str) or not prepared_at:
        raise ValueError("ECB response header prepared must be a timestamp")
    try:
        prepared = datetime.fromisoformat(prepared_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("ECB response header prepared must be a timestamp") from exc
    if prepared.tzinfo is None:
        raise ValueError("ECB response header prepared must include a timezone")
    if not isinstance(sender, Mapping) or sender.get("id") != "ECB":
        raise ValueError("ECB response header sender id must be 'ECB'")
    sender_name = sender.get("name")
    if sender_name is not None and not isinstance(sender_name, str):
        raise ValueError("ECB response header sender name must be a string")
    return {
        "response_id": response_id,
        "prepared_at": prepared_at,
        "sender": dict(sender),
    }


def parse_response(
    document: Any,
    flow: str,
    series_key: str,
    start_period: str,
    end_period: str,
    fetched_at: str,
) -> list[dict[str, Any]]:
    """Validate a complete SDMX-JSON response and return period-ordered records."""
    start, end, requested_codes = validate_request(
        flow, series_key, start_period, end_period, 1
    )
    if not isinstance(document, Mapping):
        raise ValueError("ECB response must be a JSON object")
    header_metadata = _header_metadata(document)

    structure = _required_object(document, "structure", "response")
    raw_dimensions = _required_object(structure, "dimensions", "structure")
    dataset_dimensions = _definitions(
        raw_dimensions.get("dataSet"), "dataset dimension", allow_empty_values=True
    )
    if dataset_dimensions:
        raise ValueError("ECB response must not add dataset dimensions to an exact series")
    series_dimensions = _definitions(raw_dimensions.get("series"), "series dimension")
    observation_dimensions = _definitions(
        raw_dimensions.get("observation"),
        "observation dimension",
        allow_empty_values=True,
    )
    if len(series_dimensions) != len(requested_codes):
        raise ValueError("ECB series dimension count does not match the requested key")
    if len(observation_dimensions) != 1 or observation_dimensions[0][0] != "TIME_PERIOD":
        raise ValueError("ECB response must have TIME_PERIOD as its sole observation dimension")

    selected_dimensions: dict[str, str] = {}
    dimension_labels: dict[str, str] = {}
    for requested_code, (identifier, values, _definition) in zip(
        requested_codes, series_dimensions
    ):
        matches = [value for value in values if value["id"] == requested_code]
        if len(matches) != 1:
            raise ValueError(
                f"ECB series dimension {identifier!r} does not contain requested code {requested_code!r}"
            )
        selected_dimensions[identifier] = requested_code
        dimension_labels[identifier] = str(matches[0].get("name", requested_code))

    period_values = observation_dimensions[0][1]
    parsed_periods: list[date] = []
    for raw_period in period_values:
        period = _parse_date(raw_period["id"], "ECB observation period")
        if period < start or period > end:
            raise ValueError("ECB observation period falls outside the requested window")
        parsed_periods.append(period)

    raw_attributes = _required_object(structure, "attributes", "structure")
    dataset_attribute_defs = _definitions(
        raw_attributes.get("dataSet"), "dataset attribute", allow_empty_values=True
    )
    series_attribute_defs = _definitions(
        raw_attributes.get("series"), "series attribute", allow_empty_values=True
    )
    observation_attribute_defs = _definitions(
        raw_attributes.get("observation"),
        "observation attribute",
        allow_empty_values=True,
    )

    datasets = _required_list(document, "dataSets", "response")
    if len(datasets) != 1 or not isinstance(datasets[0], Mapping):
        raise ValueError("ECB response must contain exactly one dataset object")
    dataset = datasets[0]
    dataset_attributes = _selected_attributes(
        dataset.get("attributes"), dataset_attribute_defs, "dataset"
    )
    action = dataset.get("action")
    valid_from = dataset.get("validFrom")
    if action is not None and not isinstance(action, str):
        raise ValueError("ECB dataset action must be a string")
    if valid_from is not None and not isinstance(valid_from, str):
        raise ValueError("ECB dataset validFrom must be a string")

    raw_series = dataset.get("series")
    if not isinstance(raw_series, Mapping):
        raise ValueError("ECB dataset series must be an object")
    if len(raw_series) > 1:
        raise ValueError("ECB exact-key response must contain at most one series")
    if not raw_series:
        return []

    encoded_series, series = next(iter(raw_series.items()))
    if not isinstance(series, Mapping):
        raise ValueError("ECB series payload must be an object")
    raw_series_indexes = encoded_series.split(":")
    if len(raw_series_indexes) != len(series_dimensions):
        raise ValueError("ECB series identifier has the wrong dimension count")
    returned_codes = tuple(
        values[_canonical_index(raw_index, len(values), "series identifier")]["id"]
        for raw_index, (_identifier, values, _definition) in zip(
            raw_series_indexes, series_dimensions
        )
    )
    if returned_codes != requested_codes:
        raise ValueError("ECB returned series does not match the requested exact key")

    series_attributes = _selected_attributes(
        series.get("attributes"), series_attribute_defs, "series"
    )
    observations = series.get("observations")
    if not isinstance(observations, Mapping):
        raise ValueError("ECB series observations must be an object")

    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw_period_index, raw_observation in observations.items():
        period_index = _canonical_index(
            raw_period_index, len(period_values), "observation"
        )
        if not isinstance(raw_observation, list) or not raw_observation:
            raise ValueError("ECB observation must be a non-empty array")
        if len(raw_observation) > len(observation_attribute_defs) + 1:
            raise ValueError("ECB observation has more attributes than its structure")
        rate = raw_observation[0]
        if (
            not isinstance(rate, (int, float))
            or isinstance(rate, bool)
            or not math.isfinite(rate)
        ):
            raise ValueError("ECB observation rate must be a finite number")
        observation_attributes = _selected_attributes(
            raw_observation[1:], observation_attribute_defs, "observation"
        )
        period = period_values[period_index]["id"]
        observation_id = stable_observation_id(flow, series_key, period)
        if observation_id in seen_ids:
            raise ValueError("ECB response contains duplicate observation identifiers")
        seen_ids.add(observation_id)
        records.append(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": observation_id,
                "flow": flow,
                "series_key": series_key,
                "period": period,
                "rate": rate,
                "dimensions": dict(selected_dimensions),
                "dimension_labels": dict(dimension_labels),
                "attributes": observation_attributes,
                "series_attributes": series_attributes,
                "dataset_attributes": dataset_attributes,
                "source_metadata": {
                    "provider": "European Central Bank",
                    "attribution": ATTRIBUTION,
                    **header_metadata,
                    "dataset_action": action,
                    "dataset_valid_from": valid_from,
                },
            }
        )

    records.sort(key=lambda record: record["period"])
    return records


def fetch_observations(
    flow: str = DEFAULT_FLOW,
    series_key: str = DEFAULT_SERIES_KEY,
    start_period: str | None = None,
    end_period: str | None = None,
    timeout: int = 60,
) -> list[dict[str, Any]]:
    """Make one bounded ECB request and fully validate its response."""
    default_start, default_end = default_date_window()
    start_period = default_start if start_period is None else start_period
    end_period = default_end if end_period is None else end_period
    validate_request(flow, series_key, start_period, end_period, timeout)
    url = build_url(flow, series_key, start_period, end_period)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": ACCEPT,
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        },
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = load_response(response)
    return parse_response(
        document, flow, series_key, start_period, end_period, fetched_at
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("flow", nargs="?", default=DEFAULT_FLOW)
    parser.add_argument("series_key", nargs="?", default=DEFAULT_SERIES_KEY)
    parser.add_argument("start_period", nargs="?")
    parser.add_argument("end_period", nargs="?")
    parser.add_argument("--timeout", type=int, default=60)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if (args.start_period is None) != (args.end_period is None):
        raise ValueError("start_period and end_period must be supplied together")
    records = fetch_observations(
        flow=args.flow,
        series_key=args.series_key,
        start_period=args.start_period,
        end_period=args.end_period,
        timeout=args.timeout,
    )
    lines = [
        json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        for record in records
    ]
    if lines:
        sys.stdout.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
