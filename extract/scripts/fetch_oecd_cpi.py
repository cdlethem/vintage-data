#!/usr/bin/env python3
"""Fetch OECD monthly headline CPI indices as NDJSON.

Each run makes one bounded SDMX-JSON request against the keyless OECD
public API: the ``Consumer price indices (CPIs, HICPs), COICOP 2018``
dataflow with the fixed key ``.M.N.CPI.IX._T.N._Z`` (monthly, national
methodology, all-items index level, unadjusted) for every OECD reference
area, over a rolling 15-month window (configurable, at most 36 months).

The complete response is validated before anything is written so malformed
data cannot produce a partial artifact. Records carry a revision-stable id
derived from the flow reference, reference area, and period, so daily
revision snapshots of the same month stay identifiable in RAW.

Stdlib only; the OECD public data API does not require credentials.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

SOURCE = "oecd_cpi"
DATAFLOW_AGENCY = "OECD.SDD.TPS"
DATAFLOW_ID = "DSD_PRICES_COICOP2018@DF_PRICES_C2018_ALL"
DATAFLOW_VERSION = "1.0"
FLOW_REF = f"{DATAFLOW_AGENCY},{DATAFLOW_ID},{DATAFLOW_VERSION}"
API_BASE = "https://sdmx.oecd.org/public/rest/data"
DEFAULT_KEY = ".M.N.CPI.IX._T.N._Z"
ACCEPT = "application/vnd.sdmx.data+json;version=1.0.0"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
ATTRIBUTION = (
    "OECD, Consumer price indices (CPIs, HICPs), COICOP 2018, "
    "OECD Data Explorer, https://data-explorer.oecd.org/"
)
WINDOW_MONTHS = 15
MAX_WINDOW_MONTHS = 36
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 300
DEFAULT_RETRIES = 2
MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 15
MAX_RETRY_DELAY = 300
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

# Observation dimensions must arrive in exactly this order with exactly
# these ids; anything else is schema drift, not data.
OBSERVATION_DIMENSION_IDS = (
    "REF_AREA",
    "FREQ",
    "METHODOLOGY",
    "MEASURE",
    "UNIT_MEASURE",
    "EXPENDITURE",
    "ADJUSTMENT",
    "TRANSFORMATION",
    "TIME_PERIOD",
)
# Fixed slice requested by DEFAULT_KEY, in dimension order. REF_AREA and
# TIME_PERIOD are open (all areas, the requested window).

_CODE_RE = re.compile(r"[A-Za-z0-9_-]+")
_INDEX_RE = re.compile(r"0|[1-9][0-9]*")
_MONTH_RE = re.compile(r"(19|20)\d{2}-(0[1-9]|1[0-2])")

# Mutable in place so main() can report partial progress on failure.
LAST_RUN_STATS: dict[str, Any] = {}


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    index = year * 12 + (month - 1) + delta
    return divmod(index, 12)[0], divmod(index, 12)[1] + 1


def parse_period(value: str, field: str) -> tuple[int, int]:
    """Parse a strict YYYY-MM period and return (year, month)."""
    if not isinstance(value, str) or not _MONTH_RE.fullmatch(value):
        raise ValueError(f"{field} must be a YYYY-MM period")
    year, month = (int(part) for part in value.split("-"))
    return year, month


def default_period_window(now: datetime | None = None) -> tuple[str, str]:
    """Return the rolling WINDOW_MONTHS months ending at the current UTC month."""
    moment = now or datetime.now(timezone.utc)
    year, month = moment.year, moment.month
    start_year, start_month = _shift_month(year, month, -(WINDOW_MONTHS - 1))
    return f"{start_year:04d}-{start_month:02d}", f"{year:04d}-{month:02d}"


def _month_span(start: str, end: str) -> int:
    start_year, start_month = parse_period(start, "start_period")
    end_year, end_month = parse_period(end, "end_period")
    return (end_year - start_year) * 12 + (end_month - start_month) + 1


def validate_request(
    key: str, start_period: str, end_period: str, timeout: int, retries: int,
    retry_delay: int,
) -> None:
    """Validate an exact, bounded request before any network access."""
    if not isinstance(key, str):
        raise ValueError("key must be a dot-separated observation key")
    parts = key.split(".")
    if len(parts) != 8:
        raise ValueError("key must have eight dot-separated codes: REF_AREA plus seven fixed")
    if not all(_CODE_RE.fullmatch(part) for part in parts[1:]):
        raise ValueError("key dimension codes must be non-empty codes")
    if parts[0] and not _CODE_RE.fullmatch(parts[0]):
        raise ValueError("key REF_AREA must be empty or a non-empty code")

    parse_period(start_period, "start_period")
    parse_period(end_period, "end_period")
    span = _month_span(start_period, end_period)
    if span < 1:
        raise ValueError("start_period must not be after end_period")
    if span > MAX_WINDOW_MONTHS:
        raise ValueError(f"period window must not exceed {MAX_WINDOW_MONTHS} months")

    if not isinstance(timeout, int) or isinstance(timeout, bool) or not 0 < timeout <= MAX_TIMEOUT:
        raise ValueError(f"timeout must be an integer between 1 and {MAX_TIMEOUT}")
    if not isinstance(retries, int) or isinstance(retries, bool) or not 0 <= retries <= MAX_RETRIES:
        raise ValueError(f"retries must be an integer between 0 and {MAX_RETRIES}")
    if not isinstance(retry_delay, int) or isinstance(retry_delay, bool) or not 0 <= retry_delay <= MAX_RETRY_DELAY:
        raise ValueError(f"retry_delay must be an integer between 0 and {MAX_RETRY_DELAY}")


def build_url(key: str, start_period: str, end_period: str) -> str:
    """Build the single bounded OECD data request URL."""
    encoded_key = urllib.parse.quote(key, safe=".")
    query = urllib.parse.urlencode(
        {
            "dimensionAtObservation": "AllDimensions",
            "format": "jsondata",
            "startPeriod": start_period,
            "endPeriod": end_period,
        }
    )
    return f"{API_BASE}/{FLOW_REF}/{encoded_key}?{query}"


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("OECD response contains duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"OECD response contains invalid JSON number {value}")


def load_response(body: bytes) -> Any:
    """Decode JSON while rejecting duplicate keys and non-finite constants."""
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("OECD response is not valid UTF-8 JSON") from exc
    return json.loads(
        text,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_json_constant,
    )


def _bounded_body(response: Any) -> bytes:
    """Read the response body, refusing to buffer more than MAX_RESPONSE_BYTES."""
    declared = response.headers.get("Content-Length") if response.headers else None
    if declared is not None:
        try:
            if int(declared) > MAX_RESPONSE_BYTES:
                raise ValueError(
                    f"OECD response declares {declared} bytes, exceeding the "
                    f"{MAX_RESPONSE_BYTES}-byte limit"
                )
        except ValueError:
            raise ValueError("OECD response declares an invalid Content-Length") from None
    chunks: list[bytes] = []
    total = 0
    while chunk := response.read(64 * 1024):
        total += len(chunk)
        if total > MAX_RESPONSE_BYTES:
            raise ValueError(
                f"OECD response exceeds the {MAX_RESPONSE_BYTES}-byte limit"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _request_body(url: str, timeout: int) -> tuple[bytes, int | None]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": ACCEPT,
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = _bounded_body(response)
        status = getattr(response, "status", None)
    return body, status


def _fetch_with_retries(
    url: str, timeout: int, retries: int, retry_delay: int
) -> tuple[bytes, int | None, int, int]:
    """Perform the single request, retrying only transient failures."""
    attempts = 0
    retried = 0
    while True:
        attempts += 1
        LAST_RUN_STATS["requests"]["attempted"] = attempts
        try:
            body, status = _request_body(url, timeout)
            return body, status, attempts, retried
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRIABLE_STATUSES or attempts > retries:
                raise ValueError(f"OECD request failed with HTTP {exc.code}") from exc
        except (urllib.error.URLError, socket.timeout, ConnectionError, TimeoutError) as exc:
            reason = getattr(exc, "reason", exc)
            if attempts > retries:
                raise ValueError(f"OECD request connection error: {reason}") from exc
        retried += 1
        LAST_RUN_STATS["requests"]["retried"] = retried
        print(
            f"{SOURCE}: attempt {attempts} failed; retrying in {retry_delay}s "
            f"({retries - (attempts - 1)} retry budget left)",
            file=sys.stderr,
        )
        time.sleep(retry_delay)


def _required_object(parent: Mapping[str, Any], field: str, context: str) -> Mapping[str, Any]:
    value = parent.get(field)
    if not isinstance(value, Mapping):
        raise ValueError(f"OECD {context} {field} must be an object")
    return value


def _definitions(
    raw_definitions: Any, context: str, *, allow_empty_values: bool = False
) -> list[tuple[str, list[Mapping[str, Any]]]]:
    if not isinstance(raw_definitions, list):
        raise ValueError(f"OECD {context} definitions must be an array")
    result: list[tuple[str, list[Mapping[str, Any]]]] = []
    seen_ids: set[str] = set()
    for position, raw in enumerate(raw_definitions):
        if not isinstance(raw, Mapping):
            raise ValueError(f"OECD {context} definition {position} must be an object")
        identifier = raw.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in seen_ids:
            raise ValueError(f"OECD {context} identifiers must be non-empty and unique")
        seen_ids.add(identifier)
        values = raw.get("values")
        if not isinstance(values, list) or (not values and not allow_empty_values):
            raise ValueError(f"OECD {context} {identifier!r} values must be an array")
        normalized_values: list[Mapping[str, Any]] = []
        seen_value_ids: set[str] = set()
        for raw_value in values:
            if not isinstance(raw_value, Mapping):
                raise ValueError(f"OECD {context} {identifier!r} value must be an object")
            value_id = raw_value.get("id")
            if not isinstance(value_id, str) or not value_id or value_id in seen_value_ids:
                raise ValueError(
                    f"OECD {context} {identifier!r} value identifiers must be non-empty and unique"
                )
            seen_value_ids.add(value_id)
            normalized_values.append(raw_value)
        result.append((identifier, normalized_values))
    return result


def _value_label(value: Mapping[str, Any]) -> str:
    name = value.get("name")
    if isinstance(name, str) and name:
        return name
    return str(value.get("id"))


def stable_observation_id(ref_area: str, period: str) -> str:
    """Derive revision-stable identity from the source-natural observation key."""
    canonical = json.dumps(
        [SOURCE, FLOW_REF, ref_area, period], ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _prepared_at(meta: Mapping[str, Any] | None) -> str | None:
    if meta is None:
        return None
    prepared = meta.get("prepared")
    if prepared is None:
        return None
    if not isinstance(prepared, str):
        raise ValueError("OECD meta prepared must be a string")
    try:
        parsed = datetime.fromisoformat(prepared.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("OECD meta prepared must be a timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("OECD meta prepared must include a timezone")
    return prepared


def parse_response(
    document: Any,
    key: str,
    start_period: str,
    end_period: str,
    fetched_at: str,
) -> list[dict[str, Any]]:
    """Validate a complete SDMX-JSON response and return (area, period)-ordered records."""
    validate_request(key, start_period, end_period, 1, 0, 0)
    if not isinstance(document, Mapping):
        raise ValueError("OECD response must be a JSON object")
    raw_errors = document.get("errors")
    if raw_errors is not None and (not isinstance(raw_errors, list) or raw_errors):
        raise ValueError("OECD response reports errors")
    meta = document.get("meta")
    if meta is not None and not isinstance(meta, Mapping):
        raise ValueError("OECD meta must be an object")
    prepared_at = _prepared_at(meta)

    data = _required_object(document, "data", "response")
    structure = _required_object(data, "structure", "data")
    raw_dimensions = _required_object(structure, "dimensions", "structure")
    dimensions = _definitions(
        raw_dimensions.get("observation"), "observation dimension"
    )
    if [identifier for identifier, _values in dimensions] != list(OBSERVATION_DIMENSION_IDS):
        raise ValueError("OECD observation dimensions do not match the expected structure")

    requested_codes = tuple(key.split("."))
    for position, (identifier, values) in enumerate(dimensions):
        if identifier == "REF_AREA" or identifier == "TIME_PERIOD":
            continue
        requested_code = requested_codes[position]
        matches = [value for value in values if value["id"] == requested_code]
        if len(matches) != 1:
            raise ValueError(
                f"OECD dimension {identifier!r} does not match requested code {requested_code!r}"
            )
    if requested_codes[0]:
        ref_area_values = dimensions[0][1]
        if not any(value["id"] == requested_codes[0] for value in ref_area_values):
            raise ValueError(
                f"OECD REF_AREA dimension does not contain requested code {requested_codes[0]!r}"
            )

    period_values = dimensions[8][1]
    window_start = parse_period(start_period, "start_period")
    window_end = parse_period(end_period, "end_period")
    for raw_period in period_values:
        period_id = raw_period["id"]
        period_key = parse_period(period_id, "OECD observation period")
        if not window_start <= period_key <= window_end:
            raise ValueError(f"OECD observation period {period_id} falls outside the requested window")

    raw_attributes = structure.get("attributes")
    observation_attributes: list[tuple[str, list[Mapping[str, Any]]]] = []
    if raw_attributes is not None:
        if not isinstance(raw_attributes, Mapping):
            raise ValueError("OECD structure attributes must be an object")
        raw_observation_attributes = raw_attributes.get("observation")
        if raw_observation_attributes is not None:
            observation_attributes = _definitions(
                raw_observation_attributes,
                "observation attribute",
                allow_empty_values=True,
            )

    datasets = data.get("dataSets")
    if not isinstance(datasets, list) or len(datasets) != 1 or not isinstance(datasets[0], Mapping):
        raise ValueError("OECD response must contain exactly one dataset object")
    dataset = datasets[0]
    action = dataset.get("action")
    if action is not None and not isinstance(action, str):
        raise ValueError("OECD dataset action must be a string")
    observations = dataset.get("observations")
    if not isinstance(observations, Mapping):
        raise ValueError("OECD dataset observations must be an object")

    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw_observation_key, raw_observation in observations.items():
        parts = str(raw_observation_key).split(":")
        if len(parts) != len(dimensions):
            raise ValueError("OECD observation key has the wrong dimension count")
        indexes: list[int] = []
        for part in parts:
            if not _INDEX_RE.fullmatch(part):
                raise ValueError(f"OECD observation key has invalid index {part!r}")
            indexes.append(int(part))
        if not isinstance(raw_observation, list) or not raw_observation:
            raise ValueError("OECD observation must be a non-empty array")
        if len(raw_observation) > len(observation_attributes) + 1:
            raise ValueError("OECD observation has more attributes than its structure")
        value = raw_observation[0]
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
            raise ValueError("OECD observation value must be a finite number")
        attributes: dict[str, Mapping[str, Any] | None] = {}
        for position, raw_index in enumerate(raw_observation[1:]):
            identifier, values = observation_attributes[position]
            if raw_index is None:
                attributes[identifier] = None
                continue
            if not isinstance(raw_index, int) or isinstance(raw_index, bool) or raw_index < 0:
                raise ValueError(
                    f"OECD observation {raw_observation_key!r} has an invalid attribute index"
                )
            if raw_index >= len(values):
                raise ValueError(
                    f"OECD observation {raw_observation_key!r} attribute index is out of range"
                )
            attributes[identifier] = dict(values[raw_index])
        ref_area_code = dimensions[0][1][indexes[0]]["id"]
        period = period_values[indexes[8]]["id"]
        observation_id = stable_observation_id(ref_area_code, period)
        if observation_id in seen_ids:
            raise ValueError("OECD response contains duplicate observation identifiers")
        seen_ids.add(observation_id)
        records.append(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": observation_id,
                "ref_area": ref_area_code,
                "ref_area_label": _value_label(dimensions[0][1][indexes[0]]),
                "period": period,
                "index": value,
                "dataflow": DATAFLOW_ID,
                "dataflow_version": DATAFLOW_VERSION,
                "prepared_at": prepared_at,
                "attributes": attributes,
                "source_metadata": {
                    "provider": "OECD",
                    "attribution": ATTRIBUTION,
                    "dataset_action": action,
                },
            }
        )
    records.sort(key=lambda record: (record["ref_area"], record["period"]))
    return records


def fetch_observations(
    key: str = DEFAULT_KEY,
    start_period: str | None = None,
    end_period: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_delay: int = DEFAULT_RETRY_DELAY,
) -> list[dict[str, Any]]:
    """Make one bounded OECD request, retrying transient failures, and fully validate it."""
    LAST_RUN_STATS.clear()
    LAST_RUN_STATS.update(
        {
            "source": SOURCE,
            "flow_ref": FLOW_REF,
            "key": key,
            "records": 0,
            "response_bytes": 0,
            "requests": {"attempted": 0, "retried": 0},
        }
    )
    default_start, default_end = default_period_window()
    start_period = default_start if start_period is None else start_period
    end_period = default_end if end_period is None else end_period
    validate_request(key, start_period, end_period, timeout, retries, retry_delay)
    LAST_RUN_STATS["window"] = {"start": start_period, "end": end_period}
    url = build_url(key, start_period, end_period)
    body, status, _attempts, _retried = _fetch_with_retries(url, timeout, retries, retry_delay)
    LAST_RUN_STATS["response_bytes"] = len(body)
    if status is not None:
        LAST_RUN_STATS["status"] = status
    document = load_response(body)
    records = parse_response(document, key, start_period, end_period, datetime.now(timezone.utc).isoformat())
    LAST_RUN_STATS["records"] = len(records)
    return records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("key", nargs="?", default=DEFAULT_KEY)
    parser.add_argument("start_period", nargs="?")
    parser.add_argument("end_period", nargs="?")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=int, default=DEFAULT_RETRY_DELAY)
    return parser


def _summary(*, failed: bool = False, error: str | None = None) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "health": "failed" if failed else "healthy",
        "completeness": "failed" if failed else "complete",
        "records": LAST_RUN_STATS.get("records", 0),
        "requests": dict(LAST_RUN_STATS.get("requests", {"attempted": 0, "retried": 0})),
        "metrics": {
            key: value
            for key, value in LAST_RUN_STATS.items()
            if key not in {"source", "records", "requests"}
        },
    }
    if error is not None:
        summary["error"] = error
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if (args.start_period is None) != (args.end_period is None):
        print(
            SUMMARY_PREFIX
            + json.dumps(
                _summary(failed=True, error="start_period and end_period must be supplied together"),
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1
    try:
        records = fetch_observations(
            key=args.key,
            start_period=args.start_period,
            end_period=args.end_period,
            timeout=args.timeout,
            retries=args.retries,
            retry_delay=args.retry_delay,
        )
    except Exception as exc:
        print(
            SUMMARY_PREFIX
            + json.dumps(_summary(failed=True, error=str(exc)), separators=(",", ":")),
            file=sys.stderr,
        )
        return 1
    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False))
    print(SUMMARY_PREFIX + json.dumps(_summary(), separators=(",", ":")), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
