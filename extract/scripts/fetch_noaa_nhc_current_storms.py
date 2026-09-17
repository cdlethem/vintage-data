#!/usr/bin/env python3
"""Snapshot NOAA/NHC's keyless CurrentStorms JSON feed as NDJSON.

Each output row is one official storm at one UTC fetch time. The stable NHC storm
identifier is retained separately from the observation ``id``; the latter is a
hash of the storm identifier and fetch time so unchanged storms fetched later
remain distinct observations. The complete storm object and response metadata
are retained for source fidelity and future schema evolution.

NOAA/NHC data are United States government works. Attribute the National
Hurricane Center. Stdlib only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator

SOURCE = "noaa_nhc_current_storms"
URL = "https://www.nhc.noaa.gov/CurrentStorms.json"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
BASIN_CODE = re.compile(r"^[a-zA-Z]{2}")
COORDINATE = re.compile(r"^([+-]?\d+(?:\.\d+)?)\s*([NSEW])?$", re.IGNORECASE)


def _utc_timestamp(value: Any, field: str) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO-8601 string or null")
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a UTC offset")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def _number(value: Any, field: str) -> int | float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} is not numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is not numeric") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} is not finite")
    return int(number) if number.is_integer() else number


def _coordinate(storm: dict[str, Any], axis: str) -> int | float | None:
    numeric_key = f"{axis}Numeric"
    if storm.get(numeric_key) not in (None, ""):
        number = _number(storm[numeric_key], numeric_key)
    else:
        value = storm.get(axis)
        if value in (None, ""):
            return None
        if not isinstance(value, str):
            raise ValueError(f"{axis} must be a coordinate string or null")
        match = COORDINATE.fullmatch(value.strip())
        if match is None:
            raise ValueError(f"{axis} is not a valid coordinate")
        number = float(match.group(1))
        hemisphere = (match.group(2) or "").upper()
        if hemisphere in {"S", "W"}:
            number = -abs(number)
        elif hemisphere in {"N", "E"}:
            number = abs(number)
        number = int(number) if number.is_integer() else number

    limit = 90 if axis == "latitude" else 180
    if number is not None and not -limit <= number <= limit:
        raise ValueError(f"{axis} is outside [-{limit}, {limit}]")
    return number


def _product_url(storm: dict[str, Any], key: str) -> str | None:
    product = storm.get(key)
    if product is None:
        return None
    if isinstance(product, str):
        return product or None
    if not isinstance(product, dict):
        raise ValueError(f"{key} must be an object, URL string, or null")
    url = product.get("url")
    if url is not None and not isinstance(url, str):
        raise ValueError(f"{key}.url must be a string or null")
    return url or None


def _observation_id(storm_id: str, fetched_at: str) -> str:
    identity = f"{SOURCE}|{storm_id}|{fetched_at}".encode("utf-8")
    return hashlib.sha256(identity).hexdigest()


def normalize_storm(
    storm: Any,
    fetched_at: str,
    raw_response: dict[str, Any],
) -> dict[str, Any]:
    """Validate and normalize one storm while retaining its complete payload."""
    if not isinstance(storm, dict):
        raise ValueError("NHC active storm is not an object")

    storm_id = storm.get("id")
    if not isinstance(storm_id, str) or not storm_id.strip():
        raise ValueError("NHC active storm is missing its official id")
    storm_id = storm_id.strip()

    basin = storm.get("basin")
    if basin is not None and not isinstance(basin, str):
        raise ValueError(f"NHC storm {storm_id!r} basin must be a string or null")
    if not basin:
        match = BASIN_CODE.match(storm_id)
        basin = match.group(0).upper() if match else None

    advisory_at = _utc_timestamp(storm.get("lastUpdate"), f"NHC storm {storm_id!r} lastUpdate")
    intensity = _number(storm.get("intensity"), f"NHC storm {storm_id!r} intensity")
    pressure = _number(storm.get("pressure"), f"NHC storm {storm_id!r} pressure")
    movement_direction = _number(
        storm.get("movementDir"), f"NHC storm {storm_id!r} movementDir"
    )
    movement_speed = _number(
        storm.get("movementSpeed"), f"NHC storm {storm_id!r} movementSpeed"
    )

    return {
        "source": SOURCE,
        "id": _observation_id(storm_id, fetched_at),
        "fetched_at": fetched_at,
        "storm_id": storm_id,
        "name": storm.get("name"),
        "basin": basin,
        "classification": storm.get("classification"),
        "intensity_kt": intensity,
        "pressure_mb": pressure,
        "latitude": _coordinate(storm, "latitude"),
        "longitude": _coordinate(storm, "longitude"),
        "movement_direction_degrees": movement_direction,
        "movement_speed_kt": movement_speed,
        "advisory_at": advisory_at,
        "public_advisory_url": _product_url(storm, "publicAdvisory"),
        "forecast_advisory_url": _product_url(storm, "forecastAdvisory"),
        "wind_speed_probabilities_url": _product_url(storm, "windSpeedProbabilities"),
        "raw_storm": dict(storm),
        "raw_response": raw_response,
    }


def fetch_current_storms(timeout: float = 30) -> Iterator[dict[str, Any]]:
    """Fetch and yield one complete current-storm snapshot."""
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    request = urllib.request.Request(
        URL,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        try:
            document = json.load(response)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("NHC response is malformed JSON") from exc

    if not isinstance(document, dict):
        raise ValueError("NHC response is not an object")
    storms = document.get("activeStorms")
    if not isinstance(storms, list):
        raise ValueError("NHC response is missing its activeStorms list")

    fetched_at = datetime.now(timezone.utc).isoformat()
    seen: set[str] = set()
    records: list[dict[str, Any]] = []
    for index, storm in enumerate(storms):
        try:
            record = normalize_storm(storm, fetched_at, document)
        except ValueError as exc:
            raise ValueError(f"NHC activeStorms[{index}]: {exc}") from exc
        if record["storm_id"] in seen:
            raise ValueError(f"NHC response repeats storm id {record['storm_id']!r}")
        seen.add(record["storm_id"])
        records.append(record)

    yield from records


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args(argv)

    try:
        records = list(fetch_current_storms(timeout=args.timeout))
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))
    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
