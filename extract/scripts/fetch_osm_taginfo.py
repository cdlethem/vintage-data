#!/usr/bin/env python3
"""Fetch bounded OpenStreetMap tag observations from the public Taginfo API.

Only two read-only API methods are used: ``/api/4/key/stats`` and the first,
count-descending page of ``/api/4/key/values``. The maintained key basket and
hard limits intentionally avoid corpus-wide key/value or database extraction.
All responses are validated and buffered before output, so an unavailable or
malformed endpoint cannot publish a partial snapshot.

Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from typing import Any, Iterator, Sequence
import urllib.error
import urllib.parse
import urllib.request


SOURCE = "osm_taginfo"
API_ROOT = "https://taginfo.openstreetmap.org/api/4"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_KEYS = ("amenity", "building", "highway", "shop")
DEFAULT_VALUE_LIMIT = 50
MAX_VALUE_LIMIT = 100
MAX_KEYS = 10
MAX_STAT_ROWS = 4
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120


class TaginfoError(RuntimeError):
    """A Taginfo response cannot satisfy the extractor contract."""


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def validate_keys(keys: Sequence[str]) -> tuple[str, ...]:
    """Return a bounded, duplicate-free sequence of valid OSM keys."""
    if isinstance(keys, (str, bytes)):
        raise ValueError("keys must be a sequence of OSM key strings")
    requested = tuple(keys)
    if not requested:
        raise ValueError("at least one OSM key is required")
    if len(requested) > MAX_KEYS:
        raise ValueError(f"at most {MAX_KEYS} OSM keys may be requested")
    if len(set(requested)) != len(requested):
        raise ValueError("OSM keys must not contain duplicates")
    for key in requested:
        if (
            not isinstance(key, str)
            or not key
            or len(key) > 255
            or any(ord(character) < 32 or ord(character) == 127 for character in key)
        ):
            raise ValueError(f"invalid OSM key {key!r}")
    return requested


def _request_json(endpoint: str, params: dict[str, Any], timeout: int) -> Any:
    query = urllib.parse.urlencode(params)
    url = f"{API_ROOT}/{endpoint}?{query}"
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read(MAX_RESPONSE_BYTES + 1)
            if len(payload) > MAX_RESPONSE_BYTES:
                raise TaginfoError(
                    f"Taginfo {endpoint} response exceeded {MAX_RESPONSE_BYTES} bytes "
                    f"for key {params['key']!r}"
                )
            try:
                return json.loads(payload)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise TaginfoError(
                    f"Taginfo {endpoint} returned malformed JSON for key {params['key']!r}"
                ) from error
    except urllib.error.HTTPError as error:
        raise TaginfoError(
            f"Taginfo {endpoint} returned HTTP {error.code} for key {params['key']!r}"
        ) from error
    except urllib.error.URLError as error:
        raise TaginfoError(
            f"Taginfo {endpoint} was unavailable for key {params['key']!r}: {error.reason}"
        ) from error
    except TimeoutError as error:
        raise TaginfoError(
            f"Taginfo {endpoint} timed out for key {params['key']!r}"
        ) from error


def _required_int(value: Any, field: str, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TaginfoError(f"Taginfo {context} has invalid {field}")
    return value


def _required_fraction(value: Any, field: str, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 1:
        raise TaginfoError(f"Taginfo {context} has invalid {field}")
    return float(value)


def _parse_envelope(document: Any, endpoint: str, key: str) -> tuple[list[Any], str, dict[str, Any]]:
    context = f"{endpoint} response for key {key!r}"
    if not isinstance(document, dict):
        raise TaginfoError(f"Taginfo {context} must be an object")
    data = document.get("data")
    if not isinstance(data, list):
        raise TaginfoError(f"Taginfo {context}.data must be a list")
    data_until = document.get("data_until")
    if not isinstance(data_until, str) or not data_until:
        raise TaginfoError(f"Taginfo {context} has invalid data_until")
    metadata = {name: value for name, value in document.items() if name != "data"}
    return data, data_until, metadata


def _record_id(endpoint: str, key: str, identity: str) -> str:
    encoded_key = urllib.parse.quote(key, safe="")
    encoded_identity = urllib.parse.quote(identity, safe="")
    return f"{endpoint}:{encoded_key}:{encoded_identity}"


def _normalize_stat(
    row: Any,
    key: str,
    fetched_at: str,
    data_until: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    context = f"key/stats row for key {key!r}"
    if not isinstance(row, dict):
        raise TaginfoError(f"Taginfo {context} must be an object")
    observation_type = row.get("type")
    if not isinstance(observation_type, str) or not observation_type:
        raise TaginfoError(f"Taginfo {context} has invalid type")
    count = _required_int(row.get("count"), "count", context)
    fraction = _required_fraction(row.get("count_fraction"), "count_fraction", context)
    values_count = _required_int(row.get("values"), "values", context)
    return {
        "source": SOURCE,
        "id": _record_id("key_stats", key, observation_type),
        "fetched_at": fetched_at,
        "endpoint": "key/stats",
        "key": key,
        "value": None,
        "observation_type": observation_type,
        "count": count,
        "fraction": fraction,
        "values_count": values_count,
        "data_until": data_until,
        "response_metadata": dict(metadata),
        "raw_payload": dict(row),
    }


def _normalize_value(
    row: Any,
    key: str,
    fetched_at: str,
    data_until: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    context = f"key/values row for key {key!r}"
    if not isinstance(row, dict):
        raise TaginfoError(f"Taginfo {context} must be an object")
    value = row.get("value")
    if not isinstance(value, str):
        raise TaginfoError(f"Taginfo {context} has invalid value")
    count = _required_int(row.get("count"), "count", context)
    fraction = _required_fraction(row.get("fraction"), "fraction", context)
    return {
        "source": SOURCE,
        "id": _record_id("key_values", key, value),
        "fetched_at": fetched_at,
        "endpoint": "key/values",
        "key": key,
        "value": value,
        "observation_type": "value_frequency",
        "count": count,
        "fraction": fraction,
        "values_count": None,
        "data_until": data_until,
        "response_metadata": dict(metadata),
        "raw_payload": dict(row),
    }


def fetch_taginfo(
    keys: Sequence[str] = DEFAULT_KEYS,
    value_limit: int = DEFAULT_VALUE_LIMIT,
    timeout: int = DEFAULT_TIMEOUT,
) -> Iterator[dict[str, Any]]:
    """Fetch selected key statistics and bounded top-value observations."""
    requested_keys = validate_keys(keys)
    _bounded_int(value_limit, "value_limit", 1, MAX_VALUE_LIMIT)
    _bounded_int(timeout, "timeout", 1, MAX_TIMEOUT)

    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for key in requested_keys:
        stats_document = _request_json("key/stats", {"key": key}, timeout)
        stats_rows, stats_until, stats_metadata = _parse_envelope(
            stats_document, "key/stats", key
        )
        if not stats_rows:
            raise TaginfoError(f"Taginfo key/stats returned no observations for key {key!r}")
        if len(stats_rows) > MAX_STAT_ROWS:
            raise TaginfoError(
                f"Taginfo key/stats exceeded its {MAX_STAT_ROWS}-row bound for key {key!r}"
            )
        key_records = [
            _normalize_stat(row, key, fetched_at, stats_until, stats_metadata)
            for row in stats_rows
        ]

        value_document = _request_json(
            "key/values",
            {
                "key": key,
                "page": 1,
                "rp": value_limit,
                "sortname": "count",
                "sortorder": "desc",
            },
            timeout,
        )
        value_rows, values_until, values_metadata = _parse_envelope(
            value_document, "key/values", key
        )
        page = _required_int(values_metadata.get("page"), "page", f"key/values for {key!r}")
        returned_rp = _required_int(
            values_metadata.get("rp"), "rp", f"key/values for {key!r}"
        )
        total = _required_int(
            values_metadata.get("total"), "total", f"key/values for {key!r}"
        )
        if page != 1:
            raise TaginfoError(f"Taginfo key/values for {key!r} returned page {page}, expected 1")
        if returned_rp != value_limit:
            raise TaginfoError(
                f"Taginfo key/values for {key!r} returned rp={returned_rp}, expected {value_limit}"
            )
        if len(value_rows) > value_limit:
            raise TaginfoError(
                f"Taginfo key/values for {key!r} exceeded value_limit={value_limit}"
            )
        if total < len(value_rows):
            raise TaginfoError(
                f"Taginfo key/values for {key!r} returned more rows than metadata.total"
            )
        key_records.extend(
            _normalize_value(row, key, fetched_at, values_until, values_metadata)
            for row in value_rows
        )

        for record in key_records:
            if record["id"] in seen_ids:
                raise TaginfoError(f"Taginfo response contains duplicate observation {record['id']!r}")
            seen_ids.add(record["id"])
            records.append(record)

    yield from records


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch bounded Taginfo key statistics and first-page value frequencies; "
            "repeat --key to replace the maintained key basket."
        )
    )
    parser.add_argument(
        "--key",
        action="append",
        dest="keys",
        help="OSM key to observe; repeat for multiple keys",
    )
    parser.add_argument("--value-limit", type=int, default=DEFAULT_VALUE_LIMIT)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)

    try:
        records = fetch_taginfo(
            keys=args.keys or DEFAULT_KEYS,
            value_limit=args.value_limit,
            timeout=args.timeout,
        )
        for record in records:
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    except ValueError as error:
        parser.error(str(error))
    except TaginfoError as error:
        parser.exit(1, f"Taginfo extraction failed: {error}\n")


if __name__ == "__main__":
    main()
