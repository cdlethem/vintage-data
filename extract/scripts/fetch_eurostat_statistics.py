#!/usr/bin/env python3
"""Fetch a bounded Eurostat JSON-stat dataset and emit one NDJSON row per cell.

Eurostat's dissemination endpoint serves a complete JSON-stat response for a
query. This fetcher deliberately requires both a time selection and at least
one non-time dimension selection, rather than allowing a dataset catalogue or
an unbounded table download.
"""
import argparse
import hashlib
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Sequence

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
HEADERS = {"Accept": "application/json", "User-Agent": USER_AGENT}

MAX_TIME_PERIODS = 24
MAX_FILTER_VALUES = 100
MAX_COORDINATES = 100_000
DATASET_CODE_RE = re.compile(r"^[A-Za-z0-9_]+$")
PARAMETER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
# Eurostat period codes are individual annual, quarterly, monthly, daily, or
# weekly periods. Ranges are intentionally not accepted: repeat time= instead.
TIME_PERIOD_RE = re.compile(r"^\d{4}(?:-(?:Q[1-4]|W\d{2}|\d{2}(?:-\d{2})?))?$")


class EurostatResponseError(ValueError):
    """The server returned data that is not a usable JSON-stat dataset."""


def _parse_query(query_parts: Sequence[str]) -> list[tuple[str, str]]:
    """Parse and bound URL query fragments supplied after the dataset code."""
    raw_query = "&".join(part.lstrip("?") for part in query_parts)
    if not raw_query:
        raise ValueError("at least one bounded dimension filter and time filter are required")
    try:
        pairs = urllib.parse.parse_qsl(raw_query, keep_blank_values=True, strict_parsing=True)
    except ValueError as error:
        raise ValueError("query parameters must be key=value pairs") from error
    if not pairs:
        raise ValueError("at least one bounded dimension filter and time filter are required")
    if len(pairs) > MAX_FILTER_VALUES:
        raise ValueError(f"query has more than {MAX_FILTER_VALUES} filter values")

    time_values: list[str] = []
    non_time_filters = 0
    for name, value in pairs:
        if not PARAMETER_RE.fullmatch(name) or not value:
            raise ValueError("query parameters must have safe names and non-empty values")
        if name == "time":
            time_values.append(value)
        elif name != "lang":
            non_time_filters += 1

    if not time_values:
        raise ValueError("a bounded time filter is required")
    if len(time_values) > MAX_TIME_PERIODS:
        raise ValueError(f"time filter has more than {MAX_TIME_PERIODS} periods")
    if any(not TIME_PERIOD_RE.fullmatch(period) for period in time_values):
        raise ValueError("time filters must be individual Eurostat period codes; repeat time= for each period")
    if not non_time_filters:
        raise ValueError("at least one non-time dimension filter is required")
    return pairs


def build_request(dataset_code: str, query_parts: Sequence[str]) -> urllib.request.Request:
    """Build a JSON request after rejecting catalogue-sized requests."""
    if not DATASET_CODE_RE.fullmatch(dataset_code):
        raise ValueError("dataset code must contain only letters, digits, and underscores")
    query = urllib.parse.urlencode(_parse_query(query_parts))
    return urllib.request.Request(
        f"{BASE_URL}{urllib.parse.quote(dataset_code)}?{query}", headers=HEADERS
    )


def _ordered_categories(dimension: Mapping[str, Any], expected_size: int, dimension_id: str) -> list[tuple[str, str]]:
    category = dimension.get("category")
    if not isinstance(category, Mapping):
        raise EurostatResponseError(f"dimension {dimension_id!r} has no category mapping")
    index = category.get("index")
    if isinstance(index, Mapping):
        ordered: list[tuple[str, str] | None] = [None] * expected_size
        for code, position in index.items():
            if not isinstance(code, str) or isinstance(position, bool) or not isinstance(position, int):
                raise EurostatResponseError(f"dimension {dimension_id!r} has an invalid category index")
            if position < 0 or position >= expected_size or ordered[position] is not None:
                raise EurostatResponseError(f"dimension {dimension_id!r} has a non-contiguous category index")
            ordered[position] = (code, code)
        if any(item is None for item in ordered):
            raise EurostatResponseError(f"dimension {dimension_id!r} has a non-contiguous category index")
        categories = [item for item in ordered if item is not None]
    elif isinstance(index, list):
        if len(index) != expected_size or any(not isinstance(code, str) for code in index) or len(set(index)) != len(index):
            raise EurostatResponseError(f"dimension {dimension_id!r} has an invalid category index")
        categories = [(code, code) for code in index]
    else:
        raise EurostatResponseError(f"dimension {dimension_id!r} has no category index")

    labels = category.get("label", {})
    if labels is not None and not isinstance(labels, Mapping):
        raise EurostatResponseError(f"dimension {dimension_id!r} has invalid category labels")
    return [(code, labels.get(code, code) if labels else code) for code, _ in categories]


def _dataset_shape(data: Mapping[str, Any]) -> tuple[list[str], list[int], list[list[tuple[str, str]]]]:
    if data.get("class") != "dataset":
        raise EurostatResponseError("response is not a JSON-stat dataset")
    dimension_ids = data.get("id")
    sizes = data.get("size")
    dimensions = data.get("dimension")
    if not isinstance(dimension_ids, list) or not isinstance(sizes, list) or not isinstance(dimensions, Mapping):
        raise EurostatResponseError("response has no JSON-stat dimensions")
    if not dimension_ids and not sizes:
        return [], [], []
    if len(dimension_ids) != len(sizes) or not dimension_ids:
        raise EurostatResponseError("response has mismatched JSON-stat dimensions")

    categories: list[list[tuple[str, str]]] = []
    coordinate_count = 1
    for dimension_id, size in zip(dimension_ids, sizes):
        if not isinstance(dimension_id, str) or isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise EurostatResponseError("response has invalid JSON-stat dimension sizes")
        coordinate_count *= size
        if coordinate_count > MAX_COORDINATES:
            raise EurostatResponseError(f"response exceeds {MAX_COORDINATES} coordinates")
        dimension = dimensions.get(dimension_id)
        if not isinstance(dimension, Mapping):
            raise EurostatResponseError(f"response is missing dimension {dimension_id!r}")
        categories.append(_ordered_categories(dimension, size, dimension_id))
    return dimension_ids, sizes, categories


def _coordinate_id(dataset_code: str, coordinates: Sequence[tuple[str, str]]) -> str:
    encoded = json.dumps([dataset_code, list(coordinates)], separators=(",", ":"), ensure_ascii=True)
    return "eurostat_statistics:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def normalize_response(data: Mapping[str, Any], dataset_code: str, fetched_at: str | None = None) -> Iterator[dict[str, Any]]:
    """Decode JSON-stat's row-major sparse value map into stable cell records."""
    if not isinstance(data, Mapping):
        raise EurostatResponseError("response must be a JSON object")
    dimension_ids, sizes, categories = _dataset_shape(data)
    if not dimension_ids:
        return

    values = data.get("value", {})
    statuses = data.get("status", {})
    if values is None:
        values = {}
    if statuses is None:
        statuses = {}
    if not isinstance(values, Mapping) or not isinstance(statuses, Mapping):
        raise EurostatResponseError("response values and statuses must be sparse JSON objects")

    dimension_labels = {
        dimension_id: data["dimension"][dimension_id].get("label", dimension_id)
        for dimension_id in dimension_ids
    }
    metadata = {
        key: data[key]
        for key in ("label", "source", "updated", "version")
        if key in data
    }
    fetched_at = fetched_at or datetime.now(timezone.utc).isoformat()

    for flat_index in range(_coordinate_count(sizes)):
        remainder = flat_index
        selected: list[tuple[str, str]] = []
        decoded_dimensions: dict[str, dict[str, str]] = {}
        for position in range(len(dimension_ids) - 1, -1, -1):
            remainder, category_index = divmod(remainder, sizes[position])
            code, label = categories[position][category_index]
            dimension_id = dimension_ids[position]
            selected.append((dimension_id, code))
            decoded_dimensions[dimension_id] = {
                "code": code,
                "label": label,
                "dimension_label": dimension_labels[dimension_id],
            }
        selected.reverse()
        value_key = str(flat_index)
        yield {
            "id": _coordinate_id(dataset_code, selected),
            "source": "eurostat_statistics",
            "dataset_code": dataset_code,
            "dataset_metadata": metadata,
            "updated_at": data.get("updated"),
            "fetched_at": fetched_at,
            "dimensions": decoded_dimensions,
            "value": values.get(value_key),
            "status": statuses.get(value_key),
        }


def _coordinate_count(sizes: Sequence[int]) -> int:
    count = 1
    for size in sizes:
        count *= size
    return count


def fetch(dataset_code: str, query_parts: Sequence[str]) -> Iterator[dict[str, Any]]:
    """Request one bounded table query and yield normalized JSON-stat cells."""
    request = build_request(dataset_code, query_parts)
    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.load(response)
    yield from normalize_response(data, dataset_code)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset_code", help="Eurostat dataset code, for example nama_10_gdp")
    parser.add_argument(
        "query",
        nargs="+",
        help="bounded URL parameters, for example geo=DE time=2024",
    )
    args = parser.parse_args(argv)
    try:
        for record in fetch(args.dataset_code, args.query):
            print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    except (EurostatResponseError, ValueError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    sys.exit(main())
