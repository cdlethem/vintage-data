#!/usr/bin/env python3
"""Fetch one bounded Eurostat JSON-stat dataset query as NDJSON.

The caller must select at least one non-time dataset dimension and either exact
``time`` values or both ends of a time range. Responses are decoded in JSON-stat
row-major order, including sparse value and status objects.

Stdlib only.
"""

import argparse
import hashlib
import json
import math
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Sequence

SOURCE = "eurostat_statistics"
API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
ACCEPT = "application/json"

_DATASET_RE = re.compile(r"[A-Za-z0-9_]+")
_PARAMETER_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_CANONICAL_INDEX_RE = re.compile(r"0|[1-9][0-9]*")
_CONTROL_PARAMETERS = frozenset({"format", "lang", "compressed"})
_TIME_PARAMETERS = frozenset({"time", "sinceTimePeriod", "untilTimePeriod"})
_SUPPORTED_FORMATS = frozenset({"JSON"})
_SUPPORTED_LANGUAGES = frozenset({"de", "en", "fr"})


def _single_parameter(
    grouped: Mapping[str, list[str]], name: str, default: str | None = None
) -> str | None:
    values = grouped.get(name)
    if values is None:
        return default
    if len(values) != 1:
        raise ValueError(f"query parameter {name!r} may appear only once")
    return values[0]


def validate_query(dataset_code: str, query: str, timeout: int) -> list[tuple[str, str]]:
    """Validate and normalize a bounded Eurostat query before HTTP access."""
    if not _DATASET_RE.fullmatch(dataset_code):
        raise ValueError("dataset_code must contain only letters, digits, and underscores")
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if not isinstance(query, str) or not query:
        raise ValueError("query is required")

    try:
        parameters = urllib.parse.parse_qsl(
            query, keep_blank_values=True, strict_parsing=True
        )
    except ValueError as exc:
        raise ValueError("query must be a valid URL query string") from exc
    if not parameters:
        raise ValueError("query is required")

    grouped: dict[str, list[str]] = {}
    for name, value in parameters:
        if not _PARAMETER_RE.fullmatch(name):
            raise ValueError(f"invalid query parameter name: {name!r}")
        if value == "":
            raise ValueError(f"query parameter {name!r} must not be empty")
        grouped.setdefault(name, []).append(value)

    response_format = _single_parameter(grouped, "format", "JSON")
    if response_format not in _SUPPORTED_FORMATS:
        raise ValueError("format must be JSON")
    language = _single_parameter(grouped, "lang", "en")
    if language not in _SUPPORTED_LANGUAGES:
        raise ValueError("lang must be one of: de, en, fr")
    compressed = _single_parameter(grouped, "compressed")
    if compressed is not None and compressed.lower() != "false":
        raise ValueError("compressed must be false")

    exact_times = grouped.get("time", [])
    range_start = grouped.get("sinceTimePeriod", [])
    range_end = grouped.get("untilTimePeriod", [])
    if exact_times and (range_start or range_end):
        raise ValueError("time cannot be combined with a time range")
    if exact_times:
        if len(exact_times) != len(set(exact_times)):
            raise ValueError("time values must not be duplicated")
    else:
        if len(range_start) != 1 or len(range_end) != 1:
            raise ValueError(
                "query requires time or both sinceTimePeriod and untilTimePeriod"
            )
        if range_start[0] > range_end[0]:
            raise ValueError("sinceTimePeriod must not be after untilTimePeriod")

    dimension_names = set(grouped).difference(
        _CONTROL_PARAMETERS | _TIME_PARAMETERS
    )
    if not dimension_names:
        raise ValueError("query requires a non-time dataset dimension filter")

    normalized = list(parameters)
    if "format" not in grouped:
        normalized.append(("format", "JSON"))
    if "lang" not in grouped:
        normalized.append(("lang", "en"))
    return normalized


def build_url(dataset_code: str, parameters: Sequence[tuple[str, str]]) -> str:
    """Build a statistics endpoint URL while preserving repeated dimensions."""
    encoded_dataset = urllib.parse.quote(dataset_code, safe="")
    return f"{API_BASE}/{encoded_dataset}?{urllib.parse.urlencode(parameters)}"


def _canonical_sparse_index(key: Any, coordinate_count: int, field: str) -> int:
    if not isinstance(key, str) or not _CANONICAL_INDEX_RE.fullmatch(key):
        raise ValueError(f"Eurostat {field} contains invalid sparse index {key!r}")
    index = int(key)
    if index >= coordinate_count:
        raise ValueError(
            f"Eurostat {field} sparse index {key!r} is outside "
            f"[0, {coordinate_count})"
        )
    return index


def _normalize_cells(
    value: Any, coordinate_count: int, field: str, *, optional: bool = False
) -> dict[int, Any]:
    if value is None and optional:
        return {}
    if isinstance(value, list):
        if len(value) != coordinate_count:
            raise ValueError(
                f"Eurostat {field} array length does not match coordinate count"
            )
        return dict(enumerate(value))
    if not isinstance(value, Mapping):
        raise ValueError(f"Eurostat {field} must be an array or sparse object")

    cells: dict[int, Any] = {}
    for key, cell in value.items():
        index = _canonical_sparse_index(key, coordinate_count, field)
        cells[index] = cell
    return cells


def _decode_category(
    dimension_name: str, raw_dimension: Any, expected_size: int
) -> tuple[list[str], list[str], list[Any]]:
    if not isinstance(raw_dimension, Mapping):
        raise ValueError(f"Eurostat dimension {dimension_name!r} must be an object")
    category = raw_dimension.get("category")
    if not isinstance(category, Mapping):
        raise ValueError(
            f"Eurostat dimension {dimension_name!r} has no category object"
        )
    raw_index = category.get("index")

    if isinstance(raw_index, list):
        codes = raw_index
    elif isinstance(raw_index, Mapping):
        codes_by_position: list[str | None] = [None] * expected_size
        for code, position in raw_index.items():
            if not isinstance(code, str) or not code:
                raise ValueError(
                    f"Eurostat dimension {dimension_name!r} has an invalid category code"
                )
            if (
                not isinstance(position, int)
                or isinstance(position, bool)
                or position < 0
                or position >= expected_size
                or codes_by_position[position] is not None
            ):
                raise ValueError(
                    f"Eurostat dimension {dimension_name!r} has invalid category indexes"
                )
            codes_by_position[position] = code
        if any(code is None for code in codes_by_position):
            raise ValueError(
                f"Eurostat dimension {dimension_name!r} category count does not match size"
            )
        codes = codes_by_position
    else:
        raise ValueError(
            f"Eurostat dimension {dimension_name!r} category index must be an array or object"
        )

    if (
        len(codes) != expected_size
        or any(not isinstance(code, str) or not code for code in codes)
        or len(set(codes)) != len(codes)
    ):
        raise ValueError(
            f"Eurostat dimension {dimension_name!r} category count does not match size"
        )

    raw_labels = category.get("label", {})
    if not isinstance(raw_labels, Mapping):
        raise ValueError(
            f"Eurostat dimension {dimension_name!r} category labels must be an object"
        )
    labels: list[str] = []
    for code in codes:
        label = raw_labels.get(code, code)
        if not isinstance(label, str):
            raise ValueError(
                f"Eurostat dimension {dimension_name!r} has an invalid category label"
            )
        labels.append(label)

    raw_units = category.get("unit", {})
    if not isinstance(raw_units, Mapping):
        raise ValueError(
            f"Eurostat dimension {dimension_name!r} category units must be an object"
        )
    units = [raw_units.get(code) for code in codes]
    return list(codes), labels, units


def stable_observation_id(
    dataset_code: str, dimension_names: Sequence[str], codes: Sequence[str]
) -> str:
    """Hash only the dataset and ordered coordinate codes."""
    identity = [dataset_code, list(zip(dimension_names, codes))]
    canonical = json.dumps(identity, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _coordinates(flat_index: int, sizes: Sequence[int]) -> list[int]:
    coordinates = [0] * len(sizes)
    for position in range(len(sizes) - 1, -1, -1):
        flat_index, coordinates[position] = divmod(flat_index, sizes[position])
    return coordinates


def parse_response(
    document: Any, dataset_code: str, fetched_at: str
) -> Iterator[dict[str, Any]]:
    """Validate a complete JSON-stat response, then emit every coordinate."""
    if not isinstance(document, Mapping):
        raise ValueError("Eurostat response must be a JSON object")
    if document.get("class") != "dataset":
        raise ValueError("Eurostat response class must be 'dataset'")

    dimension_names = document.get("id")
    sizes = document.get("size")
    dimensions = document.get("dimension")
    if (
        not isinstance(dimension_names, list)
        or not dimension_names
        or any(not isinstance(name, str) or not name for name in dimension_names)
        or len(set(dimension_names)) != len(dimension_names)
    ):
        raise ValueError("Eurostat response id must contain unique dimension names")
    if (
        not isinstance(sizes, list)
        or len(sizes) != len(dimension_names)
        or any(
            not isinstance(size, int) or isinstance(size, bool) or size < 0
            for size in sizes
        )
    ):
        raise ValueError("Eurostat response size must match id and be nonnegative")
    if not isinstance(dimensions, Mapping):
        raise ValueError("Eurostat response dimension must be an object")

    codes_by_dimension: list[list[str]] = []
    labels_by_dimension: list[list[str]] = []
    units_by_dimension: list[list[Any]] = []
    dimension_titles: dict[str, str] = {}
    for name, size in zip(dimension_names, sizes):
        if name not in dimensions:
            raise ValueError(f"Eurostat response is missing dimension {name!r}")
        raw_dimension = dimensions[name]
        codes, labels, units = _decode_category(name, raw_dimension, size)
        codes_by_dimension.append(codes)
        labels_by_dimension.append(labels)
        units_by_dimension.append(units)
        title = raw_dimension.get("label", name)
        if not isinstance(title, str):
            raise ValueError(f"Eurostat dimension {name!r} label must be a string")
        dimension_titles[name] = title

    coordinate_count = math.prod(sizes)
    values = _normalize_cells(document.get("value"), coordinate_count, "value")
    statuses = _normalize_cells(
        document.get("status"), coordinate_count, "status", optional=True
    )

    metadata = {
        key: value
        for key, value in document.items()
        if key not in {"value", "status", "dimension", "id", "size"}
    }
    dataset_label = document.get("label")
    dataset_source = document.get("source")
    updated_at = document.get("updated")

    for flat_index in range(coordinate_count):
        positions = _coordinates(flat_index, sizes)
        coordinate_codes = [
            codes_by_dimension[index][position]
            for index, position in enumerate(positions)
        ]
        coordinate_labels = [
            labels_by_dimension[index][position]
            for index, position in enumerate(positions)
        ]
        dimension_values = dict(zip(dimension_names, coordinate_codes))
        dimension_labels = dict(zip(dimension_names, coordinate_labels))
        category_units = {
            name: units_by_dimension[index][position]
            for index, (name, position) in enumerate(zip(dimension_names, positions))
            if units_by_dimension[index][position] is not None
        }
        unit_code = dimension_values.get("unit")

        yield {
            "source": SOURCE,
            "dataset_code": dataset_code,
            "id": stable_observation_id(
                dataset_code, dimension_names, coordinate_codes
            ),
            "fetched_at": fetched_at,
            "updated_at": updated_at,
            "dataset_label": dataset_label,
            "dataset_source": dataset_source,
            "metadata": metadata,
            "dimension_titles": dimension_titles,
            "dimensions": dimension_values,
            "labels": dimension_labels,
            "period": dimension_values.get("time"),
            "unit": unit_code,
            "unit_label": dimension_labels.get("unit") if unit_code is not None else None,
            "category_units": category_units,
            "value": values.get(flat_index),
            "status": statuses.get(flat_index),
        }


def fetch_eurostat_statistics(
    dataset_code: str, query: str, timeout: int = 60
) -> Iterator[dict[str, Any]]:
    """Fetch and decode one bounded Eurostat JSON-stat request."""
    parameters = validate_query(dataset_code, query, timeout)
    request = urllib.request.Request(
        build_url(dataset_code, parameters),
        headers={
            "Accept": ACCEPT,
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        },
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    yield from parse_response(document, dataset_code, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-code", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args(argv)

    for record in fetch_eurostat_statistics(
        dataset_code=args.dataset_code,
        query=args.query,
        timeout=args.timeout,
    ):
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
