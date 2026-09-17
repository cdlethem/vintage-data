#!/usr/bin/env python3
"""Fetch a bounded set of recent USGS Water Data OGC observations.

The scheduled source is intentionally limited to explicitly named stations and
candidate collections. Pagination follows only the provider's returned ``next``
links. A state file outside the checkout preserves both page offsets and the
next target to visit, so global limits do not let one busy target starve peers.

This module uses only the Python standard library and does not require an API
key. The source configuration remains disabled pending operator verification of
the live provider contract.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from typing import Any
import urllib.parse
import urllib.request

SOURCE = "usgs_water_data_ogc"
BASE_URL = "https://api.waterdata.usgs.gov/ogcapi/v0"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
STATE_VERSION = 1
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
CANDIDATE_COLLECTIONS = frozenset({"continuous"})
DEFAULT_STATIONS = ("USGS-01646500", "USGS-09380000")
DEFAULT_COLLECTIONS = ("continuous",)
STATION_PATTERN = re.compile(r"(?:USGS-)?[0-9]{8,15}\Z")

_STATION_FIELDS = (
    "monitoring_location_id",
    "monitoringLocationIdentifier",
    "site_no",
    "site_number",
)
_TIME_FIELDS = ("time", "datetime", "phenomenon_time", "dateTime", "timestamp")
_PARAMETER_FIELDS = ("parameter_code", "parameterCode", "observed_property_id", "observedProperty", "variable_code")
_SERIES_FIELDS = ("time_series_id", "timeseries_id", "timeSeriesId", "series_id")
_STATISTIC_FIELDS = ("statistic_id", "statistic_code", "statisticCode")
_METHOD_FIELDS = ("method_id", "method_code", "methodCode")
_UNIT_FIELDS = ("unit_of_measure", "unit", "unit_code")
_VALUE_FIELDS = ("value", "result", "measurement_value")


class OGCResponseError(ValueError):
    """The provider returned a document outside the expected OGC contract."""


def _is_within(path: Path, directory: Path) -> bool:
    return path == directory or directory in path.parents


def validate_state_path(value: str | os.PathLike[str]) -> Path:
    """Resolve a state path and reject every path that addresses this checkout."""
    expanded = Path(value).expanduser()
    lexical = Path(os.path.abspath(os.fspath(expanded)))
    resolved = lexical.resolve(strict=False)
    repository = REPOSITORY_ROOT.resolve(strict=True)
    if _is_within(lexical, repository) or _is_within(resolved, repository):
        raise ValueError(f"state path must be outside the repository: {value}")
    return resolved


def default_state_path() -> Path:
    data_root = Path(os.environ.get("EXTRACT_DATA_ROOT") or "~/.local/share/vintage-data/extract").expanduser()
    return data_root / "state" / f"{SOURCE}.json"


def load_state(path: Path) -> dict[str, Any]:
    path = validate_state_path(path)
    if not path.exists():
        return {"version": STATE_VERSION, "next_target": None, "targets": {}}
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read state file {path}: {exc}") from exc
    if not isinstance(document, dict) or document.get("version") != STATE_VERSION:
        raise ValueError(f"state file {path} has an unsupported format")
    if not isinstance(document.get("targets"), dict):
        raise ValueError(f"state file {path} has invalid targets")
    next_target = document.get("next_target")
    if next_target is not None and not isinstance(next_target, str):
        raise ValueError(f"state file {path} has invalid next_target")
    return document


def save_state(path: Path, state: dict[str, Any]) -> None:
    """Durably replace state without exposing a partially written JSON file."""
    path = validate_state_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path = validate_state_path(path)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(state, output, sort_keys=True, separators=(",", ":"))
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _normalize_station(value: str) -> str:
    station = str(value).strip().upper()
    if not STATION_PATTERN.fullmatch(station):
        raise ValueError(f"invalid USGS station identifier: {value!r}")
    return station if station.startswith("USGS-") else f"USGS-{station}"


def _validate_collection(value: str) -> str:
    collection = str(value).strip()
    if collection not in CANDIDATE_COLLECTIONS:
        raise ValueError(f"collection is not explicitly allowed: {collection!r}")
    return collection


def build_targets(stations: list[str] | tuple[str, ...], collections: list[str] | tuple[str, ...]) -> list[dict[str, str]]:
    targets: list[dict[str, str]] = []
    seen: set[str] = set()
    for collection_value in collections:
        collection = _validate_collection(collection_value)
        for station_value in stations:
            station = _normalize_station(station_value)
            key = json.dumps([collection, station], separators=(",", ":"))
            if key not in seen:
                targets.append({"key": key, "collection": collection, "station": station})
                seen.add(key)
    if not targets:
        raise ValueError("at least one station and collection are required")
    return targets


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("current time must include a timezone")
    return value.astimezone(timezone.utc)


def _timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OGCResponseError(f"observation has no usable {field}")
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError as exc:
        raise OGCResponseError(f"observation has invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise OGCResponseError(f"observation {field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _first(properties: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        value = properties.get(name)
        if value is not None and value != "":
            return value
    return None


def _number(value: Any) -> int | float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise OGCResponseError("observation value must not be boolean")
    if isinstance(value, (int, float)):
        number = value
    elif isinstance(value, str):
        try:
            number = float(value)
        except ValueError as exc:
            raise OGCResponseError(f"observation value is not numeric: {value!r}") from exc
    else:
        raise OGCResponseError(f"observation value is not numeric: {value!r}")
    if not math.isfinite(number):
        raise OGCResponseError(f"observation value is not finite: {value!r}")
    if isinstance(number, float) and number.is_integer():
        return int(number)
    return number


def normalize_feature(feature: Any, target: dict[str, str], fetched_at: str) -> dict[str, Any]:
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise OGCResponseError("collection item is not a GeoJSON Feature")
    properties = feature.get("properties")
    if not isinstance(properties, dict):
        raise OGCResponseError("collection item properties are not an object")

    raw_station = _first(properties, _STATION_FIELDS)
    if raw_station is None:
        raise OGCResponseError("observation has no monitoring-location identifier")
    try:
        station = _normalize_station(str(raw_station))
    except ValueError as exc:
        raise OGCResponseError(str(exc)) from exc
    if station != target["station"]:
        raise OGCResponseError(f"observation station {station!r} does not match requested {target['station']!r}")

    raw_time = _first(properties, _TIME_FIELDS)
    observed_at = _timestamp(raw_time, "observation time")
    parameter = _first(properties, _PARAMETER_FIELDS)
    series = _first(properties, _SERIES_FIELDS)
    statistic = _first(properties, _STATISTIC_FIELDS)
    method = _first(properties, _METHOD_FIELDS)
    unit = _first(properties, _UNIT_FIELDS)
    if series is None and parameter is None:
        raise OGCResponseError("observation has no measurement identity")
    measurement_parts = {
        "series_id": str(series) if series is not None else None,
        "parameter_code": str(parameter) if parameter is not None else None,
        "statistic_id": str(statistic) if statistic is not None else None,
        "method_id": str(method) if method is not None else None,
        "unit": str(unit) if unit is not None else None,
    }
    measurement_id = json.dumps(measurement_parts, sort_keys=True, separators=(",", ":"))
    identity = json.dumps(
        [SOURCE, target["collection"], station, observed_at, measurement_id],
        separators=(",", ":"),
    ).encode("utf-8")
    provider_id = feature.get("id")
    return {
        "source": SOURCE,
        "id": hashlib.sha256(identity).hexdigest(),
        "fetched_at": fetched_at,
        "collection_id": target["collection"],
        "station_id": station,
        "observed_at": observed_at,
        "measurement": measurement_parts,
        "value": _number(_first(properties, _VALUE_FIELDS)),
        "provider_feature_id": str(provider_id) if provider_id is not None else None,
        "raw_observation": dict(properties),
        "raw_geometry": feature.get("geometry"),
    }


def _items_path(base_url: str, collection: str) -> str:
    base = urllib.parse.urlsplit(base_url.rstrip("/"))
    return f"{base.path}/collections/{urllib.parse.quote(collection, safe='')}/items"


def initial_url(base_url: str, target: dict[str, str], page_size: int, lookback_hours: float, now: datetime) -> str:
    end = _utc(now)
    start = end - timedelta(hours=lookback_hours)
    query = urllib.parse.urlencode(
        {
            "f": "json",
            "limit": page_size,
            "monitoring_location_id": target["station"],
            "datetime": f"{start.isoformat().replace('+00:00', 'Z')}/{end.isoformat().replace('+00:00', 'Z')}",
        }
    )
    return f"{base_url.rstrip('/')}/collections/{urllib.parse.quote(target['collection'], safe='')}/items?{query}"


def _validated_next_link(href: Any, current_url: str, target: dict[str, str], base_url: str) -> str:
    if not isinstance(href, str) or not href.strip():
        raise OGCResponseError("next link has no href")
    candidate = urllib.parse.urljoin(current_url, href.strip())
    expected_origin = urllib.parse.urlsplit(base_url.rstrip("/"))
    parsed = urllib.parse.urlsplit(candidate)
    if (parsed.scheme, parsed.netloc) != (expected_origin.scheme, expected_origin.netloc):
        raise OGCResponseError("next link leaves the configured OGC origin")
    if parsed.path.rstrip("/") != _items_path(base_url, target["collection"]).rstrip("/"):
        raise OGCResponseError("next link leaves the requested OGC collection")
    return candidate


def parse_page(document: Any, current_url: str, target: dict[str, str], base_url: str) -> tuple[list[Any], str | None]:
    if not isinstance(document, dict) or document.get("type") != "FeatureCollection":
        raise OGCResponseError("OGC response is not a GeoJSON FeatureCollection")
    response_collection = document.get("collection")
    if response_collection is not None and response_collection != target["collection"]:
        raise OGCResponseError(
            f"response collection {response_collection!r} does not match requested {target['collection']!r}"
        )
    features = document.get("features")
    if not isinstance(features, list):
        raise OGCResponseError("OGC response features are not an array")
    number_returned = document.get("numberReturned")
    if number_returned is not None and (isinstance(number_returned, bool) or number_returned != len(features)):
        raise OGCResponseError("OGC numberReturned does not match the feature count")
    links = document.get("links", [])
    if not isinstance(links, list):
        raise OGCResponseError("OGC response links are not an array")
    next_urls: list[str] = []
    for link in links:
        if not isinstance(link, dict):
            raise OGCResponseError("OGC response contains a malformed link")
        if link.get("rel") == "next":
            next_urls.append(_validated_next_link(link.get("href"), current_url, target, base_url))
    if len(next_urls) > 1:
        raise OGCResponseError("OGC response contains multiple next links")
    if next_urls and next_urls[0] == current_url:
        raise OGCResponseError("OGC next link repeats the current page")
    return features, next_urls[0] if next_urls else None


def _request_document(url: str, timeout: float, metrics: dict[str, int]) -> Any:
    request = urllib.request.Request(url, headers={"Accept": "application/geo+json, application/json", "User-Agent": USER_AGENT})
    metrics["requests"] += 1
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(MAX_RESPONSE_BYTES + 1)
    metrics["response_bytes"] += len(raw)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise OGCResponseError(f"OGC response exceeds {MAX_RESPONSE_BYTES} bytes")
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OGCResponseError(f"OGC response is not valid UTF-8 JSON: {exc}") from exc


def _continuations(state: dict[str, Any], targets: list[dict[str, str]], base_url: str) -> dict[str, dict[str, Any]]:
    known = {target["key"]: target for target in targets}
    result: dict[str, dict[str, Any]] = {}
    for key, continuation in state.get("targets", {}).items():
        if key not in known:
            continue
        if not isinstance(continuation, dict):
            raise ValueError(f"state continuation for {key} is invalid")
        url = continuation.get("url")
        offset = continuation.get("offset")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError(f"state continuation offset for {key} is invalid")
        validated = _validated_next_link(url, url, known[key], base_url)
        result[key] = {"url": validated, "offset": offset}
    return result


def collect_observations(
    targets: list[dict[str, str]],
    state: dict[str, Any],
    *,
    page_size: int,
    max_pages: int,
    max_records: int,
    lookback_hours: float,
    timeout: float,
    request_delay: float,
    now: datetime,
    base_url: str = BASE_URL,
    metrics: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    """Collect one bounded run and return records, next state, and run metrics."""
    if not 1 <= page_size <= 10_000:
        raise ValueError("page_size must be between 1 and 10000")
    if max_pages < 1 or max_records < 1:
        raise ValueError("max_pages and max_records must be positive")
    if not 0 < lookback_hours <= 24 * 31:
        raise ValueError("lookback_hours must be greater than zero and at most 744")
    if timeout <= 0 or request_delay < 0:
        raise ValueError("timeout must be positive and request_delay must not be negative")
    if not targets:
        raise ValueError("targets must not be empty")

    continuations = _continuations(state, targets, base_url)
    keys = [target["key"] for target in targets]
    requested_start = state.get("next_target")
    start_index = keys.index(requested_start) if requested_start in keys else 0
    ordered = targets[start_index:] + targets[:start_index]
    next_state: dict[str, Any] = {
        "version": STATE_VERSION,
        "next_target": keys[(start_index + 1) % len(keys)],
        "targets": continuations,
    }
    records: list[dict[str, Any]] = []
    request_metrics = {"requests": 0, "response_bytes": 0}
    metrics = {} if metrics is None else metrics
    metrics.update({
        "pages": 0,
        "records_received": 0,
        "targets_attempted": 0,
        "targets_completed": 0,
        "cap_reason": None,
        "requests": request_metrics,
    })
    fetched_at = _utc(now).isoformat(timespec="seconds").replace("+00:00", "Z")

    def capped(
        reason: str, target: dict[str, str], *, advance: bool = True
    ) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        metrics["cap_reason"] = reason
        index = keys.index(target["key"])
        next_state["next_target"] = keys[(index + (1 if advance else 0)) % len(keys)]
        return records, next_state, metrics

    for target in ordered:
        if len(records) >= max_records:
            return capped("records", target, advance=False)
        if metrics["pages"] >= max_pages:
            return capped("pages", target, advance=False)
        metrics["targets_attempted"] += 1
        continuation = continuations.get(target["key"])
        current_url = continuation["url"] if continuation else initial_url(
            base_url, target, page_size, lookback_hours, now
        )
        offset = continuation["offset"] if continuation else 0
        seen_pages: set[str] = set()
        while True:
            if current_url in seen_pages:
                raise OGCResponseError("OGC pagination cycle detected")
            seen_pages.add(current_url)
            if request_metrics["requests"]:
                time.sleep(request_delay)
            document = _request_document(current_url, timeout, request_metrics)
            metrics["pages"] += 1
            features, next_url = parse_page(document, current_url, target, base_url)
            metrics["records_received"] += len(features)
            if offset > len(features):
                raise OGCResponseError(
                    f"saved page offset {offset} exceeds returned feature count {len(features)}"
                )
            for index in range(offset, len(features)):
                if len(records) >= max_records:
                    continuations[target["key"]] = {"url": current_url, "offset": index}
                    return capped("records", target)
                records.append(normalize_feature(features[index], target, fetched_at))
            offset = 0
            if next_url is None:
                continuations.pop(target["key"], None)
                metrics["targets_completed"] += 1
                break
            continuations[target["key"]] = {"url": next_url, "offset": 0}
            if len(records) >= max_records:
                return capped("records", target)
            if metrics["pages"] >= max_pages:
                return capped("pages", target)
            current_url = next_url

    return records, next_state, metrics


def run_summary(
    metrics: dict[str, Any],
    *,
    records: int,
    targets: int,
    continuations: int,
    max_pages: int,
    max_records: int,
    failed: bool = False,
    error: str | None = None,
) -> dict[str, Any]:
    cap_reason = metrics.get("cap_reason")
    complete = not failed and cap_reason is None and continuations == 0
    summary: dict[str, Any] = {
        "health": "failed" if failed else "healthy",
        "completeness": "failed" if failed else ("complete" if complete else "partial"),
        "records": records,
        "requests": {
            "attempted": metrics.get("requests", {}).get("requests", 0),
            "bytes": metrics.get("requests", {}).get("response_bytes", 0),
        },
        "partitions": {
            "configured": targets,
            "attempted": metrics.get("targets_attempted", 0),
            "completed": metrics.get("targets_completed", 0),
            "remaining": max(0, targets - metrics.get("targets_completed", 0)),
            "continuations": continuations,
        },
        "coverage": {
            "pages": metrics.get("pages", 0),
            "records_received": metrics.get("records_received", 0),
            "cap_reason": cap_reason,
            "max_pages": max_pages,
            "max_records": max_records,
        },
    }
    if error is not None:
        summary["error"] = error
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--station", action="append", default=[], help="explicit USGS monitoring location; repeatable")
    parser.add_argument("--collection", action="append", default=[], help="explicit candidate OGC collection; repeatable")
    parser.add_argument("--lookback-hours", type=float, default=48.0)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=4)
    parser.add_argument("--max-records", type=int, default=400)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--request-delay", type=float, default=1.0)
    parser.add_argument("--state-path", default=None)
    return parser


def main(argv: list[str] | None = None, *, out: Any = None, err: Any = None) -> int:
    out = sys.stdout if out is None else out
    err = sys.stderr if err is None else err
    args = _parser().parse_args(argv)
    metrics: dict[str, Any] = {
        "pages": 0,
        "records_received": 0,
        "targets_attempted": 0,
        "targets_completed": 0,
        "cap_reason": None,
        "requests": {"requests": 0, "response_bytes": 0},
    }
    targets: list[dict[str, str]] = []
    emitted = 0
    state: dict[str, Any] = {"targets": {}}
    try:
        targets = build_targets(args.station or DEFAULT_STATIONS, args.collection or DEFAULT_COLLECTIONS)
        state_path = validate_state_path(args.state_path or default_state_path())
        state = load_state(state_path)
        records, next_state, metrics = collect_observations(
            targets,
            state,
            page_size=args.page_size,
            max_pages=args.max_pages,
            max_records=args.max_records,
            lookback_hours=args.lookback_hours,
            timeout=args.timeout,
            request_delay=args.request_delay,
            now=datetime.now(timezone.utc),
            metrics=metrics,
        )
        for record in records:
            out.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            emitted += 1
        out.flush()
        save_state(state_path, next_state)
    except Exception as exc:
        summary = run_summary(
            metrics,
            records=emitted,
            targets=len(targets),
            continuations=len(state.get("targets", {})) if isinstance(state.get("targets"), dict) else 0,
            max_pages=args.max_pages,
            max_records=args.max_records,
            failed=True,
            error=f"{type(exc).__name__}: {exc}",
        )
        print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=err)
        return 1

    summary = run_summary(
        metrics,
        records=emitted,
        targets=len(targets),
        continuations=len(next_state["targets"]),
        max_pages=args.max_pages,
        max_records=args.max_records,
    )
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=err)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
