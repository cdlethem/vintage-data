#!/usr/bin/env python3
"""Fetch bounded USGS Water Data OGC API observations as NDJSON.

The USGS catalog currently identifies daily values as the ``daily`` collection.  The
extractor verifies requested collection IDs against /collections on every run rather
than assuming that identifier remains available.  It only queries explicitly named
monitoring locations; it never enumerates the national station network.

A continuation consists of the provider's next-link URL and, when a record cap stops
inside a page, an offset within that page.  State is atomically published only after
all emitted records have been written by the caller, so interrupted runs replay data
rather than skip it.
"""

import argparse
import json
import os
import pathlib
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

SOURCE = "usgs_water_data_ogc"
API_ROOT = "https://api.waterdata.usgs.gov/ogcapi/v0"
COLLECTIONS_URL = f"{API_ROOT}/collections"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
DEFAULT_USER_AGENT = "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Return the external continuation-state path."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "usgs_water_data_ogc.json"


def _ensure_external_state_path(path: pathlib.Path) -> None:
    """Do not allow the operational cursor to be persisted in this checkout."""
    try:
        path.resolve().relative_to(pathlib.Path.cwd().resolve())
    except ValueError:
        return
    raise ValueError("state file must be outside the repository working directory")


def load_state(path: pathlib.Path) -> dict:
    _ensure_external_state_path(path)
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"version": 1, "continuations": {}}
    continuations = state.get("continuations")
    if state.get("version") != 1 or not isinstance(continuations, dict):
        raise ValueError(f"malformed continuation state in {path}")
    for key, cursor in continuations.items():
        if (not isinstance(key, str) or not isinstance(cursor, dict)
                or not isinstance(cursor.get("url"), str)
                or not isinstance(cursor.get("offset"), int)
                or cursor["offset"] < 0):
            raise ValueError(f"malformed continuation state in {path}")
    return state


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish state only after the caller has emitted every record."""
    _ensure_external_state_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    try:
        with staged.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(staged, path)
    except BaseException:
        try:
            staged.unlink()
        except FileNotFoundError:
            pass
        raise


def _user_agent() -> str:
    return os.environ.get("EXTRACT_USER_AGENT") or DEFAULT_USER_AGENT


def request_json(url: str, timeout: int, opener: Callable = urllib.request.urlopen) -> dict:
    request = urllib.request.Request(url, headers={"Accept": "application/geo+json, application/json", "User-Agent": _user_agent()})
    with opener(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"response from {url} is not a JSON object")
    return payload


def select_collections(catalog: dict, requested: tuple[str, ...]) -> tuple[str, ...]:
    """Verify each configured collection against the provider catalog."""
    entries = catalog.get("collections")
    if not isinstance(entries, list):
        raise ValueError("USGS collection catalog is missing collections")
    available = {entry.get("id") for entry in entries if isinstance(entry, dict) and isinstance(entry.get("id"), str)}
    missing = [collection for collection in requested if collection not in available]
    if missing:
        raise ValueError(f"USGS collection catalog does not contain: {', '.join(missing)}")
    return requested


def _first_text(properties: dict, *names: str) -> str | None:
    for name in names:
        value = properties.get(name)
        if value is not None and str(value):
            return str(value)
    return None


def normalize_observation(feature: dict, collection: str, fetched_at: str) -> dict:
    """Produce the stable, source-neutral envelope while retaining raw fields."""
    properties = feature.get("properties")
    if not isinstance(properties, dict):
        raise ValueError("OGC feature is missing object properties")
    station = _first_text(properties, "monitoring_location_id", "site_no", "site_number")
    observed_at = _first_text(properties, "time", "datetime", "date")
    metric = _first_text(properties, "parameter_code", "parameter", "variable")
    if not station or not observed_at or not metric:
        raise ValueError("OGC feature is missing monitoring location, time, or parameter code")
    units = _first_text(properties, "unit_of_measure", "unit", "units")
    interval = _first_text(properties, "interval", "time_series_interval", "statistic_code")
    feature_id = feature.get("id")
    if feature_id is not None and str(feature_id):
        measurement_identity = str(feature_id)
    else:
        identity_parts = [metric, _first_text(properties, "statistic_code") or "", interval or "", _first_text(properties, "qualifier_code") or ""]
        measurement_identity = ":".join(identity_parts)
    source_item = urllib.parse.quote(str(feature_id), safe="") if feature_id is not None else ""
    source_url = f"{API_ROOT}/collections/{urllib.parse.quote(collection, safe='')}/items"
    if source_item:
        source_url += f"/{source_item}"
    return {
        "id": f"{SOURCE}:{collection}:{station}:{observed_at}:{measurement_identity}",
        "source": SOURCE,
        "fetched_at": fetched_at,
        "collection": collection,
        "station": station,
        "observed_at": observed_at,
        "metric": metric,
        "units": units,
        "interval": interval,
        "source_url": source_url,
        "observation": properties,
    }


def _next_link(document: dict, page_url: str) -> str | None:
    links = document.get("links", [])
    if not isinstance(links, list):
        raise ValueError("OGC response links is not a list")
    for link in links:
        if isinstance(link, dict) and link.get("rel") == "next":
            href = link.get("href")
            if not isinstance(href, str) or not href:
                raise ValueError("OGC next link is missing href")
            return urllib.parse.urljoin(page_url, href)
    return None


def _initial_items_url(collection: str, station: str, parameter_codes: tuple[str, ...], page_size: int,
                       days_back: int, now: datetime) -> str:
    start = now.astimezone(timezone.utc) - timedelta(days=days_back)
    parameters: list[tuple[str, str]] = [
        ("monitoring_location_id", station),
        ("time", f"{start.isoformat().replace('+00:00', 'Z')}/.."),
        ("limit", str(page_size)),
    ]
    parameters.extend(("parameter_code", code) for code in parameter_codes)
    return f"{API_ROOT}/collections/{urllib.parse.quote(collection, safe='')}/items?{urllib.parse.urlencode(parameters)}"


def _summary(health: str, completeness: str, records: int, requests: int, pages: int,
             failures: list[dict], stations: int, collections: tuple[str, ...], capped: bool) -> dict:
    return {
        "health": health,
        "completeness": completeness,
        "records": records,
        "requests": {"attempted": requests},
        "pagination": {"pages": pages, "capped": capped},
        "coverage": {"stations_requested": stations, "collections_requested": list(collections), "target_queries": stations * len(collections)},
        "failures": {"count": len(failures), "samples": failures},
    }


def fetch_observations(stations: tuple[str, ...], collections: tuple[str, ...] = ("daily",), *,
                       parameter_codes: tuple[str, ...] = (), days_back: int = 14, page_size: int = 100,
                       max_pages: int = 20, max_records: int = 1000, timeout: int = 30,
                       pace_seconds: float = 0.25, state: dict | None = None,
                       opener: Callable = urllib.request.urlopen, sleeper: Callable[[float], None] = time.sleep,
                       now: datetime | None = None) -> Iterator[dict]:
    """Yield bounded observations and advance in-memory continuations after each yield."""
    if not stations or any(not station for station in stations):
        raise ValueError("at least one explicit monitoring location is required")
    if not collections or any(not collection for collection in collections):
        raise ValueError("at least one collection is required")
    if not 1 <= page_size <= 1000 or max_pages < 1 or max_records < 1 or days_back < 0 or timeout < 1 or pace_seconds < 0:
        raise ValueError("invalid request bounds")
    state = state if state is not None else {"version": 1, "continuations": {}}
    continuations = state.setdefault("continuations", {})
    if not isinstance(continuations, dict):
        raise ValueError("state continuations must be an object")
    now = now or datetime.now(timezone.utc)
    fetched_at = now.astimezone(timezone.utc).isoformat()
    requests = pages = records = 0
    failures: list[dict] = []
    capped = False
    prior_request = False

    def get(url: str) -> dict:
        nonlocal requests, prior_request
        if prior_request and pace_seconds:
            sleeper(pace_seconds)
        prior_request = True
        requests += 1
        return request_json(url, timeout, opener)

    try:
        verified_collections = select_collections(get(COLLECTIONS_URL), collections)
    except Exception as error:
        state["last_summary"] = _summary("failed", "failed", records, requests, pages,
                                         [{"target": "catalog", "error": f"{type(error).__name__}: {error}"}],
                                         len(stations), collections, capped)
        raise

    for collection in verified_collections:
        for station in stations:
            if pages >= max_pages or records >= max_records:
                capped = True
                break
            key = f"{collection}|{station}|{','.join(parameter_codes)}|{days_back}"
            cursor = continuations.get(key)
            if cursor is not None and (not isinstance(cursor, dict) or not isinstance(cursor.get("url"), str)
                                       or not isinstance(cursor.get("offset"), int) or cursor["offset"] < 0):
                raise ValueError("malformed in-memory continuation")
            page_url = cursor["url"] if cursor else _initial_items_url(collection, station, parameter_codes, page_size, days_back, now)
            offset = cursor["offset"] if cursor else 0
            try:
                while pages < max_pages and records < max_records:
                    document = get(page_url)
                    pages += 1
                    features = document.get("features")
                    if not isinstance(features, list):
                        raise ValueError("OGC items response is missing features list")
                    if offset > len(features):
                        raise ValueError("continuation offset exceeds OGC page size")
                    stopped_mid_page = False
                    for index in range(offset, len(features)):
                        if records >= max_records:
                            continuations[key] = {"url": page_url, "offset": index}
                            capped = True
                            stopped_mid_page = True
                            break
                        record = normalize_observation(features[index], collection, fetched_at)
                        yield record
                        records += 1
                    if stopped_mid_page:
                        break
                    next_url = _next_link(document, page_url)
                    if not next_url:
                        continuations.pop(key, None)
                        break
                    continuations[key] = {"url": next_url, "offset": 0}
                    page_url, offset = next_url, 0
                if pages >= max_pages or records >= max_records:
                    capped = True
            except Exception as error:
                failures.append({"target": f"{collection}:{station}", "error": f"{type(error).__name__}: {error}"})
        if capped:
            break

    health = "failed" if failures and not records else ("degraded" if failures else "healthy")
    completeness = "failed" if failures and not records else ("partial" if failures or capped else "complete")
    state["last_summary"] = _summary(health, completeness, records, requests, pages, failures,
                                     len(stations), verified_collections, capped)
    if failures:
        raise RuntimeError(f"USGS OGC retrieval failed for {len(failures)} target(s)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--station", action="append", required=True, help="explicit monitoring location ID; repeat for a small fixed set")
    parser.add_argument("--collection", action="append", default=[], help="USGS OGC collection ID, verified against /collections; defaults to daily")
    parser.add_argument("--parameter-code", action="append", default=[], help="optional USGS parameter code; repeat to select metrics")
    parser.add_argument("--days-back", type=int, default=14, help="rolling OGC time window for new and revised observations")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--max-records", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--pace-seconds", type=float, default=0.25)
    parser.add_argument("--state-file", help="external state file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="do not read or persist continuation state")
    args = parser.parse_args()
    collections = tuple(args.collection or ["daily"])
    path = state_path(args.state_file)
    state = {"version": 1, "continuations": {}} if args.no_state else load_state(path)
    try:
        for record in fetch_observations(tuple(args.station), collections, parameter_codes=tuple(args.parameter_code),
                                         days_back=args.days_back, page_size=args.page_size, max_pages=args.max_pages,
                                         max_records=args.max_records, timeout=args.timeout,
                                         pace_seconds=args.pace_seconds, state=state):
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    except Exception as error:
        summary = state.get("last_summary", {"health": "failed", "completeness": "failed", "records": 0,
                                             "error": f"{type(error).__name__}: {error}"})
        print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
        return 1
    if not args.no_state:
        save_state(path, state)
    print(SUMMARY_PREFIX + json.dumps(state["last_summary"], separators=(",", ":")), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
