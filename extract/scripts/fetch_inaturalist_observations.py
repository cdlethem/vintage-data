#!/usr/bin/env python3
"""Fetch a bounded snapshot from the public iNaturalist v1 observations API.

The extractor reads only ``GET /v1/observations`` without authentication. A run
is explicitly restricted to one taxon, a bounded page size, and a bounded page
count. It is therefore a newest-first snapshot, not a complete taxon history;
records beyond ``max_pages * per_page`` are intentionally outside that run.

Only public observation fields are projected. Public coordinates and their
obscuration flags are retained, but private-location fields are neither
requested nor copied. Photo attribution and licence metadata are retained when
present; media URLs and media bytes are not emitted or downloaded. iNaturalist
observation licences vary by record, and downstream users must retain the
record-level licence and observer/media attribution. Some iNaturalist records
also reach GBIF, so cross-source deduplication is required downstream rather
than being guessed here.

The public API documents request guidance rather than a guaranteed anonymous
quota. Requests are serial, finite-timeout, and bounded; 429 and transient 5xx
responses honor bounded Retry-After/backoff before a bounded retry count.

Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import math
import os
import re
import time
from typing import Any, Iterator, Sequence
import urllib.error
import urllib.parse
import urllib.request


SOURCE = "inaturalist_observations"
API_URL = "https://api.inaturalist.org/v1/observations"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

DEFAULT_PER_PAGE = 100
MAX_PER_PAGE = 200
DEFAULT_MAX_PAGES = 2
MAX_PAGES = 10
DEFAULT_TIMEOUT = 30.0
MAX_TIMEOUT = 120.0
DEFAULT_RETRIES = 2
MAX_RETRIES = 5
DEFAULT_RETRY_BACKOFF = 1.0
MAX_RETRY_DELAY = 60.0
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
RETRYABLE_HTTP_CODES = frozenset((429, 500, 502, 503, 504))


class INaturalistError(RuntimeError):
    """The API request or response cannot satisfy the extractor contract."""


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _bounded_number(value: Any, name: str, minimum: float, maximum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not minimum <= float(value) <= maximum
    ):
        raise ValueError(f"{name} must be a finite number between {minimum:g} and {maximum:g}")
    return float(value)


def _utc_timestamp(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("fetched_at must be a non-empty UTC timestamp")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("fetched_at must be a valid UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("fetched_at must be a UTC timestamp")
    return parsed.astimezone(timezone.utc).isoformat()



def _retry_delay(error: urllib.error.HTTPError, fallback: float) -> float:
    value = error.headers.get("Retry-After") if error.headers is not None else None
    if value is None:
        return min(fallback, MAX_RETRY_DELAY)
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            delay = max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError) as exc:
            raise INaturalistError("iNaturalist returned an invalid Retry-After header") from exc
    if not math.isfinite(delay) or delay < 0:
        raise INaturalistError("iNaturalist returned an invalid Retry-After header")
    if delay > MAX_RETRY_DELAY:
        raise INaturalistError(
            f"iNaturalist requested Retry-After={delay:g}s, above the {MAX_RETRY_DELAY:g}s bound"
        )
    return delay


def _request_json(
    parameters: dict[str, Any],
    *,
    timeout: float,
    retries: int,
    retry_backoff: float,
) -> Any:
    url = API_URL + "?" + urllib.parse.urlencode(parameters)
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read(MAX_RESPONSE_BYTES + 1)
            if len(payload) > MAX_RESPONSE_BYTES:
                raise INaturalistError(
                    f"iNaturalist page {parameters['page']} exceeded {MAX_RESPONSE_BYTES} bytes"
                )
            try:
                return json.loads(payload)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise INaturalistError(
                    f"iNaturalist page {parameters['page']} returned malformed JSON"
                ) from exc
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt >= retries:
                raise INaturalistError(
                    f"iNaturalist page {parameters['page']} returned HTTP {exc.code}"
                ) from exc
            delay = _retry_delay(exc, retry_backoff * (2**attempt))
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt >= retries:
                reason = getattr(exc, "reason", str(exc))
                raise INaturalistError(
                    f"iNaturalist page {parameters['page']} was unavailable: {reason}"
                ) from exc
            delay = min(retry_backoff * (2**attempt), MAX_RETRY_DELAY)
        if delay:
            time.sleep(delay)
    raise AssertionError("bounded retry loop exhausted without returning or raising")


def _parse_page(document: Any, expected_page: int, expected_per_page: int) -> tuple[list[Any], int]:
    context = f"iNaturalist page {expected_page}"
    if not isinstance(document, dict):
        raise INaturalistError(f"{context} response must be an object")
    results = document.get("results")
    if not isinstance(results, list):
        raise INaturalistError(f"{context} results must be a list")
    page = document.get("page")
    per_page = document.get("per_page")
    total = document.get("total_results")
    if isinstance(page, bool) or not isinstance(page, int) or page != expected_page:
        raise INaturalistError(f"{context} returned invalid page metadata")
    if isinstance(per_page, bool) or not isinstance(per_page, int) or per_page != expected_per_page:
        raise INaturalistError(f"{context} returned invalid per_page metadata")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise INaturalistError(f"{context} returned invalid total_results")
    if len(results) > expected_per_page or total < len(results):
        raise INaturalistError(f"{context} result count contradicts its metadata")
    return results, total


def _optional_object(value: Any, field: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise INaturalistError(f"observation has invalid {field}")
    return value


def _public_coordinates(observation: dict[str, Any]) -> tuple[Any, Any, dict[str, Any] | None]:
    location = observation.get("location")
    if location is not None and not isinstance(location, str):
        raise INaturalistError("observation has invalid location")
    geojson = _optional_object(observation.get("geojson"), "geojson")
    latitude = longitude = None
    if geojson is not None:
        coordinates = geojson.get("coordinates")
        if not (
            isinstance(coordinates, list)
            and len(coordinates) >= 2
            and all(
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and math.isfinite(value)
                for value in coordinates[:2]
            )
        ):
            raise INaturalistError("observation has invalid geojson coordinates")
        longitude, latitude = coordinates[:2]
    elif location:
        parts = location.split(",")
        if len(parts) == 2:
            try:
                latitude, longitude = (float(part.strip()) for part in parts)
            except ValueError as exc:
                raise INaturalistError("observation has invalid location coordinates") from exc
            if not math.isfinite(latitude) or not math.isfinite(longitude):
                raise INaturalistError("observation has invalid location coordinates")
    if latitude is not None and not -90 <= latitude <= 90:
        raise INaturalistError("observation latitude is outside valid bounds")
    if longitude is not None and not -180 <= longitude <= 180:
        raise INaturalistError("observation longitude is outside valid bounds")
    return latitude, longitude, geojson


def _taxon_metadata(value: Any) -> dict[str, Any] | None:
    taxon = _optional_object(value, "taxon")
    if taxon is None:
        return None
    return {
        "id": taxon.get("id"),
        "name": taxon.get("name"),
        "preferred_common_name": taxon.get("preferred_common_name"),
        "rank": taxon.get("rank"),
        "rank_level": taxon.get("rank_level"),
        "iconic_taxon_name": taxon.get("iconic_taxon_name"),
        "ancestry": taxon.get("ancestry"),
    }


def _user_metadata(value: Any) -> dict[str, Any] | None:
    user = _optional_object(value, "user")
    if user is None:
        return None
    return {"id": user.get("id"), "login": user.get("login"), "name": user.get("name")}


def _photo_metadata(value: Any) -> list[dict[str, Any]] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise INaturalistError("observation has invalid photos")
    photos = []
    for photo in value:
        if not isinstance(photo, dict):
            raise INaturalistError("observation has invalid photo metadata")
        photos.append(
            {
                "id": photo.get("id"),
                "license_code": photo.get("license_code"),
                "attribution": photo.get("attribution"),
            }
        )
    return photos


def _sound_metadata(value: Any) -> list[dict[str, Any]] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise INaturalistError("observation has invalid sounds")
    sounds = []
    for sound in value:
        if not isinstance(sound, dict):
            raise INaturalistError("observation has invalid sound metadata")
        sounds.append(
            {
                "id": sound.get("id"),
                "license_code": sound.get("license_code"),
                "attribution": sound.get("attribution"),
            }
        )
    return sounds



def normalize_observation(observation: Any, fetched_at: str) -> dict[str, Any]:
    """Project one API observation without copying private or media-location fields."""
    if not isinstance(observation, dict):
        raise INaturalistError("iNaturalist observation must be an object")
    observation_id = observation.get("id")
    if isinstance(observation_id, bool) or not isinstance(observation_id, int) or observation_id <= 0:
        raise INaturalistError("iNaturalist observation has invalid id")
    latitude, longitude, geojson = _public_coordinates(observation)
    taxon = _taxon_metadata(observation.get("taxon"))
    user = _user_metadata(observation.get("user"))
    photos = _photo_metadata(observation.get("photos"))
    sounds = _sound_metadata(observation.get("sounds"))

    return {
        "source": SOURCE,
        "id": observation_id,
        "fetched_at": fetched_at,
        "observation_id": observation_id,
        "observed_on": observation.get("observed_on"),
        "time_observed_at": observation.get("time_observed_at"),
        "observed_on_details": observation.get("observed_on_details"),
        "created_at": observation.get("created_at"),
        "updated_at": observation.get("updated_at"),
        "taxon_id": taxon.get("id") if taxon is not None else None,
        "taxon_name": taxon.get("name") if taxon is not None else None,
        "taxon": taxon,
        "latitude": latitude,
        "longitude": longitude,
        "location": observation.get("location"),
        "geojson": geojson,
        "positional_accuracy": observation.get("positional_accuracy"),
        "geoprivacy": observation.get("geoprivacy"),
        "taxon_geoprivacy": observation.get("taxon_geoprivacy"),
        "obscured": observation.get("obscured"),
        "quality_grade": observation.get("quality_grade"),
        "identifications_count": observation.get("identifications_count"),
        "num_identification_agreements": observation.get("num_identification_agreements"),
        "num_identification_disagreements": observation.get("num_identification_disagreements"),
        "identifications_most_agree": observation.get("identifications_most_agree"),
        "identifications_some_agree": observation.get("identifications_some_agree"),
        "quality_metrics": observation.get("quality_metrics"),

        "captive": observation.get("captive"),
        "mappable": observation.get("mappable"),
        "cached_votes_total": observation.get("cached_votes_total"),
        "description": observation.get("description"),
        "place_guess": observation.get("place_guess"),
        "uri": observation.get("uri"),
        "license_code": observation.get("license_code"),
        "observer": user,
        "photos": photos,
        "sounds": sounds,
    }


def fetch_observations(
    taxon_id: int,
    *,
    per_page: int = DEFAULT_PER_PAGE,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_backoff: float = DEFAULT_RETRY_BACKOFF,
    fetched_at: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield a validated newest-first, taxon-filtered, bounded snapshot."""
    _bounded_int(taxon_id, "taxon_id", 1, 2_147_483_647)
    _bounded_int(per_page, "per_page", 1, MAX_PER_PAGE)
    _bounded_int(max_pages, "max_pages", 1, MAX_PAGES)
    _bounded_number(timeout, "timeout", 0.1, MAX_TIMEOUT)
    _bounded_int(retries, "retries", 0, MAX_RETRIES)
    _bounded_number(retry_backoff, "retry_backoff", 0.0, MAX_RETRY_DELAY)
    if fetched_at is None:
        fetched_at = datetime.now(timezone.utc).isoformat()
    else:
        fetched_at = _utc_timestamp(fetched_at)

    records: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for page in range(1, max_pages + 1):
        document = _request_json(
            {
                "taxon_id": taxon_id,
                "page": page,
                "per_page": per_page,
                "order_by": "created_at",
                "order": "desc",
            },
            timeout=timeout,
            retries=retries,
            retry_backoff=retry_backoff,
        )
        observations, total = _parse_page(document, page, per_page)
        for observation in observations:
            record = normalize_observation(observation, fetched_at)
            if record["id"] in seen_ids:
                raise INaturalistError(
                    f"iNaturalist pagination repeated observation {record['id']}"
                )
            seen_ids.add(record["id"])
            records.append(record)
        if not observations or len(observations) < per_page or len(records) >= total:
            break
    yield from records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch a bounded newest-first snapshot of public iNaturalist observations."
    )
    parser.add_argument("--taxon-id", type=int, required=True, help="iNaturalist taxon id")
    parser.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-backoff", type=float, default=DEFAULT_RETRY_BACKOFF)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        records = list(
            fetch_observations(
                args.taxon_id,
                per_page=args.per_page,
                max_pages=args.max_pages,
                timeout=args.timeout,
                retries=args.retries,
                retry_backoff=args.retry_backoff,
            )
        )
    except ValueError as exc:
        parser.error(str(exc))
    except INaturalistError as exc:
        parser.exit(1, f"iNaturalist extraction failed: {exc}\n")
    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
