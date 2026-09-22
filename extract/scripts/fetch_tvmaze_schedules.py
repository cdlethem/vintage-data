#!/usr/bin/env python3
"""Fetch one country's daily television schedule from TVMaze's keyless API.

The extractor makes exactly one bounded ``/schedule`` request. It does not use
TVMaze's full-schedule endpoint and does not make follow-up show-catalog calls;
show metadata comes only from the episode objects embedded in the daily result.
Each response is validated completely before any NDJSON record is emitted.

Stdlib only.
"""

from __future__ import annotations

import argparse
import http.client
import json
import math
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from typing import Any, Sequence

SOURCE = "tvmaze_schedules"
API_URL = "https://api.tvmaze.com/schedule"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_COUNTRY = "US"
DEFAULT_TIMEOUT = 30.0
MAX_TIMEOUT = 120.0
COUNTRY_RE = re.compile(r"[A-Za-z]{2}")
MAX_ERROR_DETAIL = 300


class TVMazeError(RuntimeError):
    """Base class for an expected TVMaze extraction failure."""

DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

class TVMazeHTTPError(TVMazeError):
    """TVMaze returned a non-success HTTP response."""


class TVMazeNetworkError(TVMazeError):
    """The TVMaze request could not be completed."""


class TVMazeJSONError(TVMazeError):
    """TVMaze returned a body that was not valid JSON."""


class TVMazeSchemaError(TVMazeError):
    """TVMaze returned JSON with an invalid schedule shape."""


def _safe_error_detail(value: Any) -> str:
    """Return one bounded, control-character-free diagnostic fragment."""
    detail = " ".join(str(value).split())
    return detail[:MAX_ERROR_DETAIL] or "unspecified error"


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _normalize_country(value: Any) -> str:
    if not isinstance(value, str) or COUNTRY_RE.fullmatch(value.strip()) is None:
        raise ValueError("country must be a two-letter country code")
    return value.strip().upper()


def _normalize_date(value: Any) -> str:
    if not isinstance(value, str) or DATE_RE.fullmatch(value) is None:
        raise ValueError("date must use zero-padded YYYY-MM-DD form")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("date must be a valid date in YYYY-MM-DD form") from exc
    return value


def _normalize_timeout(value: Any) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
        or value > MAX_TIMEOUT
    ):
        raise ValueError(f"timeout must be greater than zero and at most {MAX_TIMEOUT:g} seconds")
    return float(value)


def _collection_timestamp(value: str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    if not isinstance(value, str) or not value:
        raise ValueError("fetched_at must be a timezone-aware ISO timestamp")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("fetched_at must be a timezone-aware ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("fetched_at must be a timezone-aware ISO timestamp")
    return parsed.astimezone(timezone.utc).isoformat()


def _required_object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TVMazeSchemaError(f"{field} must be an object")
    return value


def _required_id(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TVMazeSchemaError(f"{field} must be a positive integer")
    return value


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TVMazeSchemaError(f"{field} must be a non-empty string")
    return value


def _optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TVMazeSchemaError(f"{field} must be a string or null")
    return value


def _optional_integer(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TVMazeSchemaError(f"{field} must be a non-negative integer or null")
    return value


def _optional_number(value: Any, field: str) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise TVMazeSchemaError(f"{field} must be a finite number or null")
    return value


def _optional_url(value: Any, field: str) -> str | None:
    text = _optional_string(value, field)
    if text is None:
        return None
    parsed = urllib.parse.urlsplit(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise TVMazeSchemaError(f"{field} must be an HTTP(S) URL or null")
    return text


def _normalize_image(value: Any, field: str) -> dict[str, str | None] | None:
    if value is None:
        return None
    image = _required_object(value, field)
    return {
        "medium": _optional_url(image.get("medium"), f"{field}.medium"),
        "original": _optional_url(image.get("original"), f"{field}.original"),
    }


def _normalize_rating(value: Any, field: str) -> int | float | None:
    if value is None:
        return None
    rating = _required_object(value, field)
    return _optional_number(rating.get("average"), f"{field}.average")


def _normalize_links(value: Any, field: str) -> dict[str, str] | None:
    if value is None:
        return None
    links = _required_object(value, field)
    normalized: dict[str, str] = {}
    for relation, link_value in links.items():
        if not isinstance(relation, str) or not relation:
            raise TVMazeSchemaError(f"{field} relation names must be non-empty strings")
        link = _required_object(link_value, f"{field}.{relation}")
        normalized[relation] = _optional_url(
            link.get("href"), f"{field}.{relation}.href"
        ) or ""
        if not normalized[relation]:
            raise TVMazeSchemaError(f"{field}.{relation}.href must be a non-empty HTTP(S) URL")
    return normalized


def _normalize_country_metadata(value: Any, field: str) -> dict[str, str | None] | None:
    if value is None:
        return None
    country = _required_object(value, field)
    return {
        "name": _optional_string(country.get("name"), f"{field}.name"),
        "code": _optional_string(country.get("code"), f"{field}.code"),
        "timezone": _optional_string(country.get("timezone"), f"{field}.timezone"),
    }


def _normalize_channel(value: Any, field: str) -> dict[str, Any] | None:
    if value is None:
        return None
    channel = _required_object(value, field)
    return {
        "id": _required_id(channel.get("id"), f"{field}.id"),
        "name": _required_string(channel.get("name"), f"{field}.name"),
        "country": _normalize_country_metadata(channel.get("country"), f"{field}.country"),
        "official_site": _optional_url(
            channel.get("officialSite"), f"{field}.officialSite"
        ),
    }


def _normalize_string_list(value: Any, field: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise TVMazeSchemaError(f"{field} must be a list of strings or null")
    return list(value)


def _normalize_show_schedule(value: Any, field: str) -> dict[str, Any] | None:
    if value is None:
        return None
    schedule = _required_object(value, field)
    return {
        "time": _optional_string(schedule.get("time"), f"{field}.time"),
        "days": _normalize_string_list(schedule.get("days"), f"{field}.days"),
    }


def _normalize_external_ids(value: Any, field: str) -> dict[str, str | int | None] | None:
    if value is None:
        return None
    external_ids = _required_object(value, field)
    normalized: dict[str, str | int | None] = {}
    for provider, external_id in external_ids.items():
        if not isinstance(provider, str) or not provider:
            raise TVMazeSchemaError(f"{field} provider names must be non-empty strings")
        if external_id is not None and (
            isinstance(external_id, bool) or not isinstance(external_id, (str, int))
        ):
            raise TVMazeSchemaError(f"{field}.{provider} must be a string, integer, or null")
        normalized[provider] = external_id
    return normalized


def _normalize_show(value: Any, episode_index: int) -> dict[str, Any]:
    field = f"TVMaze schedule episode {episode_index}.show"
    show = _required_object(value, field)
    show_id = _required_id(show.get("id"), f"{field}.id")
    return {
        "id": show_id,
        "name": _required_string(show.get("name"), f"{field}.name"),
        "url": _optional_url(show.get("url"), f"{field}.url"),
        "type": _optional_string(show.get("type"), f"{field}.type"),
        "language": _optional_string(show.get("language"), f"{field}.language"),
        "genres": _normalize_string_list(show.get("genres"), f"{field}.genres"),
        "status": _optional_string(show.get("status"), f"{field}.status"),
        "runtime_minutes": _optional_integer(show.get("runtime"), f"{field}.runtime"),
        "average_runtime_minutes": _optional_integer(
            show.get("averageRuntime"), f"{field}.averageRuntime"
        ),
        "premiered": _optional_string(show.get("premiered"), f"{field}.premiered"),
        "ended": _optional_string(show.get("ended"), f"{field}.ended"),
        "official_site": _optional_url(
            show.get("officialSite"), f"{field}.officialSite"
        ),
        "schedule": _normalize_show_schedule(show.get("schedule"), f"{field}.schedule"),
        "rating_average": _normalize_rating(show.get("rating"), f"{field}.rating"),
        "weight": _optional_number(show.get("weight"), f"{field}.weight"),
        "network": _normalize_channel(show.get("network"), f"{field}.network"),
        "web_channel": _normalize_channel(
            show.get("webChannel"), f"{field}.webChannel"
        ),
        "dvd_country": _normalize_country_metadata(
            show.get("dvdCountry"), f"{field}.dvdCountry"
        ),
        "external_ids": _normalize_external_ids(
            show.get("externals"), f"{field}.externals"
        ),
        "image": _normalize_image(show.get("image"), f"{field}.image"),
        "summary": _optional_string(show.get("summary"), f"{field}.summary"),
        "updated": _optional_integer(show.get("updated"), f"{field}.updated"),
        "links": _normalize_links(show.get("_links"), f"{field}._links"),
    }


def _validate_airdate(value: Any, field: str) -> str:
    text = _required_string(value, field)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise TVMazeSchemaError(f"{field} must be a valid YYYY-MM-DD date") from exc
    if parsed.isoformat() != text:
        raise TVMazeSchemaError(f"{field} must be a valid YYYY-MM-DD date")
    return text


def _validate_airstamp(value: Any, field: str) -> str:
    text = _required_string(value, field)
    parsed_text = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(parsed_text)
    except ValueError as exc:
        raise TVMazeSchemaError(f"{field} must be a timezone-aware ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise TVMazeSchemaError(f"{field} must be a timezone-aware ISO timestamp")
    return text


def normalize_episode(
    episode: Any,
    *,
    index: int,
    country: str,
    schedule_date: str,
    fetched_at: str,
) -> dict[str, Any]:
    """Validate and normalize one embedded daily-schedule episode."""
    field = f"TVMaze schedule episode {index}"
    item = _required_object(episode, field)
    episode_id = _required_id(item.get("id"), f"{field}.id")
    show = _normalize_show(item.get("show"), index)
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": str(episode_id),
        "episode_id": episode_id,
        "show_id": show["id"],
        "requested_country": country,
        "requested_date": schedule_date,
        "episode_name": _required_string(item.get("name"), f"{field}.name"),
        "season": _optional_integer(item.get("season"), f"{field}.season"),
        "number": _optional_integer(item.get("number"), f"{field}.number"),
        "episode_type": _optional_string(item.get("type"), f"{field}.type"),
        "airdate": _validate_airdate(item.get("airdate"), f"{field}.airdate"),
        "airtime": _optional_string(item.get("airtime"), f"{field}.airtime"),
        "airstamp": _validate_airstamp(item.get("airstamp"), f"{field}.airstamp"),
        "runtime_minutes": _optional_integer(item.get("runtime"), f"{field}.runtime"),
        "rating_average": _normalize_rating(item.get("rating"), f"{field}.rating"),
        "summary": _optional_string(item.get("summary"), f"{field}.summary"),
        "image": _normalize_image(item.get("image"), f"{field}.image"),
        "url": _optional_url(item.get("url"), f"{field}.url"),
        "links": _normalize_links(item.get("_links"), f"{field}._links"),
        "show": show,
    }


def parse_schedule(
    document: Any, *, country: str, schedule_date: str, fetched_at: str
) -> list[dict[str, Any]]:
    """Validate a complete response before returning any normalized records."""
    if not isinstance(document, list):
        raise TVMazeSchemaError("TVMaze schedule response must be a JSON list")
    records: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for index, episode in enumerate(document):
        record = normalize_episode(
            episode,
            index=index,
            country=country,
            schedule_date=schedule_date,
            fetched_at=fetched_at,
        )
        episode_id = record["episode_id"]
        if episode_id in seen_ids:
            raise TVMazeSchemaError(
                f"TVMaze schedule response contains duplicate episode id {episode_id}"
            )
        seen_ids.add(episode_id)
        records.append(record)
    return records


def _request_schedule(country: str, schedule_date: str, timeout: float) -> Any:
    query = urllib.parse.urlencode({"country": country, "date": schedule_date})
    request = urllib.request.Request(
        f"{API_URL}?{query}",
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            try:
                return json.load(response)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise TVMazeJSONError("TVMaze returned invalid JSON") from exc
    except urllib.error.HTTPError as exc:
        raise TVMazeHTTPError(f"TVMaze HTTP {exc.code} request failed") from exc
    except TVMazeJSONError:
        raise
    except http.client.IncompleteRead as exc:
        raise TVMazeNetworkError(
            "TVMaze network request failed (incomplete HTTP response)"
        ) from exc
    except http.client.HTTPException as exc:
        raise TVMazeNetworkError(
            "TVMaze network request failed (malformed HTTP response)"
        ) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise TVMazeNetworkError("TVMaze network request failed") from exc


def fetch_tvmaze_schedules(
    country: str = DEFAULT_COUNTRY,
    schedule_date: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    *,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch and normalize one country/date daily schedule."""
    normalized_country = _normalize_country(country)
    normalized_date = _normalize_date(schedule_date or _utc_today())
    normalized_timeout = _normalize_timeout(timeout)
    collection_time = _collection_timestamp(fetched_at)
    document = _request_schedule(normalized_country, normalized_date, normalized_timeout)
    return parse_schedule(
        document,
        country=normalized_country,
        schedule_date=normalized_date,
        fetched_at=collection_time,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch TVMaze's keyless daily schedule for one country and date."
    )
    parser.add_argument(
        "--country",
        default=DEFAULT_COUNTRY,
        help="two-letter country code (default: US)",
    )
    parser.add_argument(
        "--date",
        dest="schedule_date",
        help="schedule date in YYYY-MM-DD form (default: current UTC date)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds, at most {MAX_TIMEOUT:g} (default: {DEFAULT_TIMEOUT:g})",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        country = _normalize_country(args.country)
        schedule_date = _normalize_date(args.schedule_date or _utc_today())
        timeout = _normalize_timeout(args.timeout)
    except ValueError as exc:
        parser.error(str(exc))

    try:
        records = fetch_tvmaze_schedules(country, schedule_date, timeout)
    except TVMazeError as exc:
        print(f"error: {_safe_error_detail(exc)}", file=sys.stderr)
        return 1

    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
