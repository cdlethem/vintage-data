#!/usr/bin/env python3
"""NHC HURDAT2 — Atlantic basin tropical cyclone archive (1851-present).

The National Hurricane Center publishes the full Atlantic basin tropical
cyclone database as a single plain-text file, ``hurdat2-1851-*.txt``, in
``https://www.nhc.noaa.gov/data/hurdat/``. NHC publishes a new snapshot
whenever it issues a data revision: a new storm forming, an active storm's
track extending, a historical storm being re-analysed, or a season report
being added. The snapshot file is replaced in place (the same filename can
get new content), so change detection is by ETag/sha256, not by filename.

Each run:
  1. reads the directory listing,
  2. downloads the latest snapshot,
  3. compares it against the stored state (file, etag, per-storm hashes),
  4. emits NDJSON for every storm whose track is new or revised.

Record grain is one 3- (or 6-)hourly track point. RAW is insert-only, so a
revised historical storm is re-emitted in full when its track changes: each
release captures the archive exactly as NHC published it.

Fields per track point (HURDAT2 columns 1-21; ``-999`` means "missing" and
is stored as null):
  date, time, recurve, category (TD/TS/HU/EX/LO/DS/SS/SD/DB/WV), lat, lon,
  max_wind_kt, min_pressure_mb,
  wind_1min_radius_{ne,nw,se,sw}_km, gust_radius_{ne,nw,se,sw}_km,
  tc_radius_{ne,nw,se,sw}_km, hf_radius_ne_km

Keyless; US Government work (NOAA), public domain. Stdlib only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SOURCE = "nhc_hurdat2_atlantic"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DIRECTORY_URL = "https://www.nhc.noaa.gov/data/hurdat/"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
STATE_VERSION = 1
DEFAULT_TIMEOUT = 90
MAX_TIMEOUT = 600
MAX_FILE_BYTES = 50 * 1024 * 1024  # current file is ~7 MB; guard against a runaway listing match
MISSING = -999
MAX_ATTEMPTS = 4
RETRY_BASE_DELAY = 5.0
MAX_RETRY_DELAY = 30.0
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})

SNAPSHOT_NAME = re.compile(r"^(hurdat2-1851-\d{4}-\d{4,10}\.txt)\Z")
LISTING_LINK = re.compile(r'href="([^"]+\.txt)"[^>]*>([^<]+)<')
LISTING_MODIFIED = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})")
STORM_HEADER = re.compile(r"^(AL|EP|NP|SP)(\d{2})(\d{4}),\s*([^,]+),\s*(\d+),\s*\Z")
POINT_FIELDS = (
    "date", "time", "recurve", "category", "lat", "lon",
    "max_wind_kt", "min_pressure_mb",
    "wind_1min_radius_ne_km", "wind_1min_radius_nw_km", "wind_1min_radius_se_km", "wind_1min_radius_sw_km",
    "gust_radius_ne_km", "gust_radius_nw_km", "gust_radius_se_km", "gust_radius_sw_km",
    "tc_radius_ne_km", "tc_radius_nw_km", "tc_radius_se_km", "tc_radius_sw_km",
    "hf_radius_ne_km",
)
CATEGORIES = frozenset({"TD", "TS", "HU", "EX", "LO", "DS", "SS", "SD", "DB", "WV", "IN"})


class FetchError(RuntimeError):
    """The source did not return a parseable archive snapshot."""


class SummaryEmissionError(OSError):
    """The reserved run-summary line could not be fully emitted."""


def _retry_delay(error: urllib.error.HTTPError, fallback: float) -> float:
    """Return a courteous bounded delay from an HTTP Retry-After header."""
    retry_after = error.headers.get("Retry-After") if error.headers else None
    if retry_after:
        try:
            return max(1.0, min(float(retry_after), MAX_RETRY_DELAY))
        except ValueError:
            pass
    return min(fallback, MAX_RETRY_DELAY)


def _request(url: str, timeout: int) -> tuple[bytes, dict[str, str]]:
    """GET a URL with bounded retries on transient failures.

    Returns (body, response_headers) with headers lower-cased.
    """
    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read(MAX_FILE_BYTES + 1)
                headers = {k.lower(): v for k, v in response.headers.items()}
                if len(body) > MAX_FILE_BYTES:
                    raise FetchError(f"response exceeds {MAX_FILE_BYTES} bytes: {url}")
                return body, headers
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_HTTP_CODES and attempt < MAX_ATTEMPTS:
                delay = _retry_delay(exc, RETRY_BASE_DELAY * (2 ** (attempt - 1)))
                last_error = exc
                time.sleep(delay)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            if attempt < MAX_ATTEMPTS:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                last_error = exc
                time.sleep(delay)
                continue
            raise
    raise FetchError(f"exhausted {MAX_ATTEMPTS} attempts for {url}: {last_error}")


def parse_listing(body: bytes) -> dict[str, Any]:
    """Pick the newest full-basin snapshot from the Apache directory listing."""
    text = body.decode("utf-8", errors="replace")
    candidates: list[tuple[str, str, str]] = []  # (last_modified, name, href)
    for line in text.splitlines():
        link = LISTING_LINK.search(line)
        if not link:
            continue
        href = urllib.parse.unquote(link.group(1))
        if not SNAPSHOT_NAME.match(href):
            continue
        modified = LISTING_MODIFIED.search(line)
        candidates.append((modified.group(1) if modified else "1970-01-01 00:00", href, href))
    if not candidates:
        raise FetchError(f"no hurdat2-1851-*.txt snapshot found in directory listing: {DIRECTORY_URL}")
    candidates.sort()
    _, name, href = candidates[-1]
    url = urllib.request.urljoin(DIRECTORY_URL, urllib.request.pathname2url(href))
    return {"name": name, "url": url, "listing_modified": candidates[-1][0]}


def _coordinate(value: str, axis: str) -> float:
    match = re.match(r"^(\d{1,3}(?:\.\d)?)([NSWE])\Z", value.strip())
    if not match:
        raise FetchError(f"unparseable {axis} coordinate: {value!r}")
    number = float(match.group(1))
    direction = match.group(2)
    if axis == "lat":
        if number > 90 or direction not in "NS":
            raise FetchError(f"lat out of range or bad hemisphere: {value!r}")
        return number if direction == "N" else -number
    if number > 180 or direction not in "EW":
        raise FetchError(f"lon out of range or bad hemisphere: {value!r}")
    return number if direction == "E" else -number


def _number(value: str) -> float | None:
    text = value.strip()
    if text in ("", " "):
        return None
    try:
        number = float(text)
    except ValueError:
        raise FetchError(f"unparseable numeric field: {text!r}")
    return None if number == MISSING else number


def parse_archive(body: bytes) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Parse a HURDAT2 snapshot into {storm_id: [(raw_line, point_dict), ...]}."""
    text = body.decode("utf-8", errors="replace")
    storms: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    current: str | None = None
    for line in text.splitlines():
        if not line.strip():
            continue
        header = STORM_HEADER.match(line)
        if header:
            basin, number, year, name, count = header.groups()
            current = f"{basin}{number}{year}"
            storms[current] = [("", {"_meta": {
                "storm_id": current, "basin": basin, "number": int(number),
                "year": int(year), "name": name.strip(), "expected_points": int(count),
            }})]

            continue
        if current is None:
            raise FetchError(f"track point before any storm header: {line[:80]!r}")
        fields = [f.strip() for f in line.split(",")]
        if len(fields) < 8:
            raise FetchError(f"track point has {len(fields)} fields, want at least 8: {line[:80]!r}")
        try:
            datetime.strptime(fields[0] + fields[1], "%Y%m%d%H%M")
        except ValueError:
            raise FetchError(f"track point has unparseable date/time: {line[:80]!r}")
        if fields[3] not in CATEGORIES:
            raise FetchError(f"track point has unknown category {fields[3]!r}: {line[:80]!r}")
        record = {
            "date": fields[0], "time": fields[1], "recurve": fields[2] or None,
            "category": fields[3],
            "lat": _coordinate(fields[4], "lat"), "lon": _coordinate(fields[5], "lon"),
            "max_wind_kt": _number(fields[6]), "min_pressure_mb": _number(fields[7]),
        }
        for i, field in enumerate(POINT_FIELDS[8:21]):
            record[field] = _number(fields[8 + i]) if 8 + i < len(fields) else None
        storms[current].append((line, record))
    if not storms:
        raise FetchError("snapshot contains no storms")
    return storms


def _storm_digest(lines: list[tuple[str, dict[str, Any]]]) -> str:
    """Stable digest of a storm's raw lines (revisions change it)."""
    digest = hashlib.sha256()
    for raw, _record in lines:
        digest.update(raw.encode("utf-8") + b"\n")
    return digest.hexdigest()


def to_records(storms: dict[str, list[tuple[str, dict[str, Any]]]], release: str, fetched_at: str) -> Iterator[dict[str, Any]]:
    for storm_id in sorted(storms):
        meta = storms[storm_id][0][1]["_meta"]
        for _raw, record in storms[storm_id][1:]:
            yield {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{storm_id}:{record['date']}T{record['time']}Z",
                "storm_id": storm_id,
                "basin": meta["basin"],
                "number": meta["number"],
                "year": meta["year"],
                "name": meta["name"],
                "release": release,
                **record,
            }


def validate_counts(storms: dict[str, list[tuple[str, dict[str, Any]]]]) -> None:
    """NHC's header carries the storm's point count; a mismatch means truncation."""
    for storm_id, lines in storms.items():
        meta = lines[0][1]["_meta"]
        actual = len(lines) - 1
        if actual != meta["expected_points"]:
            raise FetchError(
                f"storm {storm_id}: header declares {meta['expected_points']} points, parsed {actual}"
            )


def state_path(explicit: str | os.PathLike[str] | None) -> Path:
    if explicit:
        return Path(explicit).expanduser()
    data_root = Path(os.environ.get("EXTRACT_DATA_ROOT") or "~/.local/share/vintage-data/extract").expanduser()
    return data_root / "state" / f"{SOURCE}.json"


def load_state(path: Path) -> dict[str, Any] | None:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    if not isinstance(document, dict) or document.get("version") != STATE_VERSION:
        return None
    return document


def save_state(path: Path, state: dict[str, Any]) -> None:
    """Durably replace state without exposing a partially written JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def emit_summary(err: Any, summary: dict[str, Any]) -> None:
    line = SUMMARY_PREFIX + json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n"
    try:
        err.write(line)
        err.flush()
    except OSError as exc:
        raise SummaryEmissionError(str(exc)) from exc


def _failure_summary(base: dict[str, Any], error: Exception) -> dict[str, Any]:
    summary = dict(base)
    summary["status"] = "failed"
    summary["error"] = " ".join(str(error).split())[:300]
    return summary


def main(argv: list[str] | None = None, *, out: Any = None, err: Any = None) -> int:
    out = sys.stdout if out is None else out
    err = sys.stderr if err is None else err
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", default=None, help="state file path (default: $EXTRACT_DATA_ROOT/state/<source>.json)")
    parser.add_argument("--no-state", action="store_true", help="ignore state; baseline run over the whole archive")
    parser.add_argument("--full", action="store_true", help="force re-emission of every point even if the file is unchanged")
    parser.add_argument("--file", default=None, help="override the snapshot URL (debugging)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)
    if not 5 <= args.timeout <= MAX_TIMEOUT:
        parser.error(f"--timeout must be between 5 and {MAX_TIMEOUT} seconds")

    base: dict[str, Any] = {"source": SOURCE, "requests": 0, "points_emitted": 0, "state_file": "disabled" if args.no_state else str(state_path(args.state_file))}
    try:
        fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        base["fetched_at"] = fetched_at

        if args.file:
            listing = {"name": args.file.rsplit("/", 1)[-1], "url": args.file, "listing_modified": None}
        else:
            body, _headers = _request(DIRECTORY_URL, args.timeout)
            base["requests"] += 1
            listing = parse_listing(body)
        base["file"] = listing["name"]

        body, headers = _request(listing["url"], args.timeout)
        base["requests"] += 1
        base["file_bytes"] = len(body)
        base["file_etag"] = headers.get("etag")
        base["file_sha256"] = hashlib.sha256(body).hexdigest()

        storms = parse_archive(body)
        validate_counts(storms)
        digests = {storm_id: _storm_digest(lines) for storm_id, lines in storms.items()}
        base["storms"] = len(storms)

        prior = None if args.no_state else load_state(state_path(args.state_file))
        if prior is not None and not args.full:
            same_file = (
                prior.get("file") == listing["name"]
                and prior.get("file_sha256") == base["file_sha256"]
            )
            if same_file and prior.get("storm_digests") == digests:
                base["status"] = "unchanged"
                base["state"] = "unchanged"
                emit_summary(err, base)
                return 0
        changed = set(digests)
        if prior is not None and not args.full:
            changed = {sid for sid, digest in digests.items() if prior.get("storm_digests", {}).get(sid) != digest}
        base["storms_changed"] = len(changed)
        base["status"] = "ok"
        base["state"] = "baseline" if prior is None else ("forced" if args.full else "updated")

        records = [record for storm_id in sorted(storms) if storm_id in changed for record in to_records({storm_id: storms[storm_id]}, listing["name"], fetched_at)]
        base["points_emitted"] = len(records)
        for record in records:
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
        out.flush()

        if not args.no_state:
            save_state(state_path(args.state_file), {
                "version": STATE_VERSION,
                "source": SOURCE,
                "file": listing["name"],
                "file_sha256": base["file_sha256"],
                "file_etag": base.get("file_etag"),
                "updated_at": fetched_at,
                "storm_digests": digests,
            })
        emit_summary(err, base)
        return 0
    except (FetchError, urllib.error.HTTPError, OSError, ValueError) as exc:
        emit_summary(err, _failure_summary(base, exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
