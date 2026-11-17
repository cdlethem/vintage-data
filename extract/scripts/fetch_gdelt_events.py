#!/usr/bin/env python3
"""Fetch the GDELT 2.0 event database's 15-minute export file as NDJSON.

GDELT publishes a fresh ``YYYYMMDDHHMMSS.export.CSV.zip`` every 15 minutes on
its public data host and points ``lastupdate.txt`` at the newest one.  Each
file holds the global news events coded in that 15-minute window (actors,
CAMEO event codes, geographies, tone, source URLs) -- the standard dataset
for world-event time series.  This is a different GDELT product from the
``gdelt_doc`` source, which polls the DOC article-search API.

State keeps the timestamp of the newest processed file.  A run whose newest
file is not newer than the checkpoint emits zero records (empty stdout is a
valid, successful run).  When the newest file is more than 15 minutes after
the checkpoint the window between them was never processed (a late file at
poll time, an outage, or a paused DAG); the run still ingests the newest file
and reports ``compliance: partial`` with the gap size so the hole is visible
in the manifest.  ``--no-state`` makes a run one-shot: it always ingests the
newest file and writes nothing back.

The first run has no baseline and ingests the newest file only; GDELT keeps
years of 15-minute files and this pipeline wants ongoing collection, not a
historical backfill.

Every line must carry all 61 GDELT 2.0 event columns and pass field-shape
checks; any malformed line fails the run rather than being silently dropped.
State is committed only after every record has been printed, so an aborted
run replays the same file (at-least-once, deduped downstream by ``id``).

Stdlib only.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import pathlib
import re
import sys
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

SOURCE = "gdelt_events"
LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

# The GDELT 2.0 event database schema, in column order.  The export CSV has
# no header row; this is the authoritative 61-column layout.
COLUMNS: tuple[str, ...] = (
    "global_event_id",
    "day",
    "month_year",
    "year",
    "fraction_date",
    "actor1_code",
    "actor1_name",
    "actor1_country_code",
    "actor1_knowngroup_code",
    "actor1_ethnic_code",
    "actor1_religion1_code",
    "actor1_religion2_code",
    "actor1_type1_code",
    "actor1_type2_code",
    "actor1_type3_code",
    "actor2_code",
    "actor2_name",
    "actor2_country_code",
    "actor2_knowngroup_code",
    "actor2_ethnic_code",
    "actor2_religion1_code",
    "actor2_religion2_code",
    "actor2_type1_code",
    "actor2_type2_code",
    "actor2_type3_code",
    "is_root_event",
    "event_code",
    "event_base_code",
    "event_root_code",
    "quad_class",
    "goldstein_scale",
    "num_mentions",
    "num_sources",
    "num_articles",
    "avg_tone",
    "actor1_geo_type",
    "actor1_geo_fullname",
    "actor1_geo_country_code",
    "actor1_geo_adm1_code",
    "actor1_geo_adm2_code",
    "actor1_geo_lat",
    "actor1_geo_long",
    "actor1_geo_feature_id",
    "actor2_geo_type",
    "actor2_geo_fullname",
    "actor2_geo_country_code",
    "actor2_geo_adm1_code",
    "actor2_geo_adm2_code",
    "actor2_geo_lat",
    "actor2_geo_long",
    "actor2_geo_feature_id",
    "actor3_geo_type",
    "actor3_geo_fullname",
    "actor3_geo_country_code",
    "actor3_geo_adm1_code",
    "actor3_geo_adm2_code",
    "actor3_geo_lat",
    "actor3_geo_long",
    "actor3_geo_feature_id",
    "date_added",
    "source_url",
)

FILE_STAMP_RE = re.compile(r"^\d{14}$")
EXPORT_URL_RE = re.compile(
    r"^https?://data\.gdeltproject\.org/gdeltv2/(\d{14})\.export\.CSV\.zip$"
)
GLOBAL_EVENT_ID_RE = re.compile(r"^\d{6,12}$")
DATE8_RE = re.compile(r"^\d{8}$")
DATE6_RE = re.compile(r"^\d{6}$")
DATE4_RE = re.compile(r"^\d{4}$")
URL_RE = re.compile(r"^https?://\S+$")

WINDOW = timedelta(minutes=15)
MAX_ZIP_BYTES = 20 * 1024 * 1024
MAX_CSV_BYTES = 200 * 1024 * 1024
MAX_ROWS_PER_FILE = 200_000


class FetchError(RuntimeError):
    """A run-level failure: bad upstream content, transport error, or cap."""


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"version": STATE_VERSION, "source": SOURCE, "last_file_stamp": None}
    if not isinstance(raw, dict):
        raise FetchError(f"malformed state in {path}: expected an object")
    if raw.get("version") != STATE_VERSION or raw.get("source") != SOURCE:
        raise FetchError(f"malformed state in {path}: incompatible version or source")
    stamp = raw.get("last_file_stamp")
    if stamp is not None and not FILE_STAMP_RE.fullmatch(str(stamp)):
        raise FetchError(f"malformed state in {path}: bad last_file_stamp {stamp!r}")
    return raw


def save_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def fetch_lastupdate(timeout: int) -> str:
    """Return the newest export file URL advertised by lastupdate.txt."""
    request = urllib.request.Request(LASTUPDATE_URL, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            text = response.read(MAX_ZIP_BYTES).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        raise FetchError(f"lastupdate.txt returned HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise FetchError(f"lastupdate.txt request failed: {exc}") from exc

    best: str | None = None
    best_stamp: str | None = None
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        url = parts[2]
        match = EXPORT_URL_RE.match(url)
        if not match:
            continue
        if best_stamp is None or match.group(1) > best_stamp:
            best, best_stamp = url, match.group(1)
    if best is None:
        raise FetchError("lastupdate.txt lists no .export.CSV.zip file")
    return best


def fetch_zip(url: str, timeout: int) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = response.read(MAX_ZIP_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise FetchError(f"export file returned HTTP {exc.code}: {url}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise FetchError(f"export file request failed: {exc}: {url}") from exc
    if len(data) > MAX_ZIP_BYTES:
        raise FetchError(f"export file exceeds {MAX_ZIP_BYTES} bytes: {url}")
    return data


def read_export_csv(data: bytes, stamp: str) -> list[str]:
    """Return the raw CSV lines of the export member, enforcing size caps."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise FetchError(f"export file is not a valid zip archive: {exc}") from exc
    expected = f"{stamp}.export.CSV"
    try:
        member = archive.getinfo(expected)
    except KeyError:
        names = [n for n in archive.namelist() if n.endswith(".export.CSV")]
        if len(names) != 1:
            raise FetchError(
                f"export zip does not contain exactly one .export.CSV member "
                f"(expected {expected!r}; found {names})"
            )
        member = archive.getinfo(names[0])
    if member.file_size > MAX_CSV_BYTES:
        raise FetchError(f"export CSV is {member.file_size} bytes, cap is {MAX_CSV_BYTES}")
    with archive.open(member) as handle:
        text = handle.read(MAX_CSV_BYTES + 1).decode("utf-8", "replace")
    if len(text.encode("utf-8", "replace")) > MAX_CSV_BYTES:
        raise FetchError(f"export CSV exceeds {MAX_CSV_BYTES} bytes")
    return [line for line in text.splitlines() if line.strip()]


def parse_row(line: str, context: str) -> dict[str, str]:
    fields = line.split("\t")
    if len(fields) != len(COLUMNS):
        raise FetchError(f"{context}: expected {len(COLUMNS)} tab-separated fields, got {len(fields)}")
    record = dict(zip(COLUMNS, fields))
    checks = (
        ("global_event_id", GLOBAL_EVENT_ID_RE),
        ("day", DATE8_RE),
        ("month_year", DATE6_RE),
        ("year", DATE4_RE),
        ("date_added", re.compile(r"^\d{14}$")),
    )
    for field, pattern in checks:
        if pattern.fullmatch(record[field]) is None:
            raise FetchError(f"{context}: field {field} has invalid value {record[field]!r}")
    if URL_RE.match(record["source_url"]) is None:
        raise FetchError(f"{context}: field source_url has invalid value {record['source_url']!r}")
    return record


def iter_records(
    lines: Sequence[str], fetched_at: str, file_stamp: str
) -> Iterator[dict[str, Any]]:
    for index, line in enumerate(lines, start=1):
        if index > MAX_ROWS_PER_FILE:
            raise FetchError(f"export file has more than {MAX_ROWS_PER_FILE} rows")
        fields = parse_row(line, f"{file_stamp} row {index}")
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": fields["global_event_id"],
            **fields,
        }


def parse_stamp(stamp: str) -> datetime:
    return datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

def emit_summary(payload: dict[str, Any]) -> None:
    print(SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")), file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="checkpoint file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="one-shot pull: ingest the newest file, do not read or write state")
    parser.add_argument("--timeout", type=int, default=30, help="per-request timeout in seconds")
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    path = None if args.no_state else state_path(args.state_file)
    state = (
        {"version": STATE_VERSION, "source": SOURCE, "last_file_stamp": None}
        if path is None
        else load_state(path)
    )
    last_stamp = state.get("last_file_stamp")

    url = fetch_lastupdate(args.timeout)
    match = EXPORT_URL_RE.match(url)
    assert match is not None  # guaranteed by fetch_lastupdate
    stamp = match.group(1)

    if last_stamp is not None and stamp <= last_stamp:
        emit_summary({
            "health": "healthy",
            "completeness": "complete",
            "records": 0,
            "requests": {"attempted": 1},
            "metrics": {"file_stamp": stamp, "skipped": True},
        })
        return 0

    if last_stamp is not None and parse_stamp(stamp) > parse_stamp(last_stamp) + WINDOW:
        gap_minutes = int((parse_stamp(stamp) - parse_stamp(last_stamp) - WINDOW).total_seconds() // 60)
        completeness = "partial"
    else:
        gap_minutes = 0
        completeness = "complete"

    data = fetch_zip(url, args.timeout)
    lines = read_export_csv(data, stamp)
    fetched_at = datetime.now(timezone.utc).isoformat()

    count = 0
    for record in iter_records(lines, fetched_at, stamp):
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
        count += 1
    sys.stdout.flush()

    state["last_file_stamp"] = stamp
    state["last_success_at"] = fetched_at
    if path is not None:
        save_state(path, state)

    health = "healthy" if count else "degraded"
    emit_summary({
        "health": health,
        "completeness": completeness,
        "records": count,
        "requests": {"attempted": 2},
        "metrics": {"file_stamp": stamp, "gap_minutes": gap_minutes},
    })
    return 0


def run(argv: Sequence[str] | None = None) -> int:
    """Entry point for the DAG and the tests; maps FetchError to exit 1."""
    try:
        return main(argv)
    except FetchError as exc:
        emit_summary({
            "health": "failed",
            "completeness": "failed",
            "records": 0,
            "requests": {"attempted": 0},
            "error": str(exc),
        })
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
