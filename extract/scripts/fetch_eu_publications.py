#!/usr/bin/env python3
"""EU Publications Office — daily new and updated EU publications (keyless).

Sector: EU legal and institutional publications. Not covered anywhere else in
this catalog: federal_register is the US side, uk_* sources are UK side, and
ted_procurement covers only EU procurement notices.

The official channel is CELLAR, the EU Publications Office's repository of
legal acts and other publications. Its ingestion feed
notification feed, served as RSS 2.0) publishes every create/update
notification in real time. No key, no login, no published rate limit —
verified live 2026-10-22: unauthenticated GET returns the current day's
notifications with 1,000 items per page and a moreEntries flag.

This extract takes the WORK-level notifications (one per new or updated
publication: Official Journal acts, regulations and directives, CJEU
judgments, Commission notices, agency reports, consolidations) and drops TED
procurement notices, which are already collected by the ted_procurement
source. Measured live 2026-10-22 (one-day window): 2,663 non-TED
notifications (345 creates, 2,318 updates) — a small, dense digest of what
the EU legally published each day.

'New' per run = every WORK notification since the previous run. The feed is
timestamp-ordered ascending, so the watermark is the newest notification date
seen; the next run re-queries startDate=<watermark> (inclusive) and the
boundary set of guids emitted at exactly that timestamp makes the day
granular at millisecond precision exact. State is written only after every
record is printed, so an aborted run re-fetches rather than skipping
(at-least-once).

Dates in the feed carry a UTC offset (CEST/ CET) that flips at the EU
summer-time transition, so watermark comparison happens on UTC-normalized
datetimes, never on raw strings.

Quirks:
  * Pagination is 1-based page= with a moreEntries flag in the request
    element of the response; there is no total count.
  * Item guids are unique per notification (cellarId + timestamp) and are
    the stable id for this source.
  * rootIdentifier values are URN-like "scheme:reference" strings (celex:,
    oj:, eli:, consolidation:, ecli:, comnat:, ...); the prefixes are
    extracted into identifier_kinds for cheap filtering downstream.
  * The feed serves UTF-8 RSS 2.0: a bare <rss>/<channel> wrapper, bare
    <item> and <guid> elements, and namespace-prefixed notifEntry: /
    notifReq: fields; parse with ElementTree, do not regex it.

Stdlib only.
"""
import argparse
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

SOURCE = "eu_publications"
FEED_URL = "https://publications.europa.eu/webapi/notification/ingestion"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
PAGE_PAUSE = 0.5  # seconds between pages; the feed has no published limit, stay far under any sensible one
MAX_ATTEMPTS = 3  # first try + two retries; the feed blips with 502/503 occasionally
RETRY_DELAYS_SECONDS = (2, 5)
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})
WEMI_CLASSES = "work"  # one notification per publication, not per manifestation
CHANGE_TYPES = frozenset({"create", "update"})
TED_PREFIX = "ted:"

NOTIF_ENTRY = "http://publications.europa.eu/rss/notificationEntry"
NOTIF_REQ = "http://publications.europa.eu/rss/notificationRequest"
NS = {"ne": NOTIF_ENTRY, "nr": NOTIF_REQ}


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    """Return {"watermark": "<UTC ISO datetime>", "boundary": [guid, ...]}."""
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict):
        raise ValueError(f"malformed watermark state in {path}")
    watermark = state.get("watermark", "")
    if watermark and not parse_datetime(watermark):
        raise ValueError(f"malformed watermark in {path}: {watermark!r}")
    boundary = state.get("boundary", [])
    if not isinstance(boundary, list) or not all(isinstance(g, str) for g in boundary):
        raise ValueError(f"malformed boundary in {path}")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def parse_datetime(value: str):
    """Parse the feed's offset-bearing ISO-8601 datetimes into aware UTC."""
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)

def format_start_date(dt: datetime) -> str:
    """Format a UTC datetime the way the feed's startDate accepts it.

    Verified live 2026-10-22: the endpoint accepts ISO-8601 with an
    explicit offset and at most millisecond fractional seconds, and
    answers HTTP 400 for microsecond precision or a 'Z' suffix.
    """
    dt = dt.astimezone(timezone.utc)
    millis = dt.microsecond // 1000
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if millis:
        base += f".{millis:03d}"
    total = int(dt.utcoffset().total_seconds())
    sign = "+" if total >= 0 else "-"
    hours, minutes = divmod(abs(total) // 60, 60)
    return f"{base}{sign}{hours:02d}:{minutes:02d}"


def _is_timeout(error):
    if isinstance(error, urllib.error.HTTPError):
        return False
    if isinstance(error, (TimeoutError,)):
        return True
    return isinstance(error, urllib.error.URLError) and isinstance(error.reason, (TimeoutError,))


def _is_retryable(error):
    return _is_timeout(error) or (
        isinstance(error, urllib.error.HTTPError)
        and error.code in RETRYABLE_HTTP_CODES
    )


def _get_page(page: int, start_date: str, timeout: int) -> bytes:
    """Fetch one feed page, retrying transient failures in a bounded way."""
    params = {
        "startDate": start_date,
        "type": ",".join(sorted(CHANGE_TYPES)),
        "wemiClasses": WEMI_CLASSES,
        "page": page,
    }
    url = f"{FEED_URL}?{urllib.parse.urlencode(params)}"
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                   "Accept": "application/rss+xml"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as error:  # noqa: BLE001 - classify below
            last_error = error
            if not _is_retryable(error) or attempt == MAX_ATTEMPTS:
                break
            time.sleep(RETRY_DELAYS_SECONDS[attempt - 1])
    raise RuntimeError(f"EU publications feed page {page} failed after {attempt} attempt(s): {last_error}")


def _parse_page(page: int, body: bytes) -> tuple[list[dict], bool]:
    """Parse one page into raw item dicts; raise ValueError on any malformation.

    The feed is RSS 2.0: <rss>/<channel> with bare <item>/<guid> elements and
    namespace-prefixed notifEntry: fields. The moreEntries flag lives on the
    channel as a notifReq: element.
    """
    try:
        root = ET.fromstring(body)
    except ET.ParseError as error:
        raise ValueError(f"feed page {page} is not well-formed XML: {error}") from error
    if root.tag != "rss":
        raise ValueError(f"feed page {page} root is {root.tag!r}, expected RSS <rss>")
    channel = root.find("channel")
    if channel is None:
        raise ValueError(f"feed page {page} lacks a <channel>")
    more = channel.find("nr:moreEntries", NS)
    has_more = more.text == "true" if more is not None and more.text else False
    items = []
    for entry in channel.findall("item"):
        guid_el = entry.find("guid")
        guid = guid_el.text if guid_el is not None else ""
        date_el = entry.find("ne:date", NS)
        date = date_el.text if date_el is not None else ""
        type_el = entry.find("ne:type", NS)
        change_type = type_el.text if type_el is not None else ""
        class_el = entry.find("ne:wemiClass", NS)
        wemi_class = class_el.text if class_el is not None else ""
        root_cellar_el = entry.find("ne:rootCellarId", NS)
        root_cellar_id = root_cellar_el.text if root_cellar_el is not None else ""
        cellar_el = entry.find("ne:cellarId", NS)
        cellar_id = cellar_el.text if cellar_el is not None else ""
        identifiers = [el.text for el in entry.findall("ne:identifiers/ne:identifier", NS) if el.text]
        identifiers += [el.text for el in entry.findall("ne:rootIdentifiers/ne:rootIdentifier", NS) if el.text]
        seen: set[str] = set()
        identifiers = [ident for ident in identifiers if not (ident in seen or seen.add(ident))]
        if not guid:
            raise ValueError(f"feed page {page} item lacks a guid")
        if not identifiers:
            raise ValueError(f"feed page {page} item {guid} has no identifiers")
        if not date or not parse_datetime(date):
            raise ValueError(f"feed page {page} item {guid} has no parseable date: {date!r}")
        if change_type not in CHANGE_TYPES:
            raise ValueError(f"feed page {page} item {guid} has unknown change type: {change_type!r}")
        if wemi_class.upper() != WEMI_CLASSES.upper():
            raise ValueError(f"feed page {page} item {guid} is {wemi_class!r}, expected {WEMI_CLASSES!r}")
        if not root_cellar_id:
            raise ValueError(f"feed page {page} item {guid} lacks rootCellarId")
        items.append({
            "guid": guid,
            "date": date,
            "change_type": change_type,
            "wemi_class": wemi_class.lower(),
            "root_cellar_id": root_cellar_id,
            "cellar_id": cellar_id,
            "identifiers": identifiers,
        })
    return items, has_more


def fetch_new(start_date: str, max_pages: int = 10, timeout: int = 60,
              watermark_utc: datetime | None = None,
              boundary: frozenset = frozenset(),
              progress: dict | None = None):
    """Yield WORK-level notifications since ``start_date``, oldest first.

    ``start_date`` is the feed's inclusive startDate (ISO datetime; a UTC
    offset is accepted and honored). Notifications at exactly the stored
    watermark timestamp that were already emitted (guids in ``boundary``)
    are skipped, which makes the millisecond-granular watermark exact.
    TED procurement notices are dropped: the ted_procurement source
    collects them separately.
    """
    for page in range(1, max_pages + 1):
        if progress is not None:
            progress["requests"] = progress.get("requests", 0) + 1
        body = _get_page(page, start_date, timeout)
        items, has_more = _parse_page(page, body)
        for item in items:
            normalized = parse_datetime(item["date"])
            if watermark_utc is not None and normalized <= watermark_utc and item["guid"] in boundary:
                continue  # already emitted at the boundary timestamp
            yield item
        if not has_more:
            return
        time.sleep(PAGE_PAUSE)


def normalize(item: dict, fetched_at: str) -> dict:
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": item["guid"],
        "cellar_id": item["cellar_id"],
        "root_cellar_id": item["root_cellar_id"],
        "change_type": item["change_type"],
        "wemi_class": item["wemi_class"],
        "publication_date": item["date"],
        "identifiers": item["identifiers"],
        "identifier_kinds": sorted({ident.split(":", 1)[0] for ident in item["identifiers"]}),
    }


def is_ted(item: dict) -> bool:
    return any(ident.startswith(TED_PREFIX) for ident in item["identifiers"])


def default_start_date(now: datetime, lookback_days: int) -> str:
    return format_start_date((now - timedelta(days=lookback_days)).astimezone(timezone.utc))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch new EU publications from the CELLAR ingestion feed as NDJSON.")
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off pull)")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="ceiling per run; a one-day window is ~5 pages of 1,000 items (measured 2026-10-22)")
    parser.add_argument("--lookback-days", type=int, default=1,
                        help="first-run window with no state; ~2,700 non-TED WORK notifications per day")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args(argv)

    records = 0
    try:
        if args.timeout <= 0:
            raise ValueError("timeout must be positive")
        if args.max_pages <= 0:
            raise ValueError("max-pages must be positive")
        if args.lookback_days <= 0:
            raise ValueError("lookback-days must be positive")
        path = state_path(args.state_file)
        state = {} if args.no_state else load_state(path)
        watermark_raw = state.get("watermark", "")
        watermark_utc = parse_datetime(watermark_raw) if watermark_raw else None
        start_date = watermark_raw if watermark_raw else default_start_date(
            datetime.now(timezone.utc), args.lookback_days)
        boundary = frozenset(state.get("boundary", []))

        fetched_at = datetime.now(timezone.utc).isoformat()
        progress = {"watermark": watermark_raw, "watermark_utc": watermark_utc,
                    "boundary": list(boundary), "requests": 0}

        def advance(normalized_dt, guid):
            """Track the newest observed timestamp; guids at that timestamp
            form the boundary for the next run's inclusive re-query."""
            if normalized_dt is None:
                return
            if progress["watermark_utc"] is None or normalized_dt > progress["watermark_utc"]:
                progress["watermark"] = format_start_date(normalized_dt)
                progress["watermark_utc"] = normalized_dt
                progress["boundary"] = []
            if normalized_dt == progress["watermark_utc"]:
                progress["boundary"].append(guid)

        for item in fetch_new(start_date, args.max_pages, args.timeout,
                              watermark_utc, boundary, progress):
            # TED notices are owned by ted_procurement, but they still advance
            # the watermark so the window does not refetch them forever.
            advance(parse_datetime(item["date"]), item["guid"])
            if is_ted(item):
                continue
            record = normalize(item, fetched_at)
            print(json.dumps(record, ensure_ascii=False))
            records += 1

        if not args.no_state and progress["watermark_utc"] is not None \
                and progress["watermark"] != watermark_raw:
            save_state(path, {"watermark": progress["watermark"],
                              "boundary": progress["boundary"]})
    except Exception as exc:  # noqa: BLE001 - the run summary is the contract
        print(SUMMARY_PREFIX + json.dumps({"health": "failed", "completeness": "failed",
                                           "records": records, "error": str(exc)}), file=sys.stderr)
        return 1
    print(SUMMARY_PREFIX + json.dumps({
        "health": "healthy",
        "completeness": "complete",
        "records": records,
        "requests": {"attempted": progress["requests"]},
        "coverage": {
            "watermark_before": watermark_raw,
            "watermark_after": progress["watermark"],
            "boundary": len(progress["boundary"]),
        },
    }), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
