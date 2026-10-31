#!/usr/bin/env python3
"""Tenders Electronic Daily (TED) — new and updated EU procurement notices.

TED is the Supplement to the Official Journal of the EU: every public
procurement notice of the EU, EEA and candidate countries is published there,
Monday to Friday. Notices are published continuously (mainly 00:01-09:00 CET,
plus amendments and award notices throughout the day); on a busy day TED
publishes 2,000+ notices.

This extractor polls the keyless RSS "generate" endpoint of the TED portal
with an all-notices query (``classification-cpv = *``). The endpoint returns
the most recent notices (a rolling window of several thousand, verified at
3,715 on 2026-10-31), newest-last, as one RSS document. Each item carries the
stable notice id (``NNNNNN-YYYY``), the notice title, the detail URL, the
publication date, the notice type and, for open tender notices, the deadline
for receipt of tenders.

The notice id is a global sequence (number + year), so the state file holds
the highest id seen and later runs emit only strictly newer notices. The
window is far larger than one 15-minute run, so overlap is normal; a gap
(feed minimum id above watermark + 1) means notices were missed and is
reported as partial completeness, not hidden.

Licence: procurement notices published in the Supplement to the Official
Journal can be freely reused, for commercial or non-commercial purposes,
under the EU Commission's reuse policy (Commission Decision of 12 December
2011 on the reuse of Commission documents). Credit: "Tenders Electronic Daily
(TED), Publications Office of the European Union".

Stdlib only. Verified live 2026-10-31.
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

SOURCE = "ted_notices"
GENERATE_URL = "https://ted.europa.eu/en/simap/rss-feed/-/rss/generate"
QUERY = "classification-cpv = *"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
STATE_VERSION = 1
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

# The endpoint ignores queries other than classification facets and returns an
# empty body for most of them; these parameters are required for a feed.
FEED_PARAMS = {
    "query": QUERY,
    "title": "TED",
    "description": "TED | All notices | EN",
    "language": "EN",
    "fields": "publication-date, deadline-receipt-request, notice-type",
    "scope": "",
}
DEFAULT_LIMIT = "5000"
DEFAULT_TIMEOUT = 60

TRANSIENT_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
MAX_ATTEMPTS = 4
MAX_RETRY_DELAY_SECONDS = 30
MAX_RETRY_CUMULATIVE_DELAY_SECONDS = 60

TITLE_PATTERN = re.compile(r"^(?P<number>\d{1,7})-(?P<year>\d{4}):\s*(?P<title>.*)$", re.DOTALL)
PUBLICATION_DATE_PATTERN = re.compile(r"^Publication date: (\d{2})/(\d{2})/(\d{4}) \((UTC[+-]\d{2})\)$")
DEADLINE_PATTERN = re.compile(r"^Deadline for receipt of tenders: (.+)$")
NOTICE_TYPE_PATTERN = re.compile(r"^Notice type: (.+)$")
DATE_PATTERN = re.compile(r"(\d{2})/(\d{2})/(\d{4})")


class FeedFetchError(RuntimeError):
    """The feed request failed after the bounded retry budget."""


class FeedParseError(RuntimeError):
    """The feed document deviates from the expected item structure."""


def _id_key(item_id: str) -> tuple[int, int]:
    return (int(item_id.split("-")[1]), int(item_id.split("-")[0]))


def _id_text(key: tuple[int, int]) -> str:
    return f"{key[1]}-{key[0]}"


def fetch_feed(limit: str, timeout: int) -> bytes:
    url = f"{GENERATE_URL}?{urllib.parse.urlencode({**FEED_PARAMS, 'limit': limit})}"
    last_error: Exception | None = None
    cumulative_delay = 0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code not in TRANSIENT_STATUSES:
                raise FeedFetchError(f"GET {url} returned HTTP {error.code}") from error
            last_error = error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last_error = error
        if attempt == MAX_ATTEMPTS:
            break
        delay = _retry_delay(getattr(last_error, "headers", None))
        if cumulative_delay + delay > MAX_RETRY_CUMULATIVE_DELAY_SECONDS:
            break
        time.sleep(delay)
        cumulative_delay += delay
    raise FeedFetchError(
        f"GET {url} failed after {attempt} attempts and {cumulative_delay} seconds: {last_error}"
    )


def _retry_delay(headers) -> int:
    value = headers.get("Retry-After") if headers is not None else None
    if value is not None:
        try:
            return min(int(value), MAX_RETRY_DELAY_SECONDS)
        except ValueError:
            pass
    return 1


def parse_items(body: bytes) -> list[dict]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError as error:
        raise FeedParseError(f"feed is not well-formed XML: {error}") from error
    if root.tag != "rss":
        raise FeedParseError(f"unexpected feed root <{root.tag}>")
    channel = root.find("channel")
    if channel is None:
        raise FeedParseError("feed has no <channel>")
    items = []
    for item in channel.findall("item"):
        title = html.unescape((item.findtext("title") or "").strip())
        match = TITLE_PATTERN.match(title)
        if not match:
            raise FeedParseError(f"unrecognised notice title: {title[:120]!r}")
        item_id = f"{match['number']}-{match['year']}"
        description = html.unescape((item.findtext("description") or "").strip())
        parts = [part.strip() for part in description.split("; ")]
        if not parts:
            raise FeedParseError(f"notice {item_id} has an empty description")
        pub_match = PUBLICATION_DATE_PATTERN.match(parts[0])
        if not pub_match:
            raise FeedParseError(f"notice {item_id} has an unrecognised publication date: {parts[0]!r}")
        record = {
            "id": item_id,
            "title": match["title"].strip(),
            "url": (item.findtext("link") or "").strip(),
            "publication_date": f"{pub_match.group(3)}-{pub_match.group(2)}-{pub_match.group(1)}",
            "notice_type": None,
            "tender_deadline": None,
        }
        for part in parts[1:]:
            deadline_match = DEADLINE_PATTERN.match(part)
            if deadline_match:
                # One date per lot; the earliest is the operative deadline.
                dates = sorted(
                    f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
                    for m in (DATE_PATTERN.match(date.strip()) for date in deadline_match.group(1).split(","))
                    if m
                )
                if dates:
                    record["tender_deadline"] = dates[0]
                continue
            type_match = NOTICE_TYPE_PATTERN.match(part)
            if type_match:
                record["notice_type"] = type_match.group(1).strip()
        if not record["url"]:
            raise FeedParseError(f"notice {item_id} has no link")
        items.append(record)
    keys = [_id_key(item["id"]) for item in items]
    if keys != sorted(keys):
        raise FeedParseError("feed items are not in ascending notice-id order")
    if len(set(keys)) != len(keys):
        raise FeedParseError("feed contains duplicate notice ids")
    return items


def load_state(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot read state file {path}: {error}") from error
    if (
        not isinstance(state, dict)
        or state.get("version") != STATE_VERSION
        or state.get("source") != SOURCE
        or not isinstance(state.get("last_id"), str)
    ):
        raise RuntimeError(f"state file {path} has an unexpected shape")
    try:
        _id_key(state["last_id"])
    except ValueError as error:
        raise RuntimeError(f"state file {path} has an unexpected last_id") from error
    return state


def save_state(path: Path, last_id_key: tuple[int, int]) -> None:
    state = {"version": STATE_VERSION, "source": SOURCE, "last_id": _id_text(last_id_key)}
    path.parent.mkdir(parents=True, exist_ok=True)
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


def validate_state_path(value: str) -> Path:
    """Reject state paths that would land inside this checkout."""
    expanded = Path(value).expanduser()
    lexical = Path(os.path.abspath(os.fspath(expanded)))
    resolved = lexical.resolve(strict=False)
    repository = REPOSITORY_ROOT.resolve(strict=True)
    if lexical == repository or repository in lexical.parents or resolved == repository or repository in resolved.parents:
        raise ValueError(f"state path must be outside the repository: {value}")
    return resolved


def default_state_path() -> Path:
    data_root = Path(os.environ.get("EXTRACT_DATA_ROOT") or "~/.local/share/vintage-data/extract").expanduser()
    return data_root / "state" / f"{SOURCE}.json"


def emit_summary(payload: dict) -> None:
    print(SUMMARY_PREFIX + json.dumps(payload, sort_keys=True, separators=(",", ":")), file=sys.stderr)
    sys.stderr.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="state file path (outside the repository)")
    parser.add_argument("--no-state", action="store_true", help="snapshot mode: emit the whole feed window, no state")
    parser.add_argument("--limit", default=DEFAULT_LIMIT, help="maximum feed items to request")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds")
    args = parser.parse_args()
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if not args.limit.isdigit() or not (1 <= int(args.limit) <= 10000):
        parser.error("--limit must be an integer between 1 and 10000")

    state_file = None if args.no_state else (
        validate_state_path(args.state_file) if args.state_file else default_state_path()
    )
    state = load_state(state_file) if state_file is not None else None
    watermark = _id_key(state["last_id"]) if state else None

    print(
        f"Fetching TED notices (all CPV, window limit {args.limit})"
        f"{'; watermark ' + state['last_id'] if state else '; first stateful run'}",
        file=sys.stderr,
    )
    try:
        body = fetch_feed(args.limit, args.timeout)
        items = parse_items(body)
    except (FeedFetchError, FeedParseError) as error:
        emit_summary({
            "health": "failed",
            "completeness": "failed",
            "requests": {"attempted": 1, "succeeded": 0, "failed": 1},
            "metrics": {"error": str(error)},
        })
        print(f"error: {error}", file=sys.stderr)
        return 1

    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    gap = bool(items) and watermark is not None and _id_key(items[0]["id"]) > (watermark[0], watermark[1] + 1)
    if gap:
        print(
            f"warning: feed window starts at {items[0]['id']}, above watermark "
            f"{_id_text(watermark)} — notices published while the pipeline was "
            "down are not in the window and are lost",
            file=sys.stderr,
        )

    if watermark is None:
        new_items = items
        mode = "first-run snapshot" if state_file is not None else "snapshot"
    else:
        new_items = [item for item in items if _id_key(item["id"]) > watermark]
        mode = "incremental"
    if not items:
        print("warning: feed returned zero items; keeping watermark", file=sys.stderr)

    for record in new_items:
        out = dict(record)
        out["source"] = SOURCE
        out["fetched_at"] = fetched_at
        print(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
    sys.stdout.flush()

    if state_file is not None and items:
        save_state(state_file, _id_key(items[-1]["id"]))

    if not items:
        completeness = "partial"
        health = "degraded"
    elif gap:
        completeness = "partial"
        health = "healthy"
    else:
        completeness = "complete"
        health = "healthy"
    emit_summary({
        "health": health,
        "completeness": completeness,
        "requests": {"attempted": 1, "succeeded": 1, "failed": 0, "payload_bytes": len(body)},
        "metrics": {
            "mode": mode,
            "feed_items": len(items),
            "emitted": len(new_items),
            "gap": gap,
            "window_start": items[0]["id"] if items else None,
            "window_end": items[-1]["id"] if items else None,
        },
    })
    print(
        f"Fetched {len(items)} feed items in {mode} mode; emitted {len(new_items)} records",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
