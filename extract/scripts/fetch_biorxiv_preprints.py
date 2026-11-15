#!/usr/bin/env python3
"""bioRxiv/medRxiv — new preprint metadata per day (keyless, JSON API).

Every day bioRxiv and medRxiv post ~250-320 and ~40-70 new preprint posts
respectively (2026-11 measured: 317 bioRxiv, 61 medRxiv for one day). Each
record carries title, abstract, authors, corresponding institution, subject
category, per-paper CC license, and DOI — an early, daily signal on where
biomedical and life-science research is heading, ahead of journal publication
and ahead of CrossRef/OpenAlex indexing of the same work.

The official public API (https://api.biorxiv.org/) is keyless and documented:
``/details/<server>/<from>/<to>/<cursor>/json`` serves 30 posts per call with
a ``messages`` block reporting the interval total. Metadata collection via
this API is the platform's stated use; no full text is fetched, so the
per-paper CC license (kept in the ``license`` field) is not exercised.

'Current' per run = posts dated on a day after the per-server watermark. The
watermark day is re-queried each run (one extra day of pagination, ~11
requests) and skipped client-side so a post that indexes late with the
watermark date is still caught; its id is stable (server:doi:vN), so the rare
re-emitted id is idempotent downstream. State is written only after every
record is printed, so an aborted run re-fetches rather than skipping
(at-least-once). The first run has no watermark and covers ``--days-back``
(default 7) as its baseline.

Verified live 2026-11-15: both servers answered keyless; a 317-post day
walked to completion via the cursor; ``offset == total`` yields
``status: "no posts found"`` with an empty collection.

Quirks:
  * Server names are lowercase in the API (``medrxiv``); ``medRxiv``/``all``
    answer "Server not recognized".
  * The ``version`` field is a string; ``published`` is the string "NA" when
    the post has no publication date.
  * Page size is fixed at 30 (the server cap); deep pages work, offset 0 is
    the cursor.

Stdlib only.
"""
import argparse
import json
import os
import pathlib
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

SOURCE = "biorxiv_preprints"
BASE = "https://api.biorxiv.org/details"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

SERVERS = ("biorxiv", "medrxiv")
PAGE_SIZE = 30  # server-side cap for /details
DEFAULT_DAYS_BACK = 7
MAX_DAYS_BACK = 365
DEFAULT_TIMEOUT = 30.0
MAX_TIMEOUT = 120.0
DEFAULT_RETRIES = 3
MAX_RETRIES = 5
DEFAULT_RETRY_BUDGET = 60.0
MAX_RETRY_BUDGET = 600.0
MIN_RETRY_DELAY = 3.0
MAX_RETRY_DELAY = 30.0
RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$")


class BiorxivError(RuntimeError):
    """A request or response cannot satisfy the extractor contract."""


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermarks live: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "biorxiv_preprints.json"


def load_state(path: pathlib.Path) -> dict:
    """Return {server: {"watermark": "YYYY-MM-DD", "boundary": [id, ...]}}."""
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict):
        raise ValueError(f"malformed watermark state in {path}")
    for server, entry in state.items():
        if not isinstance(entry, dict):
            raise ValueError(f"malformed watermark state for {server} in {path}")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the watermarks atomically so a crash can't leave them half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def http_get_json(url: str, timeout: float, retries: int, retry_budget: float,
                  sleep=time.sleep) -> dict:
    """GET ``url`` as JSON with a bounded retry budget.

    Retries HTTP 429/5xx and transport failures; other 4xx are terminal. The
    cumulative sleep across all retries is capped at ``retry_budget`` seconds
    so a throttled endpoint cannot stall the run.
    """
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    slept = 0.0
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as error:
            if error.code not in RETRYABLE_STATUSES:
                raise BiorxivError(f"HTTP {error.code} for {url}")
            last_error = f"HTTP {error.code}"
            try:
                error.close()
            except OSError:
                pass
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last_error = f"transport failure: {type(error).__name__}"
        if attempt == retries:
            break
        delay = min(MIN_RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_DELAY,
                    max(0.0, retry_budget - slept))
        if delay <= 0:
            break
        sleep(delay)
        slept += delay
    raise BiorxivError(f"{last_error} after {attempt} attempt(s) for {url}")


def normalize(item, server: str, fetched_at: str) -> dict:
    if not isinstance(item, dict):
        raise ValueError(f"{server} post is not an object")
    doi = item.get("doi")
    if not isinstance(doi, str) or not DOI_RE.match(doi):
        raise ValueError(f"{server} post has no stable source id: {doi!r}")
    posted = item.get("date")
    if not isinstance(posted, str) or not DATE_RE.match(posted):
        raise ValueError(f"{server} post has invalid date: {posted!r}")
    try:
        date.fromisoformat(posted)  # rejects 2026-13-40 style strings
    except ValueError:
        raise ValueError(f"{server} post has invalid date: {posted!r}")
    version_raw = item.get("version")
    try:
        version = int(str(version_raw))
    except (TypeError, ValueError):
        raise ValueError(f"{server} post has invalid version: {version_raw!r}")
    if version < 1:
        raise ValueError(f"{server} post has invalid version: {version_raw!r}")
    title = item.get("title")
    if not isinstance(title, str) or not title.strip():
        raise ValueError(f"{server} post has no title")
    published = item.get("published")
    if isinstance(published, str) and published.strip().upper() == "NA":
        published = None
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": f"{server}:{doi}:v{version}",
        "server": server,
        "doi": doi,
        "title": title,
        "authors": item.get("authors"),
        "author_corresponding": item.get("author_corresponding"),
        "author_corresponding_institution": item.get("author_corresponding_institution"),
        "date": posted,
        "version": version,
        "type": item.get("type"),
        "license": item.get("license"),
        "category": item.get("category"),
        "published": published,
        "abstract": item.get("abstract"),
        "jatsxml": item.get("jatsxml"),
    }


def fetch_server(server: str, start: str, end: str, watermark: str,
                 boundary: frozenset, *, timeout: float, retries: int,
                 retry_budget: float, fetched_at: str, requests: list,
                 sleep=time.sleep):
    """Yield (record, posted_date, id) for posts dated after ``watermark``.

    ``start`` is the watermark day itself (re-queried for late posts); records
    dated before the watermark are dropped, as are boundary ids already
    emitted at the watermark date.
    """
    url = f"{BASE}/{server}/{start}/{end}"
    offset = 0
    total = None
    while True:
        document = http_get_json(f"{url}/{offset}/json", timeout, retries,
                                 retry_budget, sleep=sleep)
        requests.append(1)
        messages = document.get("messages")
        if not isinstance(messages, list) or not messages:
            raise BiorxivError(f"{server} response has no messages block")
        status = messages[0].get("status")
        if status == "no posts found":
            return
        if status != "ok":
            raise BiorxivError(f"{server} response status: {str(status)[:200]!r}")
        raw_total = messages[0].get("total")
        if raw_total is not None:
            total = int(raw_total)
        items = document.get("collection")
        if not isinstance(items, list):
            raise BiorxivError(f"{server} response has no collection list")
        if not items:
            return
        for item in items:
            record = normalize(item, server, fetched_at)
            posted = record["date"]
            if watermark:
                if posted < watermark:
                    continue
                if posted == watermark and record["id"] in boundary:
                    continue
            yield record, posted, record["id"]
        offset += len(items)
        if total is not None and offset >= total:
            return


def plan_server(server: str, state: dict, end: str, days_back: int):
    """Return (start, watermark, boundary) for one server's window."""
    entry = state.get(server) or {}
    watermark = entry.get("watermark", "")
    boundary = frozenset(entry.get("boundary", []))
    if watermark:
        start = watermark
    else:
        start = (date.fromisoformat(end) - timedelta(days=days_back - 1)).isoformat()
    return start, watermark, boundary


def run(servers, days_back: int, *, timeout: float, retries: int,
        retry_budget: float, state: dict, fetched_at: str, requests: list,
        sleep=time.sleep):
    """Yield (record, server, posted_date, id) for every server in the window."""
    end = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    for server in servers:
        start, watermark, boundary = plan_server(server, state, end, days_back)
        if start > end:
            continue
        for record, posted, rec_id in fetch_server(
                server, start, end, watermark, boundary,
                timeout=timeout, retries=retries, retry_budget=retry_budget,
                fetched_at=fetched_at, requests=requests, sleep=sleep):
            yield record, server, posted, rec_id


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Fetch new bioRxiv/medRxiv preprint metadata as NDJSON.")
    parser.add_argument("days_back", nargs="?", type=int, default=DEFAULT_DAYS_BACK,
                        help=f"cold-start window (default {DEFAULT_DAYS_BACK}, "
                             f"max {MAX_DAYS_BACK}); ignored once a watermark exists")
    parser.add_argument("--server", default=",".join(SERVERS),
                        help="comma-separated subset of: " + ",".join(SERVERS))
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermarks (one-off pull)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                        help=f"per-request timeout in seconds (max {MAX_TIMEOUT:.0f})")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES,
                        help=f"total attempts per request (1-{MAX_RETRIES})")
    parser.add_argument("--retry-budget", type=float, default=DEFAULT_RETRY_BUDGET,
                        help=f"max cumulative retry sleep in seconds (max {MAX_RETRY_BUDGET:.0f})")
    args = parser.parse_args(argv)

    if not 1 <= args.days_back <= MAX_DAYS_BACK:
        parser.error(f"days_back must be 1-{MAX_DAYS_BACK}")
    if not 0 < args.timeout <= MAX_TIMEOUT:
        parser.error(f"timeout must be in (0, {MAX_TIMEOUT:.0f}]")
    if not 1 <= args.retries <= MAX_RETRIES:
        parser.error(f"retries must be 1-{MAX_RETRIES}")
    if not 0 <= args.retry_budget <= MAX_RETRY_BUDGET:
        parser.error(f"retry-budget must be in [0, {MAX_RETRY_BUDGET:.0f}]")
    servers = [s.strip().lower() for s in args.server.split(",") if s.strip()]
    if not servers:
        parser.error("server list must not be empty")
    for server in servers:
        if server not in SERVERS:
            parser.error(f"server must be one of {', '.join(SERVERS)}")
    if len(servers) != len(set(servers)):
        parser.error("server list must not repeat entries")

    path = state_path(args.state_file)
    state = {} if args.no_state else load_state(path)

    fetched_at = datetime.now(timezone.utc).isoformat()
    records = 0
    server_counts = {s: 0 for s in servers}
    new_state = {s: dict(e) for s, e in state.items()}
    requests_made = []

    try:
        for record, server, posted, rec_id in run(
                servers, args.days_back, timeout=args.timeout, retries=args.retries,
                retry_budget=args.retry_budget, state=state, fetched_at=fetched_at,
                requests=requests_made):
            print(json.dumps(record, ensure_ascii=False))
            records += 1
            server_counts[server] += 1
            entry = new_state.setdefault(
                server, {"watermark": "", "boundary": []})
            if posted > entry.get("watermark", ""):
                entry["watermark"] = posted
                entry["boundary"] = [rec_id]
            elif posted == entry.get("watermark"):
                entry.setdefault("boundary", []).append(rec_id)
    except (BiorxivError, ValueError, urllib.error.URLError, TimeoutError, OSError) as error:
        print(f"{SOURCE} extraction failed: {error}", file=sys.stderr)
        return 1

    # Only after every record is written: an aborted run re-fetches instead of
    # skipping. A server with nothing new keeps its watermark untouched.
    if not args.no_state:
        save_state(path, new_state)

    summary = {
        "status": "ok",
        "records": records,
        "requests": len(requests_made),
        "servers": server_counts,
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
