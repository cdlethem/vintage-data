#!/usr/bin/env python3
"""Fetch newly published GitHub Global Advisory Database entries as NDJSON.

Security advisories for open-source packages (pip, npm, maven, cargo, ...),
published continuously — 407 in the single UTC day of 2026-10-07 (verified
live 2026-10-08) — each carrying severity, CVSS, CWE classes, affected
package/version ranges and first-patched versions. Complements nvd_cves:
advisories appear here before, and sometimes without, NVD cataloguing, and
the package-level fix data is what downstream users act on.

Keyless endpoint: GET api.github.com/advisories with a
``published=START..END`` window. Two quirks shape this script:

  * The ``page`` parameter is ignored beyond the first 100 results (verified
    live 2026-10-08: page=2 returned the identical 100 records as page=1).
    ``collect_range`` therefore recursively bisects the time window: any
    slice that comes back with exactly 100 records is split in half and
    re-queried, down to a 5-minute floor. At the observed ~400/day volume a
    full-week catch-up costs on the order of a few dozen requests; a slice
    that is still capped at the floor raises rather than silently dropping
    records.
  * Unauthenticated rate limit is 60 requests/hour. The script self-imposes
    1 s spacing between requests, and on 403/429 with
    X-RateLimit-Remaining: 0 it sleeps until X-RateLimit-Reset and retries
    the request once. State is published only after the whole batch is
    written, so a failed run never advances the watermark and the next
    scheduled run replays the window.

Watermark is ``(last_published_at, last_id)``: an advisory is emitted only
when ``(published_at, ghsa_id)`` sorts strictly after it, so a boundary
second can never duplicate records. ``credits`` (reporter profiles: logins,
avatars, names) is excluded from emitted records as personal data; all
advisory content (summary, description, references, identifiers) is kept.

Stdlib only.
"""
import argparse
import json
import os
import pathlib
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable, TextIO

SOURCE = "github_advisories"
ENDPOINT = "https://api.github.com/advisories"
PAGE_SIZE = 100          # GitHub's maximum per_page — and the only page it serves
MIN_SLICE = timedelta(minutes=5)
DEFAULT_LOOKBACK_HOURS = 24
MAX_LOOKBACK_HOURS = 168
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
MIN_REQUEST_INTERVAL = 1.0
MAX_RATE_LIMIT_WAIT = 900  # keep a rate-limited retry inside the DAG timeout
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"


def timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or "T" not in value:
        raise ValueError(f"{field} must be an ISO-8601 timestamp, got {value!r}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} is not a valid ISO-8601 timestamp: {value!r}") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def timestamp_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def default_state() -> dict:
    return {
        "version": STATE_VERSION,
        "source": SOURCE,
        "last_published_at": None,
        "last_id": None,
    }


def state_path(explicit: str | None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def validate_state(document: object, path: pathlib.Path) -> dict:
    if not isinstance(document, dict):
        raise ValueError(f"malformed state in {path}: expected object")
    if document.get("version") != STATE_VERSION or document.get("source") != SOURCE:
        raise ValueError(f"malformed state in {path}: unexpected version or source")
    if set(document) != {"version", "source", "last_published_at", "last_id"}:
        raise ValueError(f"malformed state in {path}: unexpected fields")
    last_at, last_id = document["last_published_at"], document["last_id"]
    if last_at is None and last_id is None:
        return document
    if not isinstance(last_at, str) or not isinstance(last_id, str):
        raise ValueError(f"malformed state in {path}: watermark must be null or (timestamp, id)")
    timestamp(last_at, "state last_published_at")
    return document


def load_state(path: pathlib.Path) -> dict:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default_state()
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read state from {path}: {error}") from error
    return validate_state(document, path)


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish state after stdout has accepted the complete batch."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as output:
            temporary_name = output.name
            json.dump(state, output, ensure_ascii=False, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
        raise


def _published_param(start: datetime, end: datetime) -> str:
    """Widen the query window to whole seconds (never narrow it)."""
    start_s = start.replace(microsecond=0)
    end_s = end.replace(microsecond=0)
    if end_s < end:
        end_s += timedelta(seconds=1)
    return f"{start_s:%Y-%m-%dT%H:%M:%SZ}..{end_s:%Y-%m-%dT%H:%M:%SZ}"


def build_url(start: datetime, end: datetime) -> str:
    parameters = {
        "published": _published_param(start, end),
        "per_page": PAGE_SIZE,
        "sort": "published",
        "direction": "asc",
    }
    return f"{ENDPOINT}?{urllib.parse.urlencode(parameters)}"


def request_json(
    url: str,
    timeout: int,
    *,
    counters: dict,
    epoch_now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> list:
    """One GET, retried once after sleeping out a keyless rate-limit window."""
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": USER_AGENT,
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    for attempt in (1, 2):
        counters["attempted"] += 1
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                document = json.load(response)
        except urllib.error.HTTPError as error:
            headers = error.headers
            remaining = headers.get("X-RateLimit-Remaining") if headers else None
            reset = headers.get("X-RateLimit-Reset") if headers else None
            if attempt == 1 and error.code in (403, 429) and remaining == "0" and reset:
                counters["rate_limit_waits"] += 1
                wait = min(max(0, int(reset) - epoch_now()) + 5, MAX_RATE_LIMIT_WAIT)
                if wait > 0:
                    sleep(wait)
                continue
            raise
        counters["succeeded"] += 1
        return validate_rows(document)
    raise RuntimeError("unreachable")  # pragma: no cover


def validate_rows(document: object) -> list:
    if not isinstance(document, list):
        raise ValueError(
            f"GitHub advisories response is not a JSON list: {type(document).__name__}"
        )
    for row in document:
        if not isinstance(row, dict) or not isinstance(row.get("ghsa_id"), str) or not row["ghsa_id"]:
            raise ValueError("GitHub advisories response contains a record without a ghsa_id")
    return document


def collect_range(
    start: datetime,
    end: datetime,
    *,
    timeout: int,
    transport: Callable[..., list],
    counters: dict,
) -> list[dict]:
    """Every advisory published in [start, end], complete via bisection.

    The endpoint serves at most 100 records per query and ignores ``page``;
    a slice that comes back at the cap is split in half. Slices overlap at
    the midpoint, so dedupe on ghsa_id.
    """
    rows = validate_rows(transport(build_url(start, end), timeout, counters=counters))
    if len(rows) < PAGE_SIZE:
        return rows
    if end - start > MIN_SLICE:
        mid = start + (end - start) / 2
        left = collect_range(start, mid, timeout=timeout, transport=transport, counters=counters)
        right = collect_range(mid, end, timeout=timeout, transport=transport, counters=counters)
        merged: dict[str, dict] = {}
        for row in left + right:
            if row["ghsa_id"] not in merged:
                merged[row["ghsa_id"]] = row
        return list(merged.values())
    raise ValueError(
        f"advisory window {_published_param(start, end)} returned the {PAGE_SIZE}-record "
        "page cap; refusing to drop records — widen the schedule margin or the window"
    )


def normalize(advisory: dict, fetched_at: str) -> dict:
    ghsa_id = advisory.get("ghsa_id")
    if not isinstance(ghsa_id, str) or not ghsa_id:
        raise ValueError("advisory is missing ghsa_id")
    published_at = timestamp(advisory.get("published_at"), f"{ghsa_id} published_at")
    vulnerabilities = advisory.get("vulnerabilities")
    if not isinstance(vulnerabilities, list):
        raise ValueError(f"{ghsa_id} vulnerabilities must be a list")
    for entry in vulnerabilities:
        package = entry.get("package") if isinstance(entry, dict) else None
        if not isinstance(package, dict) or not package.get("name") or not package.get("ecosystem"):
            raise ValueError(f"{ghsa_id} has a vulnerability entry without package name/ecosystem")
    cvss = advisory.get("cvss")
    cwes = advisory.get("cwes")
    cwe_ids = (
        [c.get("cwe_id") for c in cwes if isinstance(c, dict) and c.get("cwe_id")]
        if isinstance(cwes, list)
        else []
    )
    references = advisory.get("references")
    urls = (
        [r.get("url") for r in references if isinstance(r, dict) and r.get("url")]
        if isinstance(references, list)
        else []
    )
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": ghsa_id,
        "cve_id": advisory.get("cve_id"),
        "summary": advisory.get("summary"),
        "description": advisory.get("description"),
        "severity": advisory.get("severity"),
        "cvss": cvss if isinstance(cvss, dict) else None,
        "cvss_severities": advisory.get("cvss_severities"),
        "cwes": cwe_ids,
        "vulnerabilities": [
            {
                "package": entry["package"]["name"],
                "ecosystem": entry["package"]["ecosystem"],
                "vulnerable_version_range": entry.get("vulnerable_version_range"),
                "first_patched_version": entry.get("first_patched_version"),
                "vulnerable_functions": entry.get("vulnerable_functions") or [],
            }
            for entry in vulnerabilities
        ],
        "epss": advisory.get("epss"),
        "comments": advisory.get("comments"),
        "github_reviewed_at": advisory.get("github_reviewed_at"),
        "nvd_published_at": advisory.get("nvd_published_at"),
        "published_at": timestamp_text(published_at),
        "updated_at": advisory.get("updated_at"),
        "withdrawn_at": advisory.get("withdrawn_at"),
        "references": urls,
        "identifiers": advisory.get("identifiers"),
        "type": advisory.get("type"),
        "source_code_location": advisory.get("source_code_location"),
        "url": advisory.get("url"),
        "html_url": advisory.get("html_url"),
    }


def run(
    *,
    path: pathlib.Path | None,
    output: TextIO,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    timeout: int = 30,
    transport: Callable[..., list] = request_json,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    if not 1 <= lookback_hours <= MAX_LOOKBACK_HOURS:
        raise ValueError(f"lookback must be 1-{MAX_LOOKBACK_HOURS} hours")
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    fetched_at = now()
    if fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must include a UTC offset")
    fetched_at = fetched_at.astimezone(timezone.utc)

    state = default_state() if path is None else load_state(path)
    watermark = None
    if state["last_published_at"] is not None:
        watermark = (
            timestamp(state["last_published_at"], "state last_published_at"),
            state["last_id"],
        )

    end = fetched_at
    if watermark is not None:
        start = watermark[0]
        truncated = False
    else:
        start = fetched_at - timedelta(hours=lookback_hours)
        truncated = False
    floor = fetched_at - timedelta(hours=MAX_LOOKBACK_HOURS)
    if start < floor:
        start = floor
        truncated = True

    counters = {"attempted": 0, "succeeded": 0, "rate_limit_waits": 0}
    advisories = []
    if start < end:
        last_request_at = [None]

        def spaced_transport(url: str, req_timeout: int, counters: dict = counters) -> list:
            if last_request_at[0] is not None:
                delta = MIN_REQUEST_INTERVAL - (monotonic() - last_request_at[0])
                if delta > 0:
                    sleep(delta)
            last_request_at[0] = monotonic()
            return transport(url, req_timeout, counters=counters)

        advisories = collect_range(
            start, end, timeout=timeout, transport=spaced_transport, counters=counters
        )

    records = []
    max_key = None
    for advisory in advisories:
        key = (
            timestamp(advisory.get("published_at"), f"{advisory['ghsa_id']} published_at"),
            advisory["ghsa_id"],
        )
        if watermark is not None and key <= watermark:
            continue
        if max_key is None or key > max_key:
            max_key = key
        records.append(normalize(advisory, timestamp_text(fetched_at)))

    payload = "".join(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records)
    if payload:
        written = output.write(payload)
        if written is not None and written != len(payload):
            raise OSError("short write to NDJSON output")
    output.flush()

    state_change = {}
    if path is not None and start < end:
        if max_key is not None:
            new_at, new_id = timestamp_text(max_key[0]), max_key[1]
        else:
            new_at, new_id = timestamp_text(end), ""
        save_state(
            path,
            {**state, "last_published_at": new_at, "last_id": new_id},
        )
        state_change = {"last_published_at": new_at, "last_id": new_id}

    summary = {
        "health": "healthy",
        "completeness": "complete",
        "requests": {"attempted": counters["attempted"], "succeeded": counters["succeeded"]},
        "state_change": state_change,
        "metrics": {
            "window_start": timestamp_text(start),
            "window_end": timestamp_text(end),
            "records": len(records),
            "in_window": len(advisories),
            "rate_limit_waits": counters["rate_limit_waits"],
            "truncated": truncated,
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    return len(records)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="do not read or update persistent state")
    parser.add_argument(
        "--lookback-hours", type=int, default=DEFAULT_LOOKBACK_HOURS,
        help=f"initial-run window when no state exists, 1-{MAX_LOOKBACK_HOURS} (default {DEFAULT_LOOKBACK_HOURS})",
    )
    parser.add_argument("--timeout", type=int, default=30)
    arguments = parser.parse_args()
    if arguments.no_state and arguments.state_file:
        parser.error("--no-state and --state-file are mutually exclusive")
    if not 1 <= arguments.lookback_hours <= MAX_LOOKBACK_HOURS:
        parser.error(f"--lookback-hours must be 1-{MAX_LOOKBACK_HOURS}")
    if arguments.timeout <= 0:
        parser.error("--timeout must be positive")
    try:
        count = run(
            path=None if arguments.no_state else state_path(arguments.state_file),
            output=sys.stdout,
            lookback_hours=arguments.lookback_hours,
            timeout=arguments.timeout,
        )
    except Exception as error:
        print(
            SUMMARY_PREFIX
            + json.dumps(
                {"health": "failed", "completeness": "failed", "records": 0, "error": str(error)},
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1
    print(f"Collected {count} GitHub advisories", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
