#!/usr/bin/env python3
"""Fetch recently published/updated GitHub Security Advisories as NDJSON.

The Global Advisory Database (GHSA) at ``api.github.com/advisories`` is public and
needs no API key (unauthenticated core rate limit: 60 requests/hour). Advisories
are updated throughout the day, so the fetcher works in UTC day windows: the
``updated=YYYY-MM-DD`` query parameter returns exactly the bucket of advisories
whose ``updated_at`` falls on that day, cursor-paginated via the ``Link`` header.

State is a single watermark: the greatest ``updated_at`` emitted by the last
successful run. Each run re-fetches the watermark's boundary day and skips,
client-side, every record with ``updated_at`` at or before the watermark, then
covers every day up to today. A failed run leaves state untouched, so the next
run re-covers the same window; the watermark filter keeps replay idempotent.
Windows are bounded both ways (initial lookback, maximum span) so one run can
never exceed the unauthenticated rate budget.

Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SOURCE = "ghsa_advisories"
API_BASE = "https://api.github.com"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
STATE_VERSION = 1
PAGE_SIZE = 100
DEFAULT_LOOKBACK_DAYS = 5
DEFAULT_MAX_DAYS = 14
DEFAULT_MAX_PAGES = 200
DEFAULT_MAX_RECORDS = 20000
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
GHSA_ID_PATTERN = re.compile(r"GHSA-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}\Z")
TRANSIENT_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
MAX_TRANSIENT_ATTEMPTS = 4
DEFAULT_RETRY_DELAY_SECONDS = 1
MAX_RETRY_DELAY_SECONDS = 90
MAX_RETRY_CUMULATIVE_DELAY_SECONDS = 240
DATE_FORMAT = "%Y-%m-%d"


class GhadvisoryError(ValueError):
    """The provider returned a document outside the expected advisory contract."""


class SummaryEmissionError(OSError):
    """The reserved run-summary line could not be fully emitted."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return value.astimezone(timezone.utc)


def parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise GhadvisoryError(f"advisory field {field} must be a non-empty timestamp, got {value!r}")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _utc(datetime.fromisoformat(text))
    except ValueError as exc:
        raise GhadvisoryError(f"advisory field {field} is not ISO-8601: {value!r}") from exc


def timestamp_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def _is_within(path: Path, directory: Path) -> bool:
    return path == directory or directory in path.parents


def validate_state_path(value: str | os.PathLike[str]) -> Path:
    """Resolve a state path and reject every path that addresses this checkout."""
    resolved = Path(value).expanduser().resolve()
    repository_root = Path(__file__).resolve().parents[2]
    if _is_within(resolved, repository_root):
        raise ValueError(f"state file must live outside the checkout: {resolved}")
    return resolved


def default_state_path() -> Path:
    data_root = Path(os.environ.get("EXTRACT_DATA_ROOT") or "~/.local/share/vintage-data/extract").expanduser()
    return data_root / "state" / f"{SOURCE}.json"


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": STATE_VERSION, "source": SOURCE, "watermark": None}
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"cannot read state file {path}: {exc}") from exc
    if (
        not isinstance(document, dict)
        or document.get("version") != STATE_VERSION
        or document.get("source") != SOURCE
    ):
        raise RuntimeError(f"state file {path} has an unexpected shape")
    watermark = document.get("watermark")
    if watermark is not None:
        parse_timestamp(watermark, "state watermark")
    return {"version": STATE_VERSION, "source": SOURCE, "watermark": watermark}


def save_state(path: Path, state: dict[str, Any]) -> None:
    """Durably replace state without exposing a partially written JSON file."""
    path = validate_state_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
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


def normalize_advisory(advisory: Any, fetched_at: str) -> dict[str, Any]:
    """Validate one advisory and lift its stable, queryable fields to the top level."""
    if not isinstance(advisory, dict):
        raise GhadvisoryError("advisory is not a JSON object")
    ghsa_id = advisory.get("ghsa_id")
    if not isinstance(ghsa_id, str) or not GHSA_ID_PATTERN.fullmatch(ghsa_id):
        raise GhadvisoryError(f"advisory has an invalid ghsa_id: {ghsa_id!r}")
    published_at = parse_timestamp(advisory.get("published_at"), "published_at")
    updated_at = parse_timestamp(advisory.get("updated_at"), "updated_at")
    if updated_at < published_at:
        raise GhadvisoryError(f"advisory {ghsa_id} has updated_at before published_at")
    vulnerabilities = advisory.get("vulnerabilities")
    if vulnerabilities is not None and not isinstance(vulnerabilities, list):
        raise GhadvisoryError(f"advisory {ghsa_id} has non-list vulnerabilities")
    return {
        "source": SOURCE,
        "id": ghsa_id,
        "fetched_at": fetched_at,
        "ghsa_id": ghsa_id,
        "cve_id": advisory.get("cve_id"),
        "severity": advisory.get("severity"),
        "type": advisory.get("type"),
        "summary": advisory.get("summary"),
        "published_at": timestamp_text(published_at),
        "updated_at": timestamp_text(updated_at),
        "cvss": advisory.get("cvss"),
        "vulnerabilities": vulnerabilities,
        "raw": advisory,
    }


def day_window_url(day: str) -> str:
    parameters = {
        "per_page": str(PAGE_SIZE),
        "sort": "updated",
        "direction": "asc",
        "updated": day,
    }
    return f"{API_BASE}/advisories?{urllib.parse.urlencode(parameters)}"


def next_cursor_url(link_header: Any, previous_url: str) -> str | None:
    """Follow the cursor in the ``Link`` header; nothing else may direct paging."""
    if link_header is None:
        return None
    if not isinstance(link_header, str):
        raise GhadvisoryError(f"advisory Link header is not a string: {link_header!r}")
    previous = urllib.parse.urlsplit(previous_url)
    for part in link_header.split(","):
        match = re.search(r'<([^>]+)>;\s*rel="next"', part.strip())
        if match is None:
            continue
        candidate = match.group(1)
        split = urllib.parse.urlsplit(candidate)
        if (split.scheme, split.netloc, split.path) != (previous.scheme, previous.netloc, previous.path):
            raise GhadvisoryError(f"advisory next link leaves the advisories endpoint: {candidate!r}")
        return candidate
    return None


def retry_after_delay(headers: Any) -> float:
    value = None
    get = getattr(headers, "get", None)
    if callable(get):
        value = get("Retry-After")
    if isinstance(value, str) and value.strip().isdigit():
        delay = float(value.strip())
    else:
        delay = DEFAULT_RETRY_DELAY_SECONDS
    return max(0.0, min(delay, MAX_RETRY_DELAY_SECONDS))


def _failure_message(exc: BaseException) -> str:
    message = " ".join(str(exc).split())
    if not message:
        message = exc.__class__.__name__
    return message[:500]


def request_page(
    url: str,
    timeout: float,
    metrics: dict[str, Any],
    retry_budget: list[float],
) -> tuple[Any, Any]:
    """GET one JSON page with bounded retries on transient provider failures.

    Returns the parsed document together with the response headers (the cursor
    lives in the ``Link`` header).
    """
    for attempt in range(1, MAX_TRANSIENT_ATTEMPTS + 1):
        metrics["requests"]["attempted"] += 1
        request = urllib.request.Request(
            url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read(MAX_RESPONSE_BYTES + 1)
                if len(payload) > MAX_RESPONSE_BYTES:
                    raise GhadvisoryError(f"advisory page exceeds {MAX_RESPONSE_BYTES} bytes")
                try:
                    document = json.loads(payload.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise GhadvisoryError("advisory response is not valid UTF-8 JSON") from exc
                metrics["requests"]["succeeded"] += 1
                metrics["requests"]["payload_bytes"] += len(payload)
                return document, response.headers
        except urllib.error.HTTPError as exc:
            metrics["requests"]["failed"] += 1
            status = exc.code
            # GitHub signals secondary rate limits as 403 with a Retry-After.
            rate_limited = status == 403 and retry_after_delay(exc.headers) > DEFAULT_RETRY_DELAY_SECONDS
            if (status in TRANSIENT_STATUS or rate_limited) and attempt < MAX_TRANSIENT_ATTEMPTS:
                delay = retry_after_delay(exc.headers)
                retry_budget[0] += delay
                if retry_budget[0] > MAX_RETRY_CUMULATIVE_DELAY_SECONDS:
                    raise GhadvisoryError(
                        f"GET {url} returned HTTP {status}; retry budget exhausted after {retry_budget[0]:.0f}s"
                    ) from exc
                time.sleep(delay)
                continue
            raise GhadvisoryError(f"GET {url} returned HTTP {status}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            metrics["requests"]["failed"] += 1
            if attempt < MAX_TRANSIENT_ATTEMPTS:
                retry_budget[0] += DEFAULT_RETRY_DELAY_SECONDS
                if retry_budget[0] > MAX_RETRY_CUMULATIVE_DELAY_SECONDS:
                    raise GhadvisoryError(f"{_failure_message(exc)}; retry budget exhausted") from exc
                time.sleep(DEFAULT_RETRY_DELAY_SECONDS)
                continue
            raise GhadvisoryError(_failure_message(exc)) from exc
    raise GhadvisoryError("unreachable: retry loop exited without a result")


def compute_window(
    now: datetime,
    watermark: datetime | None,
    *,
    lookback_days: int,
    max_days: int,
) -> tuple[list[str], int]:
    """Return the UTC day buckets to fetch, plus how many older days were dropped.

    First run: the last ``lookback_days`` days inclusive of today. Later runs:
    the watermark's day through today, truncated to ``max_days`` newest days
    after long outages (the dropped gap is reported, never fetched).
    """
    today = _utc(now).date()
    dropped = 0
    if watermark is None:
        start = today - timedelta(days=lookback_days - 1)
    else:
        start = watermark.date()
        if (today - start).days + 1 > max_days:
            start = today - timedelta(days=max_days - 1)
            dropped = (start - watermark.date()).days
    days = [(start + timedelta(days=offset)).strftime(DATE_FORMAT) for offset in range((today - start).days + 1)]
    return days, dropped


def collect_advisories(
    window: list[str],
    watermark: datetime | None,
    *,
    max_pages: int,
    max_records: int,
    timeout: float,
    request_delay: float,
    metrics: dict[str, Any],
) -> tuple[list[dict[str, Any]], datetime | None]:
    """Fetch every day bucket in ``window``; return emitted records and the new watermark."""
    records: list[dict[str, Any]] = []
    new_watermark: datetime | None = None
    for day in window:
        url = day_window_url(day)
        pages_for_day = 0
        while url is not None:
            if metrics["pages"] > 0 and request_delay > 0:
                time.sleep(request_delay)
            pages_for_day += 1
            metrics["pages"] += 1
            document, headers = request_page(url, timeout, metrics, metrics["_retry_budget"])
            if not isinstance(document, list):
                raise GhadvisoryError(f"advisory page for {day} is not a JSON array")
            for advisory in document:
                record = normalize_advisory(advisory, metrics["_fetched_at"])
                updated_at = parse_timestamp(advisory["updated_at"], "updated_at")
                metrics["records_received"] += 1
                if watermark is not None and updated_at <= watermark:
                    metrics["records_skipped_before_watermark"] += 1
                    continue
                if len(records) >= max_records:
                    metrics["cap_reason"] = f"max_records={max_records} exceeded on {day}"
                    return records, new_watermark
                records.append(record)
                if new_watermark is None or updated_at > new_watermark:
                    new_watermark = updated_at
            next_url = next_cursor_url(headers.get("Link"), url)
            if next_url is None:
                url = None
            elif pages_for_day >= max_pages:
                metrics["cap_reason"] = f"max_pages={max_pages} exceeded on {day}"
                url = None
            else:
                url = next_url
    return records, new_watermark


def run_summary(
    metrics: dict[str, Any],
    *,
    window: list[str],
    days_dropped: int,
    max_pages: int,
    max_records: int,
    failed: bool = False,
    error: str | None = None,
) -> dict[str, Any]:
    cap_reason = metrics["cap_reason"]
    complete = not failed and cap_reason is None
    summary: dict[str, Any] = {
        "health": "failed" if failed else "healthy",
        "completeness": "failed" if failed else ("complete" if complete else "partial"),
        "requests": {
            "attempted": metrics["requests"]["attempted"],
            "succeeded": metrics["requests"]["succeeded"],
            "failed": metrics["requests"]["failed"],
            "payload_bytes": metrics["requests"]["payload_bytes"],
        },
        "coverage": {
            "days_configured": len(window),
            "days_dropped": days_dropped,
            "pages": metrics["pages"],
            "records_received": metrics["records_received"],
            "records_skipped_before_watermark": metrics["records_skipped_before_watermark"],
            "cap_reason": cap_reason,
            "max_pages": max_pages,
            "max_records": max_records,
            "window_start": window[0] if window else None,
            "window_end": window[-1] if window else None,
        },
        "state_change": {"watermark": metrics["next_watermark"]},
        "metrics": {"records_emitted": metrics["records_emitted"]},
    }
    if error is not None:
        summary["metrics"]["error"] = error
    return summary


def _emit_summary(err: Any, summary: dict[str, Any]) -> None:
    try:
        err.write(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")) + "\n")
        err.flush()
    except Exception as exc:
        raise SummaryEmissionError(_failure_message(exc)) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="explicit state path; defaults under the extract data root")
    parser.add_argument("--no-state", action="store_true", help="run without reading or writing state")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help="days to fetch on the first run (default %(default)s)")
    parser.add_argument("--max-days", type=int, default=DEFAULT_MAX_DAYS,
                        help="maximum window span after a gap; older days are dropped (default %(default)s)")
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES,
                        help="per-day page bound (default %(default)s)")
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS,
                        help="per-run record bound (default %(default)s)")
    parser.add_argument("--timeout", type=float, default=30, help="per-request timeout seconds (default %(default)s)")
    parser.add_argument("--request-delay", type=float, default=1.0,
                        help="seconds between page requests (default %(default)s)")
    return parser


def main(argv: list[str] | None = None, *, out: Any = None, err: Any = None) -> int:
    out = sys.stdout if out is None else out
    err = sys.stderr if err is None else err
    args = _parser().parse_args(argv)
    if args.no_state and args.state_file:
        _parser().error("--no-state and --state-file cannot be combined")
    for name in ("lookback_days", "max_days", "max_pages", "max_records"):
        if getattr(args, name) < 1:
            _parser().error(f"--{name.replace('_', '-')} must be at least 1")
    if args.timeout <= 0 or args.request_delay < 0:
        _parser().error("--timeout must be positive and --request-delay non-negative")

    metrics: dict[str, Any] = {
        "pages": 0,
        "records_received": 0,
        "records_emitted": 0,
        "records_skipped_before_watermark": 0,
        "cap_reason": None,
        "next_watermark": None,
        "requests": {"attempted": 0, "succeeded": 0, "failed": 0, "payload_bytes": 0},
        "_retry_budget": [0.0],
        "_fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    window: list[str] = []
    days_dropped = 0
    state_path: Path | None = None
    try:
        if args.no_state:
            state: dict[str, Any] = {"version": STATE_VERSION, "source": SOURCE, "watermark": None}
        else:
            state_path = validate_state_path(args.state_file or default_state_path())
            state = load_state(state_path)
        watermark = (
            parse_timestamp(state["watermark"], "watermark") if state["watermark"] else None
        )
        window, days_dropped = compute_window(
            datetime.now(timezone.utc),
            watermark,
            lookback_days=args.lookback_days,
            max_days=args.max_days,
        )
        if days_dropped:
            print(
                f"Dropping {days_dropped} day(s) older than the {args.max_days}-day window "
                f"(watermark {state['watermark']})",
                file=err,
            )
        records, new_watermark = collect_advisories(
            window,
            watermark,
            max_pages=args.max_pages,
            max_records=args.max_records,
            timeout=args.timeout,
            request_delay=args.request_delay,
            metrics=metrics,
        )
        metrics["next_watermark"] = timestamp_text(new_watermark) if new_watermark else state["watermark"]
        for record in records:
            out.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            metrics["records_emitted"] += 1
        out.flush()
    except Exception as exc:
        error = _failure_message(exc)
        try:
            _emit_summary(err, run_summary(
                metrics, window=window, days_dropped=days_dropped,
                max_pages=args.max_pages, max_records=args.max_records,
                failed=True, error=error,
            ))
        except SummaryEmissionError:
            pass
        return 1

    try:
        _emit_summary(err, run_summary(
            metrics, window=window, days_dropped=days_dropped,
            max_pages=args.max_pages, max_records=args.max_records,
        ))
    except SummaryEmissionError as exc:
        print(f"failed to emit run summary: {exc}", file=err)
        return 1
    if state_path is not None and metrics["next_watermark"] is not None:
        try:
            save_state(state_path, {
                "version": STATE_VERSION,
                "source": SOURCE,
                "watermark": metrics["next_watermark"],
            })
        except Exception as exc:
            print(f"failed to write state: {exc}", file=err)
            return 1
    print(
        f"Fetched {metrics['records_received']} advisories across {len(window)} day(s); "
        f"emitted {metrics['records_emitted']}",
        file=err,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
