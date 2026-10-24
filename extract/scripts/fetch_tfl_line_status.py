#!/usr/bin/env python3
"""TfL — London public transport line and route status (keyless, JSON).

Snapshot of the live service status for every TfL-operated line and bus route:
tube (11 lines), overground (6), tram (1), DLR (1) and bus (~675 routes). Each
record carries the current severity/description, every validity-bounded status
(e.g. planned engineering works with future windows) and the active disruption
notices, so a run captures both incident onset and resolution as the notices
appear and clear during the day.

Verified live 2026-10-24: `GET https://api.tfl.gov.uk/Line/Mode/tube/Status`
returned 11 lines with clean JSON; the Piccadilly line carried a "Minor
Delays" notice created 2026-10-24T05:34:52Z (signal fault), proving the
endpoint reflects current incidents, not a stale baseline.

Terms: anonymous API access is keyless and rate-limited to 50 requests/minute
(api-portal.tfl.gov.uk); this run issues 5 requests. The data is licensed
under the Open Government Licence v3.0 (OGL v3.0); attribution to Transport
for London is required.

Quirks (important for the ingestion agent):
  * `created`/`modified` on the line object are static per deployment and do
    NOT track status changes — the change signal is in `lineStatuses`
    validity periods and `disruptions` (both carry live timestamps).
  * `created` on a lineStatus is the epoch sentinel `0001-01-01T00:00:00`;
    use the validity periods, not that field. An EMPTY validityPeriods list
    means a standing, currently-in-force status (e.g. "Good Service"); only
    incident statuses carry bounded from/to periods with isNow flags.
  * A line may carry several lineStatuses with overlapping validity periods
    (current incident plus planned works); keep them all.
  * `crowding` is an empty object for most lines; keep it only when populated.
  * Bus routes are Line objects too; `routeSections` is empty outside
    route-level disruptions and is dropped (the disruption text covers it).
  * HTTP 400 from a mode endpoint means the mode name is wrong (a config
    bug); raise it, do not treat as empty.

Stdlib only.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.tfl.gov.uk"
SOURCE = "tfl_line_status"
MODES = ("tube", "bus", "tram", "dlr", "overground")
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2


def _get_json(path: str, timeout: int):
    """GET `path` with retries for transport errors, 429s and 5xx responses."""
    url = f"{BASE}{urllib.parse.quote(path)}"
    last = None
    for attempt in range(1, ATTEMPTS + 1):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code < 500 and exc.code != 429:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last = exc
        if attempt < ATTEMPTS:
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise last


def _validity(status: dict) -> dict:
    periods = status.get("validityPeriods") or []
    is_now = any(p.get("isNow") for p in periods)
    if not periods:
        # An empty validityPeriods list means a standing status, i.e. the
        # current one (e.g. "Good Service"); only incident statuses carry
        # bounded periods.
        return {"valid_from": None, "valid_to": None, "is_now": True}
    return {
        "valid_from": min(p.get("fromDate") for p in periods),
        "valid_to": max(p.get("toDate") for p in periods),
        "is_now": is_now,
    }


def _status(status: dict) -> dict:
    return {
        "severity": status.get("statusSeverity"),
        "description": status.get("statusSeverityDescription"),
        "reason": (status.get("reason") or "").strip() or None,
        **_validity(status),
    }


def _disruption(d: dict) -> dict:
    return {
        "category": d.get("category"),
        "reason": (d.get("reason") or "").strip() or None,
        "description": (d.get("description") or "").strip() or None,
        "created": d.get("created"),
        "valid_from": d.get("validFrom"),
        "valid_to": d.get("validTo"),
        "is_now": d.get("isNow"),
    }


def fetch_lines(mode: str, timeout: int, fetched_at: str | None = None) -> list[dict]:
    """Normalized status records for one mode."""
    document = _get_json(f"/Line/Mode/{mode}/Status", timeout)
    if not isinstance(document, list):
        raise TypeError(f"TfL /Line/Mode/{mode}/Status returned a non-list payload")
    if fetched_at is None:
        fetched_at = datetime_now()
    records = []
    for line in document:
        line_id = line.get("id")
        if line_id is None:
            raise ValueError(f"TfL {mode} line entry is missing id")
        statuses = [_status(s) for s in (line.get("lineStatuses") or [])]
        if not statuses:
            raise ValueError(f"TfL {mode}:{line_id} has no lineStatuses")
        current = next((s for s in statuses if s["is_now"]), statuses[0])
        crowding = line.get("crowding")
        crowding = {k: v for k, v in crowding.items() if k != "$type"} if crowding else None
        crowding = crowding or None
        records.append({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"{mode}:{line_id}",
            "mode": mode,
            "line_id": line_id,
            "name": line.get("name"),
            "status": current["description"],
            "status_severity": current["severity"],
            "statuses": statuses,
            "disruptions": [_disruption(d) for d in (line.get("disruptions") or [])],
            "crowding": crowding,
            "line_created": line.get("created"),
            "line_modified": line.get("modified"),
        })
    return records


def datetime_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=MODES + ("all",), default="all",
                        help="single mode to fetch, or all five (default)")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    fetched_at = datetime_now()
    for mode in MODES if args.mode == "all" else (args.mode,):
        records = fetch_lines(mode, args.timeout, fetched_at)
        print(f"mode={mode} lines={len(records)}", file=sys.stderr)
        for record in records:
            print(json.dumps(record, ensure_ascii=False))
    return 0


if __name__ == "__main__":

    sys.exit(main())
