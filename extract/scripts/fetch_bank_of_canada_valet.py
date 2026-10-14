#!/usr/bin/env python3
"""Bank of Canada Valet — curated daily FX, rates and money-supply panel (keyless).

One batched Valet request per run covers all SERIES (comma-separated names on a
single /observations/{names}/json URL), so the daily run is exactly one GET.
Series mix cadences: the FX and overnight-rate series publish business-daily
(finalized at 17:00 ET, i.e. available by the next morning's 06:30 UTC run),
the money aggregates publish monthly. Per-series date watermarks handle the
mixed cadence: every run requests from the oldest stored watermark, so a
weekly/monthly series simply emits nothing until its next publication, and an
outage never creates a gap (the window slides forward only as far as data
committed).

Verified live 2026-10-14: batched request
https://www.bankofcanada.ca/valet/observations/FXUSDCAD,AVG.INTWO/json?start_date=2026-10-05
returned both series with per-date omission for dates one series had not
published. No API key; be polite via User-Agent.

Quirks:
  * Each observation is {"d": "YYYY-MM-DD", <series>: {"v": "<value>"}}; a
    series absent from an observation (or with null "v") means "no value that
    date" — it is skipped, not an error.
  * BoC occasionally revises recent values; revisions to dates at/behind the
    watermark are not re-emitted (at-least-once on new dates, same trade as
    the SWPC source).
  * A 404 means one of the curated series names was retired/renamed upstream;
    the run fails so the list can be updated, nothing is silently pruned.

Stdlib only.
"""
import argparse
import json
import os
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

SOURCE = "bank_of_canada_valet"
API_BASE = "https://www.bankofcanada.ca/valet/observations"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
DEFAULT_MAX_WINDOW_DAYS = 31
MAX_RESPONSE_BYTES = 8 * 1024 * 1024

# Curated panel: all names live-verified 2026-10-14. "Business daily" series
# first, then the monthly money aggregates.
SERIES = (
    "FXUSDCAD",     # USD/CAD
    "FXEURCAD",     # EUR/CAD
    "FXGBPCAD",     # GBP/CAD
    "FXJPYCAD",     # JPY/CAD
    "FXCHFCAD",     # CHF/CAD
    "FXAUDCAD",     # AUD/CAD
    "FXNZDCAD",     # NZD/CAD
    "FXNOKCAD",     # NOK/CAD
    "AVG.INTWO",    # Canadian Overnight Repo Rate Average (CORRA), percent
    "CL.CDN.MOST.1DL",  # Overnight money market financing rate, percent
    "V37151",       # M1+ (gross), seasonally adjusted, CAD millions
    "V41552786",    # M2 (gross), unadjusted, CAD millions
)


class FetchError(RuntimeError):
    """The Valet API could not satisfy the run."""


class MalformedResponseError(FetchError):
    """The response does not match the Valet observation contract."""


def _reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise MalformedResponseError(f"Valet response contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value):
    raise MalformedResponseError(f"Valet response contains invalid JSON number {value}")


def load_response(body: bytes) -> dict:
    """Decode JSON while rejecting duplicate object keys and non-finite constants."""
    doc = json.loads(
        body.decode("utf-8"),
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_json_constant,
    )
    if not isinstance(doc, dict):
        raise MalformedResponseError("Valet response is not a JSON object")
    if "observations" not in doc:
        # Valet answers unknown series or bad parameters with {"message": ...}.
        raise MalformedResponseError(
            f"Valet response has no 'observations': {str(doc.get('message', doc))[:200]}"
        )
    if not isinstance(doc["observations"], list):
        raise MalformedResponseError("'observations' is not a list")
    return doc


def build_url(series, start_date: str) -> str:
    query = urllib.parse.urlencode({"start_date": start_date})
    return f"{API_BASE}/{','.join(series)}/json?{query}"


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the per-series date watermarks live: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict) or "series" not in state:
        raise ValueError(f"malformed watermark state in {path}")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the watermarks atomically so a crash can't leave them half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def _parse_date(value: str, field: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{field} {value!r} is not a YYYY-MM-DD date") from None


def plan_window(watermarks: dict, start_date: str | None, today: date) -> tuple[date, str]:
    """Return (effective_start, provenance) for this run's single request."""
    if start_date:
        return _parse_date(start_date, "--start-date"), "explicit"
    if watermarks:
        start = min(_parse_date(v, "watermark") for v in watermarks.values())
        provenance = "oldest watermark"
        floor = today - timedelta(days=DEFAULT_MAX_WINDOW_DAYS)
        if start < floor:
            start = floor
            provenance += " (clamped to 31-day window)"
        return start, provenance
    return today - timedelta(days=DEFAULT_MAX_WINDOW_DAYS), "cold (31-day window)"


def _series_detail(doc: dict) -> dict:
    detail = doc.get("seriesDetail")
    return detail if isinstance(detail, dict) else {}


def _parse_value(raw):
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float)):
        raise MalformedResponseError(f"unexpected Valet value type {type(raw).__name__}: {raw!r}")
    if isinstance(raw, str):
        try:
            return float(raw)
        except ValueError:
            return raw  # defensive: keep the literal, flag it in the run summary
    return raw


def normalize_records(doc: dict, fetched_at: str, watermarks: dict):
    """Yield (record, series, period) for rows newer than that series' watermark."""
    detail = _series_detail(doc)
    meta = {}
    for name in SERIES:
        spec = detail.get(name)
        if isinstance(spec, dict):
            md = spec.get("metadata") if isinstance(spec.get("metadata"), dict) else {}
            meta[name] = (spec.get("label"), md.get("unit"))
        else:
            meta[name] = (None, None)

    for index, obs in enumerate(doc["observations"]):
        if not isinstance(obs, dict):
            raise MalformedResponseError(f"observation {index} is not an object")
        period = obs.get("d")
        if not isinstance(period, str) or _safe_date(period) is None:
            raise MalformedResponseError(f"observation {index} has no valid 'd' date: {period!r}")
        for name in SERIES:
            cell = obs.get(name)
            if cell is None:
                continue  # series did not publish that date
            if not isinstance(cell, dict):
                raise MalformedResponseError(f"observation {index} cell for {name} is not an object")
            value = _parse_value(cell.get("v"))
            if value is None:
                continue  # explicit null: no value that date
            label, unit = meta[name]
            if isinstance(value, str):
                print(
                    f"warning: non-numeric value for {name} on {period}: {value!r}",
                    file=sys.stderr,
                )
            watermark = watermarks.get(name, "")
            if watermark and period <= watermark:
                continue
            yield {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{name}:{period}",
                "series": name,
                "label": label,
                "date": period,
                "value": value,
                "unit": unit,
            }, name, period


def _safe_date(value: str):
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _emit_summary(payload: dict):
    print(SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")), file=sys.stderr)


def run(series, watermarks, start_date, timeout, fetched_at) -> tuple[int, dict, dict]:
    """Fetch one window; return (record_count, progress_watermarks, summary)."""
    plan_start, provenance = plan_window(watermarks, start_date, date.today())
    url = build_url(series, plan_start.isoformat())
    print(f"GET {url} (window from {plan_start}, {provenance})", file=sys.stderr)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        snippet = ""
        try:
            snippet = exc.read(500).decode("utf-8", "replace")
        except Exception:
            pass
        raise FetchError(f"Valet request failed: HTTP {exc.code} {snippet[:300]}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise FetchError(f"Valet request failed: {exc}") from exc
    if len(body) > MAX_RESPONSE_BYTES:
        raise MalformedResponseError(f"response exceeds {MAX_RESPONSE_BYTES} bytes")

    doc = load_response(body)
    progress = dict(watermarks)
    records = 0
    series_with_new_records = set()
    for record, name, period in normalize_records(doc, fetched_at, watermarks):
        print(json.dumps(record, ensure_ascii=False))
        records += 1
        series_with_new_records.add(name)
        if period > progress.get(name, ""):
            progress[name] = period

    summary = {
        "health": "healthy",
        "completeness": "complete",
        "requests": {"attempted": 1, "succeeded": 1, "failed": 0},
        "metrics": {
            "records": records,
            "series_requested": len(series),
            "series_with_new_records": len(series_with_new_records),
            "window_start": plan_start.isoformat(),
        },
    }
    return records, progress, summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file",
                        help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write watermarks (one-off pull)")
    parser.add_argument("--start-date",
                        help="explicit YYYY-MM-DD window start (reconciliation run)")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    args = parser.parse_args(argv)

    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    path = state_path(args.state_file)
    state = {} if args.no_state else load_state(path)
    watermarks = state.get("series", {}) if isinstance(state, dict) else {}
    fetched_at = datetime.now(timezone.utc).isoformat()

    try:
        records, progress, summary = run(
            SERIES, watermarks, args.start_date, args.timeout, fetched_at
        )
    except (FetchError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        _emit_summary({
            "health": "failed",
            "completeness": "failed",
            "requests": {"attempted": 1, "succeeded": 0, "failed": 1},
            "metrics": {"records": 0, "error": str(exc)[:500]},
        })
        raise SystemExit(1)

    # Only after every record is written: an aborted run re-fetches instead of skipping.
    if not args.no_state and progress != watermarks:
        save_state(path, {"version": STATE_VERSION, "series": progress})

    _emit_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
