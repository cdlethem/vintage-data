#!/usr/bin/env python3
"""Fetch Melbourne's past-hour pedestrian-count snapshot with an atomic watermark."""
from __future__ import annotations
import argparse, json, os, pathlib, sys, time, urllib.error, urllib.request
from datetime import datetime, timedelta, timezone

SOURCE = "melbourne_pedestrians"
URL = "https://data.melbourne.vic.gov.au/explore/dataset/pedestrian-counting-system-past-hour-counts-per-minute/download?format=json"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
COUNT_FIELDS = {"direction_1", "direction_2", "total_of_directions"}
MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit: return pathlib.Path(explicit).expanduser()
    return pathlib.Path(os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT).expanduser() / "state" / f"{SOURCE}.json"

def load_watermark(path: pathlib.Path) -> str:
    try: data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError: return ""
    value = data.get("sensing_datetime", "")
    if not isinstance(value, str): raise ValueError(f"malformed watermark state in {path}")
    return value

def save_watermark(path: pathlib.Path, watermark: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True); staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps({"sensing_datetime": watermark}) + "\n", encoding="utf-8"); os.replace(staged, path)

def _download(timeout: int) -> tuple[list[dict], int]:
    """Fetch the full snapshot with a bounded retry on transient failures.

    The City's OpenDataSoft endpoint flaps (a 2026-11-16 blip failed both task
    attempts in well under a second) and the full snapshot takes ~30s to
    download. 429/5xx responses and transport errors retry with 1s/2s backoff
    (at most ~3s of sleeping, well inside the 15-minute task timeout);
    non-transient HTTP errors and validation failures raise on first
    occurrence. Returns the validated record list and the number of HTTP
    attempts made.
    """
    req = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    for attempt in range(MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response: document = json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt == MAX_ATTEMPTS - 1:
                raise
            reason = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            reason = str(exc.reason if isinstance(exc, urllib.error.URLError) else exc)
        else:
            if not isinstance(document, list): raise TypeError("Melbourne download is not a JSON record list")
            for item in document:
                if not isinstance(item, dict) or not isinstance(item.get("fields"), dict) or not item.get("recordid"):
                    raise ValueError("Melbourne download contains malformed record envelope")
            return document, attempt + 1
        delay = RETRY_BASE_DELAY * (2 ** attempt)
        print(f"melbourne_pedestrians: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)
    raise AssertionError("unreachable")

def fetch_counts(minutes: int = 90, timeout: int = 30, watermark: str = "", progress=None):
    if minutes <= 0: return
    fetched_at = datetime.now(timezone.utc); cutoff = max(watermark, (fetched_at - timedelta(minutes=minutes)).isoformat()) if watermark else (fetched_at - timedelta(minutes=minutes)).isoformat()
    rows, attempts = _download(timeout)
    if progress is not None: progress["requests"] = attempts
    normalized = []
    for item in rows:
        fields = item["fields"]; sensed_at = fields.get("sensing_datetime"); location = fields.get("location_id")
        if not sensed_at or location is None or not COUNT_FIELDS.intersection(fields):
            raise ValueError("Melbourne record lacks sensing_datetime, location_id, or pedestrian count fields")
        if sensed_at <= cutoff: continue
        normalized.append((sensed_at, {**fields, "source": SOURCE, "fetched_at": fetched_at.isoformat(), "id": item["recordid"], "record_timestamp": item.get("record_timestamp")}))
    for sensed_at, record in sorted(normalized, key=lambda pair: pair[0], reverse=True):
        if progress is not None and sensed_at > progress.get("sensing_datetime", ""): progress["sensing_datetime"] = sensed_at
        yield record

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--minutes", type=int, default=90); parser.add_argument("--state-file"); parser.add_argument("--no-state", action="store_true"); parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)
    records = 0
    try:
        path = state_path(args.state_file)
        watermark = "" if args.no_state else load_watermark(path)
        progress = {"sensing_datetime": watermark}
        for record in fetch_counts(args.minutes, args.timeout, watermark, progress):
            print(json.dumps(record, ensure_ascii=False))
            records += 1
        if not args.no_state and progress["sensing_datetime"] > watermark:
            save_watermark(path, progress["sensing_datetime"])
    except Exception as exc:
        print(SUMMARY_PREFIX + json.dumps({"health":"failed", "completeness":"failed", "records":records, "error":str(exc)}), file=sys.stderr)
        return 1
    print(SUMMARY_PREFIX + json.dumps({"health":"healthy", "completeness":"complete", "records":records, "coverage":{"watermark_before":watermark, "watermark_after":progress["sensing_datetime"]}, "requests":{"attempted":progress.get("requests", 0)}}), file=sys.stderr)
    return 0
if __name__ == "__main__": raise SystemExit(main())
