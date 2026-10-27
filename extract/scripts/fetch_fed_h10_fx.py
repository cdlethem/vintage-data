#!/usr/bin/env python3
"""Fetch the Federal Reserve H.10 daily foreign-exchange release as NDJSON.

One request downloads the complete H.10 SDMX-XML file that the Board of
Governers republishes on federalreserve.gov every business day
(FRB_h10_xml.zip, ~2 MB). Only the daily (FREQ=9) series are collected:
23 currency rates (currency units per US dollar) plus six trade-weighted
index series. The file carries the full daily history (2006-01-02 onward),
so the first run baselines that history in one pull; each later run
re-emits only observations inside the trailing --revision-days window so
that Fed restatements land as new raw rows (RAW is insert-only).

The state file records the file's Header ``Prepared`` stamp plus each
series' newest date. When a download's ``Prepared`` stamp matches the
stored one the file is the one already collected, so the run emits nothing
and leaves the state untouched.

Observations with OBS_STATUS ND (no data for that date) are counted in the
run summary but not emitted: a gap is a gap.

The state file is published atomically only after all output has been
written and flushed, so a failed run never advances the watermark. Stdlib
only; no credentials are required.
"""

import argparse
import io
import json
import math
import os
import pathlib
import re
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timedelta, timezone
from typing import Callable, TextIO

SOURCE = "fed_h10_fx"
DATA_URL = "https://www.federalreserve.gov/releases/h10/data/FRB_h10_xml.zip"
DATA_HOST = "www.federalreserve.gov"
DATA_PATH = "/releases/h10/data/FRB_h10_xml.zip"
DATA_MEMBER = "H10_data.xml"
NS_MESSAGE = "{http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message}"
NS_COMMON = "{http://www.federalreserve.gov/structure/compact/common}"
NS_H10 = "{http://www.federalreserve.gov/structure/compact/H10_H10}"
DAILY_FREQ = "9"
STATE_VERSION = 1
DEFAULT_REVISION_DAYS = 14
MAX_REVISION_DAYS = 90
MAX_ZIP_BYTES = 50 * 1024 * 1024
MAX_OBS_PER_SERIES = 20_000
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

_CODE_RE = re.compile(r"[A-Z0-9]+")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_VALUE_RE = re.compile(r"\d+(?:\.\d+)?")
_PREPARED_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject redirects before urllib can issue a destination request."""

    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def build_http_opener(*handlers: urllib.request.BaseHandler) -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(NoRedirectHandler(), *handlers)


HTTP_OPENER = build_http_opener()


def validate_data_url(url: str) -> None:
    """Pin the download to the documented federalreserve.gov data file."""
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != DATA_HOST
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != DATA_PATH
        or parsed.fragment
    ):
        raise ValueError("download URL is not the documented H.10 data file")


def timestamp_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_date(value: object, field: str) -> date:
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise ValueError(f"{field} must be a YYYY-MM-DD date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} must be a YYYY-MM-DD date") from error


def default_state() -> dict:
    return {"version": STATE_VERSION, "source": SOURCE, "series": {}, "prepared": None}


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
    if set(document) != {"version", "source", "series", "prepared"}:
        raise ValueError(f"malformed state in {path}: unexpected fields")
    series = document["series"]
    if not isinstance(series, dict):
        raise ValueError(f"malformed state in {path}: series must be an object")
    for code, last in series.items():
        if not isinstance(code, str) or not _CODE_RE.fullmatch(code):
            raise ValueError(f"malformed state in {path}: bad series code {code!r}")
        iso_date(last, f"state series[{code}]")
    prepared = document["prepared"]
    if prepared is not None and (
        not isinstance(prepared, str) or not _PREPARED_RE.fullmatch(prepared)
    ):
        raise ValueError(f"malformed state in {path}: bad prepared stamp {prepared!r}")
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
    """Atomically publish state after stdout has accepted the complete output."""
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


def download_data(url: str, timeout: int) -> bytes:
    """Download the H.10 data file with a hard size bound."""
    validate_data_url(url)
    request = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}
    )
    try:
        with HTTP_OPENER.open(request, timeout=timeout) as response:
            data = response.read(MAX_ZIP_BYTES + 1)
    except urllib.error.HTTPError as error:
        if 300 <= error.code < 400:
            error.close()
            raise ValueError(
                f"H.10 download rejected HTTP redirect (status {error.code})"
            ) from None
        raise
    if len(data) > MAX_ZIP_BYTES:
        raise ValueError(f"H.10 data file exceeds the {MAX_ZIP_BYTES}-byte bound")
    return data


def _obs_period(obs: ET.Element, fx: str) -> date:
    raw = obs.get("TIME_PERIOD")
    if not raw or not _DATE_RE.fullmatch(raw):
        raise ValueError(f"series {fx} has an invalid TIME_PERIOD {raw!r}")
    try:
        return date.fromisoformat(raw)
    except ValueError as error:
        raise ValueError(f"series {fx} has an invalid TIME_PERIOD {raw!r}") from error


def _obs_value(obs: ET.Element, fx: str, period: date) -> float:
    raw = obs.get("OBS_VALUE")
    if raw is None or not _VALUE_RE.fullmatch(raw):
        raise ValueError(f"series {fx} {period.isoformat()} has an invalid OBS_VALUE {raw!r}")
    value = float(raw)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"series {fx} {period.isoformat()} has a non-positive OBS_VALUE")
    return value


def _header_prepared(header: ET.Element) -> str:
    test = header.findtext(NS_MESSAGE + "Test")
    if test != "false":
        raise ValueError("H.10 header Test is not 'false'; refusing test data")
    prepared = header.findtext(NS_MESSAGE + "Prepared")
    if not prepared or not _PREPARED_RE.fullmatch(prepared):
        raise ValueError(f"H.10 header Prepared is missing or malformed: {prepared!r}")
    return prepared


def parse_data(data: bytes) -> tuple[list[dict], str]:
    """Validate the complete H.10 XML file and return the daily series.

    Returns ``(series, prepared)`` where each series is a dict with keys
    ``fx``, ``kind``, ``currency_iso``, ``unit``, ``series_name``,
    ``observations`` (list of ``(date, value)`` ascending) and ``nd_count``.
    """
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as error:
        raise ValueError("H.10 download is not a zip archive") from error
    with archive:
        if DATA_MEMBER not in archive.namelist():
            raise ValueError(f"H.10 archive is missing {DATA_MEMBER}")
        try:
            root = ET.fromstring(archive.read(DATA_MEMBER))
        except ET.ParseError as error:
            raise ValueError(f"H.10 {DATA_MEMBER} is not well-formed XML: {error}") from error

    if root.tag != NS_MESSAGE + "MessageGroup":
        raise ValueError(f"H.10 root element is {root.tag!r}, not an SDMX MessageGroup")
    header = root.find(NS_MESSAGE + "Header")
    if header is None:
        raise ValueError("H.10 message has no Header")
    prepared = _header_prepared(header)

    dataset = root.find(NS_COMMON + "DataSet")
    if dataset is None or dataset.get("id") != "H10":
        raise ValueError("H.10 message has no DataSet id=H10")

    series_by_fx: dict[str, dict] = {}
    for element in dataset.findall(NS_H10 + "Series"):
        if element.get("FREQ") != DAILY_FREQ:
            continue  # weekly/monthly/annual aggregates are out of scope
        fx = element.get("FX")
        if not fx or not _CODE_RE.fullmatch(fx):
            raise ValueError(f"H.10 daily series has an invalid FX code {fx!r}")
        if fx in series_by_fx:
            raise ValueError(f"duplicate daily FX code {fx}")
        currency = element.get("CURRENCY")
        unit = element.get("UNIT")
        series_name = element.get("SERIES_NAME")
        if not currency or not _CODE_RE.fullmatch(currency):
            raise ValueError(f"series {fx} has an invalid CURRENCY code {currency!r}")
        if element.get("UNIT_MULT") != "1":
            raise ValueError(f"series {fx} has an unsupported UNIT_MULT")
        if not unit or not series_name:
            raise ValueError(f"series {fx} is missing UNIT or SERIES_NAME")

        observations: list[tuple[date, float]] = []
        nd_count = 0
        prior: date | None = None
        for obs in element.findall(NS_COMMON + "Obs"):
            period = _obs_period(obs, fx)
            if prior is not None and period <= prior:
                raise ValueError(
                    f"series {fx} observation {period.isoformat()} is not strictly "
                    f"ascending after {prior.isoformat()}"
                )
            prior = period
            if len(observations) + nd_count >= MAX_OBS_PER_SERIES:
                raise ValueError(f"series {fx} exceeds the {MAX_OBS_PER_SERIES}-observation bound")
            status = obs.get("OBS_STATUS")
            if status == "ND":
                nd_count += 1
                continue
            if status != "A":
                raise ValueError(f"series {fx} {period.isoformat()} has status {status!r}")
            observations.append((period, _obs_value(obs, fx, period)))
        if not observations:
            raise ValueError(f"series {fx} has no usable observations")

        series_by_fx[fx] = {
            "fx": fx,
            "kind": "index" if currency == "NA" else "currency",
            "currency_iso": None if currency == "NA" else currency,
            "unit": unit,
            "series_name": series_name,
            "observations": observations,
            "nd_count": nd_count,
        }

    if not series_by_fx:
        raise ValueError("H.10 archive contains no daily (FREQ=9) series")
    return list(series_by_fx.values()), prepared


def select_series(
    series_list: list[dict],
    state: dict,
    prepared: str,
    revision_days: int,
    full: bool,
) -> tuple[list[tuple[dict, list[tuple[date, float]]]], bool, bool]:
    """Pick each series' observations to emit.

    Returns ``(selected, baseline, unchanged)``:

    - ``unchanged`` is true when the download's ``Prepared`` stamp matches
      the stored one (the file was already collected); nothing is emitted.
    - ``baseline`` is true when at least one series is emitted in full
      history mode (first run, ``--full``, or a series new to the state).
    - Otherwise each stored series emits only dates after
      ``watermark - revision_days`` so restated values re-enter the raw
      stream, and any series missing from the state emits its full history.
    """
    if not full and state["prepared"] == prepared:
        return [(series, []) for series in series_list], False, True

    state_series = state["series"]
    selected: list[tuple[dict, list[tuple[date, float]]]] = []
    baseline = full
    for series in series_list:
        fx = series["fx"]
        watermark_raw = state_series.get(fx)
        if full or watermark_raw is None:
            selected.append((series, series["observations"]))
            if watermark_raw is None and not full:
                baseline = True
            continue
        watermark = iso_date(watermark_raw, f"state series[{fx}]")
        threshold = watermark - timedelta(days=revision_days)
        observations = [(d, v) for d, v in series["observations"] if d > threshold]
        selected.append((series, observations))
    return selected, baseline, False


def _bounded_int(name: str, low: int, high: int):
    def check(value: str) -> int:
        try:
            number = int(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"{name} must be an integer") from error
        if not low <= number <= high:
            raise argparse.ArgumentTypeError(f"{name} must be between {low} and {high}")
        return number

    return check


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    state_group = parser.add_mutually_exclusive_group()
    state_group.add_argument("--state-file", help="state file; defaults under $EXTRACT_DATA_ROOT/state")
    state_group.add_argument("--no-state", action="store_true", help="do not read or update persistent state")
    parser.add_argument("--full", action="store_true", help="emit every daily observation (re-baseline)")
    parser.add_argument(
        "--revision-days",
        type=_bounded_int("revision-days", 0, MAX_REVISION_DAYS),
        default=DEFAULT_REVISION_DAYS,
        help=f"trailing window re-emitted each run to catch restatements, 0-{MAX_REVISION_DAYS} (default {DEFAULT_REVISION_DAYS})",
    )
    parser.add_argument(
        "--timeout", type=_bounded_int("timeout", 1, 3600), default=120,
        help="per-request timeout in seconds, 1-3600 (default 120)",
    )
    return parser


def build_records(
    selected: list[tuple[dict, list[tuple[date, float]]]], fetched_at: str
) -> list[dict]:
    records = []
    for series, observations in selected:
        for period, value in observations:
            records.append({
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{SOURCE}|{series['fx']}|{period.isoformat()}",
                "date": period.isoformat(),
                "fx_code": series["fx"],
                "kind": series["kind"],
                "currency_iso": series["currency_iso"],
                "unit": series["unit"],
                "value": value,
                "series_name": series["series_name"],
            })
    records.sort(key=lambda record: (record["fx_code"], record["date"]))
    return records


def run(
    *,
    path: pathlib.Path | None,
    output: TextIO,
    revision_days: int = DEFAULT_REVISION_DAYS,
    timeout: int = 120,
    full: bool = False,
    download: Callable[[str, int], bytes] = download_data,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> int:
    if (
        revision_days < 0
        or revision_days > MAX_REVISION_DAYS
        or timeout <= 0
    ):
        raise ValueError("revision window or timeout is outside its allowed bound")
    fetched_at = now()
    if fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must include a UTC offset")
    fetched_at = fetched_at.astimezone(timezone.utc)

    state = default_state() if path is None else load_state(path)

    counters = {"attempted": 0, "succeeded": 0}
    data = None
    while data is None:
        counters["attempted"] += 1
        data = download(DATA_URL, timeout)
        counters["succeeded"] += 1

    series_list, prepared = parse_data(data)
    selected, baseline, unchanged = select_series(
        series_list, state, prepared, revision_days, full
    )
    records = build_records(selected, timestamp_text(fetched_at))
    nd_skipped = sum(series["nd_count"] for series, _ in selected if not unchanged)

    payload = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        for record in records
    )
    if payload:
        written = output.write(payload)
        if written is not None and written != len(payload):
            raise OSError("short write to NDJSON output")
    output.flush()

    # The watermark tracks the newest date present in the file per series,
    # not just the newest date emitted, so a truncated window cannot ratchet
    # the watermark backwards.
    new_series: dict[str, str] = {
        series["fx"]: series["observations"][-1][0].isoformat() for series in series_list
    }
    state_change = {
        fx: last
        for fx, last in new_series.items()
        if state["series"].get(fx) != last
    }
    dropped = sorted(set(state["series"]) - set(new_series))
    if dropped:
        print(f"H.10: daily series no longer present in the release: {', '.join(dropped)}",
              file=sys.stderr)
    if path is not None and (state_change or state["prepared"] != prepared):
        save_state(path, {**state, "series": new_series, "prepared": prepared})

    all_dates = [d for _, obs in selected for d, _ in obs]
    summary = {
        "health": "healthy",
        "completeness": "complete",
        "requests": counters,
        "state_change": state_change,
        "metrics": {
            "series": len(series_list),
            "records": len(records),
            "baseline": baseline,
            "unchanged": unchanged,
            "revision_days": revision_days,
            "nd_skipped": nd_skipped,
            "new_max_date": max(all_dates).isoformat() if all_dates else None,
            "prepared": prepared,
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    return len(records)



def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        count = run(
            path=None if args.no_state else state_path(args.state_file),
            output=sys.stdout,
            revision_days=args.revision_days,
            timeout=args.timeout,
            full=args.full,
        )
    except ValueError as error:
        parser.error(str(error))
    mode = "full history" if (args.full or args.no_state) else f"{args.revision_days}-day revision window"
    print(f"Fed H.10: {count} daily FX observations ({mode})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
