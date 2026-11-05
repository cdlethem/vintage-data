#!/usr/bin/env python3
"""NOAA Tsunami Warning Centers — current tsunami products (CAP 1.2).

The US National Tsunami Warning Center (PAAQ; continental US, Alaska, Canada) and
the Pacific Tsunami Warning Center (PHEB; Hawaii, US Pacific and international
regions) each publish their current tsunami product — warning, advisory, watch,
information statement, or cancellation — as CAP 1.2 XML at a stable URL.  Every
product revision carries a fresh CAP identifier such as ``PAAQ-2-to0j1g``, so a
stateful poller that emits only identifiers it has not seen before records the
complete message history without re-emitting unchanged products.  Records retain
the structured CAP fields (severity, urgency, event location, instruction,
product link).

US Government work (NOAA/NWS); public domain, no licence restrictions.  Products
are published irregularly in response to earthquakes, so poll no more often than
every 15 minutes.

Python standard library only.
"""

import argparse
import errno
import json
import os
import pathlib
import socket
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

SOURCE = "noaa_tsunami_warnings"
STATIONS = {
    "PAAQ": "https://www.tsunami.gov/events/xml/PAAQCAP.xml",
    "PHEB": "https://www.tsunami.gov/events/xml/PHEBCAP.xml",
}
CAP_NS = "{urn:oasis:names:tc:emergency:cap:1.2}"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
MAX_ATTEMPTS = 3
RETRY_DELAYS = (1, 2)
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
MAX_TRACKED_IDS = 5000
ALERT_FIELDS = ("identifier", "sender", "sent", "status", "msgType", "source", "scope", "incidents")
INFO_FIELDS = (
    "category",
    "event",
    "responseType",
    "urgency",
    "severity",
    "certainty",
    "expires",
    "senderName",
    "headline",
    "description",
    "instruction",
    "web",
)
REQUIRED_FIELDS = ("identifier", "sent", "event")


def state_path(explicit: str | None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    try:
        with path.open(encoding="utf-8") as handle:
            state = json.load(handle)
    except FileNotFoundError:
        return {"version": STATE_VERSION, "source": SOURCE, "records": {}}
    if not isinstance(state, dict):
        raise ValueError(f"malformed state in {path}: expected object")
    if state.get("version") != STATE_VERSION or state.get("source") != SOURCE:
        raise ValueError(f"malformed state in {path}: incompatible version or source")
    records = state.get("records")
    if not isinstance(records, dict) or not all(
        isinstance(record_id, str) and isinstance(entry, dict) and isinstance(entry.get("sent"), str)
        for record_id, entry in records.items()
    ):
        raise ValueError(f"malformed state in {path}: records must map ids to objects with a sent timestamp")
    return state


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish the id baseline only after stdout has accepted every record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def is_timeout_error(error: BaseException) -> bool:
    """Return whether *error* is a real timeout, including urllib wrappers."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, (TimeoutError, socket.timeout)):
            return True
        if isinstance(error, OSError) and error.errno == errno.ETIMEDOUT:
            return True
        if isinstance(error, urllib.error.URLError):
            error = error.reason
        else:
            return False
    return False


def response_context(body: bytes) -> str:
    return body[:500].decode("utf-8", errors="replace").replace("\n", " ")


def fetch_cap(url: str, timeout: int) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code not in RETRIABLE_STATUSES:
                raise RuntimeError(
                    f"CAP request failed: endpoint={url} status={error.code} "
                    f"body={response_context(error.read())!r}"
                ) from error
            retriable = True
        except Exception as error:  # URLError, timeout
            retriable = is_timeout_error(error)
            if not retriable:
                raise RuntimeError(f"CAP request failed: endpoint={url} error={error}") from error
        if attempt == MAX_ATTEMPTS:
            raise RuntimeError(
                f"CAP request failed after {attempt} attempts: endpoint={url}"
            )
        time.sleep(RETRY_DELAYS[attempt - 1])
    raise RuntimeError(f"unreachable: CAP request loop ended: endpoint={url}")


def _element_text(parent: ET.Element, tag: str) -> str | None:
    if parent is None:
        return None
    child = parent.find(CAP_NS + tag)
    if child is None or child.text is None:
        return None
    text = child.text.strip()
    return text or None


def parse_alerts(body: bytes, station: str) -> list[dict]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError as error:
        raise ValueError(f"CAP document is not XML: station={station} body={response_context(body)!r}") from error
    if root.tag == CAP_NS + "alert":
        alerts = [root]
    elif root.tag == CAP_NS + "alerts":
        alerts = root.findall(CAP_NS + "alert")
    else:
        raise ValueError(
            f"unexpected CAP root element: station={station} root={root.tag} body={response_context(body)!r}"
        )
    records = []
    for alert in alerts:
        info = alert.find(CAP_NS + "info")
        fields = {tag: _element_text(alert, tag) for tag in ALERT_FIELDS}
        fields.update({tag: _element_text(info, tag) for tag in INFO_FIELDS})
        parameters = {}
        for parameter in (info.findall(CAP_NS + "parameter") if info is not None else []):
            value_name = _element_text(parameter, "valueName")
            value = _element_text(parameter, "value")
            if value_name is not None:
                parameters[value_name] = value
        missing = [tag for tag in REQUIRED_FIELDS if fields.get(tag) is None]
        if missing:
            raise ValueError(f"CAP alert is missing required fields: station={station} missing={missing}")
        fields["station"] = station
        fields["parameters"] = parameters
        records.append(fields)
    return records


def sent_sort_key(record: dict) -> tuple[datetime, str]:
    try:
        return datetime.fromisoformat(record["sent"]), record["id"]
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc), record["id"]


def track_ids(previous: dict, emitted: list[dict]) -> dict:
    records = dict(previous)
    for record in emitted:
        records[record["id"]] = {"sent": record["sent"]}
    if len(records) > MAX_TRACKED_IDS:
        newest = sorted(records.items(), key=lambda item: sent_sort_key({"sent": item[1]["sent"], "id": item[0]}))
        records = dict(newest[-MAX_TRACKED_IDS:])
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file", help="id baseline file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="emit every product in the feeds without reading or writing state")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    print(
        "Source: NOAA Tsunami Warning Centers (PAAQ, PHEB), current CAP 1.2 products; "
        "US Government work, public domain. " + ", ".join(STATIONS.values()),
        file=sys.stderr,
    )
    path = None if args.no_state else state_path(args.state_file)
    previous = {} if path is None else load_state(path)["records"]
    seen = set() if path is None else set(previous)
    fetched_at = datetime.now(timezone.utc).isoformat()

    station_stats = {}
    emitted: list[dict] = []
    for station, url in STATIONS.items():
        body = fetch_cap(url, args.timeout)
        alerts = parse_alerts(body, station)
        new_records = 0
        for alert in alerts:
            if alert["identifier"] in seen:
                continue
            record = dict(alert)
            record.update({"source": SOURCE, "fetched_at": fetched_at, "id": alert["identifier"]})
            print(json.dumps(record, ensure_ascii=False, sort_keys=True))
            emitted.append(record)
            new_records += 1
        station_stats[station] = {"alerts": len(alerts), "new": new_records}
    sys.stdout.flush()

    if path is not None:
        tracked = track_ids(previous, emitted)
        save_state(path, {"version": STATE_VERSION, "source": SOURCE, "records": tracked})
    else:
        tracked = {}

    print(
        SUMMARY_PREFIX
        + json.dumps(
            {
                "stations": station_stats,
                "new_records": len(emitted),
                "tracked_ids": len(tracked),
            },
            separators=(",", ":"),
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
