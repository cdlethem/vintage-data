#!/usr/bin/env python3
"""CISA Known Exploited Vulnerabilities (KEV) catalog changes.

The KEV catalog is CISA's list of CVEs known to be actively exploited in the
wild: each entry names the vendor, product, the vulnerability, the required
remediation, the BOD 22-01 compliance due date, and whether the vulnerability
is tied to a known ransomware campaign. It is the subset of the CVE universe
that security teams must patch on a deadline, so it is a much more useful
series than raw CVE counts: new entries mark exploited attacks appearing in
the wild, and in-entry updates (changed required action, new notes,
ransomware reclassification) track how CISA's own assessment evolves.

The feed is the complete catalog re-published as one JSON document on every
catalog change (no per-record update timestamp exists), so the fetcher keeps
the previous catalog as a baseline in state and emits only entries that were
added, updated, or removed, each tagged with ``change``.

Verified live 2026-11-24T18:20Z: keyless GET to the catalog JSON returned
HTTP 200 (1,795 entries, catalogVersion 2026.11.24, dateReleased
2026-11-24T17:47:46.199Z). The catalog grows ~20-43 entries per month
(2026-04..2026-11), i.e. it changes several times a week; the daily poll is
therefore the honest cadence.

Quirks that matter:
  * ``count`` in the document must equal ``len(vulnerabilities)``; treat a
    mismatch as a schema break, not a partial page.
  * ``cwes`` is a list of CWE identifiers (often empty); ``notes`` and
    ``forensicTriage`` are plain strings on every current entry.
  * The CDN caches the feed (max-age ~30 min), so a poll within the cache
    window of a release sees the pre-release catalog; the daily 18:41Z poll
    sits after CISA's US business day, so the next run picks up the delta.

State is written only after every record has been printed, so an aborted run
re-emits the delta rather than skipping it (at-least-once).

Stdlib only.
"""

import argparse
import hashlib
import json
import os
import pathlib
import re
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "cisa_kev"
URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
DEFAULT_TIMEOUT = 30

# Per CISA's official JSON schema
# (https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities_schema.json),
# these eight fields are required on every entry; the rest are optional.
REQUIRED_ROW_FIELDS = frozenset(
    {
        "cveID",
        "vendorProject",
        "product",
        "vulnerabilityName",
        "dateAdded",
        "shortDescription",
        "requiredAction",
        "dueDate",
    }
)
OPTIONAL_ROW_FIELDS = frozenset({"knownRansomwareCampaignUse", "forensicTriage", "notes"})
CVE_ID_PATTERN = re.compile(r"CVE-\d{4}-\d{4,}\Z")
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")


def valid_kev_date(value: str) -> bool:
    """True for YYYY-MM-DD strings that name a real calendar day."""
    if not DATE_PATTERN.fullmatch(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


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
        isinstance(record_id, str) and isinstance(row, dict)
        for record_id, row in records.items()
    ):
        raise ValueError(f"malformed state in {path}: records must map ids to objects")
    return state


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish a baseline only after stdout has accepted every record."""
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


def response_context(body: bytes) -> str:
    return body[:500].decode("utf-8", errors="replace").replace("\n", " ")


def fetch_catalog(timeout: int) -> tuple[dict, str]:
    """Download the complete catalog in one bounded request."""
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer number of seconds")
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        raise RuntimeError(
            f"KEV request failed: endpoint={URL} status={error.code} "
            f"body={response_context(error.read())!r}"
        ) from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"KEV request failed: endpoint={URL} error={error.reason}") from error
    with response:
        body = response.read()
    try:
        document = json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"KEV response is not JSON: endpoint={URL} body={response_context(body)!r}"
        ) from error
    if not isinstance(document, dict):
        raise ValueError(f"KEV response is not an object: endpoint={URL}")
    return document, hashlib.sha256(body).hexdigest()


def validated_catalog(document: dict) -> tuple[str, str, dict[str, dict]]:
    """Validate one complete catalog document without changing source values."""
    rows = document.get("vulnerabilities")
    if not isinstance(rows, list):
        raise ValueError(f"KEV response is missing vulnerabilities list: endpoint={URL}")
    count = document.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count != len(rows):
        raise ValueError(
            f"KEV response count mismatch: endpoint={URL} "
            f"count={count!r} vulnerabilities={len(rows)}"
        )
    catalog_version = document.get("catalogVersion")
    if not isinstance(catalog_version, str) or not catalog_version:
        raise ValueError(f"KEV response has invalid catalogVersion: endpoint={URL}")
    date_released = document.get("dateReleased")
    if not isinstance(date_released, str) or not date_released:
        raise ValueError(f"KEV response has invalid dateReleased: endpoint={URL}")

    indexed: dict[str, dict] = {}
    for number, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"KEV vulnerabilities[{number}] is not an object: endpoint={URL}")
        missing = REQUIRED_ROW_FIELDS - row.keys()
        if missing:
            raise ValueError(
                f"KEV vulnerabilities[{number}] is missing fields {sorted(missing)}: endpoint={URL}"
            )
        non_string = sorted(
            field for field in REQUIRED_ROW_FIELDS
            if not isinstance(row[field], str) or not row[field]
        )
        if non_string:
            raise ValueError(
                f"KEV vulnerabilities[{number}] has invalid fields {non_string}: endpoint={URL}"
            )
        for field in sorted(OPTIONAL_ROW_FIELDS):
            value = row.get(field)
            if value is not None and not isinstance(value, str):
                raise ValueError(
                    f"KEV vulnerabilities[{number}] has invalid fields {[field]}: endpoint={URL}"
                )
        cwes = row.get("cwes")
        if cwes is not None and (
            not isinstance(cwes, list) or not all(isinstance(cwe, str) for cwe in cwes)
        ):
            raise ValueError(
                f"KEV vulnerabilities[{number}] has invalid cwes list: endpoint={URL}"
            )
        cve_id = row["cveID"]
        if not CVE_ID_PATTERN.fullmatch(cve_id):
            raise ValueError(f"KEV vulnerabilities[{number}] has invalid cveID {cve_id!r}: endpoint={URL}")
        for field in ("dateAdded", "dueDate"):
            if not valid_kev_date(row[field]):
                raise ValueError(
                    f"KEV vulnerabilities[{number}] has invalid {field} {row[field]!r}: endpoint={URL}"
                )
        if cve_id in indexed:
            raise ValueError(f"KEV has duplicate cveID {cve_id!r}: endpoint={URL}")
        indexed[cve_id] = row
    return catalog_version, date_released, indexed


def canonical(row: dict) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def changed_records(
    current: dict[str, dict],
    previous: dict[str, dict],
    fetched_at: str,
    catalog_version: str,
    date_released: str,
    snapshot_sha256: str,
):
    metadata = {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "catalog_version": catalog_version,
        "date_released": date_released,
        "snapshot_sha256": snapshot_sha256,
    }
    for record_id in sorted(current):
        row = current[record_id]
        old = previous.get(record_id)
        if old is not None and canonical(old) == canonical(row):
            continue
        record = dict(row)
        record.update(metadata)
        record["id"] = record_id
        record["change"] = "new" if old is None else "updated"
        yield record
    for record_id in sorted(previous.keys() - current.keys()):
        record = dict(previous[record_id])
        record.update(metadata)
        record["id"] = record_id
        record["change"] = "removed"
        yield record


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="baseline file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="emit the complete current catalog without reading or writing state")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    path = None if args.no_state else state_path(args.state_file)
    previous = {} if path is None else load_state(path)["records"]
    document, snapshot_sha256 = fetch_catalog(args.timeout)
    catalog_version, date_released, current = validated_catalog(document)
    fetched_at = datetime.now(timezone.utc).isoformat()

    for record in changed_records(
        current, previous, fetched_at, catalog_version, date_released, snapshot_sha256
    ):
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    sys.stdout.flush()

    if path is not None:
        save_state(
            path,
            {
                "version": STATE_VERSION,
                "source": SOURCE,
                "catalog_version": catalog_version,
                "date_released": date_released,
                "snapshot_sha256": snapshot_sha256,
                "records": current,
            },
        )


if __name__ == "__main__":
    main()
