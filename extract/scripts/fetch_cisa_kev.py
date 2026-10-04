#!/usr/bin/env python3
"""CISA KEV — Known Exploited Vulnerabilities catalog (keyless, no auth).

Sector: **security / threat state.** The CISA catalog of vulnerabilities with
known exploitation in the wild — the shortlist of CVEs CISA has seen actively
weaponised, as opposed to NVD's complete catalogue of reported ones (that is
what `nvd_cves` collects). Each entry records the affected vendor/product, a
short description, the required remediation, the date CISA added it, a
ransomware-campaign flag, and the CWE weakness classes.

**Why it's a genuine time series:** CISA adds new entries on most business
weeks (17-43 additions per month in 2025-09..2026-10), and entries are
occasionally revised in place (a `knownRansomwareCampaignUse` flag flips as
evidence arrives, `notes` change) and rarely removed. Every addition marks the
day a vulnerability crossed from "known to exist" to "known to be used
against real targets" — a structured security signal that normally reaches
people only as news coverage.

**Design note: full-catalog daily snapshot, not incremental.** The feed is one
stable JSON document with no "changes since" query and no per-entry change
history. A `dateAdded` watermark would miss in-place revisions and removals,
so each run re-emits the whole catalog and the warehouse dedupes on
`cveID`. The catalog is small (~1.7 MB, 1,733 entries) so a daily poll is
cheap.

**Verified live 2026-10-04:** keyless GET to
`https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json`
returned 200 with `catalogVersion: 2026.10.02`, `count: 1733`, all 1,733
entries carrying a unique `cveID`; newest `dateAdded` was 2026-10-02.

**Terms:** US federal government work published at cisa.gov for free download;
public domain (17 U.S.C. § 105). No key, no registration, no attribution
requirement.

Stdlib only.
"""
import argparse
import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone

SOURCE = "cisa_kev"
URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 300
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")


def normalize_entry(entry: object, fetched_at: str) -> dict:
    """Validate one catalog entry without changing source values."""
    if not isinstance(entry, dict):
        raise ValueError("CISA KEV catalog contains a non-object entry")

    cve_id = entry.get("cveID")
    if not isinstance(cve_id, str) or not cve_id:
        raise ValueError("CISA KEV entry is missing its stable cveID")

    date_added = entry.get("dateAdded")
    if not isinstance(date_added, str) or not DATE_PATTERN.fullmatch(date_added):
        raise ValueError(
            f"CISA KEV {cve_id} has missing or non-date dateAdded {date_added!r}"
        )

    record = dict(entry)
    record.update({"source": SOURCE, "id": cve_id, "fetched_at": fetched_at})
    return record


def fetch_kev(timeout: int = DEFAULT_TIMEOUT):
    """Fetch the full catalog in one bounded request."""
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or not 1 <= timeout <= MAX_TIMEOUT
    ):
        raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT} seconds")

    request = urllib.request.Request(
        URL,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    if not isinstance(document, dict):
        raise ValueError("CISA KEV response is not an object")
    vulnerabilities = document.get("vulnerabilities")
    if not isinstance(vulnerabilities, list):
        raise ValueError("CISA KEV response is missing vulnerabilities")

    # Validate the whole document before emitting anything: a mid-stream
    # failure must not leave a partial run on stdout.
    records = [normalize_entry(entry, fetched_at) for entry in vulnerabilities]
    yield from records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)

    try:
        for record in fetch_kev(timeout=args.timeout):
            print(json.dumps(record, ensure_ascii=False))
    except Exception as exc:
        print(f"cisa_kev fetch failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
