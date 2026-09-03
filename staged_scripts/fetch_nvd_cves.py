#!/usr/bin/env python3
"""NVD — newly published and newly modified CVEs (keyless).

The world's software vulnerabilities, as they get catalogued. Each record has
a free-text English description, CVSS severity vectors, CWE weakness class,
and affected-product (CPE) lists. Good for severity-trend series, vendor
comparisons, and LLM work like clustering root-cause language across decades.

The genuinely interesting angle is `lastModStartDate`: CVE records get
REVISED — severity rescored, affected versions expanded, entries rejected.
Polling the modified feed captures the revision history, so you can study how
long it takes the world to correctly assess a vulnerability. That's a much
better dataset than the static published feed most people use.

Verified live 2026-09-02T21:55Z: keyless GET to
services.nvd.nist.gov/rest/json/cves/2.0 returned current data
(totalResults=646 for keywordSearch=openssl).

Quirks that matter a lot here:
  * **Records are enormous.** A single CVE can carry hundreds of CPE match
    entries; 20 records was a very large response. This script strips to a
    flat summary by default — set `slim=False` only if you truly need the
    configuration tree.
  * Rate limits WITHOUT a key: ~5 requests per 30 seconds. With a free key
    from nvd.nist.gov/developers/request-an-api-key: ~50 per 30s. NVD
    explicitly asks for a sleep between requests; this script defaults to 6s.
  * Date windows are capped at 120 days per request for both pub* and
    lastMod* ranges. Both start and end must be supplied together.
  * `resultsPerPage` max 2000; page with `startIndex` against `totalResults`.
  * Dates are ISO-8601 WITHOUT a trailing 'Z' by convention here
    (e.g. 2026-09-01T00:00:00.000). Extended tz offsets must be URL-encoded.

Stdlib only.
"""
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://services.nvd.nist.gov/rest/json/cves/2.0"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
API_KEY = None          # optional; set to raise rate limits ~10x
SLEEP_SECONDS = 6.0     # NVD asks for spacing; 6s is safe keyless


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000")


def _get(params: dict):
    url = BASE + "?" + urllib.parse.urlencode(params)
    headers = {"User-Agent": USER_AGENT}
    if API_KEY:
        headers["apiKey"] = API_KEY
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.load(resp)


def fetch_cves(days_back: int = 1, by: str = "published",
               results_per_page: int = 200, slim: bool = True,
               no_rejected: bool = True):
    """by='published' for new CVEs, by='modified' for revised ones."""
    now = datetime.now(timezone.utc)
    lo, hi = now - timedelta(days=min(days_back, 120)), now
    prefix = "pub" if by == "published" else "lastMod"
    base_params = {
        f"{prefix}StartDate": _fmt(lo),
        f"{prefix}EndDate": _fmt(hi),
        "resultsPerPage": min(results_per_page, 2000),
    }
    if no_rejected:
        base_params["noRejected"] = ""
    start = 0
    while True:
        params = dict(base_params, startIndex=start)
        data = _get(params)
        vulns = data.get("vulnerabilities", [])
        fetched_at = datetime.now(timezone.utc).isoformat()
        for v in vulns:
            yield normalize(v.get("cve") or {}, fetched_at, slim)
        start += len(vulns)
        total = data.get("totalResults", 0)
        if not vulns or start >= total:
            break
        time.sleep(SLEEP_SECONDS)


def _best_cvss(metrics: dict):
    """Prefer newest CVSS version present. Returns (version, score, severity)."""
    for key, ver in (("cvssMetricV40", "4.0"), ("cvssMetricV31", "3.1"),
                     ("cvssMetricV30", "3.0"), ("cvssMetricV2", "2.0")):
        entries = metrics.get(key) or []
        if entries:
            d = entries[0].get("cvssData") or {}
            sev = d.get("baseSeverity") or entries[0].get("baseSeverity")
            return ver, d.get("baseScore"), sev, d.get("vectorString")
    return None, None, None, None


def normalize(cve: dict, fetched_at: str, slim: bool = True) -> dict:
    desc = next((d.get("value") for d in cve.get("descriptions", [])
                 if d.get("lang") == "en"), None)
    ver, score, sev, vector = _best_cvss(cve.get("metrics") or {})
    cwes = [d.get("value")
            for w in cve.get("weaknesses", [])
            for d in w.get("description", [])
            if d.get("lang") == "en"]
    rec = {
        "source": "nvd_cve",
        "fetched_at": fetched_at,       # snapshot; records get rescored later
        "id": cve.get("id"),
        "published": cve.get("published"),
        "last_modified": cve.get("lastModified"),
        "status": cve.get("vulnStatus"),      # Analyzed / Modified / Awaiting...
        "description": desc,
        "cvss_version": ver,
        "cvss_score": score,
        "cvss_severity": sev,
        "cvss_vector": vector,
        "cwes": cwes,
        "n_references": len(cve.get("references") or []),
        "source_identifier": cve.get("sourceIdentifier"),
    }
    if not slim:
        rec["configurations"] = cve.get("configurations")
        rec["references"] = cve.get("references")
    return rec


if __name__ == "__main__":
    by = sys.argv[1] if len(sys.argv) > 1 else "published"
    for rec in fetch_cves(days_back=2, by=by):
        print(json.dumps(rec, ensure_ascii=False))
