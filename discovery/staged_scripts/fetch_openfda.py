#!/usr/bin/env python3
"""openFDA — drug/device/food recalls and adverse events (keyless).

Weirder and more human than it sounds. The food enforcement endpoint is a
running list of everything being pulled off American shelves and *why*, in
free-text ("undeclared milk", "foreign material: metal fragments"). The drug
adverse-event endpoint (FAERS) is 20M+ narrative safety reports. Both are
excellent LLM-analysis corpora: classification, deduplication, root-cause
clustering, seasonal patterns in contaminant type.

'Current' per run = records with report_date / receivedate at or after your
stored watermark, sorted descending.

Endpoints (all under https://api.fda.gov):
  /food/enforcement.json     recalls: reason_for_recall, product_description
  /drug/enforcement.json     drug recalls
  /device/enforcement.json   device recalls
  /drug/event.json           FAERS adverse events (big, nested)
  /device/event.json         device adverse events

Docs-verified 2026-09-02 against open.fda.gov endpoint documentation, which
states no API key is required. (My sandboxed fetcher was bot-blocked, so
smoke-test once from your own machine.) A free key from
open.fda.gov/apis/authentication/ raises the per-IP cap substantially — worth
getting if you poll often.

Quirks:
  * `limit` max 1000 per call; page with `skip` (capped around 25k — slice by
    date range to go deeper).
  * HTTP 404 with an error body means "no matches", not a broken endpoint.
  * Dates are YYYYMMDD strings, and range syntax is [YYYYMMDD+TO+YYYYMMDD].
  * Data is NOT real-time; expect batch updates on the order of days to weeks.
    Poll daily, not hourly.

Stdlib only.
"""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

BASE = "https://api.fda.gov"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
API_KEY = None  # optional; set to raise rate limits

ENDPOINTS = {
    "food_recall":   ("/food/enforcement.json", "report_date"),
    "drug_recall":   ("/drug/enforcement.json", "report_date"),
    "device_recall": ("/device/enforcement.json", "report_date"),
    "drug_event":    ("/drug/event.json", "receivedate"),
}


def _get(path: str, params: dict):
    if API_KEY:
        params["api_key"] = API_KEY
    url = BASE + path + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:      # documented: zero matches
            return {"results": []}
        raise


def fetch(kind: str = "food_recall", days_back: int = 14, limit: int = 100):
    path, datefield = ENDPOINTS[kind]
    lo = (date.today() - timedelta(days=days_back)).strftime("%Y%m%d")
    hi = date.today().strftime("%Y%m%d")
    data = _get(path, {
        "search": f"{datefield}:[{lo}+TO+{hi}]",
        "sort": f"{datefield}:desc",
        "limit": min(limit, 1000),
    })
    now = datetime.now(timezone.utc).isoformat()
    for r in data.get("results", []):
        yield normalize(kind, datefield, r, now)


def normalize(kind: str, datefield: str, r: dict, now: str) -> dict:
    if kind.endswith("_recall"):
        return {
            "source": f"openfda_{kind}",
            "fetched_at": now,
            "id": r.get("recall_number") or r.get("event_id"),
            "report_date": r.get(datefield),
            "recall_initiation_date": r.get("recall_initiation_date"),
            "status": r.get("status"),
            "classification": r.get("classification"),   # I / II / III severity
            "firm": r.get("recalling_firm"),
            "state": r.get("state"),
            "country": r.get("country"),
            "product": r.get("product_description"),     # free text
            "reason": r.get("reason_for_recall"),        # free text — the good stuff
            "quantity": r.get("product_quantity"),
            "distribution": r.get("distribution_pattern"),
        }
    patient = r.get("patient") or {}
    return {
        "source": f"openfda_{kind}",
        "fetched_at": now,
        "id": r.get("safetyreportid"),
        "received": r.get(datefield),
        "serious": r.get("serious"),
        "country": r.get("occurcountry"),
        "patient_sex": patient.get("patientsex"),
        "patient_age": patient.get("patientonsetage"),
        "reactions": [x.get("reactionmeddrapt")
                      for x in (patient.get("reaction") or [])],
        "drugs": [d.get("medicinalproduct")
                  for d in (patient.get("drug") or [])],
    }


if __name__ == "__main__":
    kind = sys.argv[1] if len(sys.argv) > 1 else "food_recall"
    for rec in fetch(kind):
        print(json.dumps(rec, ensure_ascii=False))
