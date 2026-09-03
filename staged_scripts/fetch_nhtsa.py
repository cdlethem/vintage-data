#!/usr/bin/env python3
"""NHTSA — vehicle recalls and VIN decoding, keyless.

Lead #44: vehicle safety. Recalls (with crash/fire/injury/death consequence text) and
a VIN decoder. Complaint narratives are raw consumer prose in the full API, though
this script covers the two endpoints confirmed working this session (recalls, VIN
decode); complaints needs more query-parameter investigation than time allowed here.

**Verified live 2026-09-03.** The paths the exploration run tried first
(`/recalls/recalls`, `/complaints/complaints`, `/api/recalls`) all returned **403,
`{"message": "Missing Authentication Token"}`** — an AWS API Gateway error for an
*unmapped route*, not evidence that `api.nhtsa.gov` requires a key. The real paths do
not require auth:
  * `api.nhtsa.gov/recalls/recallsByVehicle?make=honda&model=accord&modelYear=2023` —
    **200, 6,140 bytes, Count: 5** real recalls, including a real seat-belt
    pretensioner recall (campaign 23V782000) with full consequence/remedy text.
  * `vpic.nhtsa.dot.gov/api/vehicles/DecodeVin/{vin}?format=json` — **200, 12,057
    bytes** — a real VIN (1HGCM82633A004352) decoded correctly to a 2003 Honda Accord,
    140 fields.

Endpoints:
    `https://api.nhtsa.gov/recalls/recallsByVehicle?make=&model=&modelYear=`
    `https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVin/{vin}?format=json`

Quirks that will cost someone an afternoon:
    * **A 403 "Missing Authentication Token" from `api.nhtsa.gov` means the path is
      wrong, not that you need a key.** This is a generic AWS API Gateway response
      for any route that doesn't match a deployed endpoint — verified live on four
      different plausible-looking-but-wrong paths, while the correctly-named
      `recallsByVehicle` path worked with no auth at all.
    * **Recalls and VIN decoding live on two different hostnames** —
      `api.nhtsa.gov` for recalls/complaints, `vpic.nhtsa.dot.gov` (a different
      subdomain of a different NHTSA domain entirely) for VIN decoding. Don't assume
      one base URL covers both.
    * `ReportReceivedDate` on a recall is `DD/MM/YYYY` format (verified:
      `"21/11/2023"`), not the ISO or US `MM/DD/YYYY` format you might expect —
      parse carefully or you'll silently swap day and month.
    * NHTSA's own docstring warns two dates conventions exist across endpoints —
      always check the specific field's format rather than assuming consistency
      across this API.
    * The VIN decoder's `Results` is a **flat list of `{Variable, Value}` pairs**
      (140 of them), not a nested object keyed by field name — build a dict from it
      if you want `decoded["Make"]`-style access.

Etiquette: keyless, no published rate limit found for either host. Self-impose 1 req/s;
this is US federal safety infrastructure.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
RECALLS_BASE = "https://api.nhtsa.gov/recalls/recallsByVehicle"
VPIC_BASE = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVin"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_recalls(make: str, model: str, model_year: int):
    """Recalls for one make/model/year. NHTSA's recall API is queried per-vehicle,
    not as a firehose -- poll a watchlist of makes/models you care about."""
    now = datetime.now(timezone.utc).isoformat()
    params = urllib.parse.urlencode({"make": make, "model": model, "modelYear": model_year})
    doc = _get(f"{RECALLS_BASE}?{params}")
    for r in doc.get("results") or []:
        yield {
            "source": "nhtsa_recalls",
            "fetched_at": now,
            "id": r.get("NHTSACampaignNumber"),
            "manufacturer": r.get("Manufacturer"),
            "component": r.get("Component"),
            "summary": r.get("Summary"),
            "consequence": r.get("Consequence"),
            "remedy": r.get("Remedy"),
            "report_received_date": r.get("ReportReceivedDate"),  # DD/MM/YYYY
            "park_it": r.get("parkIt"),
        }


def decode_vin(vin: str):
    """Decode one VIN into its ~140 structured fields."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(f"{VPIC_BASE}/{urllib.parse.quote(vin)}?format=json")
    fields = {r["Variable"]: r["Value"] for r in doc.get("Results") or [] if r.get("Value")}
    yield {
        "source": "nhtsa_vpic",
        "fetched_at": now,
        "id": vin,
        "make": fields.get("Make"),
        "model": fields.get("Model"),
        "model_year": fields.get("Model Year"),
        "manufacturer": fields.get("Manufacturer Name"),
        "vehicle_type": fields.get("Vehicle Type"),
        "fields": fields,
    }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "recalls"
    if mode == "vin":
        vin = sys.argv[2] if len(sys.argv) > 2 else "1HGCM82633A004352"
        for rec in decode_vin(vin):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        make = sys.argv[2] if len(sys.argv) > 2 else "honda"
        model = sys.argv[3] if len(sys.argv) > 3 else "accord"
        year = int(sys.argv[4]) if len(sys.argv) > 4 else 2023
        for rec in fetch_recalls(make, model, year):
            print(json.dumps(rec, ensure_ascii=False))
