#!/usr/bin/env python3
"""PubChem PUG-REST — keyless access to chemical compound records.

Lead #55: chemistry, with new substances deposited constantly. PUG-REST itself is a
lookup API (by name, CID, SMILES, formula...) rather than a firehose of new deposits —
this script covers verified compound-property lookups, the practical, confirmed-working
capability; pair it with a list of compound names/CIDs you care about to build a series.

**Verified live 2026-09-03**:
  * `pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/aspirin/property/
    MolecularFormula,MolecularWeight,IUPACName,CanonicalSMILES/JSON` — **200, 272
    bytes** — `CID: 2244, MolecularFormula: "C9H8O4", MolecularWeight: "180.16",
    IUPACName: "2-acetyloxybenzoic acid"`.
  * A bogus compound name — **404**, well-formed JSON fault body:
    `{"Fault": {"Code": "PUGREST.NotFound", "Message": "No CID found", ...}}`.

Endpoint: `https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/{namespace}/{value}/property/{props}/JSON`
    namespace: name | cid | smiles | inchikey | formula
    props: comma-separated, e.g. MolecularFormula,MolecularWeight,IUPACName,CanonicalSMILES

Quirks that will cost someone an afternoon:
    * **Requesting `CanonicalSMILES` returns a key named `ConnectivitySMILES`, not
      `CanonicalSMILES`.** Verified live: the property list in the URL used the old
      name, the response used the new one. PubChem renamed the underlying property but
      kept the old name valid as a request parameter for backward compatibility — index
      the response defensively (`.get("ConnectivitySMILES") or .get("CanonicalSMILES")`),
      don't assume the request key and response key match.
    * `MolecularWeight` comes back as a **string** (`"180.16"`), not a JSON number —
      cast it yourself if you need to compute with it.
    * A failed lookup is a genuine HTTP error (404) with a small JSON fault body
      (`Fault.Code`, `Fault.Message`) — catch the HTTPError and parse its body rather
      than treating any non-200 as an opaque failure; the message tells you why.
    * `CID` (PubChem Compound ID) is the stable identifier — a compound name can be a
      synonym shared across entries, but CID is unique. Use CID as your envelope id,
      not the name you searched with.

Etiquette: keyless, but NCBI documents a rate limit for PUG-REST: **no more than 5
requests/second**, and asks that large jobs (>10,000 requests) be run off-peak
(evenings/weekends US Eastern) or use their bulk download service instead. This script
self-imposes well under 5/s for anything beyond a single lookup.

Stdlib only.
"""
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: cdlethem@gmail.com)"
BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
PROPS = "MolecularFormula,MolecularWeight,IUPACName,CanonicalSMILES"
_MIN_INTERVAL = 0.25  # well under NCBI's documented 5 req/s ceiling
_last = [0.0]


def _get(url):
    wait = _MIN_INTERVAL - (time.monotonic() - _last[0])
    if wait > 0:
        time.sleep(wait)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            doc = json.load(resp)
    except urllib.error.HTTPError as e:
        try:
            doc = json.loads(e.read())
        except Exception:
            doc = {"Fault": {"Code": f"HTTP{e.code}", "Message": str(e)}}
    _last[0] = time.monotonic()
    return doc


def fetch_compound(name: str, namespace: str = "name"):
    """One compound's properties by name, cid, smiles, inchikey, or formula."""
    now = datetime.now(timezone.utc).isoformat()
    safe = urllib.parse.quote(name, safe="")
    url = f"{BASE}/compound/{namespace}/{safe}/property/{PROPS}/JSON"
    doc = _get(url)
    if "Fault" in doc:
        yield {
            "source": "pubchem",
            "fetched_at": now,
            "id": None,
            "query": name,
            "found": False,
            "error": doc["Fault"].get("Message"),
        }
        return
    for p in doc.get("PropertyTable", {}).get("Properties", []):
        yield {
            "source": "pubchem",
            "fetched_at": now,
            "id": p.get("CID"),
            "query": name,
            "found": True,
            "molecular_formula": p.get("MolecularFormula"),
            "molecular_weight": p.get("MolecularWeight"),
            "iupac_name": p.get("IUPACName"),
            # PubChem renamed this property; the response key differs from the
            # request key ("CanonicalSMILES" in, "ConnectivitySMILES" out)
            "smiles": p.get("ConnectivitySMILES") or p.get("CanonicalSMILES"),
        }


if __name__ == "__main__":
    names = sys.argv[1:] or ["aspirin", "caffeine", "glucose"]
    for name in names:
        for rec in fetch_compound(name):
            print(json.dumps(rec, ensure_ascii=False))
