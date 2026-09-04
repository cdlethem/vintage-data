#!/usr/bin/env python3
"""OFAC sanctions lists — who got sanctioned today (keyless, no auth).

Sector: **compliance / economic statecraft.** Entirely new to this catalog and
unlike anything else in it.

**What it is:** the US Treasury's Specially Designated Nationals (SDN) list and
Consolidated Sanctions List — every individual, company, vessel and aircraft
subject to US sanctions. Published as plain XML/CSV at a stable URL, no key, no
registration.

**Why it's a genuine time series rather than a static reference:** the list
*changes*, sometimes several times a month, and each change is a geopolitical
event with a timestamp. **Diff consecutive snapshots and you get "who was
designated today, and who was delisted"** — which is a real, structured signal
about foreign policy that normally reaches people only as news coverage.

What makes it analytically rich:
  * **Entities carry programme tags** (`programList`) — UKRAINE-EO13662, IRAN,
    SDGT, CYBER2 — so you can track designation volume per programme over time
    and watch policy attention shift between theatres.
  * Records include **aliases, addresses, dates of birth, passport numbers,
    vessel IMO numbers** — a rare, messy, real-world entity-resolution problem.
    Deduplicating aliases across the list is a legitimately hard NLP task with
    ground truth attached.
  * Delistings are as interesting as designations and get far less attention.
  * The EU and UN publish equivalent consolidated lists, so cross-jurisdiction
    comparison — who does the US sanction that the EU doesn't? — is tractable
    and genuinely novel.

Verification status 2026-09-03: **KNOWN.** These files have been published at
stable Treasury URLs for many years, but I did not fetch them this session
(sandbox restrictions). Confirm the current file layout on first run — Treasury
has been migrating between `sanctionslistservice.ofac.treas.gov` and the older
`treasury.gov/ofac/downloads` paths, and has added newer "Advanced" XML
formats alongside the legacy ones.

Sources (check both, prefer whichever currently resolves):
  https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML
  https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/CONS_PRIM.XML
  legacy: https://www.treasury.gov/ofac/downloads/sdn.xml

**Design note: this script is snapshot-and-diff, not incremental.** There's no
"changes since" endpoint — you fetch the whole list, store it keyed by entity
id, and compare against your previous snapshot. That's the correct pattern
here and the `diff_snapshots` helper implements it.

**Please be careful how you use this.** These are real named people and
companies, and false positives in sanctions screening cause serious harm to
innocent people with similar names. This is fine as a data-engineering
exercise; do not build anything that makes real compliance decisions about a
real person without professional legal review.

Stdlib only.
"""
import json
import os
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

PRIMARY = ("https://sanctionslistservice.ofac.treas.gov"
           "/api/PublicationPreview/exports/SDN.XML")
LEGACY = "https://www.treasury.gov/ofac/downloads/sdn.xml"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _fetch_xml(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


def fetch_sdn(url: str | None = None):
    """Parse the SDN list into normalized records.

    The legacy schema is namespaced; the code strips namespaces so the same
    parser survives both the old and 'Advanced' formats reasonably well.
    """
    raw = _fetch_xml(url or PRIMARY)
    root = ET.fromstring(raw)
    for el in root.iter():                       # strip namespaces
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    now = datetime.now(timezone.utc).isoformat()

    for entry in root.iter("sdnEntry"):
        def txt(tag):
            node = entry.find(tag)
            return node.text if node is not None else None

        programs = [p.text for p in entry.iter("program") if p.text]
        akas = []
        for aka in entry.iter("aka"):
            first = aka.findtext("firstName") or ""
            last = aka.findtext("lastName") or ""
            name = f"{first} {last}".strip()
            if name:
                akas.append(name)
        addresses = []
        for a in entry.iter("address"):
            parts = [a.findtext(k) for k in
                     ("address1", "city", "stateOrProvince", "country")]
            joined = ", ".join(p for p in parts if p)
            if joined:
                addresses.append(joined)
        ids = []
        for i in entry.iter("id"):
            ids.append({"type": i.findtext("idType"),
                        "number": i.findtext("idNumber"),
                        "country": i.findtext("idCountry")})

        first, last = txt("firstName"), txt("lastName")
        yield {
            "source": "ofac_sdn",
            "fetched_at": now,
            "id": txt("uid"),
            "name": " ".join(x for x in (first, last) if x) or last or first,
            "entity_type": txt("sdnType"),        # Individual / Entity / Vessel
            "title": txt("title"),
            "remarks": txt("remarks"),
            "programs": programs,                 # policy theatre tags
            "aliases": akas,
            "addresses": addresses,
            "ids": ids,
            "vessel_imo": entry.findtext("vesselInfo/otherVesselFlag"),
        }


def snapshot_index(records) -> dict:
    """Key records by uid for diffing."""
    return {r["id"]: r for r in records if r.get("id")}


def diff_snapshots(previous: dict, current: dict):
    """The actual signal: designations and delistings between two pulls."""
    now = datetime.now(timezone.utc).isoformat()
    prev_ids, cur_ids = set(previous), set(current)
    for uid in sorted(cur_ids - prev_ids):
        r = current[uid]
        yield {"source": "ofac_change", "fetched_at": now, "id": uid,
               "change": "DESIGNATED", "name": r.get("name"),
               "entity_type": r.get("entity_type"), "programs": r.get("programs")}
    for uid in sorted(prev_ids - cur_ids):
        r = previous[uid]
        yield {"source": "ofac_change", "fetched_at": now, "id": uid,
               "change": "DELISTED", "name": r.get("name"),
               "entity_type": r.get("entity_type"), "programs": r.get("programs")}
    for uid in sorted(cur_ids & prev_ids):
        a, b = previous[uid], current[uid]
        if a.get("programs") != b.get("programs"):
            yield {"source": "ofac_change", "fetched_at": now, "id": uid,
                   "change": "PROGRAMS_CHANGED", "name": b.get("name"),
                   "entity_type": b.get("entity_type"),
                   "programs": b.get("programs"), "previous_programs": a.get("programs")}


if __name__ == "__main__":
    if len(sys.argv) > 2:      # diff two saved snapshots
        prev = snapshot_index(json.load(open(sys.argv[1])))
        cur = snapshot_index(json.load(open(sys.argv[2])))
        for rec in diff_snapshots(prev, cur):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        for rec in fetch_sdn():
            print(json.dumps(rec, ensure_ascii=False))
