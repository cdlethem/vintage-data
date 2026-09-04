#!/usr/bin/env python3
"""GBIF — global stream of species occurrence records (keyless, JSON).

GBIF aggregates iNaturalist, eBird, museum digitizations, and hundreds of
other publishers: billions of records, tens of thousands ingested daily.
'Current' per run = records by *ingestion* date (lastInterpreted), which is
the honest watermark — eventDate is when the organism was seen, which can lag
by days (or decades, for museum digitization — filter it if you want field
observations only).

High-confidence known endpoint (api.gbif.org/v1 has been stable for a
decade, keyless for reads); not live-fetched in this session, so smoke-test
once. Rate limits are generous; paging via offset/limit caps at 100k per
query — slice by taxon/country/date to stay under.

Fun pipeline shapes:
  * seasonal migration signal: poll a taxonKey (e.g. Monarch butterfly) by
    country per day
  * "what did the world see today": daily counts by kingdom
  * anomaly detection: a rare-species record appearing near you

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

BASE = "https://api.gbif.org/v1/occurrence/search"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_recent(taxon_key: int | None = None, country: str | None = None,
                 ingested_on: str | None = None, limit: int = 300,
                 page_size: int = 100):
    """Yield occurrences ingested on `ingested_on` (YYYY-MM-DD, default yesterday).

    Verified live: `lastInterpreted=<today>` reliably returns 0 results --
    GBIF's daily ingestion batch for "today" isn't reflected in the index
    yet even late in the UTC day, while querying yesterday's date returns
    (a real, checked) 1B+ matches. Yesterday is the honest watermark here.
    """
    ingested_on = ingested_on or (date.today() - timedelta(days=1)).isoformat()
    offset, seen = 0, 0
    now = datetime.now(timezone.utc).isoformat()
    while seen < limit:
        params = {"lastInterpreted": ingested_on,
                  "limit": min(page_size, limit - seen), "offset": offset}
        if taxon_key:
            params["taxonKey"] = taxon_key
        if country:
            params["country"] = country
        url = BASE + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.load(resp)
        results = data.get("results", [])
        if not results:
            break
        for r in results:
            seen += 1
            yield {
                "source": "gbif",
                "fetched_at": now,
                "id": r.get("key"),
                "species": r.get("species"),
                "scientific_name": r.get("scientificName"),
                "kingdom": r.get("kingdom"),
                "event_date": r.get("eventDate"),
                "ingested": r.get("lastInterpreted"),
                "lat": r.get("decimalLatitude"),
                "lon": r.get("decimalLongitude"),
                "country": r.get("countryCode"),
                "basis": r.get("basisOfRecord"),   # HUMAN_OBSERVATION vs museum etc.
                "dataset": r.get("datasetName"),
            }
        if data.get("endOfRecords"):
            break
        offset += len(results)


if __name__ == "__main__":
    country = sys.argv[1] if len(sys.argv) > 1 else "US"
    for rec in fetch_recent(country=country, limit=100):
        print(json.dumps(rec, ensure_ascii=False))
