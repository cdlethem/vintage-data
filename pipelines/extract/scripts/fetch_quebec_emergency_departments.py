#!/usr/bin/env python3
"""Québec MSSS — hourly emergency-department operational census.

The rolling seven-day CSV exposes occupied stretchers, patients waiting, total census,
and 24/48-hour long-stay cohorts by facility. Verified live 2026-09-03; the file is
roughly 26 KB and updates hourly. Column names contain stray tabs/spaces and the file is
Windows-1252, both normalized here. Québec open-government reuse terms apply.

Stdlib only.
"""

import argparse
import csv
import io
import json
import urllib.request
from datetime import datetime, timezone

SOURCE = "quebec_emergency_departments"
URL = "https://www.msss.gouv.qc.ca/professionnels/statistiques/documents/urgences/Releve_horaire_urgences_7jours_nbpers.csv"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_departments(limit: int = 500, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("cp1252")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "No_permis_installation" not in [
        name.strip() for name in reader.fieldnames
    ]:
        raise ValueError("Québec emergency CSV is missing facility identifiers")
    for index, raw in enumerate(reader):
        if index >= limit:
            break
        row = {
            key.strip(): value.strip() for key, value in raw.items() if key is not None
        }
        facility_id = (
            row.get("No_permis_installation")
            or f"region-{row.get('RSS')}|{row.get('Nom_installation')}"
        )
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": facility_id})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_departments(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
