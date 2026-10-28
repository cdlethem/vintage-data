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
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "quebec_emergency_departments"
URL = "https://www.msss.gouv.qc.ca/professionnels/statistiques/documents/urgences/Releve_horaire_urgences_7jours_nbpers.csv"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
# The MSSS front-end intermittently stalls mid-transfer (observed 2026-10-28:
# read timeout after its 302 redirect in the task log). Retry transient
# failures in a bounded way so one slow hop does not lose a whole run.
MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})


def fetch_departments(limit: int = 500, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    for attempt in range(MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                text = response.read().decode("cp1252")
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt == MAX_ATTEMPTS - 1:
                raise
            reason = f"HTTP {exc.code}"
            exc.close()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            reason = str(exc.reason if isinstance(exc, urllib.error.URLError) else exc)
        else:
            break
        delay = min(RETRY_BASE_DELAY * (2 ** attempt), timeout)
        print(f"quebec_emergency_departments: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)
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
