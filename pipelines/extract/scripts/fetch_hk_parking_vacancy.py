#!/usr/bin/env python3
"""Hong Kong DATA.GOV.HK — real-time car-park vacancy.

Repeated snapshots create a city-wide parking-demand field across private cars, goods
vehicles, and motorcycles. Verified live 2026-09-03 through the official one-stop API.
Vehicle categories are dynamic keys containing one or more vacancy records, so this
client flattens them while retaining EV and disabled-space counts. Follow DATA.GOV.HK
reuse terms and poll no faster than every ten minutes.

Stdlib only.
"""

import argparse
import json
import urllib.request
from datetime import datetime, timezone

SOURCE = "hk_parking_vacancy"
URL = "https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_vacancies(limit: int = 2000, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("results")
    if not isinstance(rows, list):
        raise TypeError("parking response is missing results")
    emitted = 0
    for park in rows:
        park_id = park.get("park_Id")
        if not park_id:
            raise ValueError("parking record is missing park_Id")
        for vehicle_type, values in park.items():
            if vehicle_type == "park_Id" or not isinstance(values, list):
                continue
            for index, value in enumerate(values):
                vacancy_type = value.get("vacancy_type", index)
                record = dict(value)
                record.update(
                    {
                        "source": SOURCE,
                        "fetched_at": fetched_at,
                        "id": f"{park_id}|{vehicle_type}|{vacancy_type}",
                        "park_id": park_id,
                        "vehicle_type": vehicle_type,
                    }
                )
                yield record
                emitted += 1
                if emitted >= limit:
                    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_vacancies(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
