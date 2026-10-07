#!/usr/bin/env python3
"""Hong Kong Public Libraries — computer-session availability.

Ten-minute snapshots expose hyperlocal workstation demand and session sell-through.
Verified live 2026-09-03 through the official DATA.GOV.HK-linked JSON endpoint. The
response nests future sessions and workstation groups under each library; this client
emits both one library-state record and normalized availability records so closed or
sessionless libraries remain observable.

Stdlib only.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})

SOURCE = "hk_library_computers"
URL = "https://sls.hkpl.gov.hk/api/cfm-admin-service/open-api/library/selectLibraryPageInfoForPSI?language=en-US"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _load_libraries(timeout: int):
    """Fetch the library list, retrying transient failures with a finite bound.

    sls.hkpl.gov.hk intermittently accepts connections and then stalls before
    sending any response (observed 2026-10-07: 30-second read timeout in the
    task log on both attempts of a scheduled run, and on live probes:
    TCP/TLS connect fine, zero bytes for 40+ seconds). Transport failures and
    429/5xx responses retry with 1s/2s backoff (at most ~93 seconds, well
    inside the ten-minute task timeout); non-transient errors and exhausted
    attempts raise the final failure unchanged.
    """
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    for attempt in range(MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt == MAX_ATTEMPTS - 1:
                raise
            reason = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            reason = str(exc.reason if isinstance(exc, urllib.error.URLError) else exc)
        delay = RETRY_BASE_DELAY * (2 ** attempt)
        print(f"hk_library_computers: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)


def fetch_availability(limit: int = 2000, timeout: int = 30):
    if limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    libraries = _load_libraries(timeout)
    if not isinstance(libraries, list):
        raise TypeError("HKPL response is not a library list")
    emitted = 0
    for library in libraries:
        code = library.get("libraryCode")
        if not code:
            raise ValueError("HKPL library is missing libraryCode")
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"library|{code}",
            "record_type": "library",
            "library_code": code,
            "library_name": library.get("libraryDisplayName"),
            "district": library.get("district"),
            "is_open": library.get("isOpen"),
            "publisher_updated_at": library.get("lastUpdateDate"),
        }
        emitted += 1
        if emitted >= limit:
            return
        for session in library.get("sessionList") or []:
            for group in session.get("workstationGroup") or []:
                group_id = group.get("groupId")
                start = session.get("sessionStart")
                if not group_id or not start:
                    raise ValueError(
                        "HKPL availability is missing groupId or sessionStart"
                    )
                yield {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": f"availability|{code}|{start}|{group_id}",
                    "record_type": "availability",
                    "library_code": code,
                    "library_name": library.get("libraryDisplayName"),
                    "is_open": library.get("isOpen"),
                    "session_start": start,
                    "session_end": session.get("sessionEnd"),
                    "group_id": group_id,
                    "group_name": group.get("groupName"),
                    "available_workstations": group.get("availableWktNumber"),
                    "publisher_updated_at": library.get("lastUpdateDate"),
                }
                emitted += 1
                if emitted >= limit:
                    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_availability(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
