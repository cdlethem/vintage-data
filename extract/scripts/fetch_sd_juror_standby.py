#!/usr/bin/env python3
"""San Diego Superior Court — telephone-standby reporting instructions per courthouse.

The public page states, per courthouse and service date, which summoned group ranges must
report, remain on standby, or have completed service. Only aggregate group ranges are
collected — never individual juror information. Four courthouse blocks were verified live
2026-09-04. Court website terms apply.

Publisher wording is kept verbatim; group ranges are additionally parsed so cohort
activation can be measured without re-reading prose.

Stdlib only.
"""

import argparse
import json
import os
import re
import urllib.request
from datetime import datetime, timezone

SOURCE = "sd_juror_standby"
URL = "https://www.sdcourt.ca.gov/sdcourt/jury2/telephonestandby"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
COURTHOUSE = re.compile(
    r"^(Central Courthouse|North County Regional Center|South County Regional Center|East County Regional Center)$"
)
DATE = re.compile(r"^[A-Z][a-z]+ \d{1,2}, \d{4}$")
GROUPS = re.compile(r"\b(\d{2,4})\s*-\s*(\d{2,4})\b")


def _lines(text: str):
    stripped = re.sub(r"<[^>]+>", "\n", text).replace("&nbsp;", " ")
    stripped = stripped.replace("&amp;", "&").replace("&#39;", "'").replace("&quot;", '"')
    return [line.strip() for line in stripped.split("\n") if line.strip()]


def fetch_instructions(timeout: int = 30):
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        lines = _lines(response.read().decode("utf-8", "replace"))
    starts = [
        index
        for index, line in enumerate(lines)
        if COURTHOUSE.match(line) and index + 1 < len(lines) and DATE.match(lines[index + 1])
    ]
    if not starts:
        raise ValueError("page exposes no dated courthouse instruction blocks")
    for position, index in enumerate(starts):
        end = starts[position + 1] if position + 1 < len(starts) else len(lines)
        courthouse, posted = lines[index], lines[index + 1]
        body = [
            line
            for line in lines[index + 2 : end]
            if not line.startswith("If you need to speak") and line not in {".", "Thank you"}
        ]
        message = " ".join(body)
        if not message:
            raise ValueError(f"{courthouse}: instruction block has no message text")
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"{courthouse}:{posted}",
            "courthouse": courthouse,
            "publisher_updated_at": posted,
            "instructions": message,
            "group_ranges": [f"{low}-{high}" for low, high in GROUPS.findall(message)],
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_instructions(args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
