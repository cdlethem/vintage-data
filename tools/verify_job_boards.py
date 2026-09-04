#!/usr/bin/env python3
"""Live-verify candidate ATS job-board tokens.

Reads candidate records (JSON list or NDJSON) from a file or stdin, queries each
board's real public endpoint, and prints only those that answered with open
postings. Nothing is guessed into the output.

Endpoints, pagination and throttling come from ``job_boards_lib``, the same
code the scheduled fetcher uses, so verification cannot drift from production.

Candidate record:
    {"provider": "greenhouse", "token": "stripe", "company": "Stripe",
     "country": "US", "industry": "Financial Services"}

Workday tokens are composite: "tenant|wdN|site".

Usage:
    python tools/verify_job_boards.py candidates.json > verified.json
    cat candidates.ndjson | python tools/verify_job_boards.py - > verified.json
"""
from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines" / "extract" / "scripts"))

from job_boards_lib.adapters import count, is_permanent_miss  # noqa: E402

PARALLEL = 24


def verify(candidate: dict) -> dict:
    result = dict(candidate)
    provider, token = candidate["provider"], candidate["token"]
    error = "unknown"
    for attempt in range(2):
        try:
            postings = count(provider, token)
        except Exception as exc:  # noqa: BLE001 - classify, never abort the batch
            if is_permanent_miss(exc):
                result["status"] = f"http-{getattr(exc, 'code', 'error')}"
                return result
            error = f"http-{exc.code}" if hasattr(exc, "code") else type(exc).__name__
            if attempt == 0:
                time.sleep(1.5)
            continue
        if postings is None:
            result["status"] = "not-a-board"
        elif postings == 0:
            result["status"], result["open_postings"] = "empty", 0
        else:
            result["status"], result["open_postings"] = "ok", postings
        return result
    result["status"] = error
    return result


def main() -> int:
    source = sys.argv[1] if len(sys.argv) > 1 else "-"
    raw = (sys.stdin.read() if source == "-"
           else Path(source).read_text(encoding="utf-8")).strip()
    if not raw:
        raise ValueError("no candidates supplied")
    candidates = (json.loads(raw) if raw.startswith("[")
                  else [json.loads(line) for line in raw.splitlines() if line.strip()])

    seen, unique = set(), []
    for candidate in candidates:
        key = (candidate["provider"], candidate["token"])
        if key not in seen:
            seen.add(key)
            unique.append(candidate)

    with ThreadPoolExecutor(PARALLEL) as pool:
        results = list(pool.map(verify, unique))

    verified = [row for row in results if row.get("status") == "ok"]
    tally: dict[str, int] = {}
    for row in results:
        tally[row["status"]] = tally.get(row["status"], 0) + 1
    print(json.dumps(sorted(verified, key=lambda row: -row["open_postings"]),
                     indent=1, ensure_ascii=False))
    print(f"candidates={len(unique)} verified={len(verified)} "
          f"postings={sum(row['open_postings'] for row in verified)} breakdown={tally}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
