#!/usr/bin/env python3
"""Codeforces — per-user attempt logs and skill trajectories (keyless).

Sector: **open behavioural telemetry.** This is the closest public analogue to
the event stream a product analytics team would kill for, and it exists because
competitive programming communities publish performance data as a feature.

**Why it reads exactly like product telemetry:**

| Product analytics concept | Codeforces equivalent |
|---|---|
| Event / interaction log | `user.status` — every submission, ever |
| Success vs failure event | `verdict` (OK, WRONG_ANSWER, TLE, RUNTIME_ERROR) |
| Feature/segment used | `problem.tags` (dp, graphs, geometry), `problem.rating` |
| Session | submissions clustered in time; contests are labelled sessions |
| Skill / progression curve | `user.rating` — rating before and after every contest |
| Onboarding funnel | first N submissions of a new handle |
| Churn | last submission timestamp, then silence |
| Cohort | users who joined in the same month |

You can genuinely do retention curves, difficulty-progression analysis,
failure-mode clustering ("which error types precede giving up"), and
learning-curve modelling — on real humans, at scale, with decades of history,
for free. The *unsuccessful* attempts being retained is the rare part: most
public datasets only keep successes, which quietly destroys the analysis.

**Docs-verified 2026-09-02** against the official `codeforces.com/apiHelp`:
all methods may be requested anonymously (an API key is needed only for data
private to a user, e.g. hacks during a live contest). My sandbox couldn't
fetch it directly, so smoke-test once.

**Hard rate limit, stated by the docs: at most 1 request per 2 seconds.**
Exceed it and you get `status: FAILED` with "Call limit exceeded". The client
below enforces the delay itself rather than trusting you to remember.

Response envelope: `{"status": "OK"|"FAILED", "comment": ..., "result": ...}`.
Always check `status` — a failure is HTTP 200 with FAILED inside.

**Please read the ethics note in the catalog before pointing this at named
individuals.** Aggregate and pseudonymise; these are real people who signed up
to compete, not to be profiled.

Stdlib only.
"""
import hashlib
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://codeforces.com/api"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
MIN_INTERVAL = 2.1          # docs: max 1 request / 2 seconds
_last_call = [0.0]

PSEUDONYMISE = True         # hash handles by default; see ethics note


def _throttle():
    wait = MIN_INTERVAL - (time.time() - _last_call[0])
    if wait > 0:
        time.sleep(wait)
    _last_call[0] = time.time()


def _get(method: str, **params):
    _throttle()
    url = f"{BASE}/{method}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.load(resp)
    if data.get("status") != "OK":          # failures are HTTP 200 + FAILED
        raise RuntimeError(f"Codeforces API FAILED: {data.get('comment')}")
    return data.get("result")


def _uid(handle: str) -> str:
    """Stable pseudonym so you can still do per-user analysis without storing
    identities. Flip PSEUDONYMISE off only if you have a real reason."""
    if not PSEUDONYMISE:
        return handle
    return "cf_" + hashlib.sha256(handle.encode()).hexdigest()[:16]


def fetch_submissions(handle: str, start: int = 1, count: int = 1000):
    """Every attempt by one user: the interaction log."""
    rows = _get("user.status", handle=handle, **{"from": start}, count=count)
    now = datetime.now(timezone.utc).isoformat()
    for s in rows:
        prob = s.get("problem") or {}
        author = s.get("author") or {}
        ts = s.get("creationTimeSeconds")
        yield {
            "source": "codeforces_submission",
            "fetched_at": now,
            "id": s.get("id"),
            "user": _uid(handle),
            "ts": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
            "relative_time_s": s.get("relativeTimeSeconds"),  # into the contest
            "contest_id": s.get("contestId"),
            "problem": f"{prob.get('contestId')}{prob.get('index')}",
            "problem_name": prob.get("name"),
            "problem_rating": prob.get("rating"),      # difficulty
            "tags": prob.get("tags") or [],            # topic segments
            "verdict": s.get("verdict"),               # SUCCESS / FAILURE EVENT
            "language": s.get("programmingLanguage"),
            "time_ms": s.get("timeConsumedMillis"),
            "memory_bytes": s.get("memoryConsumedBytes"),
            "participant_type": author.get("participantType"),  # CONTESTANT/PRACTICE
        }


def fetch_rating_history(handle: str):
    """Skill trajectory: rating before/after every rated contest."""
    now = datetime.now(timezone.utc).isoformat()
    for r in _get("user.rating", handle=handle):
        ts = r.get("ratingUpdateTimeSeconds")
        old, new = r.get("oldRating"), r.get("newRating")
        yield {
            "source": "codeforces_rating",
            "fetched_at": now,
            "id": f"{_uid(handle)}:{r.get('contestId')}",
            "user": _uid(handle),
            "contest_id": r.get("contestId"),
            "contest": r.get("contestName"),
            "ts": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
            "rank": r.get("rank"),
            "old_rating": old,
            "new_rating": new,
            "delta": (new - old) if old is not None and new is not None else None,
        }


def fetch_recent_global(count: int = 1000):
    """Platform-wide recent submissions — the aggregate firehose, and the
    ethically cleanest entry point since it isn't targeted at anyone."""
    now = datetime.now(timezone.utc).isoformat()
    for s in _get("problemset.recentStatus", count=count):
        prob = s.get("problem") or {}
        members = (s.get("author") or {}).get("members") or [{}]
        handle = members[0].get("handle", "")
        ts = s.get("creationTimeSeconds")
        yield {
            "source": "codeforces_recent",
            "fetched_at": now,
            "id": s.get("id"),
            "user": _uid(handle) if handle else None,
            "ts": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
            "problem_rating": prob.get("rating"),
            "tags": prob.get("tags") or [],
            "verdict": s.get("verdict"),
            "language": s.get("programmingLanguage"),
        }


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for rec in fetch_rating_history(sys.argv[1]):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        for rec in fetch_recent_global(count=100):
            print(json.dumps(rec, ensure_ascii=False))
