#!/usr/bin/env python3
"""GH Archive — hourly archives of every public GitHub event since 2011.

Lead #76: "the closest public thing to a raw clickstream that exists." Pushes, PRs,
stars, forks, issues, comments, releases — every public event, with actor, repo, type
and timestamp. Billions of events total; one hour is already a real firehose. The live
equivalent for "right now" is `api.github.com/events`, included here as a bonus mode.

**Verified live 2026-09-03** against a recent complete hour:
  * `data.gharchive.org/2026-09-03-11.json.gz` — **200**, gzip transparently handled
    by `probe.py`, decompressing to **13,097,788 bytes of newline-delimited JSON**,
    2,693 events. Top event types this hour: PullRequestEvent (1,142),
    IssueCommentEvent (449), IssuesEvent (348), PullRequestReviewEvent (255),
    WatchEvent (204), ReleaseEvent (90), ForkEvent (42).
  * `api.github.com/events?per_page=5` — **200, 2,963 bytes, 5 events** — the live tail,
    for polling "right now" instead of downloading a whole finished hour.

Format: **not a JSON array** — one JSON object per line (JSONL), gzip-compressed on
the wire. Every event has `id`, `type`, `actor`, `repo`, `payload`, `created_at`,
`public`. `payload`'s shape depends entirely on `type`.

Endpoints:
    https://data.gharchive.org/{YYYY-MM-DD}-{H}.json.gz    H is 0-23, no leading zero
    https://api.github.com/events?per_page=N                 the live tail (last ~90 events)

Quirks that will cost someone an afternoon:
    * **It's gzipped JSONL, not a JSON array.** `json.load()`ing the raw file fails
      immediately; split on newlines and `json.loads()` each line. (If you fetch with
      plain `urllib` instead of this catalog's `probe.py`, note the server also sends
      `content-encoding: gzip`, which `urllib` does not auto-decompress — see the same
      gotcha documented in `fetch_nextstrain.py`.)
    * **`payload` is a different shape for every `type`, and some are enormous.** A
      single `ForkEvent`'s payload embeds the *entire* forked repo object (~4 KB, every
      GitHub repo API field) just to report a fork happened. Do not flatten `payload`
      into your envelope uniformly — keep it as a nested raw blob and pull out only the
      2-3 fields you actually need per event type.
    * The hour in the filename is **UTC, no leading zero** (`2026-09-03-11.json.gz`,
      not `-011-`). An hour is only complete (and the archive file stable) once it's
      fully in the past — don't request the current in-progress hour.
    * `actor` and `repo` were both present on every one of 2,693 events sampled here,
      but GH Archive's own docs note anonymized/deleted accounts can appear as partial
      records in older archives — don't assume presence is guaranteed for all history.

Etiquette: keyless, hosted on a public bucket. One hourly file can run tens of megabytes
compressed; fetch each hour once and cache it — these files never change once published.
`api.github.com/events` is keyless too but GitHub's general unauthenticated rate limit
(60 req/hour) applies; an OAuth token raises that if you need faster live polling.

Stdlib only.
"""
import json
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: cdlethem@gmail.com)"


def _get_bytes(url, headers=None):
    hdrs = {"User-Agent": USER_AGENT}
    hdrs.update(headers or {})
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read()
    if resp.headers.get("content-encoding") == "gzip" or body[:2] == b"\x1f\x8b":
        import gzip
        body = gzip.decompress(body)
    return body


def _norm(e, now):
    repo = e.get("repo") or {}
    actor = e.get("actor") or {}
    return {
        "source": "gh_archive",
        "fetched_at": now,
        "id": e.get("id"),
        "type": e.get("type"),
        "created_at": e.get("created_at"),
        "actor_login": actor.get("login"),
        "repo_name": repo.get("name"),
        "public": e.get("public"),
        # keep the type-dependent payload as a nested raw blob rather than flattening
        "payload": e.get("payload"),
    }


def fetch_hour(hour_utc: datetime):
    """One archived hour, e.g. datetime(2026, 9, 3, 11, tzinfo=timezone.utc).
    Only request hours fully in the past -- the current hour's file isn't final yet."""
    now = datetime.now(timezone.utc).isoformat()
    url = (f"https://data.gharchive.org/{hour_utc.year:04d}-{hour_utc.month:02d}-"
          f"{hour_utc.day:02d}-{hour_utc.hour}.json.gz")
    body = _get_bytes(url)
    for line in body.decode("utf-8").splitlines():
        if line.strip():
            yield _norm(json.loads(line), now)


def fetch_live(per_page: int = 30):
    """The live tail from api.github.com -- last ~90 events, unauthenticated rate
    limit 60 req/hour applies."""
    now = datetime.now(timezone.utc).isoformat()
    url = f"https://api.github.com/events?per_page={per_page}"
    body = _get_bytes(url, headers={"Accept": "application/vnd.github+json"})
    for e in json.loads(body):
        yield _norm(e, now)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "live"
    if mode == "hour":
        if len(sys.argv) > 2:
            y, m, d, h = (int(x) for x in sys.argv[2].split("-"))
            when = datetime(y, m, d, h, tzinfo=timezone.utc)
        else:
            when = (datetime.now(timezone.utc) - timedelta(hours=3)).replace(
                minute=0, second=0, microsecond=0)
        for rec in fetch_hour(when):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        for rec in fetch_live():
            print(json.dumps(rec, ensure_ascii=False))
