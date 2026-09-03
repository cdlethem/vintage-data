#!/usr/bin/env python3
"""Hugging Face Hub API — keyless per-author model and dataset uploads.

Lead #85: creator-side telemetry for the ML ecosystem, moving fast. Every model and
dataset repo pushed to the Hub is queryable, sortable by creation time, with likes and
download counts that drift as the community discovers it.

**Verified live 2026-09-03** — this is about as close to a raw firehose as this catalog
gets:
  * `huggingface.co/api/models?sort=createdAt&direction=-1&limit=10` — **200, 3,017
    bytes, 10 rows**, newest `createdAt: 2026-09-03T14:36:47.000Z` — **47 seconds before
    the probe that fetched it.**
  * `huggingface.co/api/datasets?sort=createdAt&direction=-1&limit=10` — **200, 5,903
    bytes, 10 rows**, newest `createdAt` **23 seconds** before the probe.

Endpoints (base `https://huggingface.co/api`):
    /models?sort=createdAt&direction=-1&limit=N     newest model repos first
    /datasets?sort=createdAt&direction=-1&limit=N    newest dataset repos first
    /models?search=<term>                            filter by name/tag substring
    /models/{repo_id}                                 one repo's full metadata

Quirks:
    * **Most of what streams past is noise, and that's real, not a bug.** The freshest
      models observed live were named things like `MyAwesomeModel-TestRepo` — throwaway
      test uploads with `likes:0, downloads:0`, not finished releases. If you want
      "interesting" models rather than "just created," sort by `likes` or `downloads`
      instead of `createdAt`, or filter on the presence of real `tags`/`pipeline_tag`.
    * **Models and datasets return different shapes.** A model row's identifying field
      is `id` (also mirrored as `modelId`); a dataset row's is also `id` but carries
      `author`, `sha`, and `lastModified` that models don't expose in the list view.
      Don't assume one normalizer covers both without checking.
    * `tags` for a brand-new repo can be as thin as `["region:us"]` — a placeholder,
      not real metadata. Do not treat a short tag list as a data-quality problem; the
      repo may just be seconds old.
    * `likes`/`downloads` are cumulative counters, not deltas — diff two `fetched_at`
      snapshots of the same repo id if you want the rate of change, which is the more
      interesting signal for an already-known repo.

Etiquette: keyless, no rate limit published for the list API (the inference API has
separate limits that don't apply here). Poll `createdAt`-sorted lists at a cadence that
matches upload volume — this sandbox saw new repos appear within a minute of each
other, so polling every few minutes is reasonable; polling every second is not.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: cdlethem@gmail.com)"
BASE = "https://huggingface.co/api"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_repos(kind: str = "models", sort: str = "createdAt", limit: int = 20):
    """kind is 'models' or 'datasets'. sort='createdAt' for the newest-first firehose,
    or 'likes'/'downloads' for what's actually gaining traction."""
    now = datetime.now(timezone.utc).isoformat()
    params = urllib.parse.urlencode({"sort": sort, "direction": -1, "limit": limit})
    for r in _get(f"{BASE}/{kind}?{params}"):
        yield {
            "source": f"huggingface_{kind}",
            "fetched_at": now,
            "id": r.get("id") or r.get("modelId"),
            "author": r.get("author") or (r.get("id") or "").split("/")[0],
            "created_at": r.get("createdAt"),
            "last_modified": r.get("lastModified"),
            "likes": r.get("likes"),
            "downloads": r.get("downloads"),
            "private": r.get("private"),
            "gated": r.get("gated"),
            "tags": r.get("tags") or [],
        }


if __name__ == "__main__":
    kind = sys.argv[1] if len(sys.argv) > 1 else "models"
    sort = sys.argv[2] if len(sys.argv) > 2 else "createdAt"
    for rec in fetch_repos(kind, sort):
        print(json.dumps(rec, ensure_ascii=False))
