#!/usr/bin/env python3
"""Manifold Markets — crowd probability estimates on arbitrary questions (keyless).

Play-money prediction market (no crypto, no real currency). Thousands of open
questions ranging from serious ("Will the U.S. prime-age employment-population
ratio be below 50% in Aug 2029?") to gloriously mundane ("How hot will my
bedroom get on September 2nd?"). Each has a live probability.

Two extraction modes, and the second is the interesting one:
  1. fetch_markets(): newest markets — a stream of *questions people are
     asking about the future*. Fantastic LLM corpus: cluster by topic, track
     what the forecasting crowd starts worrying about and when.
  2. fetch_probability_snapshot(): re-poll a watchlist of market IDs every
     run. Probability is a float that moves continuously — you get a genuine,
     dense, self-labeling time series, and every market eventually RESOLVES,
     giving you free ground-truth labels for calibration analysis. That last
     property is rare and makes this a superb ML dataset.

Verified live 2026-09-02: GET https://api.manifold.markets/v0/markets?limit=3
returned current markets, no auth.

Quirks:
  * Documented limit ~500 requests/minute per IP.
  * All timestamps are UNIX **milliseconds**.
  * `limit` maxes at 1000; page backwards with `before=<marketId>`.
  * Non-binary types (MULTIPLE_CHOICE, MULTI_NUMERIC, DATE, POLL) have no
    top-level `probability` — handle None rather than assuming binary.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.manifold.markets/v0"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def _get(path: str, **params):
    url = f"{BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _ms(v):
    return datetime.fromtimestamp(v / 1000, tz=timezone.utc).isoformat() if v else None


def _norm(m: dict, now: str) -> dict:
    return {
        "source": "manifold",
        "fetched_at": now,               # snapshot time — the time-series key
        "id": m.get("id"),
        "question": m.get("question"),   # the text corpus
        "creator": m.get("creatorUsername"),
        "created": _ms(m.get("createdTime")),
        "close_time": _ms(m.get("closeTime")),
        "outcome_type": m.get("outcomeType"),
        "probability": m.get("probability"),   # None for non-binary
        "volume": m.get("volume"),
        "volume_24h": m.get("volume24Hours"),
        "n_bettors": m.get("uniqueBettorCount"),
        "resolved": m.get("isResolved"),
        "resolution": m.get("resolution"),     # ground truth once resolved
        "resolution_prob": m.get("resolutionProbability"),
        "last_bet": _ms(m.get("lastBetTime")),
        "url": m.get("url"),
    }


def fetch_markets(limit: int = 500, before: str | None = None):
    """Newest markets first. Pass `before=<id>` to page further back."""
    params = {"limit": min(limit, 1000)}
    if before:
        params["before"] = before
    now = datetime.now(timezone.utc).isoformat()
    for m in _get("markets", **params):
        yield _norm(m, now)


def fetch_probability_snapshot(market_ids):
    """Re-poll specific markets to build a probability time series."""
    now = datetime.now(timezone.utc).isoformat()
    for mid in market_ids:
        yield _norm(_get(f"market/{mid}"), now)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gen = fetch_probability_snapshot(sys.argv[1:])
    else:
        gen = fetch_markets(limit=50)
    for rec in gen:
        print(json.dumps(rec, ensure_ascii=False))
