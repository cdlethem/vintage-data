#!/usr/bin/env python3
"""Tatoeba — crowd-contributed sentences and translations, keyless, growing daily.

Lead #54: a parallel corpus you can watch being built. Volunteers add sentences and
translations across hundreds of languages continuously; searching by creation date and
polling gives a live view of that growth.

**Verified live 2026-09-03**:
  * `tatoeba.org/en/api_v0/search?from=eng&to=fra&query=&sort=created&sort_reverse=yes`
    — **200, 18,788 bytes** — but the top result was sentence id **1276**, one of the
    oldest in the corpus, despite asking for newest-first. `sort_reverse=yes` did
    **not** reverse the order (`direction: "asc"` in the response's own paging block).
  * `...&sort=created&direction=desc` — **200, 6,471 bytes** — top result id
    **14037517**, `"We all have to pull our weight."`, genuinely the newest match.
    The correct parameter is `direction`, not `sort_reverse`.

Endpoint: `https://tatoeba.org/en/api_v0/search`
    from=<iso639-3>       source language (e.g. eng)
    to=<iso639-3>          target language (e.g. fra) — omit for monolingual search
    query=<text>            leave empty to browse rather than search
    sort=created            sort by creation date (also: relevance, words, random)
    direction=desc          desc for newest-first — NOT `sort_reverse`, see quirks

Quirks that will cost someone an afternoon:
    * **`sort_reverse=yes` is not the parameter that reverses sort order.** It looks
      like it should be from the name, and the API accepts it without error, but
      verified live it has no effect — the response's own `paging.Sentences.direction`
      stayed `"asc"` and the results stayed oldest-first. Use `direction=desc`.
    * **`translations` is a list of lists**, one inner list per translation "tier"
      (direct translations, then indirect translations-of-translations) — not a flat
      list of translation objects. A translation with no entries in a tier is an empty
      list `[]`, not absent.
    * A sentence can have **zero translations into your target language** even when
      matched by the `to=` filter — the search matches on having *some* translation
      relationship, not necessarily into exactly the language you asked for.
    * `correctness` is present but was `0` for every real record sampled — treat 0 as
      "not flagged," not literally zero quality.
    * `license` is per-sentence (`"CC BY 2.0 FR"` observed), not global — check it per
      record if you redistribute anything, not just once for the dataset.

Etiquette: keyless, no published rate limit, but Tatoeba is a nonprofit-run community
project. Self-impose gentle polling (this is a slow-growing corpus; hourly is plenty).

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: cdlethem@gmail.com)"
BASE = "https://tatoeba.org/en/api_v0/search"


def _get(**params):
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_recent(from_lang: str = "eng", to_lang: str = "fra"):
    """Newest sentences with a translation relationship into to_lang, newest first.
    direction=desc is required -- sort_reverse does NOT reverse the order."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(**{"from": from_lang, "to": to_lang, "query": "",
                 "sort": "created", "direction": "desc"})
    for s in doc.get("results") or []:
        all_translations = []
        for tier in s.get("translations") or []:
            all_translations.extend(tier)
        yield {
            "source": "tatoeba",
            "fetched_at": now,
            "id": s["id"],
            "text": s.get("text"),
            "lang": s.get("lang"),
            "license": s.get("license"),
            "translations": [{"id": t.get("id"), "text": t.get("text"),
                             "lang": t.get("lang")} for t in all_translations],
        }


if __name__ == "__main__":
    from_lang = sys.argv[1] if len(sys.argv) > 1 else "eng"
    to_lang = sys.argv[2] if len(sys.argv) > 2 else "fra"
    for rec in fetch_recent(from_lang, to_lang):
        print(json.dumps(rec, ensure_ascii=False))
