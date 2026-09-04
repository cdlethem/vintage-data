#!/usr/bin/env python3
"""TED (Tenders Electronic Daily) — EU public procurement, in 24 languages.

Gap-list item: **multilingual procurement.** This is simultaneously the
strongest non-English source in the catalog and one of the largest money
feeds in it.

**What it is:** every public contract notice above the EU threshold, from
every member state, published to the EU's Official Journal supplement. Who is
buying what, from whom, for how much, with deadlines — thousands of documents
a day.

**Verified live 2026-09-03:**
  * `POST api.ted.europa.eu/v3/notices/search` with
    `{"query": "publication-date>=today(-1)"}` → **6,623 notices** for a
    single day; a two-day window returned 9,635.
  * Notice `602738-2026`, published 2026-09-02, buyer *DB Energie GmbH*
    (DEU), CPV `09310000` (electricity), notice type `can-standard`.
  * Its `notice-title` came back in **24 languages** — bul, ces, dan, deu,
    ell, eng, est, fin, fra, gle, hrv, hun, ita, lav, lit, mlt, nld, pol,
    por, ron, slk, slv, spa, swe.

**Why it is the best multilingual source here, and it is not close.** The same
notice title is machine-translated into all 24 official EU languages and
served in one response:

    eng: "Germany – Electricity – Langfrist-PPA Solar"
    deu: "Deutschland – Elektrizität – Langfrist-PPA Solar"
    hun: "Németország – Villamos energia – Langfrist-PPA Solar"
    gle: "An Ghearmáin – Vehicle towing-away services – ..."   (another notice)

That is a **sentence-aligned parallel corpus across 24 languages, growing by
thousands of rows a day, free.** Parallel corpora of that width are normally
licensed products. And note the tail of each title: the buyer's original
free-text description (`Langfrist-PPA Solar`) is passed through *untranslated*
in every language variant, so each record also gives you a clean
translated/untranslated boundary to test extraction against.

**Other things you can do with it that people don't:**
  * **CPV codes are a controlled vocabulary** (`09310000` = electricity), so
    you get category labels for free — no classifier needed, and you can use
    them as ground truth to evaluate one.
  * **Award value vs. estimated value** appears across notice types: a
    contract notice states an estimate, the later award notice states what was
    actually paid. Joining them on the procurement reference is a real
    forecast-vs-outcome dataset about public money.
  * **Deadline pressure.** `deadline-receipt-request` minus `publication-date`
    is the response window buyers allow; it varies by country, sector and
    urgency, and short windows are a well-known procurement-integrity signal.
  * Cross-border comparison of what states buy, by CPV, over time.

Quirks that will cost you an afternoon:

  * **This endpoint is POST-only. A GET returns HTTP 405**, which reads like
    the API is down when it isn't.
  * **`publication-date` is `"2026-09-02+02:00"` — a date with a timezone
    offset and no time.** This is the nastiest trap here, because
    `datetime.fromisoformat("2026-09-02+02:00")` **succeeds** and silently
    returns `2026-09-02 02:00:00`: it reads the offset as a clock time. You
    get a plausible-looking wrong timestamp with no error. `parse_ted_date()`
    takes the date part deliberately and keeps the offset separately.
  * **Multilingual fields are dicts keyed by 3-letter ISO 639-2 codes**
    (`deu`, not `de`; `ell` for Greek, `gle` for Irish). Codes differ from the
    2-letter codes almost everything else in this document uses.
  * **`buyer-name` is only present in the original language**, unlike
    `notice-title`. Don't expect `eng` to be there — fall back to whatever
    single key exists.
  * **Repeated fields arrive with duplicates**: the live sample returned
    `classification-cpv: ["09310000", "09310000"]` and
    `contract-nature: ["supplies", "supplies"]`. Deduplicate or your category
    counts double.
  * **`limit` caps at 250** and returns an explicit `SEARCH_EXCEEDS_MAX_LIMIT`
    error above it. Page-based paging works to at least offset 10,000; for
    anything deeper use `paginationMode: "ITERATION"` and pass the returned
    `iterationNextToken` back, which is what this script does by default.
  * **Fields are omitted entirely when absent**, not returned as null —
    `total-value` and `deadline-receipt-request` simply won't be keys.
  * The query language is TED's own expert syntax, not SQL and not Lucene.
    `publication-date>=today(-7)` is the watermark idiom; quote string
    literals with single quotes.

**Etiquette and licensing.** TED data is published for reuse under the
Commission's open-data decision — reuse is free, including commercially, with
source acknowledgement. It is still a public institution's API: notices
publish on business days in batches, so **poll daily** and watermark on
`publication-date`. Past notices are immutable once published; fetch once and
cache. Requesting all 24 language variants of a field you only read one of is
the main way to waste their bandwidth and yours — ask for `notice-title` only
when you actually want the parallel text.

Endpoints:
  POST https://api.ted.europa.eu/v3/notices/search
       {"query": ..., "fields": [...], "limit": 1..250,
        "page": n | "paginationMode": "ITERATION", "iterationNextToken": ...}
  Notice renderings: https://ted.europa.eu/{lang}/notice/{number}/{xml|pdf}

Stdlib only.
"""
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.ted.europa.eu/v3/notices/search"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

MAX_LIMIT = 250          # hard, enforced server-side

# ISO 639-2/B, as TED keys its multilingual dicts. Not the 2-letter codes.
EU_LANGS = ["bul", "ces", "dan", "deu", "ell", "eng", "est", "fin", "fra",
            "gle", "hrv", "hun", "ita", "lav", "lit", "mlt", "nld", "pol",
            "por", "ron", "slk", "slv", "spa", "swe"]

DEFAULT_FIELDS = [
    "publication-number", "publication-date", "notice-title", "notice-type",
    "buyer-name", "buyer-country", "contract-nature", "classification-cpv",
    "total-value", "deadline-receipt-request", "place-of-performance",
]


def _post(payload: dict):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(BASE, data=body, method="POST", headers={
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:400]
        raise RuntimeError(f"TED {e.code}: {detail}") from e


def parse_ted_date(s):
    """'2026-09-02+02:00' -> ('2026-09-02', '+02:00').

    Do NOT hand this to datetime.fromisoformat: it parses without error and
    returns 2026-09-02T02:00:00, silently inventing a time from the offset.
    """
    if not s:
        return None, None
    head = s[:10]
    rest = s[10:] or None
    if rest and rest[0] == "T":       # a full timestamp, keep it whole
        return s, None
    return head, rest


def _dedupe(seq):
    """Repeated fields arrive with literal duplicates. Order-preserving."""
    if not seq:
        return []
    if isinstance(seq, str):
        return [seq]
    out, seen = [], set()
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _one_lang(field, prefer="eng"):
    """Pick one string out of a {lang: value} dict.

    buyer-name exists only in the original language, so 'eng' is often absent
    — fall back to whatever single key is there rather than returning None.
    """
    if field is None:
        return None, None
    if isinstance(field, str):
        return field, None
    if isinstance(field, list):
        return (field[0] if field else None), None
    lang = prefer if prefer in field else next(iter(field), None)
    if lang is None:
        return None, None
    v = field[lang]
    if isinstance(v, list):
        v = v[0] if v else None
    return v, lang


def normalize(n: dict, now: str, keep_all_langs: bool = False):
    pub_date, pub_offset = parse_ted_date(n.get("publication-date"))
    deadline, _ = parse_ted_date(n.get("deadline-receipt-request"))
    title = n.get("notice-title") or {}
    title_en, _ = _one_lang(title, "eng")
    buyer, buyer_lang = _one_lang(n.get("buyer-name"), "eng")

    rec = {
        "source": "ted_procurement",
        "fetched_at": now,
        "id": n.get("publication-number"),
        "publication_number": n.get("publication-number"),
        # date and offset kept apart on purpose; see parse_ted_date
        "publication_date": pub_date,
        "publication_utc_offset": pub_offset,
        "deadline": deadline,
        "notice_type": n.get("notice-type"),
        "title_en": title_en,
        "n_title_languages": len(title) if isinstance(title, dict) else 0,
        "buyer_name": buyer,
        "buyer_name_lang": buyer_lang,      # the notice's original language
        "buyer_country": _dedupe(n.get("buyer-country")),
        "contract_nature": _dedupe(n.get("contract-nature")),
        "cpv_codes": _dedupe(n.get("classification-cpv")),
        "total_value": n.get("total-value"),      # absent, not null, when n/a
        "place_of_performance": _dedupe(n.get("place-of-performance")),
        "xml_url": ((n.get("links") or {}).get("xml") or {}).get("MUL"),
    }
    if keep_all_langs:
        # the parallel corpus itself
        rec["title_by_lang"] = title if isinstance(title, dict) else {}
    return rec


def search(query: str = "publication-date>=today(-1)", fields=None,
           limit: int = MAX_LIMIT, max_records: int = 1000,
           keep_all_langs: bool = False):
    """Notices matching a TED expert query, paged by iteration token.

    `query` uses TED's own syntax, not SQL: `publication-date>=today(-7)`,
    `buyer-country=DEU`, `classification-cpv=09310000`, joined with AND/OR.
    """
    if limit > MAX_LIMIT:
        raise ValueError(f"limit caps at {MAX_LIMIT} server-side")
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "query": query,
        "fields": list(fields or DEFAULT_FIELDS),
        "limit": limit,
        "paginationMode": "ITERATION",
    }
    seen = 0
    while seen < max_records:
        doc = _post(payload)
        notices = doc.get("notices") or []
        if not notices:
            break
        for n in notices:
            yield normalize(n, now, keep_all_langs)
            seen += 1
            if seen >= max_records:
                return
        token = doc.get("iterationNextToken")
        if not token:
            break
        payload["iterationNextToken"] = token


def count(query: str = "publication-date>=today(-1)") -> int:
    """Size a query for one record's worth of bandwidth before paging it."""
    doc = _post({"query": query, "fields": ["publication-number"],
                 "limit": 1, "page": 1})
    return doc.get("totalNoticeCount", 0)


def parallel_titles(query: str = "publication-date>=today(-1)",
                    max_records: int = 100, langs=None):
    """**The parallel corpus.** One row per (notice, language) — aligned
    translations of the same title, ready for MT evaluation or cross-lingual
    retrieval work."""
    now = datetime.now(timezone.utc).isoformat()
    wanted = set(langs or EU_LANGS)
    for rec in search(query, fields=["publication-number", "publication-date",
                                     "notice-title", "buyer-country"],
                      max_records=max_records, keep_all_langs=True):
        for lang, text in sorted((rec.get("title_by_lang") or {}).items()):
            if lang not in wanted:
                continue
            if isinstance(text, list):
                text = text[0] if text else None
            yield {
                "source": "ted_parallel_title",
                "fetched_at": now,
                "id": f"{rec['publication_number']}:{lang}",
                "publication_number": rec["publication_number"],
                "publication_date": rec["publication_date"],
                "buyer_country": rec["buyer_country"],
                "lang": lang,                    # ISO 639-2/B
                "title": text,
            }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "recent"
    q = sys.argv[2] if len(sys.argv) > 2 else "publication-date>=today(-1)"
    if mode == "count":
        print(json.dumps({"query": q, "total": count(q)}))
    elif mode == "parallel":
        for r in parallel_titles(q, max_records=20):
            print(json.dumps(r, ensure_ascii=False))
    else:
        for r in search(q, max_records=50):
            print(json.dumps(r, ensure_ascii=False))
