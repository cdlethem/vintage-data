#!/usr/bin/env python3
"""Avalanche danger forecasts — North America (avalanche.org) + Europe (EAWS).

Gap-list item: **avalanche forecasting.** Also one of the strongest
non-English sources in this catalog — see the multilingual note below.

**What it is:** every avalanche warning service in the northern hemisphere
publishes a daily bulletin per forecast zone: a danger rating on the
five-level European scale, the *avalanche problems* driving it (wind slab,
persistent weak layer, wet snow), and a narrative explaining the reasoning.
Two networks cover almost all of it, and both are keyless:

  * **North America** — `api.avalanche.org/v2` aggregates 29 forecast centers
    (NWAC, CAIC, Sierra, Utah, ...) behind one API.
  * **Europe** — EAWS members publish a standard CAAML/JSON bulletin, and
    `static.avalanche.report` archives every one of them, date-partitioned,
    back to 2021.

**Verified live 2026-09-03:**
  * `api.avalanche.org/v2/public/products/map-layer` → **83 forecast zones
    across 29 centers**, every one currently `danger_level: -1` ("no rating")
    with **47 of 83 flagged `off_season: true`** — because it is September.
  * `api.avalanche.org/v2/public/products?avalanche_center_id=NWAC` → **9,616
    products, 10 MB**, spanning 2019-04-17 to 2026-04-20; 8,336 carry danger
    ratings.
  * EAWS archive index → **1,966 dated directories**, 2021-01-25 onward.
  * `.../eaws_bulletins/2026-02-15/` → **30 region files**: AD, AT-02…AT-08,
    CH, CZ, DE-BY, ES, ES-CT-L, FI, FR, GB, IT-21, IT-23, IT-25, IT-32-BZ,
    IT-32-TN, IT-34, IT-36, IT-57, NO, PL-12, RO, SE, SI, SK.

**This source is SEASONAL, and that is the first thing to understand.** In
September the northern-hemisphere network is dormant: every US zone returns
"no rating" and European bulletins simply do not exist for the date. That is
not a broken integration, and a pipeline that pages someone at 3am because
the avalanche feed went quiet in July has misunderstood the source. Poll it
November–May; treat off-season emptiness as expected.

**Why it's a good series despite that:**

  * **Forecast-vs-outcome is built in.** Each US product's `danger` array
    carries entries for `valid_day: "current"` *and* `valid_day: "tomorrow"`.
    Tomorrow's rating, published today, can be scored against the "current"
    rating published tomorrow — the same free forecast-scoring structure as
    UK Carbon Intensity (#14), with no archiving work of your own.
  * **Danger is a 5-level ordinal that moves daily** across ~100 zones, with
    strong spatial correlation between neighbouring zones. Good material for
    ordinal forecasting and spatial smoothing.
  * **The narrative is the corpus.** Every rating comes with prose explaining
    *why* — this is a rare dataset of expert reasoning under uncertainty,
    written daily by professionals, with a structured outcome attached. There
    is very little else like it publicly.
  * **Three elevation bands per zone per day** (`lower`/`middle`/`upper` =
    below / near / above treeline). Danger frequently differs across bands,
    so flattening to one number per zone throws away most of the signal.

**The multilingual part is real, not aspirational.** EAWS bulletins come from
national providers and are published in the provider's language. Confirmed by
sampling 2026-02-15: **RO** carries `lang: "ro"` and genuinely Romanian text
("Buletin nivometeorologic… RISC 3 - ÎNSEMNAT"), while providers include
AEMET (Spain), Météo-France, HS ČR (Czechia), Tatrzańskie OPR (Poland),
ARSO (Slovenia), AINEVA (Italy), SLF (Switzerland), NVE (Norway) and
ANM (Romania). It is the same document type, same schema, ~10 languages —
which makes it an unusually clean **cross-lingual** corpus: comparable
technical prose about comparable hazards.

Quirks that will cost you an afternoon:

  * **`lang` is frequently `null` even when text is present.** Confirmed live:
    ES, FR, CZ, PL-12 and NO all returned `lang: None` with populated
    narratives. Do not filter on `lang` and do not assume `None` means
    English — detect from the text or key off the provider.
  * **The US `products` endpoint has no date filter and returns the entire
    archive** — 10 MB and 9,616 records for a single center. Use
    `products/map-layer` for a cheap current-state poll and only pull the full
    archive when you actually want history.
  * **US `bottom_line` is HTML with HTML entities** (`<p>`, `&rsquo;`). It
    needs unescaping *and* tag-stripping, in that order.
  * **`danger_level: -1` and `"no rating"` are not danger level 0.** -1 means
    nobody issued a forecast; 0/`no_rating` in the European scheme means the
    same. Storing either as a number in a series makes "no forecast" look
    safer than "low", which is exactly backwards.
  * **US `danger` has a `tomorrow` entry whose values are usually `null`.**
    Don't record those nulls as a forecast; skip them.
  * European `.ratings.json` keys are compound:
    `"AT-07-01:high:am"` = region : elevation band : half-day. Splitting on
    `:` is required, and the bare `"AT-07-01"` key is the day-max rollup —
    counting both double-counts the zone.
  * One EAWS region file holds **several bulletins**, one per sub-region
    grouping, each with its own `regions[]` list — not one bulletin per file.

**Etiquette and licensing.** Both are public-safety services run on public
money; they are not high-capacity commercial APIs. Bulletins update once or
twice a day, so **poll daily** — anything faster is waste. avalanche.org is a
non-profit; EAWS terms vary per member service, several require attribution
to the issuing warning service. Attribute the provider named in
`source.provider.name`, which the normalizer preserves for that reason.

**And a real-world caution:** this is life-safety information with a legally
significant publisher. Never present a cached or derived rating as a current
forecast to anyone who might act on it in the field. Link people to the
issuing service. A stale avalanche rating can kill someone.

Endpoints:
  https://api.avalanche.org/v2/public/products/map-layer        current, all US zones
  https://api.avalanche.org/v2/public/products?avalanche_center_id=NWAC
  https://api.avalanche.org/v2/public/avalanche-center/{id}
  https://static.avalanche.report/eaws_bulletins/                   index
  https://static.avalanche.report/eaws_bulletins/{date}/{date}-{region}.json
  https://static.avalanche.report/eaws_bulletins/{date}/{date}-{region}.ratings.json

Stdlib only.
"""
import html
import json
import re
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

US_BASE = "https://api.avalanche.org/v2/public"
EAWS_BASE = "https://static.avalanche.report/eaws_bulletins"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"

# The five-level European Avalanche Danger Scale, used on both continents.
DANGER_SCALE = {1: "low", 2: "moderate", 3: "considerable", 4: "high",
                5: "very_high"}
DANGER_BY_NAME = {v: k for k, v in DANGER_SCALE.items()}
# -1 / 0 / "no rating" all mean "nobody issued a forecast". NOT level 0.
NO_RATING = {-1, 0, None}

# Confirmed present in the 2026-02-15 archive directory.
EAWS_REGIONS = [
    "AD", "AT-02", "AT-03", "AT-04", "AT-05", "AT-06", "AT-07", "AT-08",
    "CH", "CZ", "DE-BY", "ES", "ES-CT-L", "FI", "FR", "GB", "IT-21", "IT-23",
    "IT-25", "IT-32-BZ", "IT-32-TN", "IT-34", "IT-36", "IT-57", "NO",
    "PL-12", "RO", "SE", "SI", "SK",
]

_TAGS = re.compile(r"<[^>]+>")


def _get(url: str, allow_404: bool = False):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        # Off-season / not-yet-published dates 404. That is "no bulletin",
        # not a failure — see the seasonality note in the module docstring.
        if allow_404 and e.code == 404:
            return None
        raise


def strip_html(s):
    """US narratives are HTML with entities: unescape THEN strip tags."""
    if not s:
        return None
    return " ".join(_TAGS.sub(" ", html.unescape(s)).split()) or None


def _danger_num(v):
    """Normalise a rating to 1-5, or None for 'no forecast issued'."""
    if isinstance(v, str):
        v = DANGER_BY_NAME.get(v.strip().lower().replace(" ", "_"))
    if v in NO_RATING:
        return None
    return v if isinstance(v, int) and 1 <= v <= 5 else None


# ---------------------------------------------------------------- North America

def fetch_us_current():
    """Cheap current-state poll: every US forecast zone, one request.

    In summer every zone is `danger_level: -1`. That is correct, not broken.
    """
    now = datetime.now(timezone.utc).isoformat()
    fc = _get(f"{US_BASE}/products/map-layer") or {}
    for feat in fc.get("features") or []:
        p = feat.get("properties") or {}
        yield {
            "source": "avalanche_us",
            "fetched_at": now,
            "id": f"us:{p.get('center_id')}:{feat.get('id')}",
            "network": "avalanche.org",
            "center_id": p.get("center_id"),
            "center": p.get("name"),
            "zone_id": feat.get("id"),
            "state": p.get("state"),
            "timezone": p.get("timezone"),
            "danger_level": _danger_num(p.get("danger_level")),
            "danger_text": p.get("danger"),        # 'no rating' when dormant
            "off_season": bool(p.get("off_season")),
            "start_date": p.get("start_date"),
            "end_date": p.get("end_date"),
            "travel_advice": p.get("travel_advice"),
            "warning": ((p.get("warning") or {}).get("product") is not None),
            "link": p.get("link"),
            "provider": p.get("center"),           # attribute this
        }


def fetch_us_products(center_id: str = "NWAC", since: str | None = None,
                      forecasts_only: bool = True):
    """Full archive for one center. **No server-side date filter exists** —
    this is ~10 MB and ~9,600 records for NWAC. Filter client-side via
    `since` (ISO date) and cache; do not re-pull history you already have.
    """
    now = datetime.now(timezone.utc).isoformat()
    rows = _get(f"{US_BASE}/products?avalanche_center_id={center_id}") or []
    for p in rows:
        if forecasts_only and p.get("product_type") != "forecast":
            continue
        published = p.get("published_time")
        if since and published and published[:10] < since:
            continue
        zones = p.get("forecast_zone") or [{}]
        zone = zones[0] if zones else {}
        for d in p.get("danger") or []:
            # 'tomorrow' rows are usually all-null; that is not a forecast.
            bands = {b: _danger_num(d.get(b))
                     for b in ("lower", "middle", "upper")}
            if all(v is None for v in bands.values()):
                continue
            yield {
                "source": "avalanche_us_forecast",
                "fetched_at": now,
                "id": f"us:{p.get('id')}:{d.get('valid_day')}",
                "network": "avalanche.org",
                "product_id": p.get("id"),
                "center_id": center_id,
                "center": (p.get("avalanche_center") or {}).get("name"),
                "zone_id": zone.get("id"),
                "zone": zone.get("name"),
                "published_time": published,
                "expires_time": p.get("expires_time"),
                "valid_day": d.get("valid_day"),      # 'current' | 'tomorrow'
                # below / near / above treeline — do not collapse these
                "danger_below_treeline": bands["lower"],
                "danger_near_treeline": bands["middle"],
                "danger_above_treeline": bands["upper"],
                "danger_max": max((v for v in bands.values() if v is not None),
                                  default=None),
                "danger_text": p.get("danger_level_text"),
                "author": p.get("author"),
                "bottom_line": strip_html(p.get("bottom_line")),
                "n_problems": len(p.get("forecast_avalanche_problems") or []),
            }


# ---------------------------------------------------------------------- Europe

def fetch_eaws_bulletins(day: str | None = None, regions=None):
    """European bulletins for one date. Multilingual; see docstring.

    A region file holds SEVERAL bulletins, each covering a set of sub-regions.
    Missing region/date combinations 404 and are skipped silently — that is
    the normal off-season and not-a-member case.
    """
    day = day or date.today().isoformat()
    now = datetime.now(timezone.utc).isoformat()
    for region in (regions or EAWS_REGIONS):
        doc = _get(f"{EAWS_BASE}/{day}/{day}-{region}.json", allow_404=True)
        if not doc:
            continue
        for b in doc.get("bulletins") or []:
            act = b.get("avalancheActivity") or {}
            snow = b.get("snowpackStructure") or {}
            provider = ((b.get("source") or {}).get("provider") or {})
            ratings = b.get("dangerRatings") or []
            nums = [_danger_num(r.get("mainValue")) for r in ratings]
            nums = [n for n in nums if n is not None]
            region_ids = [r.get("regionID") for r in b.get("regions") or []]
            # bulletinID is OPTIONAL — Romania (ANM) publishes it as null on
            # every bulletin. Falling back to the sub-region list keeps the
            # several bulletins in one file from collapsing onto one id.
            key = b.get("bulletinID") or "+".join(
                sorted(r for r in region_ids if r)) or f"idx{len(nums)}"
            yield {
                "source": "avalanche_eaws",
                "fetched_at": now,
                "id": f"eaws:{day}:{region}:{key}",
                "network": "EAWS",
                "bulletin_id": b.get("bulletinID"),
                "date": day,
                "country": region.split("-")[0],
                "region_file": region,
                "regions": region_ids,
                # lang is OFTEN null even with text present. Do not filter on it.
                "lang": b.get("lang"),
                "provider": provider.get("name"),      # attribute this
                "provider_url": provider.get("website"),
                "publication_time": b.get("publicationTime"),
                "valid_start": (b.get("validTime") or {}).get("startTime"),
                "valid_end": (b.get("validTime") or {}).get("endTime"),
                "unscheduled": b.get("unscheduled"),
                "danger_max": max(nums) if nums else None,
                "danger_ratings": [
                    {"value": _danger_num(r.get("mainValue")),
                     "elevation": r.get("elevation") or {},
                     "valid_period": r.get("validTimePeriod")}
                    for r in ratings
                ],
                "problems": [
                    {"type": p.get("problemType"),
                     "aspects": p.get("aspects") or [],
                     "size": p.get("avalancheSize"),
                     "frequency": p.get("frequency"),
                     "stability": p.get("snowpackStability"),
                     "elevation": p.get("elevation") or {}}
                    for p in b.get("avalancheProblems") or []
                ],
                "highlights": act.get("highlights"),
                "activity_comment": act.get("comment"),   # the corpus
                "snowpack_comment": snow.get("comment"),
            }


def fetch_eaws_ratings(day: str | None = None, regions=None):
    """The compact numeric series: max danger per sub-region.

    Keys are compound — `"AT-07-01:high:am"` is region:elevation:half-day, and
    the bare `"AT-07-01"` is the whole-day rollup. Both are emitted, tagged, so
    you can pick one; summing across all of them double-counts every zone.
    """
    day = day or date.today().isoformat()
    now = datetime.now(timezone.utc).isoformat()
    bands = {"low", "high", "middle"}
    halves = {"am", "pm"}
    for region in (regions or EAWS_REGIONS):
        doc = _get(f"{EAWS_BASE}/{day}/{day}-{region}.ratings.json",
                   allow_404=True)
        if not doc:
            continue
        for key, value in (doc.get("maxDangerRatings") or {}).items():
            parts = key.split(":")
            sub = parts[0]
            elevation = next((p for p in parts[1:] if p in bands), None)
            half_day = next((p for p in parts[1:] if p in halves), None)
            yield {
                "source": "avalanche_eaws_rating",
                "fetched_at": now,
                "id": f"eaws:{day}:{key}",
                "network": "EAWS",
                "date": day,
                "country": region.split("-")[0],
                "region_file": region,
                "sub_region": sub,
                "elevation_band": elevation,      # None == all elevations
                "half_day": half_day,             # None == whole day
                "is_rollup": len(parts) == 1,     # the day-max; don't double count
                "danger_level": _danger_num(value),
                "danger_text": DANGER_SCALE.get(_danger_num(value)),
            }


def summarize(rows):
    """QC first. If everything is None in July, that is the season, not a bug."""
    rows = list(rows)
    rated = [r for r in rows if r.get("danger_level") or r.get("danger_max")]
    countries = {}
    for r in rows:
        k = r.get("country") or r.get("state") or "?"
        countries[k] = countries.get(k, 0) + 1
    return {"rows": len(rows), "with_rating": len(rated),
            "off_season": sum(1 for r in rows if r.get("off_season")),
            "areas": sorted(countries.items(), key=lambda x: -x[1])[:15]}


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "us"
    arg = sys.argv[2] if len(sys.argv) > 2 else None
    if mode == "us":
        for r in fetch_us_current():
            print(json.dumps(r, ensure_ascii=False))
    elif mode == "us-summary":
        print(json.dumps(summarize(fetch_us_current()), indent=2))
    elif mode == "us-history":
        for r in fetch_us_products(arg or "NWAC", since="2026-01-01"):
            print(json.dumps(r, ensure_ascii=False))
    elif mode == "eaws":
        for r in fetch_eaws_bulletins(arg):
            print(json.dumps(r, ensure_ascii=False))
    elif mode == "eaws-ratings":
        for r in fetch_eaws_ratings(arg):
            print(json.dumps(r, ensure_ascii=False))
    else:
        print(__doc__)
