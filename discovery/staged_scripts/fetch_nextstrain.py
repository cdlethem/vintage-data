#!/usr/bin/env python3
"""Nextstrain — live pathogen phylogenetics, with retrievable past builds.

Gap-list item: **pathogen phylogenetics.**

**What it is:** Nextstrain continuously rebuilds evolutionary trees for flu,
SARS-CoV-2, H5N1 avian influenza, mpox, RSV, dengue, Zika and dozens more, and
publishes each build as a single self-contained JSON ("Auspice v2" format). You
are looking at genomic epidemiology as it is produced — which clades exist,
where they were sampled, and how the tree is inferred to be shaped.

**Verified live 2026-09-03:**
  * `nextstrain.org/charon/getAvailable` → **310 datasets, 261 narratives**
  * `charon/getDataset?prefix=/seasonal-flu/h3n2/ha/2y` → build dated
    **2026-08-27**, 5,973 tree nodes (3,117 tips / 2,856 internal), provenance
    GISAID, maintained by the Bedford and Neher labs
  * `...&prefix=/seasonal-flu/h3n2/ha/2y@2026-06-01` → build dated **2026-05-28**

**Why it's a genuinely unusual time series, and it's that third bullet.**
Nextstrain keeps **dated snapshots of past builds**, addressable by appending
`@YYYY-MM-DD` to the prefix. That means you can *backfill a forecast-revision
dataset without having polled at the time* — the same rare property as the MLB
live feed's `timecode` (#22), and the same analytical shape as JPL close
approaches (#7) or UK Carbon Intensity (#14): you get to score a past inference
against what was later believed.

Concretely, the questions this opens up:
  * **When did a clade become visible?** Walk snapshots backward and find the
    first build in which a clade appears at all. That is a detection-latency
    measurement for genomic surveillance.
  * **Do clade frequency estimates get revised?** They do — sequences arrive
    late, and a clade's share of last month's samples looks different a month
    later. `clade_frequencies()` over several snapshot dates gives you the
    revision series directly.
  * **Sampling bias as a first-class object.** `country` counts per build tell
    you who is sequencing, not where the pathogen is. That gap is the single
    most important caveat in genomic epi and it's measurable here.

**The tree is the payload, and it is recursive.** One JSON document holds
`meta` (colorings, provenance, build date) and `tree` (a nested node object).
`flatten_tree()` walks it; tips are nodes with no `children`.

Quirks that will cost you an afternoon:

  * **The server sends gzip whether or not you ask for it.** `content-encoding:
    gzip` comes back on a plain request, and `urllib` does *not* transparently
    decompress. Read it through `gzip` (this script sniffs the magic bytes and
    handles both, so it survives the day they turn it off).
  * **`node_attrs` values are wrapped in `{"value": ...}` — except `div`,
    which is a bare float.** Verified by walking every node of a live build:
    35 wrapped keys, exactly one bare (`div`). A naive `attrs[k]["value"]`
    crashes on divergence; a naive `attrs[k]` returns a dict everywhere else.
  * **`num_date` is a decimal year**, e.g. `2018.826`, not a date string.
    `decimal_year_to_iso()` converts, leap years included.
  * **Internal nodes are inferred, tips are observed.** Internal nodes carry no
    `country` and their `num_date` has a `confidence` interval with no
    `inferred` key; tips have `inferred: false`. Mixing them silently doubles
    your sample count and invents geography. `flatten_tree(tips_only=True)` is
    the default for that reason.
  * `meta.updated` is the **build** date, which is not today's date — a build
    dated 2026-08-27 is what `latest` served on 2026-09-03. Record both.
  * A dataset path that doesn't exist returns an HTML error page, not JSON.

**Etiquette and licensing.** Builds are large (600 KB gzipped for a 2-year flu
tree; SARS-CoV-2 builds run far bigger), and rebuilds happen on the order of
days — **polling faster than daily is pure waste.** Nextstrain data is CC-BY,
but the *underlying sequences* may not be: builds sourced from **GISAID carry
GISAID's terms**, which restrict redistribution — check `meta.data_provenance`
before republishing anything derived. Builds from open sources (most
SARS-CoV-2 and mpox builds use GenBank) are unrestricted.

Endpoints:
  https://nextstrain.org/charon/getAvailable           list datasets/narratives
  https://nextstrain.org/charon/getDataset?prefix=...  one build
  https://data.nextstrain.org/<name>.json              direct S3, same format

Stdlib only.
"""
import gzip
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

BASE = "https://nextstrain.org/charon"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

# Verified live 2026-09-03. There are 310; these are the fast-moving ones.
INTERESTING = [
    "/seasonal-flu/h3n2/ha/2y",
    "/seasonal-flu/h1n1pdm/ha/2y",
    "/avian-flu/h5n1/ha/2y",
    "/avian-flu/h5n1-cattle-outbreak/genome",
    "/mpox/clade-IIb",
    "/rsv/a/genome",
]

# node_attrs values are {"value": ...} wrappers, except these bare scalars.
BARE_ATTRS = {"div"}


def _get(url: str):
    """Charon sends gzip unconditionally and urllib won't decode it."""
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        raw = resp.read()
    if raw[:2] == b"\x1f\x8b":            # gzip magic; survives them stopping
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def decimal_year_to_iso(dy):
    """num_date is a decimal year (2018.826), not a date. Leap-year aware."""
    if dy is None:
        return None
    year = int(dy)
    start = date(year, 1, 1)
    days_in_year = (date(year + 1, 1, 1) - start).days      # 365 or 366
    return (start + timedelta(days=(dy - year) * days_in_year)).isoformat()


def _attr(attrs: dict, key):
    """Unwrap {"value": x}; `div` and friends arrive bare."""
    v = attrs.get(key)
    if v is None:
        return None
    if key in BARE_ATTRS or not isinstance(v, dict):
        return v
    return v.get("value")


def list_datasets():
    """All builds Nextstrain currently serves. 310 as of 2026-09-03."""
    d = _get(f"{BASE}/getAvailable")
    now = datetime.now(timezone.utc).isoformat()
    for ds in d.get("datasets", []):
        yield {
            "source": "nextstrain_dataset",
            "fetched_at": now,
            "id": ds.get("request"),
            "prefix": ds.get("request"),
            "has_snapshots": bool(ds.get("snapshots")),
            "second_tree_options": ds.get("secondTreeOptions") or [],
            "build_url": ds.get("buildUrl"),
        }


def get_build(prefix: str, snapshot_date: str | None = None):
    """Fetch one build. `snapshot_date` ('2026-06-01') retrieves the build as
    it stood on that date — this is the backfill trick; see module docstring."""
    p = prefix if prefix.startswith("/") else "/" + prefix
    if snapshot_date:
        p = f"{p}@{snapshot_date}"
    url = f"{BASE}/getDataset?" + urllib.parse.urlencode({"prefix": p})
    return _get(url)


def flatten_tree(build: dict, prefix: str, tips_only: bool = True,
                 snapshot_date: str | None = None):
    """Walk the recursive tree into flat records.

    tips_only=True (default) keeps only observed samples. Internal nodes are
    *inferred ancestors* — they have no country and their dates are estimates,
    so counting them as observations is wrong.
    """
    now = datetime.now(timezone.utc).isoformat()
    meta = build.get("meta") or {}
    built_at = meta.get("updated")
    provenance = [p.get("name") for p in meta.get("data_provenance") or []]

    def walk(node, parent=None, depth=0):
        if not node:                      # empty/absent tree, not a real tip
            return
        attrs = node.get("node_attrs") or {}
        children = node.get("children") or []
        is_tip = not children
        if not (tips_only and not is_tip):
            nd = attrs.get("num_date") or {}
            conf = nd.get("confidence") if isinstance(nd, dict) else None
            branch = node.get("branch_attrs") or {}
            muts = branch.get("mutations") or {}
            yield {
                "source": "nextstrain",
                "fetched_at": now,
                # id must distinguish the same strain across snapshot dates
                "id": f"{prefix}@{snapshot_date or 'latest'}:{node.get('name')}",
                "dataset": prefix,
                "snapshot_date": snapshot_date,     # None == latest build
                "built_at": built_at,               # meta.updated, NOT today
                "strain": node.get("name"),
                "is_tip": is_tip,
                "parent": parent,
                "depth": depth,
                "num_date": _attr(attrs, "num_date"),        # decimal year
                "date": decimal_year_to_iso(_attr(attrs, "num_date")),
                "date_confidence": [decimal_year_to_iso(c) for c in conf]
                                   if conf else None,
                "date_inferred": nd.get("inferred") if isinstance(nd, dict)
                                 else None,
                "divergence": _attr(attrs, "div"),           # bare float
                "clade": _attr(attrs, "clade_membership"),
                "subclade": _attr(attrs, "subclade"),
                "country": _attr(attrs, "country"),
                "region": _attr(attrs, "region"),
                "division": _attr(attrs, "division"),
                "accession": _attr(attrs, "accession_ha")
                             or _attr(attrs, "accession"),
                "submitting_lab": _attr(attrs, "submitting_lab"),
                "n_aa_mutations": sum(len(v) for k, v in muts.items()
                                      if k != "nuc"),
                "n_nuc_mutations": len(muts.get("nuc") or []),
                "provenance": provenance,     # 'GISAID' => redistribution terms
            }
        for c in children:
            yield from walk(c, parent=node.get("name"), depth=depth + 1)

    yield from walk(build.get("tree") or {})


def clade_frequencies(prefix: str, snapshot_dates=None, key: str = "clade"):
    """**The revision series.** Counts tips per clade for each snapshot date.

    Run it over several dates and diff: a clade's share of a fixed past window
    changes as late sequences land, so this measures how much you should trust
    a fresh frequency estimate. Emits one row per (snapshot, clade).
    """
    now = datetime.now(timezone.utc).isoformat()
    for snap in (snapshot_dates or [None]):
        build = get_build(prefix, snapshot_date=snap)
        counts, total = {}, 0
        for rec in flatten_tree(build, prefix, tips_only=True,
                                snapshot_date=snap):
            c = rec.get(key) or "unassigned"
            counts[c] = counts.get(c, 0) + 1
            total += 1
        built_at = (build.get("meta") or {}).get("updated")
        for clade, n in sorted(counts.items(), key=lambda x: -x[1]):
            yield {
                "source": "nextstrain_clade_freq",
                "fetched_at": now,
                "id": f"{prefix}@{snap or 'latest'}:{clade}",
                "dataset": prefix,
                "snapshot_date": snap,
                "built_at": built_at,
                "clade": clade,
                "n_tips": n,
                "total_tips": total,
                "frequency": round(n / total, 6) if total else None,
            }


def sampling_by_country(build: dict, prefix: str):
    """Who is sequencing — NOT where the pathogen is. The key epi caveat,
    made measurable."""
    now = datetime.now(timezone.utc).isoformat()
    counts = {}
    for rec in flatten_tree(build, prefix, tips_only=True):
        counts[rec["country"] or "unknown"] = \
            counts.get(rec["country"] or "unknown", 0) + 1
    total = sum(counts.values())
    for country, n in sorted(counts.items(), key=lambda x: -x[1]):
        yield {
            "source": "nextstrain_sampling",
            "fetched_at": now,
            "id": f"{prefix}:{country}",
            "dataset": prefix,
            "country": country,
            "n_sequences": n,
            "share": round(n / total, 6) if total else None,
        }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "tips"
    prefix = sys.argv[2] if len(sys.argv) > 2 else "/seasonal-flu/h3n2/ha/2y"

    if mode == "datasets":
        for r in list_datasets():
            print(json.dumps(r, ensure_ascii=False))
    elif mode == "revision":
        # the headline demo: same build, seen from three different dates
        today = date.today()
        dates = [None,
                 (today - timedelta(days=30)).isoformat(),
                 (today - timedelta(days=90)).isoformat()]
        for r in clade_frequencies(prefix, dates):
            print(json.dumps(r, ensure_ascii=False))
    elif mode == "sampling":
        for r in sampling_by_country(get_build(prefix), prefix):
            print(json.dumps(r, ensure_ascii=False))
    else:
        for r in flatten_tree(get_build(prefix), prefix):
            print(json.dumps(r, ensure_ascii=False))
