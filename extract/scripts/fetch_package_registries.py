#!/usr/bin/env python3
"""Software supply chain firehose — PyPI + crates.io (keyless).

The world's programmers publish code continuously, and the registries announce
it in public feeds. This is a *fast* stream (PyPI alone: a new release every
few seconds) with rich text: package names, one-line summaries, author emails.

'Current' per run:
  * PyPI /rss/updates.xml  — ~40 most recent RELEASES (any version)
  * PyPI /rss/packages.xml — ~40 most recent BRAND-NEW package names
  * crates.io /api/v1/summary — new crates, most-downloaded, just-updated
Poll every few minutes and dedupe on (name, version); the RSS windows are
short, so a slow cadence silently drops records.

Verified live 2026-09-02 23:0x GMT: all three returned entries timestamped
within the same minute as the request.

Etiquette: crates.io REQUIRES a descriptive User-Agent with contact info and
rate-limits ~1 req/sec. PyPI asks the same. Do not hammer.

Ideas: name-entropy over time, "how fast does a new AI buzzword propagate
into package names", typosquat detection (new package with small edit
distance to a popular one), release cadence as a developer-activity index.

Stdlib only.
"""
import json
import os
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

PYPI_FEEDS = {
    "release": "https://pypi.org/rss/updates.xml",
    "new_package": "https://pypi.org/rss/packages.xml",
}
CRATES_SUMMARY = "https://crates.io/api/v1/summary"


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def fetch_pypi(kind: str = "release"):
    """kind: 'release' (any new version) or 'new_package' (first-ever upload)."""
    root = ET.fromstring(_get(PYPI_FEEDS[kind]))
    now = datetime.now(timezone.utc).isoformat()
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        # "pkgname 1.2.3"  OR  "pkgname added to PyPI"
        m = re.match(r"^(\S+)\s+(.*)$", title)
        name = m.group(1) if m else title
        version = m.group(2) if m and not m.group(2).startswith("added") else None
        yield {
            "source": f"pypi_{kind}",
            "fetched_at": now,
            "id": f"{name}@{version}" if version else name,
            "package": name,
            "version": version,
            "summary": (item.findtext("description") or "").strip(),
            "author": (item.findtext("author") or "").strip() or None,
            "published": (item.findtext("pubDate") or "").strip(),
            "url": (item.findtext("link") or "").strip(),
        }


def fetch_crates():
    data = json.loads(_get(CRATES_SUMMARY))
    now = datetime.now(timezone.utc).isoformat()
    yield {"source": "crates_totals", "fetched_at": now,
           "num_crates": data.get("num_crates"),
           "num_downloads": data.get("num_downloads")}
    for bucket in ("new_crates", "just_updated", "most_recently_downloaded"):
        for c in data.get(bucket, []):
            yield {
                "source": f"crates_{bucket}",
                "fetched_at": now,
                "id": f"{c.get('id')}@{c.get('newest_version')}",
                "package": c.get("id"),
                "version": c.get("newest_version"),
                "summary": c.get("description"),
                "created": c.get("created_at"),
                "updated": c.get("updated_at"),
                "downloads": c.get("downloads"),
                "recent_downloads": c.get("recent_downloads"),
            }


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "pypi"
    gen = fetch_crates() if which == "crates" else fetch_pypi(
        "new_package" if which == "pypi_new" else "release")
    for rec in gen:
        print(json.dumps(rec, ensure_ascii=False))
