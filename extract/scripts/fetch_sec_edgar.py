#!/usr/bin/env python3
"""SEC EDGAR latest filings — keyless Atom feed of filings as they land.

Lead #17: financial in subject but fundamentally a *document* firehose, not price
data — filings appear every few minutes during market hours. Investigated by a local
agent, which found the working feed after `/search-faq` 404'd and one atom request
timed out on a retry.

**Verified live 2026-09-03** — `sec.gov/cgi-bin/browse-edgar?action=getcurrent&
type=&company=&dateb=&owner=include&count=40&output=atom` returned **200, 22,867
bytes**, feed title `"Latest Filings - Thu, 03 Sep 2026 11:16:42 EDT"`. Top entries
`updated: 2026-09-03T11:16:14-04:00` — **28 seconds before the feed's own stated
generation time.**

Endpoint: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=&company=
&dateb=&owner=include&count=N&output=atom`
    `type=` filters to a form type (e.g. `type=144`, `type=8-K`) — leave empty for all.
    `count=` is documented up to 100 per request.
    Per-company detail: `data.sec.gov/submissions/CIK{10-digit-zero-padded}.json`.

Quirks that will cost someone an afternoon:
    * **The same accession number appears as multiple separate `<entry>` elements**,
      one per party role, and **the same role can repeat with a different party on
      the same accession too.** Verified live twice over: accession
      `0001959173-26-006688` appeared once as "Reporting" and once as "Subject"; a
      separate accession appeared as "Filer" *twice*, for two different CIKs
      (Citigroup Inc and its affiliate Citigroup Global Markets Holdings, a joint
      filing). Neither accession number nor accession+role alone is a unique key —
      the id here is accession + role + CIK.
    * **`title` packs form type, party name, CIK and role into one string**
      (`"144 - Steinfort Matt (0001717324) (Reporting)"`) — parsed with a regex here
      rather than treated as free text, since every field in it is structured data.
      **Both the form type and the role can contain spaces** — `"SCHEDULE 13D/A"` and
      `"Filed by"` both appeared live in this run. A first-pass regex assumed a
      single-token form type and role (single-char-class regexes) and silently mis-parsed both
      fields on this exact entry; fixed to match non-greedily up to the next
      delimiter instead of assuming no spaces.
    * **`summary` is HTML-escaped text with the real fields embedded as bold labels**
      (`&lt;b&gt;Filed:&lt;/b&gt; 2026-09-03 &lt;b&gt;AccNo:&lt;/b&gt; ...`), not a
      clean key-value structure — extracted with a regex over the unescaped text.
    * This is Atom XML, not JSON — parsed with `xml.etree.ElementTree` and the Atom
      namespace, per this catalog's stdlib-only convention.
    * One atom request timed out on a retry attempt during investigation while a
      plain HTML request to the same base endpoint succeeded — SEC.gov can be slow
      under load; a generous timeout and a retry are worth having.

Etiquette: keyless, but SEC.gov **requires** a descriptive `User-Agent` identifying
your organisation and a contact (stated in their own fair-access policy, verified via
`sec.gov/os/accessing-edgar-data`) — a generic or missing User-Agent gets blocked. No
documented hard rate limit for this feed, but SEC publishes a general guideline of
staying under 10 requests/second across all their endpoints combined.

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
NS = {"a": "http://www.w3.org/2005/Atom"}
BASE = "https://www.sec.gov/cgi-bin/browse-edgar"
TITLE_RE = re.compile(r"^(.+?)\s*-\s*(.+?)\s*\((\d+)\)\s*\(([^)]+)\)\s*$")
SUMMARY_RE = re.compile(r"Filed:</b>\s*([\d-]+).*?AccNo:</b>\s*(\S+).*?Size:</b>\s*(.+?)\s*$",
                        re.S)


def _get_atom(form_type: str = "", count: int = 40):
    params = (f"?action=getcurrent&type={form_type}&company=&dateb=&owner=include"
             f"&count={count}&output=atom")
    req = urllib.request.Request(BASE + params, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return resp.read()


def fetch_recent(form_type: str = "", count: int = 40):
    """Latest filings, newest first. form_type filters (e.g. '8-K', '144'); empty
    means all types."""
    now = datetime.now(timezone.utc).isoformat()
    root = ET.fromstring(_get_atom(form_type, count))
    for entry in root.findall("a:entry", NS):
        title = (entry.findtext("a:title", default="", namespaces=NS) or "").strip()
        summary_html = entry.findtext("a:summary", default="", namespaces=NS) or ""
        m = TITLE_RE.match(title)
        form, party_name, cik, role = m.groups() if m else (None, title, None, None)
        s = SUMMARY_RE.search(summary_html)
        filed, acc_no, size = s.groups() if s else (None, None, None)
        link_el = entry.find("a:link[@rel='alternate']", NS)
        yield {
            "source": "sec_edgar",
            "fetched_at": now,
            # accession + role, since one accession can appear once per party role
            "id": f"{acc_no}:{role}:{cik}" if acc_no else entry.findtext("a:id", namespaces=NS),
            "form_type": form,
            "party_name": party_name,
            "cik": cik,
            "role": role,
            "accession_number": acc_no,
            "filed_date": filed,
            "size": size,
            "updated": entry.findtext("a:updated", default="", namespaces=NS),
            "url": link_el.get("href") if link_el is not None else None,
        }


if __name__ == "__main__":
    form_type = sys.argv[1] if len(sys.argv) > 1 else ""
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 40
    for rec in fetch_recent(form_type, count=count):
        print(json.dumps(rec, ensure_ascii=False))
