#!/usr/bin/env python3
"""Certificate Transparency — every TLS certificate issued, via crt.sh's search API.

Lead #14: every certificate issued on the internet, live — used for phishing-domain
detection, and a genuinely strange "new domain names appearing" text corpus.

**Verified live 2026-09-03, and the direct route doesn't work from this sandbox.**
Every raw RFC6962 CT log endpoint tried failed: `ct.googleapis.com/.../get-sth`
**404** on multiple log names/paths (Google's older logs have been retired/renamed),
`logs-01.hardenedspace.com` and `magma.ct.letsencrypt.org` **DNS failures**,
`wyvern.ct.digicert.com` and `elephant2026h2.ct.sectigo.com` **404/empty**. One log
(`ct.cloudflare.com/logs/nimbus2026/v2/get-sth`) returned **200** but with
`content-type: text/html` — it was Cloudflare Radar's dashboard *page*, not a real
STH response; verified by checking the content-type, not just the status code.

**What did work**: crt.sh's own keyless JSON search API over its aggregated CT log
database — `crt.sh/?q=<domain-or-wildcard>&output=json`. `q=google.com` returned
**200, 499,539 bytes, 1,152 certificates**. `q=%25.wikipedia.org` (wildcard, properly
URL-encoded) returned **200, 79,936 bytes, 236 certificates**, most recent issued
2026-08-05 by Let's Encrypt.

Endpoint: `https://crt.sh/?q=<query>&output=json`
    q=example.com          certs with that exact common/alt name
    q=%25.example.com       (URL-encoded `%.example.com`) — wildcard, any subdomain

Quirks that will cost someone an afternoon:
    * **This is a search API, not a firehose** — there is no "give me every cert
      issued in the last minute" endpoint here. Watch specific domains/patterns you
      care about (a wildcard query on a TLD or org name), not the whole internet.
      True log-tailing needs the raw RFC6962 `get-entries` API against a specific
      log's known tree range, which is a materially bigger undertaking than this
      script attempts.
    * **The `%` SQL-wildcard character must be sent as `%25` in the URL, and a
      literal double `%%` breaks the query outright** — confirmed live:
      `q=%%.google.com` (unencoded) returned 200 but with an error body "Unsupported
      use of '%'"; the properly-encoded `q=%25.wikipedia.org` worked. Always
      URL-encode the query parameter.
    * `output=json` with **no `q=` fails** ("Unsupported output type: json") even
      though the status is 200 — check the body, not just the status, exactly like
      the Cloudflare Radar false-positive above.
    * `name_value` can hold **multiple SANs joined by literal newlines in one JSON
      string** — verified live (`"*.m.wikipedia.org\n*.wikipedia.org\nwikipedia.org"`)
      — split on `\n` if you want individual hostnames.
    * `result_count` in each record is crt.sh's own total-matches count for the
      query, not a per-certificate field — don't mistake it for something about that
      one certificate.

Etiquette: keyless, but crt.sh is a single-operator service (Sectigo-run) that
explicitly asks for gentle use — no documented hard limit, but heavy/scripted querying
gets IPs blocked. Query specific domains you actually care about, not broad wildcards
in a loop, and self-impose real pacing (a few requests per minute at most).

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
BASE = "https://crt.sh/"


def _get(query):
    url = f"{BASE}?{urllib.parse.urlencode({'q': query, 'output': 'json'})}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read()
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return []  # an error page (e.g. malformed query) is HTML, not JSON -- treat as empty


def fetch_certs(domain: str, wildcard: bool = False):
    """Certificates for a domain, or (wildcard=True) any subdomain of it."""
    now = datetime.now(timezone.utc).isoformat()
    query = f"%.{domain}" if wildcard else domain
    for c in _get(query):
        yield {
            "source": "certificate_transparency",
            "fetched_at": now,
            "id": c.get("id"),
            "common_name": c.get("common_name"),
            "names": (c.get("name_value") or "").split("\n"),
            "issuer": c.get("issuer_name"),
            "not_before": c.get("not_before"),
            "not_after": c.get("not_after"),
            "serial_number": c.get("serial_number"),
        }


if __name__ == "__main__":
    domain = sys.argv[1] if len(sys.argv) > 1 else "wikipedia.org"
    wildcard = len(sys.argv) > 2 and sys.argv[2] == "wildcard"
    for rec in fetch_certs(domain, wildcard=wildcard):
        print(json.dumps(rec, ensure_ascii=False))
