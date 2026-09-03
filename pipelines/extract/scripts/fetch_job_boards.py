#!/usr/bin/env python3
"""Company job boards (Greenhouse / Lever / Ashby) — the hiring firehose.

Sector: labor market. Not covered anywhere else in this catalog.

Most companies don't run their own careers page — they embed a hosted ATS,
and **those ATSs expose public, keyless JSON board endpoints** because the
company's own site consumes them client-side. That means you can watch hiring
happen across hundreds of named organizations without scraping a single line
of HTML, and without touching a job aggregator's terms of service.

Why it's a strong time series:
  * A posting's **appearance and disappearance** are both signals. Track a
    company's open-roles count daily and you get a hiring/freeze/layoff proxy
    that leads public announcements by weeks.
  * **Time-to-fill**: the gap between a posting appearing and vanishing.
  * Job descriptions are long free text with a strong internal structure
    (responsibilities, requirements, salary bands where legally mandated) —
    good LLM territory for skill-demand extraction over time.
  * Location fields let you watch remote-vs-onsite policy drift per company.

Endpoint patterns (all public, no key):
  Greenhouse: https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true
  Lever:      https://api.lever.co/v0/postings/{token}?mode=json
  Ashby:      https://api.ashbyhq.com/posting-api/job-board/{token}

`{token}` is the company's board slug, visible in its careers-page URL
(e.g. boards.greenhouse.io/<token>). Build your watchlist by hand — that's
the honest, low-volume, ethical way to do this.

Verification status 2026-09-02: KNOWN endpoint patterns, widely used and
long-stable, but **not fetched during this session** (my sandbox restricts
outbound hosts). Smoke-test each provider once and confirm the field names
before relying on them — Greenhouse in particular returns HTML in `content`.

Ethics, and this one genuinely matters here:
  * These are public endpoints a company *intends* to serve, but job postings
    can contain named recruiter contacts. Don't build a people-tracking
    dataset; keep it at the organization level.
  * Poll daily at most. Hiring does not change by the minute, and a polite
    cadence keeps you far away from anyone's abuse threshold.
  * Keep your watchlist small and explicit rather than enumerating every
    board token you can guess.

Stdlib only.
"""
import html
import json
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"

PROVIDERS = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true",
    "lever": "https://api.lever.co/v0/postings/{token}?mode=json",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{token}",
}


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:      # unknown board token
            return None
        raise


def _strip_html(s: str | None) -> str | None:
    if not s:
        return None
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html.unescape(s))).strip()


def fetch_board(provider: str, token: str, company: str | None = None):
    """Yield normalized open postings for one company board."""
    data = _get(PROVIDERS[provider].format(token=token))
    if not data:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    company = company or token
    if provider == "greenhouse":
        for j in data.get("jobs", []):
            yield {
                "source": "jobs_greenhouse", "fetched_at": fetched_at,
                "company": company, "id": f"greenhouse:{token}:{j.get('id')}",
                "title": j.get("title"),
                "location": (j.get("location") or {}).get("name"),
                "posted": j.get("updated_at") or j.get("first_published"),
                "department": ", ".join(d.get("name") for d in j.get("departments", [])),
                "url": j.get("absolute_url"),
                "description": _strip_html(j.get("content")),  # arrives as HTML
            }
    elif provider == "lever":
        for j in data:
            cat = j.get("categories") or {}
            created = j.get("createdAt")
            yield {
                "source": "jobs_lever", "fetched_at": fetched_at,
                "company": company, "id": f"lever:{token}:{j.get('id')}",
                "title": j.get("text"),
                "location": cat.get("location"),
                "posted": datetime.fromtimestamp(created / 1000, tz=timezone.utc
                                                 ).isoformat() if created else None,
                "department": cat.get("team") or cat.get("department"),
                "url": j.get("hostedUrl"),
                "description": _strip_html(j.get("descriptionPlain")
                                           or j.get("description")),
            }
    elif provider == "ashby":
        for j in data.get("jobs", []):
            yield {
                "source": "jobs_ashby", "fetched_at": fetched_at,
                "company": company, "id": f"ashby:{token}:{j.get('id')}",
                "title": j.get("title"),
                "location": j.get("location"),
                "posted": j.get("publishedAt"),
                "department": j.get("department") or j.get("team"),
                "url": j.get("jobUrl"),
                "description": _strip_html(j.get("descriptionPlain")
                                           or j.get("descriptionHtml")),
            }


def fetch_watchlist(watchlist):
    """watchlist: iterable of (provider, token, company)."""
    for provider, token, company in watchlist:
        yield from fetch_board(provider, token, company)


WATCHLIST = [
    # ("greenhouse", "examplecorp", "Example Corp"),
    # ("lever", "examplecorp", "Example Corp"),
]

if __name__ == "__main__":
    if len(sys.argv) > 2:
        rows = fetch_board(sys.argv[1], sys.argv[2])
    else:
        rows = fetch_watchlist(WATCHLIST)
    for rec in rows:
        print(json.dumps(rec, ensure_ascii=False))
