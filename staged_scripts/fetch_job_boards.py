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
    """watchlist: iterable of (provider, token, company). One company's
    failure (timeout, malformed response) is logged and skipped rather than
    aborting a run covering dozens of companies."""
    for provider, token, company in watchlist:
        try:
            yield from fetch_board(provider, token, company)
        except Exception as exc:  # noqa: BLE001 - keep the watchlist run going
            print(f"job_boards: skipping {provider}/{token!r}: {exc!r}", file=sys.stderr)


# 74 real, live-verified company boards (2026-09-04) -- each tested live and
# confirmed to return real open postings (SpaceX 2,309; Databricks 864; OpenAI
# (Ashby) 767; Stripe 612; Datadog 444; MongoDB 406; ...). Candidates that
# 404'd or errored during verification were dropped, not guessed into place.
WATCHLIST = [
    ("greenhouse", "calendly", "Calendly"),
    ("greenhouse", "netlify", "Netlify"),
    ("greenhouse", "squarespace", "Squarespace"),
    ("greenhouse", "webflow", "Webflow"),
    ("greenhouse", "vercel", "Vercel"),
    ("greenhouse", "discord", "Discord"),
    ("greenhouse", "chime", "Chime"),
    ("greenhouse", "dropbox", "Dropbox"),
    ("greenhouse", "instacart", "Instacart"),
    ("greenhouse", "scaleai", "Scale AI"),
    ("greenhouse", "twilio", "Twilio"),
    ("greenhouse", "asana", "Asana"),
    ("greenhouse", "pinterest", "Pinterest"),
    ("greenhouse", "samsara", "Samsara"),
    ("greenhouse", "figma", "Figma"),
    ("greenhouse", "brex", "Brex"),
    ("greenhouse", "affirm", "Affirm"),
    ("greenhouse", "airbnb", "Airbnb"),
    ("greenhouse", "robinhood", "Robinhood"),
    ("greenhouse", "coinbase", "Coinbase"),
    ("greenhouse", "lyft", "Lyft"),
    ("greenhouse", "stripe", "Stripe"),
    ("greenhouse", "roblox", "Roblox"),
    ("greenhouse", "elastic", "Elastic"),
    ("greenhouse", "mongodb", "MongoDB"),
    ("greenhouse", "reddit", "Reddit"),
    ("greenhouse", "cloudflare", "Cloudflare"),
    ("greenhouse", "gitlab", "GitLab"),
    ("greenhouse", "twitch", "Twitch"),
    ("greenhouse", "databricks", "Databricks"),
    ("greenhouse", "medium", "Medium"),
    ("greenhouse", "carta", "Carta"),
    ("greenhouse", "faire", "Faire"),
    ("greenhouse", "nextdoor", "Nextdoor"),
    ("greenhouse", "flexport", "Flexport"),
    ("greenhouse", "okta", "Okta"),
    ("greenhouse", "toast", "Toast"),
    ("greenhouse", "gusto", "Gusto"),
    ("greenhouse", "greenhouse", "Greenhouse"),
    ("greenhouse", "checkr", "Checkr"),
    ("greenhouse", "betterment", "Betterment"),
    ("greenhouse", "branch", "Branch"),
    ("greenhouse", "lattice", "Lattice"),
    ("greenhouse", "amplitude", "Amplitude"),
    ("greenhouse", "salesloft", "Salesloft"),
    ("greenhouse", "pagerduty", "PagerDuty"),
    ("greenhouse", "newrelic", "New Relic"),
    ("greenhouse", "airtable", "Airtable"),
    ("greenhouse", "mixpanel", "Mixpanel"),
    ("greenhouse", "verkada", "Verkada"),
    ("greenhouse", "intercom", "Intercom"),
    ("greenhouse", "sofi", "SoFi"),
    ("greenhouse", "earnin", "EarnIn"),
    ("greenhouse", "datadog", "Datadog"),
    ("greenhouse", "gemini", "Gemini"),
    ("greenhouse", "astranis", "Astranis"),
    ("greenhouse", "nuro", "Nuro"),
    ("greenhouse", "glossier", "Glossier"),
    ("greenhouse", "spacex", "SpaceX"),
    ("greenhouse", "chargepoint", "ChargePoint"),
    ("greenhouse", "lucidmotors", "Lucid Motors"),
    ("greenhouse", "waymo", "Waymo"),
    ("ashby", "linear", "Linear"),
    ("ashby", "posthog", "PostHog"),
    ("ashby", "warp", "Warp"),
    ("ashby", "render", "Render"),
    ("ashby", "replit", "Replit"),
    ("ashby", "supabase", "Supabase"),
    ("ashby", "notion", "Notion"),
    ("ashby", "ramp", "Ramp"),
    ("ashby", "cursor", "Cursor"),
    ("lever", "spotify", "Spotify"),
    ("ashby", "openai", "OpenAI"),
    ("lever", "palantir", "Palantir"),
]

if __name__ == "__main__":
    if len(sys.argv) > 2:
        rows = fetch_board(sys.argv[1], sys.argv[2])
    else:
        rows = fetch_watchlist(WATCHLIST)
    for rec in rows:
        print(json.dumps(rec, ensure_ascii=False))
