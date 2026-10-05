#!/usr/bin/env python3
"""arXiv — new preprint submissions per category (keyless, Atom XML).

'Current' per run = papers with submittedDate inside a trailing window for a
category (e.g. cs.AI, astro-ph.HE). Titles + full abstracts make this a
first-class LLM-analysis corpus (topic drift, embedding trajectories,
"what is physics worried about this week").

Docs-verified 2026-09-02 against the official user manual
(https://info.arxiv.org/help/api/user-manual.html). Endpoint
https://export.arxiv.org/api/query is explicitly public; hard limit 3 req/sec,
recommended <= 1 req / 3 sec with pagination sleeps.

Quirks:
  * Response is Atom 1.0 XML, not JSON — parsed here with stdlib ElementTree.
  * New submissions are announced in daily batches (Sun-Thu ~20:00 ET), so a
    smart cadence is ~1-4 polls/day per category, not minutes.
  * date filter format: submittedDate:[YYYYMMDDHHMM TO YYYYMMDDHHMM] (GMT).
  * HTTP 429 (per-IP rate limiting) is retried up to six total attempts,
    sleeping the server's Retry-After when present, else
    15s/30s/60s/120s/180s, capped at 405s of cumulative sleep (inside the
    ten-minute task timeout). arXiv throttles an IP for minutes at a time
    around the daily new-submission announcement batch, so a short budget
    fails exactly then.
  * Transport failures (read timeouts, connection errors) keep the
    three-attempt budget with 3s and 6s backoffs; their sleeps draw on the
    same cumulative budget, so a mixed run never exceeds the 405-second cap.
  * HTTP 406 is not retried; diagnostics retain only allowlisted content type
    and a bounded response-body shape, never raw headers or response text.

Stdlib only.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

BASE = "https://export.arxiv.org/api/query"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
NS = {"a": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}

MAX_ATTEMPTS = 3
MIN_RETRY_DELAY_SECONDS = 3
MAX_RETRY_DELAY_SECONDS = 30
MAX_CUMULATIVE_SLEEP_SECONDS = 60

# arXiv throttles per-IP for minutes at a time (observed 2026-10-05: 20+
# minutes of 429s with no Retry-After header, in the hours after the daily
# new-submission announcement). 429 therefore gets its own longer budget,
# bounded so that full exhaustion plus six 60-second read timeouts stays
# inside the ten-minute task timeout for the normal fast-429 case.
RATE_LIMIT_MAX_ATTEMPTS = 6
RATE_LIMIT_FALLBACK_DELAYS_SECONDS = (15, 30, 60, 120, 180)
RATE_LIMIT_MAX_CUMULATIVE_SLEEP_SECONDS = 405


def parse_retry_after(headers, now: datetime):
    """Return the server-requested delay in seconds, or None if unusable."""
    value = headers.get("Retry-After") if headers is not None else None
    if not isinstance(value, str):
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        retry_at = parsedate_to_datetime(value)
    except (IndexError, TypeError, ValueError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return (retry_at - now).total_seconds()


def rate_limit_delay(headers, now: datetime, retries: int, remaining: float) -> float:
    """Bounded 429 delay: Retry-After when usable, else the backoff schedule."""
    requested = parse_retry_after(headers, now)
    if requested is None:
        delay = RATE_LIMIT_FALLBACK_DELAYS_SECONDS[min(
            retries, len(RATE_LIMIT_FALLBACK_DELAYS_SECONDS) - 1)]
    else:
        delay = max(MIN_RETRY_DELAY_SECONDS, requested)
    return max(0.0, min(delay, remaining))


def _close_http_error(error):
    try:
        error.close()
    except OSError:
        pass

def _rejection_summary(error):
    """Retain response shape without logging untrusted headers or body text."""
    content_type = error.headers.get("Content-Type") if error.headers else None
    media_type = content_type.split(";", 1)[0].strip().lower() if content_type else None
    if media_type not in ("text/html", "text/plain", "application/xml", "application/atom+xml"):
        media_type = "other" if media_type else "missing"
    try:
        prefix = error.read(513)
    except (OSError, ValueError):
        return f"content_type={media_type}, body=unavailable"
    body = prefix[:512].lstrip().lower()
    kind = ("empty" if not prefix else "html" if body.startswith((b"<!doctype html", b"<html"))
            else "xml" if body.startswith((b"<?xml", b"<feed")) else "other")
    return (f"content_type={media_type}, body={kind}, "
            f"body_prefix_bytes={min(len(prefix), 512)}, body_truncated={len(prefix) > 512}")




def fetch_new_papers(category: str = "cs.AI", hours_back: int = 24,
                     max_results: int = 200, sleep=None, clock=None):
    sleep = time.sleep if sleep is None else sleep
    clock = (lambda: datetime.now(timezone.utc)) if clock is None else clock
    now = clock()
    lo = (now - timedelta(hours=hours_back)).strftime("%Y%m%d%H%M")
    hi = now.strftime("%Y%m%d%H%M")
    search = f"cat:{category} AND submittedDate:[{lo} TO {hi}]"
    params = urllib.parse.urlencode({
        "search_query": search,
        "sortBy": "submittedDate", "sortOrder": "descending",
        "start": 0, "max_results": max_results,
    })
    req = urllib.request.Request(f"{BASE}?{params}",
                                 headers={"User-Agent": USER_AGENT})
    slept = 0
    rate_limit_retries = 0
    for attempt in range(1, RATE_LIMIT_MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                root = ET.fromstring(resp.read())
            break
        except urllib.error.HTTPError as error:
            status, headers = error.code, error.headers
            if status == 406:
                error.arxiv_response_summary = _rejection_summary(error)
            _close_http_error(error)
            if status != 429:
                raise
            rate_limit_retries += 1
            if rate_limit_retries >= RATE_LIMIT_MAX_ATTEMPTS:
                raise
            delay = rate_limit_delay(headers, clock(), rate_limit_retries - 1,
                                     RATE_LIMIT_MAX_CUMULATIVE_SLEEP_SECONDS - slept)
            sleep(delay)
            slept += delay
        except (urllib.error.URLError, TimeoutError, OSError):
            if attempt >= MAX_ATTEMPTS:
                raise
            delay = max(0.0, min(MIN_RETRY_DELAY_SECONDS * (2 ** (attempt - 1)),
                                 MAX_RETRY_DELAY_SECONDS,
                                 MAX_CUMULATIVE_SLEEP_SECONDS - slept))
            sleep(delay)
            slept += delay
    fetched_at = now.isoformat()
    for entry in root.findall("a:entry", NS):
        yield parse_entry(entry, fetched_at)


def parse_entry(entry, fetched_at: str) -> dict:
    def text(tag):
        el = entry.find(tag, NS)
        return el.text.strip() if el is not None and el.text else None

    record_id = text("a:id")
    if not record_id:
        raise ValueError("arXiv entry missing id")
    cats = [c.get("term") for c in entry.findall("a:category", NS)]
    primary = entry.find("arxiv:primary_category", NS)
    return {
        "source": "arxiv",
        "fetched_at": fetched_at,
        "id": record_id,                       # http://arxiv.org/abs/XXXX.YYYYYvN
        "published": text("a:published"),
        "updated": text("a:updated"),
        "title": " ".join((text("a:title") or "").split()),
        "abstract": " ".join((text("a:summary") or "").split()),
        "authors": [a.findtext("a:name", None, NS)
                    for a in entry.findall("a:author", NS)],
        "primary_category": primary.get("term") if primary is not None else None,
        "categories": cats,
    }


def _terminal_failure(message):
    print(f"arXiv extraction failed: {message}", file=sys.stderr)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch arXiv new submissions as JSON lines.")
    parser.add_argument("category", nargs="?", default="cs.AI",
                        help="arXiv category (default: cs.AI)")
    category = parser.parse_args(argv).category
    try:
        for record in fetch_new_papers(category=category):
            print(json.dumps(record, ensure_ascii=False))
    except urllib.error.HTTPError as error:
        attempts = RATE_LIMIT_MAX_ATTEMPTS if error.code == 429 else 1
        summary = f"; {error.arxiv_response_summary}" if error.code == 406 else ""
        _terminal_failure(f"HTTP {error.code} after {attempts} attempt(s){summary}")
        return 1
    except (urllib.error.URLError, TimeoutError, OSError):
        _terminal_failure(f"transport failure after {MAX_ATTEMPTS} attempt(s)")
        return 1
    except ET.ParseError:
        _terminal_failure("XML parsing failure")
        return 1
    except ValueError:
        _terminal_failure("record validation failure")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
