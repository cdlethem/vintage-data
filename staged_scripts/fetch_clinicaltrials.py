#!/usr/bin/env python3
"""ClinicalTrials.gov v2 — trial registrations and status transitions (keyless).

Sector: clinical medicine. Not covered anywhere else in this catalog.

The registry of 500,000+ studies run by the US National Library of Medicine.
Every record carries long free-text fields — brief summary, detailed
description, and eligibility criteria written as prose inclusion/exclusion
lists. That eligibility text is one of the most interesting under-used
corpora on the public internet: it is semi-structured natural language that
encodes how medicine defines a patient population, and it changes over time.

The time-series angle most people miss: **`overallStatus` transitions.**
A trial moves NOT_YET_RECRUITING -> RECRUITING -> ACTIVE_NOT_RECRUITING ->
COMPLETED, or it goes TERMINATED / WITHDRAWN / SUSPENDED. Re-poll a cohort
and you capture those transitions with timestamps, which gives you trial
survival curves and, for terminated studies, a `whyStopped` free-text field
explaining the failure. Very few people build that; it's a genuinely
valuable dataset.

Docs-verified 2026-09-02 (multiple independent sources incl. the NLM
technical bulletin): base https://clinicaltrials.gov/api/v2/studies, no key,
no auth, JSON. My sandboxed fetcher was robots-blocked, so smoke-test once
from your own machine.

Quirks:
  * **Page with `pageToken`, never a numeric offset** — the API cannot jump
    to page N. Carry `nextPageToken` from each response.
  * Responses are deeply module-nested (protocolSection.statusModule, etc).
    Use the `fields` parameter to request only what you want; it dramatically
    shrinks payloads.
  * No published hard rate limit for anonymous use, but ~50 req/min is the
    commonly cited safe ceiling. Pace yourself and cache.
  * Sort by last update to build a watermark:
    `sort=LastUpdatePostDate:desc`.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://clinicaltrials.gov/api/v2/studies"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"

# Keep payloads small: request only the modules we normalize below.
FIELDS = ",".join([
    "protocolSection.identificationModule",
    "protocolSection.statusModule",
    "protocolSection.sponsorCollaboratorsModule",
    "protocolSection.designModule",
    "protocolSection.conditionsModule",
    "protocolSection.armsInterventionsModule",
    "protocolSection.eligibilityModule",
])


def _get(params: dict):
    url = BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def fetch_recent(page_size: int = 100, pages: int = 1,
                 condition: str | None = None, sort: str = "LastUpdatePostDate:desc"):
    """Most-recently-updated studies first. Use as a change watermark."""
    token = None
    for _ in range(pages):
        params = {"pageSize": min(page_size, 1000), "sort": sort, "fields": FIELDS}
        if condition:
            params["query.cond"] = condition
        if token:
            params["pageToken"] = token
        data = _get(params)
        fetched_at = datetime.now(timezone.utc).isoformat()
        for s in data.get("studies", []):
            yield normalize(s, fetched_at)
        token = data.get("nextPageToken")
        if not token:
            break


def normalize(study: dict, fetched_at: str) -> dict:
    p = study.get("protocolSection") or {}
    ident = p.get("identificationModule") or {}
    status = p.get("statusModule") or {}
    design = p.get("designModule") or {}
    elig = p.get("eligibilityModule") or {}
    sponsor = (p.get("sponsorCollaboratorsModule") or {}).get("leadSponsor") or {}
    conditions = (p.get("conditionsModule") or {}).get("conditions") or []
    interventions = [i.get("name") for i in
                     (p.get("armsInterventionsModule") or {}).get("interventions", [])]
    enroll = design.get("enrollmentInfo") or {}
    return {
        "source": "clinicaltrials",
        "fetched_at": fetched_at,          # snapshot; status transitions over time
        "id": ident.get("nctId"),
        "title": ident.get("briefTitle"),
        "status": status.get("overallStatus"),      # the value that transitions
        "why_stopped": status.get("whyStopped"),    # free text, only if halted
        "start_date": (status.get("startDateStruct") or {}).get("date"),
        "completion_date": (status.get("primaryCompletionDateStruct") or {}).get("date"),
        "last_update": (status.get("lastUpdatePostDateStruct") or {}).get("date"),
        "study_type": design.get("studyType"),
        "phases": design.get("phases") or [],
        "enrollment": enroll.get("count"),
        "enrollment_type": enroll.get("type"),      # ACTUAL vs ESTIMATED
        "sponsor": sponsor.get("name"),
        "sponsor_class": sponsor.get("class"),      # INDUSTRY / NIH / OTHER
        "conditions": conditions,
        "interventions": interventions,
        "sex": elig.get("sex"),
        "min_age": elig.get("minimumAge"),
        "max_age": elig.get("maximumAge"),
        "healthy_volunteers": elig.get("healthyVolunteers"),
        # the corpus: prose inclusion/exclusion criteria
        "eligibility_criteria": elig.get("eligibilityCriteria"),
    }


if __name__ == "__main__":
    cond = sys.argv[1] if len(sys.argv) > 1 else None
    for rec in fetch_recent(page_size=20, condition=cond):
        print(json.dumps(rec, ensure_ascii=False))
