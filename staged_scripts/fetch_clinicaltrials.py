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
no auth, JSON.

'Current' per run = every study updated since the previous run, and nothing
else. There is no server-side date filter (v1's `filter=LastUpdatePostDate:`
is gone; v2 rejects it with a 400 — verified live 2026-09-04), so the
watermark is applied client-side against `sort=LastUpdatePostDate:desc`:
walk newest-first and stop at the first study older than the stored date.
`lastUpdatePostDate` is day-granular, so the boundary day needs the set of
NCT ids already emitted on it to avoid re-emitting them; a study whose status
changes later moves to a newer date and re-emits, which is the signal.
Measured live 2026-09-04: ~1,100 studies are updated per day, so a
steady-state run reads one or two pages. State is written only after every
record is printed, so an aborted run re-fetches rather than skipping
(at-least-once).

The first run has no watermark and emits the newest `--max-pages` worth of
updates as its baseline; the registry's full 600k-study back catalogue is a
deliberate non-goal here, since the point of this source is the transitions
going forward.

Quirks:
  * **Page with `pageToken`, never a numeric offset** — the API cannot jump
    to page N. Carry `nextPageToken` from each response.
  * `pageSize` is capped at 1000 and an over-cap value is silently clamped,
    not rejected — count rows, don't trust the parameter you sent.
  * Responses are deeply module-nested (protocolSection.statusModule, etc).
    Use the `fields` parameter to request only what you want; it dramatically
    shrinks payloads.
  * No published hard rate limit for anonymous use, but ~50 req/min is the
    commonly cited safe ceiling. Pace yourself and cache.
  * `contactsLocationsModule` exposes real investigator names, phone numbers
    and email addresses. It is deliberately not in FIELDS below and must stay
    out of any bulk pull.

Stdlib only.
"""
import argparse
import json
import os
import pathlib
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://clinicaltrials.gov/api/v2/studies"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
DEFAULT_DATA_ROOT = "~/dev/data/extract"
PAGE_PAUSE = 0.2  # seconds between pages; stays far under the ~50 req/min ceiling

# Keep payloads small: request only the modules we normalize below.
# contactsLocationsModule is deliberately excluded (personal contact data).
FIELDS = ",".join([
    "protocolSection.identificationModule",
    "protocolSection.statusModule",
    "protocolSection.sponsorCollaboratorsModule",
    "protocolSection.designModule",
    "protocolSection.conditionsModule",
    "protocolSection.armsInterventionsModule",
    "protocolSection.eligibilityModule",
])


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "clinicaltrials.json"


def load_state(path: pathlib.Path) -> dict:
    """Return {"last_update": "YYYY-MM-DD", "boundary": [nctId, ...]}."""
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict):
        raise ValueError(f"malformed watermark state in {path}")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def _get(params: dict):
    url = BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def fetch_recent(page_size: int = 1000, max_pages: int = 8,
                 condition: str | None = None, watermark: str = "",
                 boundary: frozenset = frozenset()):
    """Yield studies updated since ``watermark``, most-recently-updated first.

    ``watermark`` is a ``YYYY-MM-DD`` lastUpdatePostDate; the walk stops at the
    first older study because the sort guarantees the rest are older too.
    ``boundary`` is the set of NCT ids already emitted on exactly that date,
    which is what makes a day-granular watermark exact.
    """
    token = None
    for page in range(max_pages):
        params = {"pageSize": min(page_size, 1000), "sort": "LastUpdatePostDate:desc",
                  "fields": FIELDS}
        if condition:
            params["query.cond"] = condition
        if token:
            params["pageToken"] = token
        data = _get(params)
        fetched_at = datetime.now(timezone.utc).isoformat()
        studies = data.get("studies", [])
        if not studies:
            return
        for s in studies:
            record = normalize(s, fetched_at)
            updated = record.get("last_update") or ""
            if watermark and updated:
                if updated < watermark:
                    return  # sorted desc: everything from here on is older
                if updated == watermark and record["id"] in boundary:
                    continue  # already emitted on the boundary day
            yield record
        token = data.get("nextPageToken")
        if not token:
            return
        time.sleep(PAGE_PAUSE)


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("condition", nargs="?", help="optional condition filter")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-pages", type=int, default=8,
                        help="ceiling per run; 8 pages covers ~a week of updates")
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off pull)")
    args = parser.parse_args()

    path = state_path(args.state_file)
    state = {} if args.no_state else load_state(path)
    watermark = state.get("last_update", "")
    boundary = frozenset(state.get("boundary", []))

    newest = watermark
    emitted_on_newest: set[str] = set(boundary) if watermark else set()
    for record in fetch_recent(args.page_size, args.max_pages, args.condition,
                               watermark, boundary):
        updated = record.get("last_update") or ""
        if updated > newest:
            newest, emitted_on_newest = updated, set()
        if updated and updated == newest:
            emitted_on_newest.add(record["id"])
        print(json.dumps(record, ensure_ascii=False))
    # Only after every record is written: an aborted run re-fetches instead of
    # skipping. An empty pull leaves the watermark untouched (nothing changed).
    if not args.no_state and newest:
        save_state(path, {"last_update": newest,
                          "boundary": sorted(emitted_on_newest)})


if __name__ == "__main__":
    main()
