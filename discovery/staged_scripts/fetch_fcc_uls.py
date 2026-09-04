#!/usr/bin/env python3
"""FCC ULS — US spectrum licences granted, modified and cancelled, daily.

Gap-list item: **spectrum licensing.**

**What it is:** the Universal Licensing System is the FCC's register of every
wireless licence in the United States — amateur radio, cellular, microwave,
maritime, aviation, paging, land mobile. Every business day the FCC publishes
that day's *transactions* as pipe-delimited files in a zip. Who was newly
allowed to transmit, on what service, where, and whose licence just died.

**Verified live 2026-09-03:** `data.fcc.gov/download/pub/uls/daily/l_am_tue.zip`
downloaded (102 KB) with `File Creation Date: Wed Sep 2 08:00:10 EDT 2026` and
contained one business day of amateur-radio licence activity:

    HD.dat   819 rows   licence headers (status, grant/expiry dates)
    EN.dat   819 rows   entity: name, address, FRN
    AM.dat   819 rows   amateur specifics: operator class, previous call sign
    HS.dat 3,234 rows   **licence status history — the actual event log**
    CO.dat    91 rows   free-text comments
    SC.dat    27 rows   special conditions
    LA.dat     3 rows   licence attachments / actions

**Why it's a good series.** This is an institutional decision log with a
one-day cadence and a stable schema going back decades:

  * **HS.dat is a state-transition table**, not a snapshot — 3,234 transitions
    against 819 licences in a single day, each a `(call sign, date, code)`
    triple like `LIMOD` (modified), `LIISS` (issued), `LICAN` (cancelled).
    Genuine lifecycle data, with the failure states retained.
  * **Amateur radio gives you a human progression curve.** `AM.dat` carries
    `operator_class` (Novice → Technician → General → Advanced → Extra) and
    `previous_callsign`, so upgrades are directly observable — a licensing
    ladder for hundreds of thousands of people, with dates.
  * **Vanity call signs are a preference-revelation dataset.** People choose
    short, memorable call signs and the FCC records the change; which strings
    are contested tells you something odd and real about what people want.
  * **Service-code volume is an industry indicator.** Counting new licences
    per radio service over months tracks where spectrum activity is actually
    going, months before it appears in trade press.

Quirks that will cost you an afternoon:

  * **THE BIG ONE: files are named by weekday and overwritten weekly.**
    There is no `l_am_2026-09-01.zip`. There is `l_am_mon.zip`, and it is
    replaced every Monday. Verified live — on Wednesday 2026-09-02 the seven
    amateur files carried last-modified dates of Tue 01 Sep (mon), **Wed 02
    Sep (tue)**, **Thu 27 Aug (wed)**, Fri 28 Aug (thu), Sat 29 Aug (fri),
    Sun 30 Aug (sat), Mon 31 Aug (sun). So `wed.zip` was still *last*
    Wednesday's file, hours from being overwritten. **You get a rolling
    seven-day window and no more.** Miss a week and that week is gone —
    fall back to the weekly full dumps under `.../uls/complete/`. This is the
    single most important operational fact about this source.
  * **A day's file is published the morning after the day it covers.**
    `l_am_tue.zip` was written Wednesday 12:00 GMT and holds Tuesday's
    transactions. Your watermark is the day *before* the file date.
  * **Sundays are nearly empty** — `l_am_sun.zip` was 212 bytes. An empty file
    is normal, not a failed download.
  * **Sending a `User-Agent` without an `Accept-Encoding` gets you a 403.**
    This one is genuinely maddening and cost an afternoon to find. The FCC
    edge runs a bot filter that fingerprints the *header set*, not the UA
    string. Verified against the live host: `User-Agent` alone → **403**;
    `User-Agent` + `Accept: */*` → **403**; `Accept-Encoding: identity` →
    **403**; `Accept-Encoding: gzip` → **403**; and
    **`Accept-Encoding: gzip, deflate` → 200**. Bare `urlopen` with no headers
    at all also works, which is why this only bites you *after* you politely
    add contact info as every other source in this catalog asks you to. Send
    both headers. (The server then serves the zip uncompressed anyway.)
  * **A bad filename returns 302 or 403, never a clean 404.** A typo'd service
    code yields an HTML redirect that `zipfile` then rejects with
    `BadZipFile`. Check the magic bytes, which `_download()` does.
  * **The .dat files have no header row and no field names** — everything is
    positional, pipe-delimited. Layouts are below and were verified against
    the live file (HD has 59 fields, EN 30, AM 18).
  * **Dates are `MM/DD/YYYY`**, not ISO, and empty strings are the null.
  * **The files are latin-1, not UTF-8.** Decoding as UTF-8 raises on real
    rows. There is no BOM and no declared encoding.
  * Records are keyed by `unique_system_identifier`, *not* call sign — a call
    sign can be reassigned to a different person after expiry, so keying on it
    silently merges two people's licence histories.
  * Service codes are two letters (`HA`/`HV` = amateur, `CL` = cellular,
    `WS`/`WU` = microwave, `MC`= maritime coast) and are not in the file — you
    need the FCC's service-code table to read them.

**PRIVACY — read this before you store anything.** `EN.dat` contains the
**full legal name and residential street address** of individual licensees.
That is by design: US amateur licensees' addresses are public record by
statute, and the FCC publishes them. It is still, concretely, a bulk file of
several hundred thousand people's home addresses, and "it's public" is not the
same as "build a searchable index of it."

Following the same stance as the behavioural-telemetry section of this
catalog, this script **redacts personal names and street addresses by
default** (`REDACT_PII = True`) and keeps only coarse geography — city, state,
first three ZIP digits — which is what almost every legitimate analysis
actually needs. Organisational licensees (entity_type not individual) are not
redacted, because a company's licensed address is a business fact. Turning
redaction off should require a reason you could defend out loud, and if you do
turn it off, do not republish the result.

**Etiquette.** These are large files on a government host. They regenerate
once a day, so **fetch each weekday file once a day at most** — the file will
not change again until next week. Cache by the file's `Last-Modified`. Use the
weekly `complete/` dumps for backfill rather than hammering the daily
endpoints. Data is US Government work: public domain, no key, no attribution
required.

Endpoints:
  https://data.fcc.gov/download/pub/uls/daily/                 index
  https://data.fcc.gov/download/pub/uls/daily/l_<svc>_<dow>.zip   licences
  https://data.fcc.gov/download/pub/uls/daily/a_<svc>_<dow>.zip   applications
  https://data.fcc.gov/download/pub/uls/complete/                 weekly full dumps
  <svc>: am ac cg cl fc gm lb lc lp mi mk mw pg rb sh ...   <dow>: mon..sun

Stdlib only.
"""
import io
import json
import os
import sys
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timezone

BASE = "https://data.fcc.gov/download/pub/uls/daily"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

# Names and home addresses of private individuals. See the privacy note.
REDACT_PII = True

DOW = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

# Field positions verified against the live 2026-09-02 amateur file.
# (name, index) — everything is positional; there is no header row.
LAYOUTS = {
    "HD": {  # licence header, 59 fields
        "usi": 1, "file_number": 2, "call_sign": 4, "license_status": 5,
        "radio_service_code": 6, "grant_date": 7, "expired_date": 8,
        "cancellation_date": 9, "first_name": 30, "mi": 31, "last_name": 32,
        "effective_date": 42, "last_action_date": 43,
    },
    "EN": {  # entity, 30 fields
        "usi": 1, "call_sign": 4, "entity_type": 5, "licensee_id": 6,
        "entity_name": 7, "first_name": 8, "mi": 9, "last_name": 10,
        "suffix": 11, "street_address": 15, "city": 16, "state": 17,
        "zip_code": 18, "po_box": 19, "attention_line": 20, "frn": 22,
        "applicant_type_code": 23,
    },
    "AM": {  # amateur-specific, 18 fields
        "usi": 1, "call_sign": 4, "operator_class": 5, "group_code": 6,
        "region_code": 7, "trustee_call_sign": 8,
        "systematic_call_sign_change": 12, "vanity_call_sign_change": 13,
        "previous_call_sign": 15, "previous_operator_class": 16,
    },
    "HS": {  # licence status HISTORY — the event log
        "usi": 1, "call_sign": 3, "log_date": 4, "code": 5,
    },
    "CO": {  # comments
        "usi": 1, "call_sign": 3, "comment_date": 4, "description": 5,
    },
    "LA": {  # licence actions
        "usi": 1, "call_sign": 2, "action_type": 3, "description": 4,
        "action_date": 5, "status": 7,
    },
}

OPERATOR_CLASS = {"N": "Novice", "T": "Technician", "G": "General",
                  "A": "Advanced", "E": "Amateur Extra", "P": "Technician Plus"}
LICENSE_STATUS = {"A": "Active", "C": "Cancelled", "E": "Expired",
                  "L": "Pending Legal Status", "P": "Parent Station",
                  "T": "Terminated", "X": "Term Pending"}
# HS.dat transition codes, with observed counts from the verified file.
HISTORY_CODES = {
    "LIREN": "licence renewed",            # 771
    "LIAUA": "auto-update applied",        # 748
    "LIISS": "licence issued",             # 619
    "SYSGRT": "systematic call sign granted",   # 399
    "VANGRT": "vanity call sign granted",       # 207
    "LIMOD": "licence modified",           # 200
    "LICAN": "licence cancelled",          # 88
    "LIEXP": "licence expired",            # 66
    "LITIN": "licence terminated",         # 58
    "LTSFRN": "FRN change",                # 27
    "ESCFRN": "FRN escrow change",         # 24
    "COR": "correction",                   # 13
}
# Individual (vs organisation) entity types. These get redacted.
INDIVIDUAL_TYPES = {"I", "L"}       # L = licensee (individuals in AM files)


def _download(service: str = "am", dow: str | None = None,
              kind: str = "l") -> zipfile.ZipFile:
    """Fetch one weekday file. Remember it is overwritten weekly."""
    dow = dow or DOW[date.today().weekday()]
    url = f"{BASE}/{kind}_{service}_{dow}.zip"
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        # REQUIRED, and fussy — see the Accept-Encoding note in the docstring.
        # Exactly this value passes; 'identity' and 'gzip' alone both 403.
        "Accept-Encoding": "gzip, deflate",
    })
    with urllib.request.urlopen(req, timeout=300) as resp:
        blob = resp.read()
        modified = resp.headers.get("Last-Modified")
        if resp.headers.get("Content-Encoding") == "gzip":
            import gzip as _gzip
            blob = _gzip.decompress(blob)
    # A bad service/dow gives a 302 to an HTML page, never a clean 404.
    if blob[:2] != b"PK":
        raise RuntimeError(
            f"{url} did not return a zip (got {blob[:60]!r}); "
            "check the service code and weekday")
    z = zipfile.ZipFile(io.BytesIO(blob))
    z._uls_url, z._uls_modified = url, modified
    return z


def parse_uls_date(s):
    """'09/01/2026' -> '2026-09-01'. Empty string is the null."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def _rows(z: zipfile.ZipFile, member: str):
    """Files are latin-1 with no header row. UTF-8 raises on real data."""
    try:
        raw = z.read(member)
    except KeyError:
        return
    for line in raw.decode("latin-1").splitlines():
        if not line:
            continue
        parts = line.split("|")
        if parts[0] != member[:2]:
            continue
        yield parts


def _f(parts, idx):
    """Positional field; missing/blank -> None."""
    if idx >= len(parts):
        return None
    return parts[idx].strip() or None


def _extract(parts, table):
    return {k: _f(parts, i) for k, i in LAYOUTS[table].items()}


def _redact(rec: dict, entity_type: str | None):
    """Drop name and street address for private individuals; keep coarse geo."""
    if not REDACT_PII or (entity_type and entity_type not in INDIVIDUAL_TYPES):
        rec["pii_redacted"] = False
        return rec
    for k in ("first_name", "mi", "last_name", "suffix", "entity_name",
              "street_address", "po_box", "attention_line"):
        if k in rec:
            rec[k] = None
    if rec.get("zip_code"):
        rec["zip_code"] = rec["zip_code"][:3]       # ZIP3, not ZIP5
    rec["pii_redacted"] = True
    return rec


def fetch_licenses(service: str = "am", dow: str | None = None):
    """One weekday's licence grants/modifications, joined across HD/EN/AM."""
    z = _download(service, dow)
    now = datetime.now(timezone.utc).isoformat()
    file_date = z._uls_modified

    entities, amateur = {}, {}
    for p in _rows(z, "EN.dat"):
        entities[_f(p, 1)] = _extract(p, "EN")
    for p in _rows(z, "AM.dat"):
        amateur[_f(p, 1)] = _extract(p, "AM")

    for p in _rows(z, "HD.dat"):
        hd = _extract(p, "HD")
        usi = hd["usi"]
        en = entities.get(usi, {})
        am = amateur.get(usi, {})
        rec = {
            "source": "fcc_uls",
            "fetched_at": now,
            # key on the system id: call signs get reassigned after expiry
            "id": f"uls:{usi}",
            "usi": usi,
            "call_sign": hd["call_sign"],
            "service": service,
            "radio_service_code": hd["radio_service_code"],
            "license_status": hd["license_status"],
            "license_status_text": LICENSE_STATUS.get(hd["license_status"]),
            "grant_date": parse_uls_date(hd["grant_date"]),
            "expired_date": parse_uls_date(hd["expired_date"]),
            "cancellation_date": parse_uls_date(hd["cancellation_date"]),
            "effective_date": parse_uls_date(hd["effective_date"]),
            "last_action_date": parse_uls_date(hd["last_action_date"]),
            "entity_type": en.get("entity_type"),
            "entity_name": en.get("entity_name"),
            "first_name": en.get("first_name"),
            "last_name": en.get("last_name"),
            "street_address": en.get("street_address"),
            "city": en.get("city"),
            "state": en.get("state"),
            "zip_code": en.get("zip_code"),
            "frn": en.get("frn"),
            "operator_class": am.get("operator_class"),
            "operator_class_text": OPERATOR_CLASS.get(am.get("operator_class")),
            "previous_call_sign": am.get("previous_call_sign"),
            "previous_operator_class": am.get("previous_operator_class"),
            # NOT a Y/N flag: it holds a vanity *relationship* code A-F
            # (238 'E', 13 'F', 12 'A', ... in the verified file). Any
            # non-empty value means a vanity call sign was requested.
            "vanity_relationship": am.get("vanity_call_sign_change"),
            "is_vanity": bool(am.get("vanity_call_sign_change")),
            "is_upgrade": bool(am.get("previous_operator_class")
                               and am.get("previous_operator_class")
                               != am.get("operator_class")),
            "source_file": z._uls_url,
            "file_modified": file_date,
        }
        yield _redact(rec, en.get("entity_type"))


def fetch_status_history(service: str = "am", dow: str | None = None):
    """**HS.dat — the state-transition log.** 3,234 rows against 819 licences
    in the verified sample: the richest part of this source and the part
    almost nobody reads."""
    z = _download(service, dow)
    now = datetime.now(timezone.utc).isoformat()
    for p in _rows(z, "HS.dat"):
        h = _extract(p, "HS")
        yield {
            "source": "fcc_uls_history",
            "fetched_at": now,
            "id": f"uls:{h['usi']}:{h['log_date']}:{h['code']}",
            "usi": h["usi"],
            "call_sign": h["call_sign"],
            "log_date": parse_uls_date(h["log_date"]),
            "code": h["code"],          # LIMOD, LIISS, LICAN, ...
            "code_text": HISTORY_CODES.get(h["code"]),
            "service": service,
            "source_file": z._uls_url,
        }


def fetch_comments(service: str = "am", dow: str | None = None):
    """CO.dat — free text the FCC attached to an action."""
    z = _download(service, dow)
    now = datetime.now(timezone.utc).isoformat()
    for p in _rows(z, "CO.dat"):
        c = _extract(p, "CO")
        yield {
            "source": "fcc_uls_comment",
            "fetched_at": now,
            "id": f"uls:{c['usi']}:{c['comment_date']}",
            "usi": c["usi"],
            "call_sign": c["call_sign"],
            "comment_date": parse_uls_date(c["comment_date"]),
            "description": c["description"],
            "service": service,
        }


def week_window():
    """Which calendar day each weekday file currently holds.

    Files are published the morning AFTER the day they cover and are
    overwritten weekly, so this is the whole retrievable history: 7 days.
    """
    today = date.today()
    out = []
    for i in range(1, 8):
        d = date.fromordinal(today.toordinal() - i)
        out.append({"covers_date": d.isoformat(),
                    "file_dow": DOW[d.weekday()],
                    "published_on": date.fromordinal(d.toordinal() + 1)
                                        .isoformat()})
    return out


def summarize(rows):
    rows = list(rows)
    def tally(k):
        c = {}
        for r in rows:
            c[r.get(k)] = c.get(r.get(k), 0) + 1
        return sorted(c.items(), key=lambda x: -x[1])[:8]
    return {"rows": len(rows),
            "redacted": sum(1 for r in rows if r.get("pii_redacted")),
            "by_status": tally("license_status_text"),
            "by_class": tally("operator_class_text"),
            "by_state": tally("state"),
            "vanity": sum(1 for r in rows if r.get("is_vanity")),
            "upgrades": sum(1 for r in rows if r.get("is_upgrade"))}


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "summary"
    service = sys.argv[2] if len(sys.argv) > 2 else "am"
    dow = sys.argv[3] if len(sys.argv) > 3 else None
    if mode == "summary":
        print(json.dumps(summarize(fetch_licenses(service, dow)), indent=2))
    elif mode == "window":
        print(json.dumps(week_window(), indent=2))
    elif mode == "history":
        for r in fetch_status_history(service, dow):
            print(json.dumps(r, ensure_ascii=False))
    elif mode == "comments":
        for r in fetch_comments(service, dow):
            print(json.dumps(r, ensure_ascii=False))
    else:
        for r in fetch_licenses(service, dow):
            print(json.dumps(r, ensure_ascii=False))
