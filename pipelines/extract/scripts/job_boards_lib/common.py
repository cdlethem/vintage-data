"""Shared models, HTTP policy, and normalization for public job boards."""
from __future__ import annotations

import html
import json
import re
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"

# Applied to every request, including pagination requests. Board-level pacing is
# insufficient: one large Workday tenant can require 100 pages.
DEFAULT_POLICY = (8, 0.0)
POLICIES = {
    "personio": (2, 0.6),
    "workday": (4, 0.15),
    "smartrecruiters": (6, 0.10),
    # Measured 2026-09-04: 429s appear at default concurrency on back-to-back runs.
    "recruitee": (4, 0.25),
    # Measured 2026-09-04: Workable 429s hard above one paced request at a time.
    "workable": (1, 3.0),
}


@dataclass(frozen=True)
class Board:
    provider: str
    token: str
    company: str
    country: str
    industry: str


class HttpClient:
    """Thread-safe per-provider concurrency and minimum-request-gap policy."""

    def __init__(self):
        self._sems: dict[str, threading.Semaphore] = {}
        self._next_at: dict[str, float] = {}
        self._lock = threading.Lock()

    def _sem(self, provider: str) -> threading.Semaphore:
        with self._lock:
            if provider not in self._sems:
                self._sems[provider] = threading.Semaphore(
                    POLICIES.get(provider, DEFAULT_POLICY)[0]
                )
            return self._sems[provider]

    def request(self, provider, url, method="GET", payload=None, timeout=60) -> bytes:
        sem = self._sem(provider)
        headers = {"User-Agent": USER_AGENT}
        body = None
        if payload is not None:
            body = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"

        for attempt in range(3):
            with sem:
                gap = POLICIES.get(provider, DEFAULT_POLICY)[1]
                if gap:
                    with self._lock:
                        now = time.monotonic()
                        wait = max(0.0, self._next_at.get(provider, 0.0) - now)
                        self._next_at[provider] = max(
                            now, self._next_at.get(provider, 0.0)
                        ) + gap
                    if wait:
                        time.sleep(wait)
                req = urllib.request.Request(
                    url, data=body, headers=headers, method=method
                )
                try:
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        return resp.read()
                except urllib.error.HTTPError as exc:
                    transient = exc.code == 429 or 500 <= exc.code < 600
                    if not transient or attempt == 2:
                        raise
                    retry_after = exc.headers.get("Retry-After")
            try:
                delay = min(float(retry_after), 30.0) if retry_after else 2 ** attempt
            except ValueError:
                delay = 2 ** attempt
            time.sleep(delay)
        raise AssertionError("unreachable")

    def json(self, provider, url, method="GET", payload=None, timeout=60):
        return json.loads(self.request(provider, url, method, payload, timeout))


CLIENT = HttpClient()


def strip_html(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"<[^>]+>", " ", html.unescape(value))
    return re.sub(r"\s+", " ", text).strip() or None


def iso_datetime(value) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()
    text = str(value).strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text) / 1000, tz=timezone.utc).isoformat()
    parsed = text.replace("Z", "+00:00")
    parsed = re.sub(
        r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) UTC$",
        r"\1T\2+00:00",
        parsed,
    )
    try:
        return datetime.fromisoformat(parsed).astimezone(timezone.utc).isoformat()
    except ValueError:
        return text


# Conservative ISO-3166 alpha-2 resolution for provider-supplied free text.
# Unknown is None; a missing country is less damaging than a wrong country.
COUNTRY_NAMES = {
    "afghanistan": "AF", "albania": "AL", "algeria": "DZ", "andorra": "AD", "angola": "AO",
    "argentina": "AR", "armenia": "AM", "aruba": "AW", "australia": "AU", "austria": "AT",
    "azerbaijan": "AZ", "bahamas": "BS", "bahrain": "BH", "bangladesh": "BD", "barbados": "BB",
    "belarus": "BY", "belgium": "BE", "belize": "BZ", "benin": "BJ", "bermuda": "BM",
    "bolivia": "BO", "bosnia and herzegovina": "BA", "botswana": "BW", "brazil": "BR",
    "brunei": "BN", "bulgaria": "BG", "burkina faso": "BF", "burundi": "BI", "cambodia": "KH",
    "cameroon": "CM", "canada": "CA", "cape verde": "CV", "cayman islands": "KY", "chad": "TD",
    "chile": "CL", "china": "CN", "colombia": "CO", "costa rica": "CR", "croatia": "HR",
    "cuba": "CU", "curacao": "CW", "cyprus": "CY", "czechia": "CZ", "czech republic": "CZ",
    "denmark": "DK", "dominican republic": "DO", "ecuador": "EC", "egypt": "EG",
    "el salvador": "SV", "estonia": "EE", "eswatini": "SZ", "ethiopia": "ET", "fiji": "FJ",
    "finland": "FI", "france": "FR", "gabon": "GA", "gambia": "GM", "georgia": "GE",
    "germany": "DE", "ghana": "GH", "gibraltar": "GI", "greece": "GR", "greenland": "GL",
    "guatemala": "GT", "guernsey": "GG", "guinea": "GN", "guyana": "GY", "haiti": "HT",
    "honduras": "HN", "hong kong": "HK", "hong kong sar": "HK", "hungary": "HU",
    "iceland": "IS", "india": "IN", "indonesia": "ID", "iran": "IR", "iraq": "IQ",
    "ireland": "IE", "isle of man": "IM", "israel": "IL", "italy": "IT", "ivory coast": "CI",
    "cote d'ivoire": "CI", "jamaica": "JM", "japan": "JP", "jersey": "JE", "jordan": "JO",
    "kazakhstan": "KZ", "kenya": "KE", "kosovo": "XK", "kuwait": "KW", "kyrgyzstan": "KG",
    "laos": "LA", "latvia": "LV", "lebanon": "LB", "liberia": "LR", "libya": "LY",
    "liechtenstein": "LI", "lithuania": "LT", "luxembourg": "LU", "macau": "MO",
    "madagascar": "MG", "malawi": "MW", "malaysia": "MY", "maldives": "MV", "mali": "ML",
    "malta": "MT", "mauritius": "MU", "mexico": "MX", "moldova": "MD", "monaco": "MC",
    "mongolia": "MN", "montenegro": "ME", "morocco": "MA", "mozambique": "MZ", "myanmar": "MM",
    "namibia": "NA", "nepal": "NP", "netherlands": "NL", "the netherlands": "NL",
    "new zealand": "NZ", "nicaragua": "NI", "niger": "NE", "nigeria": "NG",
    "north macedonia": "MK", "macedonia": "MK", "norway": "NO", "oman": "OM", "pakistan": "PK",
    "palestine": "PS", "panama": "PA", "papua new guinea": "PG", "paraguay": "PY", "peru": "PE",
    "philippines": "PH", "the philippines": "PH", "poland": "PL", "portugal": "PT",
    "puerto rico": "PR", "qatar": "QA", "romania": "RO", "russia": "RU",
    "russian federation": "RU", "rwanda": "RW", "saudi arabia": "SA", "senegal": "SN",
    "serbia": "RS", "singapore": "SG", "slovakia": "SK", "slovenia": "SI", "somalia": "SO",
    "south africa": "ZA", "south korea": "KR", "korea": "KR", "republic of korea": "KR",
    "spain": "ES", "sri lanka": "LK", "sudan": "SD", "suriname": "SR", "sweden": "SE",
    "switzerland": "CH", "taiwan": "TW", "tajikistan": "TJ", "tanzania": "TZ",
    "thailand": "TH", "togo": "TG", "trinidad and tobago": "TT", "tunisia": "TN",
    "turkey": "TR", "turkiye": "TR", "uganda": "UG", "ukraine": "UA",
    "united arab emirates": "AE", "uae": "AE", "united kingdom": "GB", "uk": "GB",
    "great britain": "GB", "england": "GB", "scotland": "GB", "wales": "GB",
    "northern ireland": "GB", "united states": "US", "united states of america": "US",
    "usa": "US", "u.s.": "US", "u.s.a.": "US", "america": "US", "uruguay": "UY",
    "uzbekistan": "UZ", "venezuela": "VE", "vietnam": "VN", "viet nam": "VN", "yemen": "YE",
    "zambia": "ZM", "zimbabwe": "ZW",
}
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY", "DC",
}
CA_PROVINCES = {"ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE", "YT", "NT", "NU"}
ISO2 = set(
    "AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI "
    "BJ BL BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN "
    "CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK "
    "FM FO FR GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM "
    "HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG KH KI KM KN "
    "KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK "
    "ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP "
    "NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW "
    "SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF "
    "TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI "
    "VN VU WF WS YE YT ZA ZM ZW".split()
)


def country_from_text(text: str | None) -> str | None:
    if not text:
        return None
    parts = [part.strip() for part in re.split(r"[,/|;]| - ", text) if part.strip()]
    for part in reversed(parts):
        low = part.lower().strip(". ")
        if low in COUNTRY_NAMES:
            return COUNTRY_NAMES[low]
        upper = part.upper().strip(". ")
        if upper in US_STATES:
            return "US"
        if upper in CA_PROVINCES:
            return "CA"
        if len(upper) == 2 and upper in ISO2:
            return upper
    return None


def normalize_country(code: str | None, fallback_text: str | None = None) -> str | None:
    if code:
        code = str(code).strip()
        if len(code) == 2 and code.upper() in ISO2:
            return code.upper()
        if code.lower() in COUNTRY_NAMES:
            return COUNTRY_NAMES[code.lower()]
    return country_from_text(fallback_text)


def record(board: Board, fetched_at: str, **values) -> dict:
    """Build one stable-schema record; adapters only provide populated fields."""
    result = {
        "source": "job_boards", "fetched_at": fetched_at, "id": None,
        "provider": board.provider, "company": board.company,
        "company_country": board.country, "company_industry": board.industry,
        "title": None, "department": None, "employment_type": None,
        "experience_level": None, "location": None, "location_country": None,
        "workplace_type": None, "is_remote": None, "posted": None, "url": None,
        "salary_text": None, "salary_min": None, "salary_max": None,
        "salary_currency": None, "salary_interval": None, "description": None,
    }
    result.update(values)
    return result


def fetched_now() -> str:
    return datetime.now(timezone.utc).isoformat()
