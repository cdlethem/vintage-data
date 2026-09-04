"""Public ATS adapters: endpoint, pagination, verification, normalization."""
from __future__ import annotations

import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET
from collections.abc import Iterator

from .common import (
    CLIENT,
    Board,
    country_from_text,
    fetched_now,
    iso_datetime,
    normalize_country,
    record,
    strip_html,
)


def _greenhouse(board: Board, limit: int) -> Iterator[dict]:
    data = CLIENT.json("greenhouse", f"https://boards-api.greenhouse.io/v1/boards/{board.token}/jobs?content=true")
    stamp = fetched_now()
    for job in data.get("jobs", [])[:limit]:
        location = (job.get("location") or {}).get("name")
        yield record(
            board, stamp, id=f"greenhouse:{board.token}:{job.get('id')}",
            title=job.get("title"), location=location,
            location_country=country_from_text(location),
            posted=iso_datetime(job.get("first_published") or job.get("updated_at")),
            department=", ".join(d.get("name", "") for d in job.get("departments", [])) or None,
            url=job.get("absolute_url"), description=strip_html(job.get("content")),
        )


def _lever(board: Board, limit: int) -> Iterator[dict]:
    data = CLIENT.json("lever", f"https://api.lever.co/v0/postings/{board.token}?mode=json")
    stamp = fetched_now()
    for job in (data if isinstance(data, list) else [])[:limit]:
        categories = job.get("categories") or {}
        location = categories.get("location")
        workplace = job.get("workplaceType")
        yield record(
            board, stamp, id=f"lever:{board.token}:{job.get('id')}", title=job.get("text"),
            location=location, location_country=normalize_country(job.get("country"), location),
            posted=iso_datetime(job.get("createdAt")),
            department=categories.get("team") or categories.get("department"),
            employment_type=categories.get("commitment"), workplace_type=workplace,
            is_remote=(workplace == "remote") if workplace else None,
            url=job.get("hostedUrl"),
            description=strip_html(job.get("descriptionPlain") or job.get("description")),
        )


def _ashby(board: Board, limit: int) -> Iterator[dict]:
    data = CLIENT.json(
        "ashby",
        f"https://api.ashbyhq.com/posting-api/job-board/{board.token}?includeCompensation=true",
    )
    stamp = fetched_now()
    for job in data.get("jobs", [])[:limit]:
        address = ((job.get("address") or {}).get("postalAddress") or {})
        compensation = job.get("compensation") or {}
        salary_min = salary_max = currency = interval = None
        for tier in compensation.get("compensationTiers") or []:
            salary = next(
                (component for component in tier.get("components") or []
                 if component.get("compensationType") == "Salary"
                 and component.get("minValue") is not None),
                None,
            )
            if salary:
                salary_min, salary_max = salary.get("minValue"), salary.get("maxValue")
                currency, interval = salary.get("currencyCode"), salary.get("interval")
                break
        yield record(
            board, stamp, id=f"ashby:{board.token}:{job.get('id')}", title=job.get("title"),
            location=job.get("location"),
            location_country=normalize_country(address.get("addressCountry"), job.get("location")),
            posted=iso_datetime(job.get("publishedAt")),
            department=job.get("department") or job.get("team"),
            employment_type=job.get("employmentType"), workplace_type=job.get("workplaceType"),
            is_remote=job.get("isRemote"), url=job.get("jobUrl"),
            salary_text=compensation.get("compensationTierSummary"),
            salary_min=salary_min, salary_max=salary_max,
            salary_currency=currency, salary_interval=interval,
            description=strip_html(job.get("descriptionPlain") or job.get("descriptionHtml")),
        )


def _smartrecruiters(board: Board, limit: int) -> Iterator[dict]:
    token = urllib.parse.quote(board.token)
    offset = 0
    stamp = fetched_now()
    while offset < limit:
        page = CLIENT.json(
            "smartrecruiters",
            f"https://api.smartrecruiters.com/v1/companies/{token}/postings?limit=100&offset={offset}",
        )
        jobs = page.get("content") or []
        if not jobs:
            return
        for job in jobs[:limit - offset]:
            location = job.get("location") or {}
            country = (location.get("country") or "").upper() or None
            pretty = location.get("fullLocation") or ", ".join(
                value for value in (location.get("city"), location.get("region"), country) if value
            ) or None
            yield record(
                board, stamp, id=f"smartrecruiters:{board.token}:{job.get('id')}",
                title=job.get("name"), location=pretty,
                location_country=normalize_country(country, pretty),
                posted=iso_datetime(job.get("releasedDate")),
                department=(job.get("department") or {}).get("label")
                or (job.get("function") or {}).get("label"),
                employment_type=(job.get("typeOfEmployment") or {}).get("label"),
                experience_level=(job.get("experienceLevel") or {}).get("label"),
                is_remote=location.get("remote"),
                workplace_type=("remote" if location.get("remote") else
                                "hybrid" if location.get("hybrid") else None),
                url=job.get("ref"),
            )
        offset += len(jobs)
        if offset >= int(page.get("totalFound", 0)):
            return


def _workable(board: Board, limit: int) -> Iterator[dict]:
    url = f"https://apply.workable.com/api/v3/accounts/{board.token}/jobs"
    payload: dict = {"query": ""}
    emitted = 0
    stamp = fetched_now()
    while emitted < limit:
        page = CLIENT.json("workable", url, method="POST", payload=payload)
        jobs = page.get("results") or []
        if not jobs:
            return
        for job in jobs[:limit - emitted]:
            location = job.get("location") or {}
            pretty = ", ".join(
                value for value in (location.get("city"), location.get("region"),
                                    location.get("country")) if value
            ) or None
            shortcode = job.get("shortcode")
            yield record(
                board, stamp, id=f"workable:{board.token}:{shortcode or job.get('id')}",
                title=job.get("title"), location=pretty,
                location_country=normalize_country(location.get("countryCode"), pretty),
                posted=iso_datetime(job.get("published")),
                department=", ".join(job.get("department") or []) or None,
                employment_type=job.get("type"), workplace_type=job.get("workplace"),
                is_remote=job.get("remote"),
                url=f"https://apply.workable.com/{board.token}/j/{shortcode}/" if shortcode else None,
            )
        emitted += len(jobs)
        token = page.get("nextPage")
        if not token:
            return
        payload = {"query": "", "token": token}


def _recruitee(board: Board, limit: int) -> Iterator[dict]:
    data = CLIENT.json("recruitee", f"https://{board.token}.recruitee.com/api/offers/")
    stamp = fetched_now()
    for job in (data.get("offers") or [])[:limit]:
        salary = job.get("salary") or {}
        location = job.get("location") or ", ".join(
            value for value in (job.get("city"), job.get("country")) if value
        ) or None
        workplace = ("remote" if job.get("remote") else "hybrid" if job.get("hybrid")
                     else "onsite" if job.get("on_site") else None)
        yield record(
            board, stamp, id=f"recruitee:{board.token}:{job.get('id')}",
            title=job.get("title") or job.get("position"), location=location,
            location_country=normalize_country(job.get("country_code"), location),
            posted=iso_datetime(job.get("published_at") or job.get("created_at")),
            department=job.get("department"), employment_type=job.get("employment_type_code"),
            experience_level=job.get("experience_code"), is_remote=job.get("remote"),
            workplace_type=workplace, url=job.get("careers_url") or job.get("careers_apply_url"),
            salary_min=salary.get("min"), salary_max=salary.get("max"),
            salary_currency=salary.get("currency"), salary_interval=salary.get("period"),
            description=strip_html(" ".join(filter(None, [job.get("description"),
                                                           job.get("requirements")]))),
        )


def _personio(board: Board, limit: int) -> Iterator[dict]:
    root = ET.fromstring(CLIENT.request(
        "personio", f"https://{board.token}.jobs.personio.de/xml"
    ))
    stamp = fetched_now()
    for job in list(root.iter("position"))[:limit]:
        def text(tag):
            element = job.find(tag)
            return element.text.strip() if element is not None and element.text else None
        descriptions = [
            f"{part.findtext('name') or ''}: {part.findtext('value') or ''}"
            for part in job.iter("jobDescription")
        ]
        job_id, location = text("id"), text("office")
        yield record(
            board, stamp, id=f"personio:{board.token}:{job_id}", title=text("name"),
            location=location, location_country=country_from_text(location),
            posted=iso_datetime(text("createdAt")), department=text("department"),
            employment_type=text("employmentType") or text("recruitingCategory"),
            experience_level=text("seniority"),
            url=f"https://{board.token}.jobs.personio.de/job/{job_id}" if job_id else None,
            description=strip_html(" ".join(descriptions)) if descriptions else None,
        )


def _breezy(board: Board, limit: int) -> Iterator[dict]:
    data = CLIENT.json("breezy", f"https://{board.token}.breezy.hr/json")
    stamp = fetched_now()
    for job in (data if isinstance(data, list) else [])[:limit]:
        location = job.get("location") or {}
        country = (location.get("country") or {}).get("id")
        yield record(
            board, stamp, id=f"breezy:{board.token}:{job.get('id')}", title=job.get("name"),
            location=location.get("name"),
            location_country=normalize_country(country, location.get("name")),
            posted=iso_datetime(job.get("published_date")), department=job.get("department"),
            employment_type=(job.get("type") or {}).get("name"),
            is_remote=location.get("is_remote"), url=job.get("url"),
            salary_text=job.get("salary") or None,
        )


def _rippling(board: Board, limit: int) -> Iterator[dict]:
    data = CLIENT.json(
        "rippling",
        f"https://api.rippling.com/platform/api/ats/v1/board/{board.token}/jobs",
    )
    stamp = fetched_now()
    for job in (data if isinstance(data, list) else [])[:limit]:
        location = (job.get("workLocation") or {}).get("label")
        yield record(
            board, stamp, id=f"rippling:{board.token}:{job.get('uuid')}", title=job.get("name"),
            location=location, location_country=country_from_text(location),
            department=(job.get("department") or {}).get("label"), url=job.get("url"),
        )


def _workday(board: Board, limit: int) -> Iterator[dict]:
    tenant, host, site = board.token.split("|")
    base = f"https://{tenant}.{host}.myworkdayjobs.com"
    api = f"{base}/wday/cxs/{tenant}/{site}/jobs"
    offset = 0
    total = None
    stamp = fetched_now()
    while offset < limit:
        page = CLIENT.json(
            "workday", api, method="POST",
            payload={"appliedFacets": {}, "limit": 20, "offset": offset, "searchText": ""},
        )
        jobs = page.get("jobPostings") or []
        if not jobs:
            return
        if total is None:
            total = int(page.get("total") or 0)
        for job in jobs[:limit - offset]:
            location = job.get("locationsText")
            path = job.get("externalPath") or ""
            yield record(
                board, stamp,
                id=f"workday:{tenant}:{site}:{path.rsplit('_', 1)[-1] or path}",
                title=job.get("title"), location=location,
                location_country=country_from_text(location),
                posted=job.get("postedOn"),
                url=f"{base}/en-US/{site}{path}" if path else None,
            )
        offset += len(jobs)
        if total and offset >= total:
            return


FETCHERS = {
    "greenhouse": _greenhouse,
    "lever": _lever,
    "ashby": _ashby,
    "smartrecruiters": _smartrecruiters,
    "workable": _workable,
    "recruitee": _recruitee,
    "personio": _personio,
    "breezy": _breezy,
    "rippling": _rippling,
    "workday": _workday,
}


def fetch(board: Board, limit: int = 10000) -> Iterator[dict]:
    yield from FETCHERS[board.provider](board, limit)


def count(provider: str, token: str) -> int | None:
    """Probe the cheapest real endpoint and return its open-posting count."""
    if provider == "greenhouse":
        data = CLIENT.json(provider, f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs")
        return len(data.get("jobs", []))
    if provider == "lever":
        data = CLIENT.json(provider, f"https://api.lever.co/v0/postings/{token}?mode=json")
        return len(data) if isinstance(data, list) else None
    if provider == "ashby":
        data = CLIENT.json(provider, f"https://api.ashbyhq.com/posting-api/job-board/{token}")
        return len(data.get("jobs", []))
    if provider == "smartrecruiters":
        data = CLIENT.json(provider, f"https://api.smartrecruiters.com/v1/companies/{urllib.parse.quote(token)}/postings?limit=1")
        return int(data.get("totalFound", 0))
    if provider == "workable":
        data = CLIENT.json(provider, f"https://apply.workable.com/api/v3/accounts/{token}/jobs",
                           method="POST", payload={"query": ""})
        return int(data.get("total", 0))
    if provider == "recruitee":
        data = CLIENT.json(provider, f"https://{token}.recruitee.com/api/offers/")
        return len(data.get("offers", []))
    if provider == "breezy":
        data = CLIENT.json(provider, f"https://{token}.breezy.hr/json")
        return len(data) if isinstance(data, list) else None
    if provider == "rippling":
        data = CLIENT.json(provider, f"https://api.rippling.com/platform/api/ats/v1/board/{token}/jobs")
        return len(data) if isinstance(data, list) else None
    if provider == "personio":
        root = ET.fromstring(CLIENT.request(provider, f"https://{token}.jobs.personio.de/xml"))
        return sum(1 for _ in root.iter("position"))
    if provider == "workday":
        tenant, host, site = token.split("|")
        data = CLIENT.json(
            provider,
            f"https://{tenant}.{host}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs",
            method="POST",
            payload={"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""},
        )
        return int(data.get("total", 0))
    raise ValueError(f"unknown provider {provider!r}")


def is_permanent_miss(exc: BaseException) -> bool:
    return isinstance(exc, urllib.error.HTTPError) and exc.code in (403, 404, 410)
