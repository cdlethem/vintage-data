#!/usr/bin/env python3
"""Official, keyless cross-company job feeds with one provider per invocation.

These complement the named-company ATS catalog: The Muse adds broad US roles,
Arbeitnow adds Europe, Get on Board adds Latin America, and the remaining feeds
cover remote work. Airflow schedules each provider as a separate simple DAG, so
one upstream cannot suppress another provider's data or retry state.

Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from job_boards_lib.common import CLIENT, country_from_text, iso_datetime, strip_html


def base(provider, job_id, company, title, **values):
    row = {
        "source": f"job_feed_{provider}",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "id": f"{provider}:{job_id}", "provider": provider, "company": company,
        "company_country": None, "company_industry": None, "title": title,
        "department": None, "employment_type": None, "experience_level": None,
        "location": None, "location_country": None, "workplace_type": None,
        "is_remote": None, "posted": None, "url": None, "salary_text": None,
        "salary_min": None, "salary_max": None, "salary_currency": None,
        "salary_interval": None, "description": None,
    }
    row.update(values)
    return row


def themuse(max_pages):
    for page in range(1, max_pages + 1):
        data = CLIENT.json("themuse", f"https://www.themuse.com/api/public/jobs?page={page}")
        jobs = data.get("results") or []
        if not jobs:
            return
        for job in jobs:
            locations = ", ".join(x.get("name", "") for x in job.get("locations", [])) or None
            yield base(
                "themuse", job.get("id"), (job.get("company") or {}).get("name"), job.get("name"),
                department=", ".join(x.get("name", "") for x in job.get("categories", [])) or None,
                experience_level=", ".join(x.get("name", "") for x in job.get("levels", [])) or None,
                location=locations, location_country=country_from_text(locations),
                posted=iso_datetime(job.get("publication_date")),
                url=(job.get("refs") or {}).get("landing_page"),
                description=strip_html(job.get("contents")),
            )
        if page >= int(data.get("page_count") or 0):
            return


def arbeitnow(max_pages):
    for page in range(1, max_pages + 1):
        data = CLIENT.json("arbeitnow", f"https://www.arbeitnow.com/api/job-board-api?page={page}")
        jobs = data.get("data") or []
        if not jobs:
            return
        for job in jobs:
            location = job.get("location")
            yield base(
                "arbeitnow", job.get("slug"), job.get("company_name"), job.get("title"),
                department=", ".join(job.get("tags") or []) or None,
                employment_type=", ".join(job.get("job_types") or []) or None,
                location=location, location_country=country_from_text(location),
                workplace_type="remote" if job.get("remote") else None,
                is_remote=job.get("remote"), posted=iso_datetime(job.get("created_at")),
                url=job.get("url"), description=strip_html(job.get("description")),
            )
        if not (data.get("links") or {}).get("next"):
            return


def getonboard(max_pages):
    for page in range(1, max_pages + 1):
        url = f"https://www.getonbrd.com/api/v0/search/jobs?remote=true&per_page=100&page={page}"
        data = CLIENT.json("getonboard", url)
        jobs = data.get("data") or []
        if not jobs:
            return
        for wrapper in jobs:
            job = wrapper.get("attributes") or {}
            company = job.get("company") or {}
            countries = job.get("countries") or []
            location = ", ".join(filter(None, [
                ", ".join(job.get("location_cities") or []),
                ", ".join(c.get("name", "") if isinstance(c, dict) else str(c) for c in countries),
                job.get("remote_zone"),
            ])) or None
            yield base(
                "getonboard", wrapper.get("id"), company.get("name") if isinstance(company, dict) else company,
                job.get("title"), department=job.get("category_name"),
                experience_level=job.get("seniority"), location=location,
                location_country=country_from_text(location),
                workplace_type=job.get("modality") or job.get("remote_modality"),
                is_remote=job.get("remote"), posted=iso_datetime(job.get("published_at")),
                url=(wrapper.get("links") or {}).get("public_url"),
                salary_min=job.get("min_salary"), salary_max=job.get("max_salary"),
                description=strip_html(job.get("description")),
            )
        if page >= int((data.get("meta") or {}).get("total_pages") or 0):
            return


def remotive(_max_pages):
    data = CLIENT.json("remotive", "https://remotive.com/api/remote-jobs")
    for job in data.get("jobs", []):
        location = job.get("candidate_required_location")
        yield base(
            "remotive", job.get("id"), job.get("company_name"), job.get("title"),
            department=job.get("category"), employment_type=job.get("job_type"),
            location=location, location_country=country_from_text(location),
            workplace_type="remote", is_remote=True,
            posted=iso_datetime(job.get("publication_date")), url=job.get("url"),
            salary_text=job.get("salary") or None, description=strip_html(job.get("description")),
        )


def remoteok(_max_pages):
    data = CLIENT.json("remoteok", "https://remoteok.com/api")
    for job in data if isinstance(data, list) else []:
        if not job.get("id") or not job.get("position"):
            continue  # first row is API terms metadata
        location = job.get("location")
        yield base(
            "remoteok", job.get("id"), job.get("company"), job.get("position"),
            department=", ".join(job.get("tags") or []) or None,
            location=location, location_country=country_from_text(location),
            workplace_type="remote", is_remote=True, posted=iso_datetime(job.get("date")),
            url=job.get("url"), salary_min=job.get("salary_min") or None,
            salary_max=job.get("salary_max") or None, description=strip_html(job.get("description")),
        )


def himalayas(max_pages):
    cursor = None
    for _ in range(max_pages):
        query = "?limit=20" + ("&cursor=" + urllib.parse.quote(cursor) if cursor else "")
        data = CLIENT.json("himalayas", "https://himalayas.app/jobs/api" + query)
        jobs = data.get("jobs") or []
        if not jobs:
            return
        for job in jobs:
            restrictions = job.get("locationRestrictions") or []
            location = ", ".join(
                x.get("name", "") if isinstance(x, dict) else str(x) for x in restrictions
            ) or "Remote"
            yield base(
                "himalayas", job.get("guid"), job.get("companyName"), job.get("title"),
                department=", ".join(job.get("categories") or []) or None,
                employment_type=job.get("employmentType"), experience_level=job.get("seniority"),
                location=location, location_country=country_from_text(location),
                workplace_type="remote", is_remote=True, posted=iso_datetime(job.get("pubDate")),
                url=job.get("applicationLink"), salary_min=job.get("minSalary"),
                salary_max=job.get("maxSalary"), salary_currency=job.get("currency"),
                salary_interval=job.get("salaryPeriod"), description=strip_html(job.get("description")),
            )
        cursor = data.get("nextCursor")
        if not cursor:
            return


def jobicy(_max_pages):
    data = CLIENT.json("jobicy", "https://jobicy.com/api/v2/remote-jobs?count=50")
    for job in data.get("jobs", []):
        location = job.get("jobGeo")
        yield base(
            "jobicy", job.get("id"), job.get("companyName"), job.get("jobTitle"),
            department=job.get("jobIndustry"), employment_type=job.get("jobType"),
            experience_level=job.get("jobLevel"), location=location,
            location_country=country_from_text(location), workplace_type="remote", is_remote=True,
            posted=iso_datetime(job.get("pubDate")), url=job.get("url"),
            salary_min=job.get("salaryMin"), salary_max=job.get("salaryMax"),
            salary_currency=job.get("salaryCurrency"), salary_interval=job.get("salaryPeriod"),
            description=strip_html(job.get("jobDescription")),
        )


def workingnomads(_max_pages):
    data = CLIENT.json("workingnomads", "https://www.workingnomads.com/api/exposed_jobs/")
    for job in data if isinstance(data, list) else []:
        location = job.get("location")
        yield base(
            "workingnomads", job.get("id") or job.get("url"), job.get("company_name"),
            job.get("title"), department=job.get("category_name"), location=location,
            location_country=country_from_text(location), workplace_type="remote", is_remote=True,
            posted=iso_datetime(job.get("pub_date")), url=job.get("url"),
            description=strip_html(job.get("description")),
        )


def weworkremotely(_max_pages):
    root = ET.fromstring(CLIENT.request("weworkremotely", "https://weworkremotely.com/remote-jobs.rss"))
    for item in root.findall("./channel/item"):
        def text(tag):
            node = item.find(tag)
            return node.text.strip() if node is not None and node.text else None
        title = text("title") or ""
        company, _, role = title.partition(": ")
        location = text("region") or text("country") or text("state")
        posted = text("pubDate")
        try:
            posted = parsedate_to_datetime(posted).astimezone(timezone.utc).isoformat() if posted else None
        except ValueError:
            pass
        yield base(
            "weworkremotely", text("guid") or text("link"), company or None, role or title,
            department=text("category"), employment_type=text("type"), location=location,
            location_country=country_from_text(location), workplace_type="remote", is_remote=True,
            posted=posted, url=text("link"), description=strip_html(text("description")),
        )


FEEDS = {
    "themuse": themuse, "arbeitnow": arbeitnow, "getonboard": getonboard,
    "remotive": remotive, "remoteok": remoteok, "himalayas": himalayas,
    "jobicy": jobicy, "workingnomads": workingnomads, "weworkremotely": weworkremotely,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("provider", choices=sorted(FEEDS))
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args()
    if args.max_pages < 1:
        parser.error("--max-pages must be positive")
    for row in FEEDS[args.provider](args.max_pages):
        print(json.dumps(row, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
