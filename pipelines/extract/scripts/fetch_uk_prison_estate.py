#!/usr/bin/env python3
"""UK Ministry of Justice — weekly prison population and estate capacity.

Population versus usable operational capacity exposes estate pressure. Verified live
2026-09-03 through the GOV.UK Content API and its newest ODS attachment. The spreadsheet
uses repeated cells, merged headings, comma-formatted integers, and three comparison
periods; this parser expands those cells into stable period/metric/population-group
records. The publication normally updates Monday. Data is under the Open Government
Licence; discover daily and expect records weekly.

Stdlib only.
"""
import argparse
import io
import json
import re
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

SOURCE = "uk_prison_weekly_estate"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
TABLE_NS = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
REPEAT = f"{{{TABLE_NS}}}number-columns-repeated"
METRICS = {
    "Population",
    "Useable Operational Capacity",
    "Headroom",
    "Home Detention Curfew caseload",
}
GROUPS = ("Total", "Adult Male", "Female", "YCS")


def _content_url(year: int):
    return (
        "https://www.gov.uk/api/content/government/publications/"
        f"prison-population-weekly-estate-figures-{year}"
    )


def _get_json(url: str, timeout: int):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def _get_bytes(url: str, timeout: int):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def _iso_date(label: str | None):
    match = re.search(r"(?:Monday\s+)?(\d{1,2}\s+[A-Za-z]+\s+\d{4})", label or "")
    if not match:
        return None
    return datetime.strptime(match.group(1), "%d %B %Y").date().isoformat()


def _expanded_rows(content_xml: bytes):
    root = ET.fromstring(content_xml)
    rows = []
    for row in root.findall(f".//{{{TABLE_NS}}}table-row"):
        values = []
        for cell in row.findall(f"{{{TABLE_NS}}}table-cell"):
            value = " ".join("".join(cell.itertext()).split())
            repeat = min(int(cell.get(REPEAT, "1")), 64)
            values.extend([value] * repeat)
        if any(values):
            rows.append(values)
    return rows


def fetch_estate(limit: int = 100, year: int | None = None, timeout: int = 30):
    if limit <= 0:
        return
    requested_year = year
    year = year or datetime.now(timezone.utc).year
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        page = _get_json(_content_url(year), timeout)
    except urllib.error.HTTPError as error:
        if error.code != 404 or requested_year is not None:
            raise
        year -= 1
        page = _get_json(_content_url(year), timeout)

    attachments = page.get("details", {}).get("attachments", [])
    attachment = next(
        (item for item in attachments if str(item.get("url", "")).lower().endswith(".ods")),
        None,
    )
    if not attachment:
        raise ValueError("GOV.UK publication has no ODS attachment")

    body = _get_bytes(attachment["url"], timeout)
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        rows = _expanded_rows(archive.read("content.xml"))

    report_label = next(
        (cell for row in rows for cell in row if "Population and Capacity Briefing" in cell),
        attachment.get("title", ""),
    )
    report_date = _iso_date(report_label)
    if not report_date:
        raise ValueError("ODS does not contain a recognizable report date")

    period_name, period_date = "current", report_date
    headers = {}
    emitted = 0
    for cells in rows:
        joined = " ".join(cells)
        if "Last Week:" in joined:
            period_name, period_date = "last_week", _iso_date(joined)
        elif "12 Months Ago:" in joined:
            period_name, period_date = "year_ago", _iso_date(joined)

        if "Total" in cells and any(group in cells for group in GROUPS[1:]):
            headers = {index: value for index, value in enumerate(cells) if value in GROUPS}
            continue
        metric = next((value for value in cells if value in METRICS), None)
        if not metric or not period_date:
            continue
        for index, group in headers.items():
            value = cells[index] if index < len(cells) else ""
            if not value:
                continue
            compact = value.replace(",", "")
            yield {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{period_date}|{metric}|{group}",
                "report_date": report_date,
                "period": period_name,
                "period_date": period_date,
                "metric": metric,
                "population_group": group,
                "value": int(compact) if compact.isdigit() else value,
                "attachment_title": attachment.get("title"),
                "attachment_url": attachment.get("url"),
                "publisher_updated_at": page.get("public_updated_at"),
            }
            emitted += 1
            if emitted >= limit:
                return
    if not emitted:
        raise ValueError("ODS contains no recognized prison estate metrics")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--year", type=int)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_estate(args.limit, args.year, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
