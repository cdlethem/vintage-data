#!/usr/bin/env python3
"""Fetch a bounded ILOSTAT employment dataflow as SDMX-CSV NDJSON.

The response is streamed rather than buffered. SDMX columns remain on each output
record verbatim; the additional envelope makes the dataflow, dimensions, measure,
unit, status, period, and fetch time explicit. By default only the current and
previous calendar years are requested.

Stdlib only.
"""

import argparse
import csv
import hashlib
import io
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Iterator, Mapping, Sequence

SOURCE = "ilostat_sdmx"
DATAFLOW = "ILO,DF_EMP_TEMP_SEX_AGE_NB,1.0"
API_URL = f"https://rplumber.ilo.org/data/data/{DATAFLOW}/."
ACCEPT = "application/vnd.sdmx.data+csv;version=2.0.0"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

_REQUIRED_COLUMNS = {
    "FREQ",
    "REF_AREA",
    "SOURCE",
    "INDICATOR",
    "SEX",
    "CLASSIF1",
    "TIME_PERIOD",
    "OBS_VALUE",
    "UNIT_MEASURE",
    "OBS_STATUS",
}
_NON_DIMENSION_COLUMNS = {
    "STRUCTURE",
    "STRUCTURE_ID",
    "STRUCTURE_NAME",
    "ACTION",
    "DATAFLOW",
    "DATAFLOW_ID",
}


def default_period_window(now: datetime | None = None) -> tuple[str, str]:
    """Return a rolling two-calendar-year window for bounded unattended runs."""
    current_year = (now or datetime.now(timezone.utc)).year
    return str(current_year - 1), str(current_year)


def _validate_request(start_period: str, end_period: str, timeout: int) -> None:
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if not start_period or not end_period:
        raise ValueError("start_period and end_period are required")
    if start_period > end_period:
        raise ValueError("start_period must not be after end_period")


def build_url(start_period: str, end_period: str) -> str:
    """Build the single bounded SDMX data request; no catalogue is queried."""
    query = urllib.parse.urlencode(
        {
            "startPeriod": start_period,
            "endPeriod": end_period,
            "labels": "both",
        }
    )
    return f"{API_URL}?{query}"


def _dimension_columns(fieldnames: Sequence[str]) -> tuple[str, ...]:
    try:
        measure_index = fieldnames.index("OBS_VALUE")
    except ValueError as exc:
        raise ValueError("ILOSTAT CSV is missing required columns: ['OBS_VALUE']") from exc

    dimensions = tuple(
        name
        for name in fieldnames[:measure_index]
        if name not in _NON_DIMENSION_COLUMNS and not name.endswith("_LABEL")
    )
    if "TIME_PERIOD" not in dimensions:
        raise ValueError("ILOSTAT CSV does not identify TIME_PERIOD as a dimension")
    return dimensions


def stable_observation_id(dimensions: Mapping[str, str]) -> str:
    """Derive an ID from the dataflow and every code-valued observation dimension."""
    identity = [DATAFLOW, sorted(dimensions.items())]
    canonical = json.dumps(identity, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_observation(
    row: Mapping[str, str], dimension_columns: Sequence[str], fetched_at: str
) -> dict[str, object]:
    """Add a stable envelope while preserving every SDMX-CSV field verbatim."""
    dimensions = {name: row[name] for name in dimension_columns}
    missing_dimensions = [name for name, value in dimensions.items() if not value]
    if missing_dimensions:
        raise ValueError(
            f"ILOSTAT observation has empty dimensions: {sorted(missing_dimensions)}"
        )

    units = {
        name: value
        for name, value in row.items()
        if name == "UNIT_MEASURE" or name.startswith("UNIT_MEASURE_") or name == "UNIT_MULT" or name.startswith("UNIT_MULT_")
    }
    record: dict[str, object] = dict(row)
    record.update(
        {
            "source": SOURCE,
            "dataflow": DATAFLOW,
            "id": stable_observation_id(dimensions),
            "fetched_at": fetched_at,
            "dimensions": dimensions,
            "measures": {"OBS_VALUE": row["OBS_VALUE"]},
            "units": units,
            "status": row["OBS_STATUS"],
            "period": row["TIME_PERIOD"],
        }
    )
    return record


def fetch_observations(
    start_period: str | None = None,
    end_period: str | None = None,
    timeout: int = 60,
) -> Iterator[dict[str, object]]:
    """Stream observations from one bounded, labelled SDMX-CSV response."""
    default_start, default_end = default_period_window()
    start_period = default_start if start_period is None else start_period
    end_period = default_end if end_period is None else end_period
    _validate_request(start_period, end_period, timeout)

    request = urllib.request.Request(
        build_url(start_period, end_period),
        headers={
            "Accept": ACCEPT,
            "User-Agent": USER_AGENT,
        },
    )
    fetched_at = datetime.now(timezone.utc).isoformat()

    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = io.TextIOWrapper(response, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text)
        if not reader.fieldnames:
            raise ValueError("ILOSTAT response is empty or has no CSV header")
        fieldnames = tuple(reader.fieldnames)
        if len(fieldnames) != len(set(fieldnames)):
            raise ValueError("ILOSTAT CSV contains duplicate columns")
        missing = sorted(_REQUIRED_COLUMNS.difference(fieldnames))
        if missing:
            raise ValueError(f"ILOSTAT CSV is missing required columns: {missing}")
        dimension_columns = _dimension_columns(fieldnames)

        for row_number, row in enumerate(reader, start=2):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"ILOSTAT CSV row {row_number} has the wrong column count")
            yield normalize_observation(row, dimension_columns, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    default_start, default_end = default_period_window()
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-period", default=default_start)
    parser.add_argument("--end-period", default=default_end)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args(argv)

    for record in fetch_observations(
        start_period=args.start_period,
        end_period=args.end_period,
        timeout=args.timeout,
    ):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
