"""Offline contract tests for the Eurostat JSON-stat extractor."""
import importlib.util
import io
from pathlib import Path
from unittest.mock import patch

import pytest


MODULE_PATH = Path(__file__).with_name("fetch_eurostat_statistics.py")
SPEC = importlib.util.spec_from_file_location("fetch_eurostat_statistics", MODULE_PATH)
eurostat = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(eurostat)


FIXTURE = {
    "class": "dataset",
    "label": "Gross domestic product",
    "source": "Eurostat",
    "updated": "2026-09-01T11:00:00+0200",
    "id": ["geo", "unit", "time"],
    "size": [2, 1, 2],
    "dimension": {
        "geo": {
            "label": "Geopolitical entity",
            "category": {
                # Mapping positions, not insertion order, define the coordinate order.
                "index": {"FR": 1, "DE": 0},
                "label": {"DE": "Germany", "FR": "France"},
            },
        },
        "unit": {
            "label": "Unit of measure",
            "category": {"index": ["CP_MEUR"], "label": {"CP_MEUR": "Million euro"}},
        },
        "time": {
            "label": "Time",
            "category": {"index": {"2023": 0, "2024": 1}},
        },
    },
    # JSON-stat uses row-major flattened indexes; omitted value 1 is null.
    "value": {"0": 4100000, "2": 3000000, "3": 3200000},
    "status": {"3": "p"},
}


def test_normalize_decodes_category_indexes_row_major_values_and_nulls():
    rows = list(eurostat.normalize_response(FIXTURE, "nama_10_gdp", fetched_at="2026-09-13T00:00:00+00:00"))

    assert [(row["dimensions"]["geo"]["code"], row["dimensions"]["time"]["code"], row["value"]) for row in rows] == [
        ("DE", "2023", 4100000),
        ("DE", "2024", None),
        ("FR", "2023", 3000000),
        ("FR", "2024", 3200000),
    ]
    assert rows[0]["dimensions"]["geo"]["label"] == "Germany"
    assert rows[0]["dimensions"]["unit"] == {
        "code": "CP_MEUR",
        "label": "Million euro",
        "dimension_label": "Unit of measure",
    }
    assert rows[3]["status"] == "p"
    assert rows[0]["dataset_metadata"] == {
        "label": "Gross domestic product",
        "source": "Eurostat",
        "updated": "2026-09-01T11:00:00+0200",
    }


def test_coordinate_ids_are_stable_and_distinguish_coordinates():
    first = list(eurostat.normalize_response(FIXTURE, "nama_10_gdp", fetched_at="first"))
    changed_values = {**FIXTURE, "value": {"0": 999}}
    second = list(eurostat.normalize_response(changed_values, "nama_10_gdp", fetched_at="second"))

    assert first[0]["id"] == second[0]["id"]
    assert first[0]["id"] != first[1]["id"]
    assert first[0]["id"].startswith("eurostat_statistics:")


def test_empty_dataset_emits_no_rows():
    empty = {"class": "dataset", "id": [], "size": [], "dimension": {}, "value": {}}

    assert list(eurostat.normalize_response(empty, "empty", fetched_at="now")) == []


@pytest.mark.parametrize(
    "data",
    [
        {},
        {"class": "dataset", "id": ["geo"], "size": [1], "dimension": {}},
        {
            "class": "dataset",
            "id": ["geo"],
            "size": [2],
            "dimension": {"geo": {"category": {"index": {"DE": 0}}}},
        },
        {
            "class": "dataset",
            "id": ["geo"],
            "size": [1],
            "dimension": {"geo": {"category": {"index": ["DE"]}}},
            "value": [1],
        },
    ],
)
def test_malformed_responses_fail_before_emission(data):
    with pytest.raises(eurostat.EurostatResponseError):
        list(eurostat.normalize_response(data, "nama_10_gdp", fetched_at="now"))


def test_request_requires_bounded_time_and_dimension_filters():
    with pytest.raises(ValueError, match="time filter"):
        eurostat.build_request("nama_10_gdp", ["geo=DE"])
    with pytest.raises(ValueError, match="non-time"):
        eurostat.build_request("nama_10_gdp", ["time=2024"])
    with pytest.raises(ValueError, match="individual Eurostat period"):
        eurostat.build_request("nama_10_gdp", ["geo=DE", "time=2020..2024"])

    request = eurostat.build_request("nama_10_gdp", ["geo=DE&time=2023", "time=2024", "unit=CP_MEUR"])
    assert request.full_url.endswith("nama_10_gdp?geo=DE&time=2023&time=2024&unit=CP_MEUR")
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("User-agent") == eurostat.USER_AGENT


def test_fetch_uses_single_mocked_bounded_request():
    class MockResponse(io.StringIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    with patch.object(eurostat.urllib.request, "urlopen", return_value=MockResponse(__import__("json").dumps(FIXTURE))) as urlopen:
        rows = list(eurostat.fetch("nama_10_gdp", ["geo=DE", "time=2024"]))

    assert len(rows) == 4
    assert urlopen.call_count == 1
    request = urlopen.call_args.args[0]
    assert "geo=DE&time=2024" in request.full_url
    assert urlopen.call_args.kwargs == {"timeout": 30}
