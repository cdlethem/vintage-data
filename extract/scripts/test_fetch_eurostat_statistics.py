import contextlib
from datetime import datetime, timezone
import importlib.util
import io
import json
import pathlib
import re
import urllib.error
import urllib.parse
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).with_name("fetch_eurostat_statistics.py")
SPEC = importlib.util.spec_from_file_location("fetch_eurostat_statistics", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
CONFIG = SCRIPT.parents[1] / "sources" / "eurostat_statistics.yml"


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def dataset_document():
    return {
        "version": "2.0",
        "class": "dataset",
        "label": "Population on 1 January",
        "source": "ESTAT",
        "updated": "2026-09-15T23:00:00+0200",
        "id": ["freq", "unit", "geo", "time"],
        "size": [1, 2, 2, 2],
        "dimension": {
            "freq": {
                "label": "Time frequency",
                "category": {
                    "index": {"A": 0},
                    "label": {"A": "Annual"},
                },
            },
            "unit": {
                "label": "Unit of measure",
                "category": {
                    "index": ["NR", "PC"],
                    "label": {"NR": "Number", "PC": "Percent"},
                    "unit": {
                        "NR": {"base": "number", "decimals": 0},
                        "PC": {"base": "percent", "decimals": 1},
                    },
                },
            },
            "geo": {
                "label": "Geopolitical entity",
                "category": {
                    "index": {"DE": 0, "FR": 1},
                    "label": {"DE": "Germany", "FR": "France"},
                },
            },
            "time": {
                "label": "Time",
                "category": {
                    "index": {"2024": 0, "2025": 1},
                    "label": {"2024": "2024", "2025": "2025"},
                },
            },
        },
        "value": {"0": 83_456_045, "1": None, "6": 51.2, "7": 51.4},
        "status": {"0": "p", "7": "e"},
        "note": ["Offline fixture"],
    }


def fetch(document=None, **overrides):
    arguments = {
        "dataset_code": "demo_pjan",
        "query": "sex=T&age=TOTAL&time=2024&time=2025",
        "timeout": 17,
    }
    arguments.update(overrides)
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=response(document if document is not None else dataset_document()),
    ) as urlopen:
        records = list(MODULE.fetch_eurostat_statistics(**arguments))
    return records, urlopen


def test_decodes_row_major_coordinates_labels_units_metadata_and_nulls():
    records, _ = fetch()

    assert len(records) == 8
    assert [record["dimensions"] for record in records] == [
        {"freq": "A", "unit": "NR", "geo": "DE", "time": "2024"},
        {"freq": "A", "unit": "NR", "geo": "DE", "time": "2025"},
        {"freq": "A", "unit": "NR", "geo": "FR", "time": "2024"},
        {"freq": "A", "unit": "NR", "geo": "FR", "time": "2025"},
        {"freq": "A", "unit": "PC", "geo": "DE", "time": "2024"},
        {"freq": "A", "unit": "PC", "geo": "DE", "time": "2025"},
        {"freq": "A", "unit": "PC", "geo": "FR", "time": "2024"},
        {"freq": "A", "unit": "PC", "geo": "FR", "time": "2025"},
    ]
    first = records[0]
    assert first["source"] == "eurostat_statistics"
    assert first["dataset_code"] == "demo_pjan"
    assert first["dataset_label"] == "Population on 1 January"
    assert first["dataset_source"] == "ESTAT"
    assert first["updated_at"] == "2026-09-15T23:00:00+0200"
    assert datetime.fromisoformat(first["fetched_at"]).tzinfo == timezone.utc
    assert first["period"] == "2024"
    assert first["unit"] == "NR"
    assert first["unit_label"] == "Number"
    assert first["category_units"] == {
        "unit": {"base": "number", "decimals": 0}
    }
    assert first["labels"] == {
        "freq": "Annual",
        "unit": "Number",
        "geo": "Germany",
        "time": "2024",
    }
    assert first["dimension_titles"] == {
        "freq": "Time frequency",
        "unit": "Unit of measure",
        "geo": "Geopolitical entity",
        "time": "Time",
    }
    assert first["metadata"]["note"] == ["Offline fixture"]
    assert first["value"] == 83_456_045
    assert first["status"] == "p"

    assert records[1]["value"] is None  # explicit JSON null
    assert records[1]["status"] is None
    assert records[2]["value"] is None  # omitted sparse cell
    assert records[2]["status"] is None
    assert records[-1]["value"] == 51.4
    assert records[-1]["status"] == "e"


def test_stable_ids_use_dataset_and_coordinate_codes_only():
    first, _ = fetch()
    changed_payload = dataset_document()
    changed_payload["value"]["0"] = 1
    changed_payload["dimension"]["geo"]["category"]["label"]["DE"] = "Allemagne"
    changed_payload["updated"] = "2026-09-16T23:00:00+0200"
    second, _ = fetch(changed_payload)

    assert first[0]["id"] == second[0]["id"]
    assert first[0]["id"] != first[1]["id"]
    assert len(first[0]["id"]) == 64
    assert first[0]["fetched_at"] != ""


def test_request_is_json_bounded_and_uses_project_user_agent():
    _, urlopen = fetch(
        query=(
            "geo=DE&geo=FR&sex=T&sinceTimePeriod=2024&"
            "untilTimePeriod=2025&lang=fr&format=JSON&compressed=false"
        )
    )

    request = urlopen.call_args.args[0]
    parsed = urllib.parse.urlsplit(request.full_url)
    assert parsed.path.endswith("/data/demo_pjan")
    assert urllib.parse.parse_qsl(parsed.query) == [
        ("geo", "DE"),
        ("geo", "FR"),
        ("sex", "T"),
        ("sinceTimePeriod", "2024"),
        ("untilTimePeriod", "2025"),
        ("lang", "fr"),
        ("format", "JSON"),
        ("compressed", "false"),
    ]
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("Accept-encoding") == "identity"
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert urlopen.call_args.kwargs == {"timeout": 17}


def test_default_json_controls_are_explicit_on_request():
    _, urlopen = fetch(query="geo=DE&time=2024")

    query = urllib.parse.parse_qs(urllib.parse.urlsplit(
        urlopen.call_args.args[0].full_url
    ).query)
    assert query["format"] == ["JSON"]
    assert query["lang"] == ["en"]


@pytest.mark.parametrize(
    "query",
    [
        "time=2024&format=JSON",
        "time=2024&lang=fr",
        "time=2024&compressed=false",
        "time=2024&format=JSON&lang=en&compressed=false",
    ],
)
def test_api_controls_do_not_count_as_dataset_dimension_filters(query):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match="non-time dataset dimension"):
            list(MODULE.fetch_eurostat_statistics("demo_pjan", query))
    urlopen.assert_not_called()


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ("geo=DE&format=TSV&time=2024", "format must be JSON"),
        ("geo=DE&lang=it&time=2024", "lang must be one of"),
        ("geo=DE&compressed=true&time=2024", "compressed must be false"),
        ("geo=DE&time=2024&sinceTimePeriod=2023&untilTimePeriod=2024", "cannot be combined"),
        ("geo=DE&sinceTimePeriod=2025&untilTimePeriod=2024", "must not be after"),
        ("geo=DE&sinceTimePeriod=2024", "requires time or both"),
        ("geo=DE", "requires time or both"),
        ("geo=DE&time=2024&time=2024", "must not be duplicated"),
        ("geo=&time=2024", "must not be empty"),
    ],
)
def test_invalid_queries_fail_before_http(query, message):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match=message):
            list(MODULE.fetch_eurostat_statistics("demo_pjan", query))
    urlopen.assert_not_called()


def test_legitimate_exact_and_range_queries_reach_http():
    exact, exact_open = fetch(query="geo=DE&time=2024&format=JSON&lang=en")
    bounded, bounded_open = fetch(
        query="geo=DE&sinceTimePeriod=2024&untilTimePeriod=2025&lang=de"
    )

    assert len(exact) == 8
    assert len(bounded) == 8
    exact_open.assert_called_once()
    bounded_open.assert_called_once()


@pytest.mark.parametrize("field", ["value", "status"])
def test_sparse_mappings_accept_valid_first_and_last_indexes(field):
    document = dataset_document()
    document[field] = {"0": None, "7": "last"}

    records, _ = fetch(document)

    assert len(records) == 8
    assert records[0][field] is None
    assert records[-1][field] == "last"


@pytest.mark.parametrize("field", ["value", "status"])
@pytest.mark.parametrize("key", ["-1", "8", "not-a-number", "01", "+1", 1])
def test_rejects_every_invalid_sparse_key_before_first_record(field, key):
    document = dataset_document()
    document[field] = {key: "bad"}
    iterator = MODULE.parse_response(
        document, "demo_pjan", "2026-09-17T12:00:00+00:00"
    )

    with pytest.raises(ValueError, match="sparse index"):
        next(iterator)


def test_empty_coordinate_response_is_empty():
    document = {
        "version": "2.0",
        "class": "dataset",
        "id": ["geo", "time"],
        "size": [0, 1],
        "dimension": {
            "geo": {"category": {"index": [], "label": {}}},
            "time": {"category": {"index": ["2024"], "label": {"2024": "2024"}}},
        },
        "value": {},
        "status": {},
    }

    assert list(MODULE.parse_response(
        document, "demo_pjan", "2026-09-17T12:00:00+00:00"
    )) == []


@pytest.mark.parametrize("field", ["value", "status"])
def test_empty_coordinate_response_rejects_any_sparse_index(field):
    document = {
        "class": "dataset",
        "id": ["geo"],
        "size": [0],
        "dimension": {"geo": {"category": {"index": []}}},
        "value": {},
        "status": {},
    }
    document[field] = {"0": None}

    with pytest.raises(ValueError, match=re.escape("outside [0, 0)")):
        list(MODULE.parse_response(
            document, "demo_pjan", "2026-09-17T12:00:00+00:00"
        ))


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda document: document.update({"class": "error"}), "class must be"),
        (lambda document: document.update({"size": [1]}), "size must match"),
        (lambda document: document["dimension"].pop("geo"), "missing dimension"),
        (
            lambda document: document["dimension"]["geo"]["category"].update(
                {"index": {"DE": 0, "FR": 0}}
            ),
            "invalid category indexes",
        ),
        (lambda document: document.update({"value": [1]}), "array length"),
        (lambda document: document.update({"status": "p"}), "array or sparse object"),
    ],
)
def test_rejects_malformed_json_stat_responses(mutation, message):
    document = dataset_document()
    mutation(document)

    with pytest.raises(ValueError, match=message):
        list(MODULE.parse_response(
            document, "demo_pjan", "2026-09-17T12:00:00+00:00"
        ))


def test_http_failure_propagates_without_emitting_records():
    error = urllib.error.HTTPError(MODULE.API_BASE, 503, "unavailable", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
        iterator = MODULE.fetch_eurostat_statistics(
            "demo_pjan", "geo=DE&time=2024"
        )
        with pytest.raises(urllib.error.HTTPError) as caught:
            next(iterator)
    assert caught.value.code == 503


def test_main_emits_one_compact_json_object_per_coordinate():
    stdout = io.StringIO()
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=response(dataset_document()),
    ):
        with contextlib.redirect_stdout(stdout):
            MODULE.main([
                "--dataset-code",
                "demo_pjan",
                "--query",
                "geo=DE&time=2024",
            ])

    lines = stdout.getvalue().splitlines()
    assert len(lines) == 8
    assert json.loads(lines[0])["dimensions"]["geo"] == "DE"
    assert ": " not in lines[0]
    assert ", " not in lines[0]


def test_source_configuration_is_disabled_bounded_and_fixed_cadence():
    config = CONFIG.read_text(encoding="utf-8")

    assert re.search(r"(?m)^enabled:\s*false\s*$", config)
    assert "--dataset-code" in config
    assert "--query" in config
    assert "sinceTimePeriod=2025" in config
    assert "untilTimePeriod=2026" in config
    assert re.search(r"(?ms)^cadence:\s*\n\s+auto:\s*false\s*$", config)
