import contextlib
from datetime import datetime, timezone
import importlib.util
import io
import json
from pathlib import Path
import urllib.error
import urllib.parse
from unittest import mock

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "fetch_inaturalist_observations.py"
SPEC = importlib.util.spec_from_file_location("fetch_inaturalist_observations", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
FETCHED_AT = "2026-09-17T12:34:56+00:00"


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    payload = document if isinstance(document, bytes) else json.dumps(document).encode("utf-8")
    return Response(payload)


def observation(observation_id=101, **changes):
    value = {
        "id": observation_id,
        "observed_on": "2026-09-16",
        "time_observed_at": "2026-09-16T08:30:00-07:00",
        "observed_on_details": {"date": "2026-09-16", "hour": 8},
        "created_at": "2026-09-16T16:00:00+00:00",
        "updated_at": "2026-09-16T17:00:00+00:00",
        "taxon": {
            "id": 47158,
            "name": "Corvus brachyrhynchos",
            "preferred_common_name": "American Crow",
            "rank": "species",
            "rank_level": 10,
            "iconic_taxon_name": "Aves",
            "ancestry": "48460/1/2/3",
        },
        "user": {"id": 9, "login": "observer", "name": "Observer Name"},
        "location": "40.1,-74.2",
        "geojson": {"type": "Point", "coordinates": [-74.2, 40.1]},
        "positional_accuracy": 25,
        "geoprivacy": "obscured",
        "taxon_geoprivacy": None,
        "obscured": True,
        "quality_grade": "research",
        "identifications_count": 4,
        "num_identification_agreements": 3,
        "num_identification_disagreements": 1,
        "identifications_most_agree": True,
        "identifications_some_agree": True,
        "quality_metrics": [{"metric": "wild", "agree": True}],

        "captive": False,
        "mappable": True,
        "cached_votes_total": 2,
        "description": "Calling from an oak.",
        "place_guess": "New Jersey, USA",
        "uri": f"https://www.inaturalist.org/observations/{observation_id}",
        "license_code": "cc-by-nc",
        "photos": [
            {
                "id": 700,
                "license_code": "cc-by",
                "attribution": "(c) Observer, some rights reserved (CC BY)",
                "url": "https://static.inaturalist.org/photos/700/square.jpg",
                "original_dimensions": {"width": 2000, "height": 1000},
            }
        ],
        "sounds": [
            {
                "id": 800,
                "license_code": "cc0",
                "attribution": "Observer, CC0",
                "file_url": "https://static.inaturalist.org/sounds/800.mp3",
            }
        ],

        "private_location": "39.999,-74.111",
        "private_geojson": {"type": "Point", "coordinates": [-74.111, 39.999]},
    }
    value.update(changes)
    return value


def page(results, *, number=1, per_page=2, total=None):
    return {
        "total_results": len(results) if total is None else total,
        "page": number,
        "per_page": per_page,
        "results": results,
    }


def fetch(documents, **changes):
    responses = [response(document) for document in documents]
    arguments = {
        "taxon_id": 3,
        "per_page": 2,
        "max_pages": 3,
        "timeout": 17,
        "retries": 0,
        "fetched_at": FETCHED_AT,
    }
    arguments.update(changes)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
        records = list(MODULE.fetch_observations(**arguments))
    return records, urlopen


def test_requests_taxon_filtered_newest_first_pages_with_timeout_and_headers():
    records, urlopen = fetch(
        [
            page([observation(101), observation(102)], total=3),
            page([observation(103)], number=2, total=3),
        ]
    )

    assert [record["id"] for record in records] == [101, 102, 103]
    assert urlopen.call_count == 2
    for page_number, call in enumerate(urlopen.call_args_list, 1):
        request = call.args[0]
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
        assert urllib.parse.urlsplit(request.full_url).path == "/v1/observations"
        assert query == {
            "taxon_id": ["3"],
            "page": [str(page_number)],
            "per_page": ["2"],
            "order_by": ["created_at"],
            "order": ["desc"],
        }
        assert request.get_header("Accept") == "application/json"
        assert request.get_header("User-agent") == MODULE.USER_AGENT
        assert call.kwargs == {"timeout": 17.0}


def test_stops_on_empty_short_and_total_complete_pages():
    cases = (
        ([page([], total=20)], 0),
        ([page([observation()], total=20)], 1),
        ([page([observation(1), observation(2)], total=2)], 2),
    )
    for documents, expected_records in cases:
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[response(document) for document in documents],
        ) as urlopen:
            records = list(
                MODULE.fetch_observations(
                    3, per_page=2, max_pages=3, retries=0, fetched_at=FETCHED_AT
                )
            )
        assert len(records) == expected_records
        assert urlopen.call_count == 1


def test_hard_page_cap_stops_a_larger_snapshot():
    documents = [
        page([observation(1), observation(2)], number=1, total=100),
        page([observation(3), observation(4)], number=2, total=100),
    ]
    records, urlopen = fetch(documents, max_pages=2)
    assert [record["id"] for record in records] == [1, 2, 3, 4]
    assert urlopen.call_count == 2


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"taxon_id": 0}, "taxon_id"),
        ({"taxon_id": True}, "taxon_id"),
        ({"per_page": 0}, "per_page"),
        ({"per_page": MODULE.MAX_PER_PAGE + 1}, "per_page"),
        ({"max_pages": 0}, "max_pages"),
        ({"max_pages": MODULE.MAX_PAGES + 1}, "max_pages"),
        ({"timeout": 0}, "timeout"),
        ({"timeout": MODULE.MAX_TIMEOUT + 1}, "timeout"),
        ({"retries": -1}, "retries"),
        ({"retries": MODULE.MAX_RETRIES + 1}, "retries"),
        ({"retry_backoff": -1}, "retry_backoff"),
        ({"fetched_at": ""}, "fetched_at"),
        ({"fetched_at": "2026-09-17T12:34:56"}, "UTC timestamp"),
        ({"fetched_at": "2026-09-17T12:34:56-04:00"}, "UTC timestamp"),

    ],
)
def test_invalid_arguments_fail_before_http(changes, message):
    arguments = {
        "taxon_id": 3,
        "per_page": 2,
        "max_pages": 1,
        "retries": 0,
        "fetched_at": FETCHED_AT,
    }
    arguments.update(changes)
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match=message):
            list(MODULE.fetch_observations(**arguments))
    urlopen.assert_not_called()


@pytest.mark.parametrize(
    "document, message",
    [
        ([], "must be an object"),
        ({"page": 1, "per_page": 2, "total_results": 0}, "results must be a list"),
        (page([], number=2), "page metadata"),
        ({**page([]), "per_page": 1}, "per_page metadata"),
        ({**page([]), "total_results": -1}, "total_results"),
        (page([observation(1), observation(2), observation(3)], per_page=2), "contradicts"),
        (page([{"id": None}], per_page=2), "invalid id"),
        (page([observation(1, geojson={"coordinates": ["x", 2]})]), "geojson"),
        (page([observation(1), observation(1)], total=2), "repeated observation"),
    ],
)
def test_rejects_malformed_responses(document, message):
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(document)
    ):
        with pytest.raises(MODULE.INaturalistError, match=message):
            list(
                MODULE.fetch_observations(
                    3, per_page=2, max_pages=1, retries=0, fetched_at=FETCHED_AT
                )
            )


def test_malformed_json_and_oversize_responses_fail_without_output():
    cases = (
        (response(b"not-json"), "malformed JSON"),
        (response(b" " * (MODULE.MAX_RESPONSE_BYTES + 1)), "exceeded"),
    )
    for api_response, message in cases:
        stdout = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=api_response):
            with contextlib.redirect_stdout(stdout):
                with pytest.raises(MODULE.INaturalistError, match=message):
                    list(MODULE.fetch_observations(3, retries=0))
        assert stdout.getvalue() == ""


def test_nullable_optional_metadata_is_preserved_without_fabrication():
    original = observation(
        taxon=None,
        user=None,
        photos=None,
        location=None,
        geojson=None,
        observed_on=None,
        time_observed_at=None,
        license_code=None,
        uri=None,
    )
    record = MODULE.normalize_observation(original, FETCHED_AT)

    assert record["id"] == 101
    assert record["taxon"] is None
    assert record["taxon_id"] is None
    assert record["observer"] is None
    assert record["photos"] is None
    assert record["latitude"] is None
    assert record["longitude"] is None
    assert record["license_code"] is None
    assert "private_location" not in record
    assert "private_geojson" not in record


def test_preserves_public_identity_quality_location_and_attribution_without_media_urls():
    record = MODULE.normalize_observation(observation(), FETCHED_AT)

    assert record["source"] == "inaturalist_observations"
    assert record["id"] == record["observation_id"] == 101
    assert record["taxon_id"] == 47158
    assert record["taxon_name"] == "Corvus brachyrhynchos"
    assert (record["latitude"], record["longitude"]) == (40.1, -74.2)
    assert record["obscured"] is True
    assert record["geoprivacy"] == "obscured"
    assert record["quality_grade"] == "research"
    assert record["uri"].endswith("/101")
    assert record["license_code"] == "cc-by-nc"
    assert record["observer"]["login"] == "observer"
    assert record["photos"] == [
        {
            "id": 700,
            "license_code": "cc-by",
            "attribution": "(c) Observer, some rights reserved (CC BY)",
        }
    ]
    assert "url" not in record["photos"][0]
    assert record["identifications_most_agree"] is True
    assert record["quality_metrics"] == [{"metric": "wild", "agree": True}]
    assert record["sounds"] == [
        {"id": 800, "license_code": "cc0", "attribution": "Observer, CC0"}
    ]
    assert "file_url" not in record["sounds"][0]


def test_ids_are_stable_and_all_records_share_one_utc_run_timestamp():
    first, _ = fetch([page([observation(11), observation(12)], total=2)])
    second, _ = fetch(
        [page([observation(11), observation(12)], total=2)],
        fetched_at="2026-09-18T00:00:00+00:00",
    )

    assert [record["id"] for record in first] == [record["id"] for record in second]
    assert {record["fetched_at"] for record in first} == {FETCHED_AT}
    assert datetime.fromisoformat(first[0]["fetched_at"]).tzinfo == timezone.utc


def http_error(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.API_URL, code, "error", headers, None)


def test_retries_rate_limits_with_retry_after_then_succeeds():
    sleeps = []
    effects = [
        http_error(429, "2"),
        response(page([observation()], per_page=1, total=1)),
    ]
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=effects) as urlopen, mock.patch.object(
        MODULE.time, "sleep", side_effect=sleeps.append
    ):
        records = list(
            MODULE.fetch_observations(
                3,
                per_page=1,
                max_pages=1,
                retries=1,
                retry_backoff=0.25,
                fetched_at=FETCHED_AT,
            )
        )
    assert [record["id"] for record in records] == [101]
    assert urlopen.call_count == 2
    assert sleeps == [2.0]


def test_transient_errors_use_bounded_exponential_retries_and_stop():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=http_error(503)
    ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
        with pytest.raises(MODULE.INaturalistError, match="HTTP 503"):
            list(
                MODULE.fetch_observations(
                    3, retries=2, retry_backoff=0.5, fetched_at=FETCHED_AT
                )
            )
    assert urlopen.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [0.5, 1.0]


def test_nonretryable_http_error_is_not_retried():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=http_error(404)
    ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
        with pytest.raises(MODULE.INaturalistError, match="HTTP 404"):
            list(MODULE.fetch_observations(3, retries=3, fetched_at=FETCHED_AT))
    assert urlopen.call_count == 1
    sleep.assert_not_called()


def test_retry_after_above_bound_fails_without_sleeping():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=http_error(429, "61")
    ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
        with pytest.raises(MODULE.INaturalistError, match="above the 60s bound"):
            list(MODULE.fetch_observations(3, retries=3, fetched_at=FETCHED_AT))
    assert urlopen.call_count == 1
    sleep.assert_not_called()


def test_main_emits_valid_compact_ndjson_only_after_complete_validation():
    stdout = io.StringIO()
    document = page([observation(1), observation(2)], per_page=2, total=2)
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)), mock.patch.object(
        MODULE, "datetime"
    ) as clock, contextlib.redirect_stdout(stdout):
        clock.now.return_value = datetime.fromisoformat(FETCHED_AT)
        MODULE.main(["--taxon-id", "3", "--per-page", "2", "--max-pages", "1", "--retries", "0"])

    lines = stdout.getvalue().splitlines()
    assert len(lines) == 2
    assert [json.loads(line)["id"] for line in lines] == [1, 2]
    assert all(json.loads(line)["source"] == MODULE.SOURCE for line in lines)
    assert all(": " not in line for line in lines)


def test_main_buffers_pages_so_a_later_failure_emits_no_partial_snapshot():
    stdout = io.StringIO()
    effects = [
        response(page([observation(1), observation(2)], total=3)),
        response(b"not-json"),
    ]
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=effects), contextlib.redirect_stdout(stdout):
        with pytest.raises(SystemExit) as raised:
            MODULE.main(
                [
                    "--taxon-id",
                    "3",
                    "--per-page",
                    "2",
                    "--max-pages",
                    "2",
                    "--retries",
                    "0",
                ]
            )
    assert raised.value.code == 1
    assert stdout.getvalue() == ""
