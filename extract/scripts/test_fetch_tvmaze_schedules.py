import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import urllib.error
import urllib.parse
from unittest import mock

import pytest
import yaml


SCRIPT = Path(__file__).with_name("fetch_tvmaze_schedules.py")
CONFIG = SCRIPT.parents[1] / "sources" / "tvmaze_schedules.yml"
DAG_FACTORY = SCRIPT.parents[2] / "orchestration" / "dags" / "extract_dags.py"
SPEC = importlib.util.spec_from_file_location("fetch_tvmaze_schedules", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

FETCHED_AT = "2026-09-17T12:34:56+00:00"
SCHEDULE_DATE = "2026-09-16"


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def episode(*, episode_id=101, show_id=55, include_optional=True):
    item = {
        "id": episode_id,
        "name": "The Return",
        "airdate": SCHEDULE_DATE,
        "airstamp": "2026-09-17T00:00:00+00:00",
        "show": {"id": show_id, "name": "Example Show"},
    }
    if not include_optional:
        return item
    item.update(
        {
            "url": "https://www.tvmaze.com/episodes/101/the-return",
            "season": 2,
            "number": 3,
            "type": "regular",
            "airtime": "20:00",
            "runtime": 60,
            "rating": {"average": 8.1},
            "image": {
                "medium": "https://static.tvmaze.com/uploads/images/medium/1.jpg",
                "original": "https://static.tvmaze.com/uploads/images/original/1.jpg",
            },
            "summary": "<p>An episode summary.</p>",
            "_links": {
                "self": {"href": "https://api.tvmaze.com/episodes/101"},
                "show": {"href": "https://api.tvmaze.com/shows/55"},
            },
        }
    )
    item["show"].update(
        {
            "url": "https://www.tvmaze.com/shows/55/example-show",
            "type": "Scripted",
            "language": "English",
            "genres": ["Drama", "Mystery"],
            "status": "Running",
            "runtime": 60,
            "averageRuntime": 58,
            "premiered": "2024-01-02",
            "ended": None,
            "officialSite": "https://example.test/show",
            "schedule": {"time": "20:00", "days": ["Wednesday"]},
            "rating": {"average": 7.8},
            "weight": 91,
            "network": {
                "id": 12,
                "name": "Example Network",
                "country": {
                    "name": "United States",
                    "code": "US",
                    "timezone": "America/New_York",
                },
                "officialSite": "https://network.example.test/",
            },
            "webChannel": None,
            "dvdCountry": None,
            "externals": {"tvrage": None, "thetvdb": 987, "imdb": "tt1234567"},
            "image": {
                "medium": "https://static.tvmaze.com/uploads/images/medium/show.jpg",
                "original": "https://static.tvmaze.com/uploads/images/original/show.jpg",
            },
            "summary": "<p>A show summary.</p>",
            "updated": 1789000000,
            "_links": {
                "self": {"href": "https://api.tvmaze.com/shows/55"},
                "previousepisode": {
                    "href": "https://api.tvmaze.com/episodes/100"
                },
            },
        }
    )
    return item


def fetch(document, **overrides):
    arguments = {
        "country": "US",
        "schedule_date": SCHEDULE_DATE,
        "timeout": 17,
        "fetched_at": FETCHED_AT,
    }
    arguments.update(overrides)
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(document)
    ) as urlopen:
        records = MODULE.fetch_tvmaze_schedules(**arguments)
    return records, urlopen


def test_normalizes_complete_episode_show_network_images_summaries_and_links():
    records, urlopen = fetch([episode()])

    assert len(records) == 1
    record = records[0]
    assert record["source"] == "tvmaze_schedules"
    assert record["fetched_at"] == FETCHED_AT
    assert record["id"] == "101"
    assert record["episode_id"] == 101
    assert record["show_id"] == 55
    assert record["requested_country"] == "US"
    assert record["requested_date"] == SCHEDULE_DATE
    assert record["episode_name"] == "The Return"
    assert record["season"] == 2
    assert record["number"] == 3
    assert record["airdate"] == SCHEDULE_DATE
    assert record["airtime"] == "20:00"
    assert record["airstamp"] == "2026-09-17T00:00:00+00:00"
    assert record["runtime_minutes"] == 60
    assert record["rating_average"] == 8.1
    assert record["summary"] == "<p>An episode summary.</p>"
    assert record["image"]["original"].endswith("/1.jpg")
    assert record["links"] == {
        "self": "https://api.tvmaze.com/episodes/101",
        "show": "https://api.tvmaze.com/shows/55",
    }

    show = record["show"]
    assert show["id"] == 55
    assert show["name"] == "Example Show"
    assert show["runtime_minutes"] == 60
    assert show["average_runtime_minutes"] == 58
    assert show["summary"] == "<p>A show summary.</p>"
    assert show["image"]["medium"].endswith("/show.jpg")
    assert show["external_ids"] == {
        "tvrage": None,
        "thetvdb": 987,
        "imdb": "tt1234567",
    }
    assert show["network"] == {
        "id": 12,
        "name": "Example Network",
        "country": {
            "name": "United States",
            "code": "US",
            "timezone": "America/New_York",
        },
        "official_site": "https://network.example.test/",
    }
    assert show["web_channel"] is None

    request = urlopen.call_args.args[0]
    assert request.get_method() == "GET"
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query) == {
        "country": ["US"],
        "date": [SCHEDULE_DATE],
    }
    assert urlopen.call_args.kwargs == {"timeout": 17.0}


def test_episode_identity_is_stable_across_repeated_polls():
    first, _ = fetch([episode()], fetched_at="2026-09-17T01:00:00Z")
    second, _ = fetch([episode()], fetched_at="2026-09-17T07:00:00Z")

    assert first[0]["id"] == second[0]["id"] == "101"
    assert first[0]["episode_id"] == second[0]["episode_id"] == 101
    assert first[0]["fetched_at"] != second[0]["fetched_at"]


def test_empty_schedule_is_successful():
    records, urlopen = fetch([])
    assert records == []
    urlopen.assert_called_once()


def test_absent_optional_metadata_is_emitted_as_null_not_rejected():
    records, _ = fetch([episode(include_optional=False)])
    record = records[0]

    assert record["season"] is None
    assert record["number"] is None
    assert record["episode_type"] is None
    assert record["airtime"] is None
    assert record["runtime_minutes"] is None
    assert record["rating_average"] is None
    assert record["summary"] is None
    assert record["image"] is None
    assert record["url"] is None
    assert record["links"] is None
    assert record["show"]["network"] is None
    assert record["show"]["web_channel"] is None
    assert record["show"]["genres"] is None
    assert record["show"]["summary"] is None


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda item: item.pop("id"), r"episode 0.id must be a positive integer"),
        (lambda item: item.update(id=True), r"episode 0.id must be a positive integer"),
        (lambda item: item.update(name=""), r"episode 0.name must be a non-empty string"),
        (lambda item: item.update(airdate="2026-02-30"), r"episode 0.airdate"),
        (lambda item: item.update(airstamp="2026-09-17T00:00:00"), r"timezone-aware"),
        (lambda item: item.update(show=None), r"episode 0.show must be an object"),
        (
            lambda item: item["show"].update(id="55"),
            r"episode 0.show.id must be a positive integer",
        ),
        (
            lambda item: item["show"].update(name=None),
            r"episode 0.show.name must be a non-empty string",
        ),
    ],
)
def test_rejects_malformed_required_fields(mutate, message):
    item = episode(include_optional=False)
    mutate(item)
    with pytest.raises(MODULE.TVMazeSchemaError, match=message):
        fetch([item])


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda item: item.update(runtime="60"), r"runtime must be a non-negative integer"),
        (lambda item: item.update(image=[]), r"image must be an object"),
        (lambda item: item.update(url="not-a-url"), r"must be an HTTP\(S\) URL"),
        (lambda item: item.update(rating={"average": "good"}), r"rating.average"),
        (lambda item: item["show"].update(genres="Drama"), r"genres must be a list"),
        (lambda item: item["show"].update(network=[]), r"network must be an object"),
    ],
)
def test_rejects_present_but_malformed_optional_metadata(mutate, message):
    item = episode()
    mutate(item)
    with pytest.raises(MODULE.TVMazeSchemaError, match=message):
        fetch([item])


def test_rejects_non_list_response_and_duplicate_episode_ids():
    with pytest.raises(MODULE.TVMazeSchemaError, match="must be a JSON list"):
        fetch({"id": 101})

    with pytest.raises(MODULE.TVMazeSchemaError, match="duplicate episode id 101"):
        fetch([episode(), episode(show_id=56)])


def test_http_network_json_and_schema_failures_are_distinct():
    http_error = urllib.error.HTTPError(
        MODULE.API_URL, 503, "service unavailable", {}, None
    )
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=http_error):
        with pytest.raises(MODULE.TVMazeHTTPError, match="HTTP 503"):
            MODULE.fetch_tvmaze_schedules("US", SCHEDULE_DATE)

    network_error = urllib.error.URLError("temporary DNS failure")
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=network_error):
        with pytest.raises(MODULE.TVMazeNetworkError, match="network request failed"):
            MODULE.fetch_tvmaze_schedules("US", SCHEDULE_DATE)

    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=Response(b"not-json")
    ):
        with pytest.raises(MODULE.TVMazeJSONError, match="invalid JSON"):
            MODULE.fetch_tvmaze_schedules("US", SCHEDULE_DATE)

    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response({"unexpected": []})
    ):
        with pytest.raises(MODULE.TVMazeSchemaError, match="must be a JSON list"):
            MODULE.fetch_tvmaze_schedules("US", SCHEDULE_DATE)


def test_defaults_to_current_utc_date_and_us_country():
    with (
        mock.patch.object(MODULE, "_utc_today", return_value="2026-09-17") as utc_today,
        mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([])) as urlopen,
    ):
        assert MODULE.fetch_tvmaze_schedules(fetched_at=FETCHED_AT) == []

    utc_today.assert_called_once_with()
    request = urlopen.call_args.args[0]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert query == {"country": ["US"], "date": ["2026-09-17"]}
    assert urlopen.call_args.kwargs == {"timeout": 30.0}


def test_explicit_arguments_are_normalized_and_timeout_is_bounded():
    records, urlopen = fetch([], country="ca", schedule_date="2025-12-31", timeout=0.5)
    assert records == []
    query = urllib.parse.parse_qs(
        urllib.parse.urlsplit(urlopen.call_args.args[0].full_url).query
    )
    assert query == {"country": ["CA"], "date": ["2025-12-31"]}
    assert urlopen.call_args.kwargs == {"timeout": 0.5}

    for arguments, message in (
        ({"country": "USA"}, "two-letter"),
        ({"schedule_date": "2026-9-1"}, "zero-padded"),
        ({"schedule_date": "2026-02-30"}, "valid date"),
        ({"timeout": 0}, "greater than zero"),
        ({"timeout": MODULE.MAX_TIMEOUT + 1}, "at most 120"),
    ):
        with mock.patch.object(MODULE.urllib.request, "urlopen") as no_request:
            with pytest.raises(ValueError, match=message):
                MODULE.fetch_tvmaze_schedules(**arguments)
        no_request.assert_not_called()


def test_cli_emits_compact_repository_envelope_ndjson(capsys):
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response([episode()])
    ):
        status = MODULE.main(
            ["--country", "ca", "--date", SCHEDULE_DATE, "--timeout", "9"]
        )

    captured = capsys.readouterr()
    assert status == 0
    assert captured.err == ""
    lines = captured.out.splitlines()
    assert len(lines) == 1
    assert ": " not in lines[0]
    record = json.loads(lines[0])
    assert record["source"] == "tvmaze_schedules"
    assert record["id"] == "101"
    assert record["requested_country"] == "CA"
    assert record["requested_date"] == SCHEDULE_DATE
    assert record["fetched_at"]


def test_cli_preserves_failure_status_and_bounds_single_line_stderr(capsys):
    reason = "secret\n" + "x" * 1000
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        side_effect=urllib.error.URLError(reason),
    ):
        status = MODULE.main(["--date", SCHEDULE_DATE])

    captured = capsys.readouterr()
    assert status == 1
    assert captured.out == ""
    assert len(captured.err.splitlines()) == 1
    assert len(captured.err) <= MODULE.MAX_ERROR_DETAIL + len("error: \n")
    assert "network request failed" in captured.err
    assert "secret" not in captured.err
    assert "x" * 20 not in captured.err


def test_yaml_is_valid_bounded_disabled_and_matches_extractor_arguments():
    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))

    assert config["name"] == MODULE.SOURCE
    assert config["script"] == SCRIPT.name
    assert config["enabled"] is False
    assert config["schedule"] == "37 */6 * * *"
    assert config["cadence"] == {"auto": False}
    assert config["sink"] == "local"
    assert config["timeout_minutes"] == 5
    assert (SCRIPT.parent / config["script"]).is_file()
    parsed = MODULE.build_parser().parse_args(config["args"])
    assert parsed.country == "US"
    assert parsed.schedule_date is None
    assert 0 < parsed.timeout <= MODULE.MAX_TIMEOUT

    text = CONFIG.read_text(encoding="utf-8")
    assert "Do not enable until" in text
    assert "live" in text
    assert "external acceptance" in text
    assert "full-schedule" in text


def test_isolated_dag_factory_resolves_script_cadence_and_paused_activation():
    class FakeDAG:
        current = None

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.tasks = []

        def __enter__(self):
            type(self).current = self
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            type(self).current = None

    class FakePythonOperator:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            assert FakeDAG.current is not None
            FakeDAG.current.tasks.append(self)

    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG
    python_operator = types.ModuleType("airflow.providers.standard.operators.python")
    python_operator.PythonOperator = FakePythonOperator
    pendulum = types.ModuleType("pendulum")
    pendulum.datetime = lambda *args, **kwargs: (args, kwargs)
    cadence_plan = types.ModuleType("cadence_plan")
    cadence_plan.plan_sources = lambda: {}
    cadence_plan.effective_schedule = lambda cfg, plan: (cfg["schedule"], "fixed")
    extract_runner = types.ModuleType("extract_runner")
    extract_runner.SCRIPTS_DIR = SCRIPT.parent
    extract_runner.run = lambda cfg: cfg

    fake_modules = {
        "airflow": airflow,
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.standard": types.ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.operators": types.ModuleType(
            "airflow.providers.standard.operators"
        ),
        "airflow.providers.standard.operators.python": python_operator,
        "pendulum": pendulum,
        "cadence_plan": cadence_plan,
        "extract_runner": extract_runner,
    }
    dag_spec = importlib.util.spec_from_file_location(
        "isolated_tvmaze_extract_dags", DAG_FACTORY
    )
    dag_module = importlib.util.module_from_spec(dag_spec)
    assert dag_spec.loader is not None
    with (
        mock.patch.dict(sys.modules, fake_modules),
        mock.patch.object(Path, "glob", return_value=[CONFIG]),
    ):
        dag_spec.loader.exec_module(dag_module)

    dag = dag_module.extract__tvmaze_schedules
    assert dag.dag_id == "extract__tvmaze_schedules"
    assert dag.schedule == "37 */6 * * *"
    assert dag.is_paused_upon_creation is True
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert len(dag.tasks) == 1
    task = dag.tasks[0]
    assert task.op_kwargs["cfg"]["enabled"] is False
    assert task.op_kwargs["cfg"]["script"] == SCRIPT.name
    assert (extract_runner.SCRIPTS_DIR / task.op_kwargs["cfg"]["script"]).is_file()
    assert task.execution_timeout.total_seconds() == 300
