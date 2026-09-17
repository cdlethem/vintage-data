from __future__ import annotations

import importlib.util
import io
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import urllib.error
from datetime import datetime, timezone
from unittest import mock

import duckdb
import pytest
import yaml


SCRIPT = pathlib.Path(__file__).with_name("fetch_noaa_nhc_current_storms.py")
REPO_ROOT = SCRIPT.parents[2]
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "noaa_nhc_current_storms.yml"
BASE_SQL = REPO_ROOT / "transform" / "models" / "base" / "base_noaa_nhc_current_storms.sql"
BASE_YAML = BASE_SQL.with_suffix(".yml")
LOAD_ROOT = REPO_ROOT / "load"
SPEC = importlib.util.spec_from_file_location("fetch_noaa_nhc_current_storms", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 9, 17, 12, 34, 56, 123456, tzinfo=timezone.utc)


def response(document):
    if isinstance(document, bytes):
        return Response(document)
    return Response(json.dumps(document).encode("utf-8"))


def storm(**changes):
    value = {
        "id": "al072026",
        "name": "GABRIELLE",
        "classification": "HU",
        "intensity": "85",
        "pressure": "972",
        "latitude": "24.5N",
        "longitude": "63.2W",
        "movementDir": 315,
        "movementSpeed": "9",
        "lastUpdate": "2026-09-17T09:00:00-04:00",
        "publicAdvisory": {"url": "https://www.nhc.noaa.gov/text/MIATCPAT2.shtml"},
        "forecastAdvisory": {"url": "https://www.nhc.noaa.gov/text/MIATCMAT2.shtml"},
        "windSpeedProbabilities": {
            "url": "https://www.nhc.noaa.gov/text/MIAPWSAT2.shtml"
        },
        "futureField": {"kept": True},
    }
    value.update(changes)
    return value


def document(storms=None, **metadata):
    value = {
        "activeStorms": [storm()] if storms is None else storms,
        "generatedAt": "2026-09-17T13:01:00Z",
        "futureResponseField": ["kept"],
    }
    value.update(metadata)
    return value


def fetch(payload=None, **changes):
    arguments = {"timeout": 19}
    arguments.update(changes)
    with mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=response(document() if payload is None else payload),
    ) as urlopen:
        records = list(MODULE.fetch_current_storms(**arguments))
    return records, urlopen


def test_populated_feed_normalizes_units_times_links_and_preserves_unknown_fields():
    records, urlopen = fetch()

    assert len(records) == 1
    record = records[0]
    assert record == {
        "source": "noaa_nhc_current_storms",
        "id": record["id"],
        "fetched_at": "2026-09-17T12:34:56.123456+00:00",
        "storm_id": "al072026",
        "name": "GABRIELLE",
        "basin": "AL",
        "classification": "HU",
        "intensity_kt": 85,
        "pressure_mb": 972,
        "latitude": 24.5,
        "longitude": -63.2,
        "movement_direction_degrees": 315,
        "movement_speed_kt": 9,
        "advisory_at": "2026-09-17T13:00:00+00:00",
        "public_advisory_url": "https://www.nhc.noaa.gov/text/MIATCPAT2.shtml",
        "forecast_advisory_url": "https://www.nhc.noaa.gov/text/MIATCMAT2.shtml",
        "wind_speed_probabilities_url": "https://www.nhc.noaa.gov/text/MIAPWSAT2.shtml",
        "raw_storm": document()["activeStorms"][0],
        "raw_response": document(),
    }
    assert re.fullmatch(r"[0-9a-f]{64}", record["id"])
    assert record["raw_storm"]["futureField"] == {"kept": True}
    assert record["raw_response"]["futureResponseField"] == ["kept"]

    request = urlopen.call_args.args[0]
    assert request.full_url == MODULE.URL
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert urlopen.call_args.kwargs == {"timeout": 19}


def test_optional_fields_remain_nullable_and_numeric_coordinates_are_supported():
    sparse = {
        "id": "ep012026",
        "name": None,
        "basin": "EP",
        "classification": None,
        "latitudeNumeric": 10.25,
        "longitudeNumeric": -120.75,
    }
    record = fetch(document([sparse]))[0][0]

    assert record["storm_id"] == "ep012026"
    assert record["basin"] == "EP"
    assert record["latitude"] == 10.25
    assert record["longitude"] == -120.75
    for key in (
        "name",
        "classification",
        "intensity_kt",
        "pressure_mb",
        "movement_direction_degrees",
        "movement_speed_kt",
        "advisory_at",
        "public_advisory_url",
        "forecast_advisory_url",
        "wind_speed_probabilities_url",
    ):
        assert record[key] is None


def test_valid_empty_storm_list_succeeds_without_fabricated_rows():
    records, _ = fetch(document([]))
    assert records == []


def test_official_identifier_is_stable_while_fetch_time_keys_each_snapshot():
    original = storm()
    changed = storm(name="RENAMED", intensity=100)
    first = MODULE.normalize_storm(original, "2026-09-17T12:00:00+00:00", document([original]))
    same_fetch = MODULE.normalize_storm(changed, "2026-09-17T12:00:00+00:00", document([changed]))
    later = MODULE.normalize_storm(original, "2026-09-17T13:00:00+00:00", document([original]))

    assert first["storm_id"] == same_fetch["storm_id"] == later["storm_id"] == "al072026"
    assert first["id"] == same_fetch["id"]
    assert first["id"] != later["id"]
    assert first["advisory_at"] == later["advisory_at"]
    assert first["fetched_at"] != later["fetched_at"]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "response is not an object"),
        ({}, "missing its activeStorms list"),
        ({"activeStorms": {}}, "missing its activeStorms list"),
        (document([None]), "active storm is not an object"),
        (document([{}]), "missing its official id"),
        (document([storm(), storm()]), "repeats storm id"),
        (document([storm(lastUpdate="yesterday")]), "not a valid ISO-8601 timestamp"),
        (document([storm(intensity="strong")]), "intensity is not numeric"),
        (document([storm(latitude="north")]), "latitude is not a valid coordinate"),
        (document([storm(publicAdvisory=[])]), "publicAdvisory must be an object"),
    ],
)
def test_malformed_responses_fail_explicitly(payload, message):
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(payload)):
        with pytest.raises(ValueError, match=message):
            list(MODULE.fetch_current_storms())


def test_malformed_json_fails_explicitly():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(b"{not-json")
    ):
        with pytest.raises(ValueError, match="malformed JSON"):
            list(MODULE.fetch_current_storms())


def test_http_errors_retain_status_and_are_not_converted_to_empty_snapshots():
    error = urllib.error.HTTPError(MODULE.URL, 503, "unavailable", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(urllib.error.HTTPError) as caught:
            list(MODULE.fetch_current_storms())
    assert caught.value.code == 503


def test_invalid_timeout_fails_before_http():
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match="timeout must be positive"):
            list(MODULE.fetch_current_storms(timeout=0))
    urlopen.assert_not_called()


def test_main_emits_compact_ndjson_only_after_complete_validation(capsys):
    with mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=response(document([storm(), storm(id="ep012026")])),
    ):
        MODULE.main(["--timeout", "7"])
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2
    assert [json.loads(line)["storm_id"] for line in lines] == ["al072026", "ep012026"]
    assert all(": " not in line for line in lines)

    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=response(document([storm(), {"name": "missing id"}])),
    ):
        with pytest.raises(SystemExit) as caught:
            MODULE.main([])
    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == ""
    assert "missing its official id" in captured.err


def test_source_registration_is_paused_hourly_and_documents_replay_and_external_gates():
    config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
    assert config["name"] == MODULE.SOURCE
    assert config["script"] == SCRIPT.name
    assert config["enabled"] is False
    assert config["schedule"].split() == ["23", "*", "*", "*", "*"]
    assert config["args"] == ["--timeout", "30"]
    assert "ledger-idempotent" in config["replay_note"]
    assert "live endpoint smoke" in config["activation_note"]
    assert "raw load" in config["activation_note"]
    assert "scheduling approval" in config["activation_note"]


def _load_snapshot(raw_db: pathlib.Path, path: pathlib.Path, records, batch: str):
    sys.path.insert(0, str(LOAD_ROOT))
    try:
        from loader.config import SourceSettings
        from loader.destinations.base import LoadRequest
        from loader.destinations.duckdb_dest import DuckDBDestination
        from loader.discovery import SinkFile
        from loader.schema import infer_columns, table_columns
    finally:
        sys.path.pop(0)

    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    stat = path.stat()
    file = SinkFile(
        source=MODULE.SOURCE,
        path=path,
        dt="2026-09-17",
        batch_id=batch,
        size_bytes=stat.st_size,
        mtime=stat.st_mtime,
        records=len(records),
        extract_started_at="2026-09-17T12:00:00+00:00",
        script=SCRIPT.name,
    )
    settings = SourceSettings(
        name=MODULE.SOURCE,
        sample_lines=0,
        min_age_s=0,
        column_types={"source": "string", "id": "string", "fetched_at": "timestamp"},
    )
    columns = table_columns(infer_columns(records, settings), settings)
    destination = DuckDBDestination(
        {"name": "test", "database": str(raw_db), "lock_timeout_s": 1},
        "raw",
        "_load",
    )
    destination.connect()
    destination.ensure_schemas()
    columns = destination.sync_table(MODULE.SOURCE, columns, batch)
    result = destination.load_file(
        LoadRequest(
            source=MODULE.SOURCE,
            table=MODULE.SOURCE,
            file=file,
            columns=columns,
            load_id=f"load-{batch}",
            loaded_at=datetime(2026, 9, 17, 14, 0, tzinfo=timezone.utc),
            keep_payload=True,
        )
    )
    replay_reason = destination.should_skip(file.key, destination.ledger())
    destination.close()
    return result, replay_reason


def _execute_base_model(raw_db: pathlib.Path, transform_db: pathlib.Path):
    sql = BASE_SQL.read_text(encoding="utf-8").replace(
        "{{ source('raw_noaa_nhc_current_storms', 'noaa_nhc_current_storms') }}",
        '"extract"."raw"."noaa_nhc_current_storms"',
    )
    assert "{{" not in sql
    connection = duckdb.connect(str(transform_db))
    quoted_raw_db = str(raw_db).replace("'", "''")
    connection.execute(f"ATTACH '{quoted_raw_db}' AS extract (READ_ONLY)")
    connection.execute(f"create view base_noaa_nhc_current_storms as {sql}")
    return connection


def test_repository_raw_loading_and_actual_model_sql_preserve_snapshots_types_and_replay():
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        raw_db = root / "warehouse.duckdb"
        first_storm = storm()
        first = MODULE.normalize_storm(
            first_storm, "2026-09-17T12:00:00+00:00", document([first_storm])
        )
        second_storm = storm()
        second = MODULE.normalize_storm(
            second_storm, "2026-09-17T13:00:00+00:00", document([second_storm])
        )
        loaded_one, replay_reason = _load_snapshot(
            raw_db, root / "snapshot-one.ndjson", [first], "snapshot-one"
        )
        loaded_two, _ = _load_snapshot(
            raw_db, root / "snapshot-two.ndjson", [second], "snapshot-two"
        )

        assert loaded_one.rows == loaded_two.rows == 1
        assert replay_reason == "already loaded"
        connection = _execute_base_model(raw_db, root / "transform.duckdb")
        rows = connection.execute(
            """
            select storm_id, fetched_at, advisory_at, intensity_kt, pressure_mb,
                   latitude, longitude, movement_direction_degrees, movement_speed_kt,
                   typeof(fetched_at), typeof(advisory_at), typeof(intensity_kt),
                   json_extract_string(raw_storm, '$.futureField.kept'),
                   json_extract_string(raw_response, '$.futureResponseField[0]')
            from base_noaa_nhc_current_storms
            order by fetched_at
            """
        ).fetchall()
        connection.close()

    assert len(rows) == 2
    assert [row[0] for row in rows] == ["al072026", "al072026"]
    assert rows[0][1] != rows[1][1]
    assert rows[0][2] == rows[1][2]
    assert rows[0][3:9] == (85.0, 972.0, 24.5, -63.2, 315.0, 9.0)
    assert rows[0][9:12] == (
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP WITH TIME ZONE",
        "DOUBLE",
    )
    assert rows[0][12:] == ("true", "kept")


def test_repository_raw_loading_and_actual_model_sql_accept_zero_storm_snapshot():
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        raw_db = root / "warehouse.duckdb"
        result, replay_reason = _load_snapshot(
            raw_db, root / "empty.ndjson", [], "empty-snapshot"
        )
        assert result.rows == 0
        assert replay_reason == "already loaded"
        connection = _execute_base_model(raw_db, root / "transform.duckdb")
        assert connection.execute(
            "select count(*) from base_noaa_nhc_current_storms"
        ).fetchone() == (0,)
        connection.close()


def _run_checked(argv, *, cwd, env, redactions=()):
    result = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode:
        diagnostic = (result.stdout + "\n" + result.stderr)[-4000:]
        for secret in redactions:
            diagnostic = diagnostic.replace(secret, "[REDACTED]")
        raise RuntimeError(f"child exited with status {result.returncode}: {diagnostic}")
    return result


def test_child_failure_diagnostic_retains_status_and_bounds_and_redacts_stderr():
    secret = "fixture-secret"
    with pytest.raises(RuntimeError) as caught:
        _run_checked(
            [
                sys.executable,
                "-c",
                "import sys; sys.stderr.write('x' * 5000 + sys.argv[1]); raise SystemExit(7)",
                secret,
            ],
            cwd=REPO_ROOT,
            env=os.environ.copy(),
            redactions=(secret,),
        )
    message = str(caught.value)
    assert "status 7" in message
    assert secret not in message
    assert "[REDACTED]" in message
    assert len(message) < 4100


def test_offline_development_dbt_parse_with_temporary_local_profile():
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        raw_db = root / "warehouse.duckdb"
        duckdb.connect(str(raw_db)).close()
        profiles = root / "profiles"
        profiles.mkdir()
        (profiles / "profiles.yml").write_text(
            yaml.safe_dump(
                {
                    "vintage_data": {
                        "target": "dev",
                        "outputs": {
                            "dev": {
                                "type": "duckdb",
                                "path": str(root / "dev.duckdb"),
                                "schema": "transform",
                                "threads": 1,
                                "attach": [
                                    {
                                        "path": str(raw_db),
                                        "alias": "extract",
                                        "read_only": True,
                                    }
                                ],
                            }
                        },
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        result = _run_checked(
            [
                sys.executable,
                "-c",
                "import multiprocessing; import dbt.mp_context; dbt.mp_context._MP_CONTEXT = multiprocessing.get_context('fork'); from dbt.cli.main import cli; cli()",
                "parse",
                "--project-dir",
                str(REPO_ROOT / "transform"),
                "--profiles-dir",
                str(profiles),
                "--target",
                "dev",
                "--target-path",
                str(root / "target"),
                "--log-path",
                str(root / "logs"),
                "--no-partial-parse",
            ],
            cwd=REPO_ROOT,
            env={**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"},
        )
        assert result.returncode == 0
        manifest = json.loads((root / "target" / "manifest.json").read_text(encoding="utf-8"))
        assert "model.vintage_data.base_noaa_nhc_current_storms" in manifest["nodes"]


def test_model_documentation_names_units_payloads_and_distinct_times():
    schema = yaml.safe_load(BASE_YAML.read_text(encoding="utf-8"))
    model = next(item for item in schema["models"] if item["name"] == "base_noaa_nhc_current_storms")
    columns = {column["name"]: column for column in model["columns"]}
    for name in (
        "storm_id",
        "fetched_at",
        "advisory_at",
        "intensity_kt",
        "pressure_mb",
        "latitude",
        "longitude",
        "movement_direction_degrees",
        "movement_speed_kt",
        "public_advisory_url",
        "forecast_advisory_url",
        "wind_speed_probabilities_url",
        "raw_storm",
        "raw_response",
        "_payload",
    ):
        assert name in columns
    assert "knots" in columns["intensity_kt"]["description"]
    assert "millibars" in columns["pressure_mb"]["description"]
    assert "distinct from fetched_at" in columns["advisory_at"]["description"]
    assert "unknown fields" in columns["raw_storm"]["description"]
