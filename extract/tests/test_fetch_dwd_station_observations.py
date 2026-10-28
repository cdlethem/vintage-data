import importlib.util
import io
import json
import pathlib
import sys
import urllib.error
from datetime import datetime, timezone
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_dwd_station_observations.py"
SPEC = importlib.util.spec_from_file_location("fetch_dwd_station_observations", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

HEADER = (
    "surface observations;Parameter description;cloud_cover_total;"
    "dry_bulb_temperature_at_2_meter_above_ground;wind_speed;air_pressure"
)
LABELS = (
    "Datum;Uhrzeit (UTC);Wolkenbedeckung;Temperatur (2m);"
    "Windgeschwindigkeit;Luftdruck"
)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 9, 17, 12, 34, 56, tzinfo=timezone.utc)


def units_row(station):
    return f"{station};Unit;%;Grad C;km/h;hPa"


def station_csv(station, rows):
    lines = [HEADER, units_row(station), LABELS]
    for date, time_, cloud, temp, wind, pressure in rows:
        lines.append(f"{date};{time_};{cloud};{temp};{wind};{pressure}")
    return "\n".join(lines) + "\n"


def listing_html(names):
    items = "".join(f'<a href="{name}"> {name}</a>\n' for name in names)
    return f"<html><body><pre>{items}</pre></body></html>"


def fake_urlopen(listing, stations, failures=()):
    """stations: (filename, body) pairs; failures: (filename, exception) pairs."""
    calls = []

    def urlopen(request, timeout=None):
        url = request.full_url
        calls.append((url, timeout))
        for name, exc in failures:
            if url.endswith(name):
                raise exc
        if url.endswith("poi/"):
            return Response(listing.encode("utf-8"))
        for name, body in stations:
            if url.endswith(name):
                if isinstance(body, bytes):
                    return Response(body)
                return Response(body.encode("utf-8"))
        raise urllib.error.HTTPError(url, 404, "not found", {}, None)

    return urlopen, calls


def run_main(argv, listing, stations, failures=()):
    urlopen, calls = fake_urlopen(listing, stations, failures)
    stdout, stderr = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urlopen), \
         mock.patch.object(MODULE, "datetime", FixedDateTime), \
         mock.patch.object(MODULE.time, "sleep", lambda *_: None), \
         mock.patch.object(sys, "stdout", stdout), \
         mock.patch.object(sys, "stderr", stderr):
        exit_code = MODULE.main(argv)
    records = [json.loads(line) for line in stdout.getvalue().splitlines()]
    summary = None
    for line in stderr.getvalue().splitlines():
        if line.startswith("VINTAGE_RUN_SUMMARY\t"):
            summary = json.loads(line.split("\t", 1)[1])
    assert summary is not None, f"no VINTAGE_RUN_SUMMARY in stderr: {stderr.getvalue()!r}"
    return exit_code, records, summary, calls


LISTING = listing_html(["01008-BEOB.csv", "01025-BEOB.csv", "../", "content.log.bz2"])


def test_parse_listing_extracts_sorted_station_ids():
    assert MODULE.parse_listing(LISTING.encode("utf-8")) == ["01008", "01025"]


def test_parse_station_csv_normalizes_values_and_utc_times():
    body = station_csv(
        "01008",
        [
            ("28.10.26", "04:00", "---", "3,7", "112", "1000,1"),
            ("28.10.26", "03:00", "100", "3.4", "111", "1000"),
        ],
    )
    parameters, rows = MODULE.parse_station_csv(body.encode("utf-8"), "01008")
    assert parameters == [
        "cloud_cover_total",
        "dry_bulb_temperature_at_2_meter_above_ground",
        "wind_speed",
        "air_pressure",
    ]
    observed, values = rows[0]
    assert observed == datetime(2026, 10, 28, 4, 0, tzinfo=timezone.utc)
    assert values == {
        "cloud_cover_total": None,
        "dry_bulb_temperature_at_2_meter_above_ground": 3.7,
        "wind_speed": 112,
        "air_pressure": 1000.1,
    }
    observed, values = rows[1]
    assert observed == datetime(2026, 10, 28, 3, 0, tzinfo=timezone.utc)
    assert values["air_pressure"] == 1000
    assert values["wind_speed"] == 111


def test_parse_station_csv_year_pivot():
    body = station_csv("01008", [("01.01.70", "00:00", "---", "---", "---", "---")])
    _, rows = MODULE.parse_station_csv(body.encode("utf-8"), "01008")
    assert rows[0][0] == datetime(1970, 1, 1, tzinfo=timezone.utc)
    body = station_csv("01008", [("01.01.99", "00:00", "---", "---", "---", "---")])
    _, rows = MODULE.parse_station_csv(body.encode("utf-8"), "01008")
    assert rows[0][0] == datetime(1999, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda lines: ["bogus header"] + lines[1:], "header has no parameters"),
        (lambda lines: ["surface observations"] + lines[1:], "header has no parameters"),
        (lambda lines: ["surface observations;Bogus label;x;y;z;w"] + lines[1:], "unexpected second header cell"),
        (lambda lines: lines[:3] + ["28.10.26;04:00;1;2"], "cells"),
        (lambda lines: lines[:3] + ["28.10;04:00;1;2;3;4"], "unparseable observation date"),
        (lambda lines: lines[:3], "expected"),
    ],
)
def test_parse_station_csv_rejects_malformed_files(mutate, message):
    base = [HEADER, units_row("01008"), LABELS, "28.10.26;04:00;1;2;3;4"]
    body = "\n".join(mutate(base)) + "\n"
    with pytest.raises(ValueError, match=message):
        MODULE.parse_station_csv(body.encode("utf-8"), "01008")


def test_first_run_emits_full_window_and_saves_watermarks(tmp_path):
    state = tmp_path / "state.json"
    a = station_csv("01008", [("28.10.26", "10:00", "1", "3,0", "10", "1000"),
                              ("28.10.26", "11:00", "2", "3,1", "11", "1001")])
    b = station_csv("01025", [("28.10.26", "10:00", "---", "4,0", "20", "990")])
    exit_code, records, summary, calls = run_main(
        ["--state-file", str(state), "--retries", "1", "--retry-delay", "0"],
        LISTING,
        [("01008-BEOB.csv", a), ("01025-BEOB.csv", b)],
    )
    assert exit_code == 0
    assert [record["id"] for record in records] == [
        "01008|2026-10-28T10:00:00+00:00",
        "01008|2026-10-28T11:00:00+00:00",
        "01025|2026-10-28T10:00:00+00:00",
    ]
    assert len({record["fetched_at"] for record in records}) == 1
    assert all(record["source"] == "dwd_station_observations" for record in records)
    assert summary["health"] == "healthy"
    assert summary["completeness"] == "complete"
    assert summary["partitions"] == {
        "attempted": 2, "succeeded": 2, "failed": 0, "failures": [], "failures_truncated": 0,
    }
    assert state.exists()
    saved = json.loads(state.read_text(encoding="utf-8"))
    assert saved["stations"] == {
        "01008": "2026-10-28T11:00:00+00:00",
        "01025": "2026-10-28T10:00:00+00:00",
    }
    assert len(calls) == 3  # one listing + one file per station


def test_second_run_emits_only_new_rows(tmp_path):
    state = tmp_path / "state.json"
    state.write_text(json.dumps({
        "version": MODULE.STATE_VERSION,
        "source": MODULE.SOURCE,
        "stations": {
            "01008": "2026-10-28T11:00:00+00:00",
            "01025": "2026-10-28T10:00:00+00:00",
        },
    }) + "\n", encoding="utf-8")
    a = station_csv("01008", [("28.10.26", "10:00", "1", "3,0", "10", "1000"),
                              ("28.10.26", "11:00", "2", "3,1", "11", "1001"),
                              ("28.10.26", "12:00", "3", "3,2", "12", "1002")])
    b = station_csv("01025", [("28.10.26", "10:00", "---", "4,0", "20", "990")])
    exit_code, records, summary, _ = run_main(
        ["--state-file", str(state), "--retries", "1", "--retry-delay", "0"],
        LISTING,
        [("01008-BEOB.csv", a), ("01025-BEOB.csv", b)],
    )
    assert exit_code == 0
    assert [record["id"] for record in records] == ["01008|2026-10-28T12:00:00+00:00"]
    assert summary["metrics"]["records_emitted"] == 1
    saved = json.loads(state.read_text(encoding="utf-8"))
    assert saved["stations"]["01008"] == "2026-10-28T12:00:00+00:00"
    assert saved["stations"]["01025"] == "2026-10-28T10:00:00+00:00"


def test_station_failure_keeps_old_watermark_and_degrades_summary(tmp_path):
    state = tmp_path / "state.json"
    state.write_text(json.dumps({
        "version": MODULE.STATE_VERSION,
        "source": MODULE.SOURCE,
        "stations": {
            "01008": "2026-10-28T11:00:00+00:00",
            "01025": "2026-10-28T09:00:00+00:00",
        },
    }) + "\n", encoding="utf-8")
    a = station_csv("01008", [("28.10.26", "12:00", "3", "3,2", "12", "1002")])
    b = station_csv("01025", [("28.10.26", "10:00", "---", "4,0", "20", "990"),
                              ("28.10.26", "11:00", "---", "4,1", "21", "989")])
    failure = urllib.error.HTTPError(
        "https://opendata.dwd.de/weather/weather_reports/poi/01025-BEOB.csv",
        503, "unavailable", {}, None,
    )
    exit_code, records, summary, _ = run_main(
        ["--state-file", str(state), "--retries", "0", "--retry-delay", "0"],
        LISTING,
        [("01008-BEOB.csv", a), ("01025-BEOB.csv", b)],
        failures=[("01025-BEOB.csv", failure)],
    )
    assert exit_code == 0
    assert [record["id"] for record in records] == ["01008|2026-10-28T12:00:00+00:00"]
    assert summary["health"] == "degraded"
    assert summary["completeness"] == "partial"
    assert summary["partitions"]["failed"] == 1
    assert summary["partitions"]["failures"][0]["station"] == "01025"
    saved = json.loads(state.read_text(encoding="utf-8"))
    assert saved["stations"]["01025"] == "2026-10-28T09:00:00+00:00"  # old watermark kept

    # Next run: the failed station's missed rows are re-emitted, not lost.
    exit_code, records, summary, _ = run_main(
        ["--state-file", str(state), "--retries", "0", "--retry-delay", "0"],
        LISTING,
        [("01008-BEOB.csv", a), ("01025-BEOB.csv", b)],
    )
    assert exit_code == 0
    assert [record["id"] for record in records] == [
        "01025|2026-10-28T10:00:00+00:00",
        "01025|2026-10-28T11:00:00+00:00",
    ]
    assert summary["health"] == "healthy"


def test_majority_station_failures_fail_the_run(tmp_path):
    state = tmp_path / "state.json"
    a = station_csv("01008", [("28.10.26", "10:00", "1", "3,0", "10", "1000")])
    b = station_csv("01025", [("28.10.26", "10:00", "---", "4,0", "20", "990")])
    failure_a = urllib.error.HTTPError(
        "https://opendata.dwd.de/weather/weather_reports/poi/01008-BEOB.csv",
        503, "unavailable", {}, None,
    )
    failure_b = urllib.error.HTTPError(
        "https://opendata.dwd.de/weather/weather_reports/poi/01025-BEOB.csv",
        503, "unavailable", {}, None,
    )
    exit_code, records, summary, _ = run_main(
        ["--state-file", str(state), "--retries", "0", "--retry-delay", "0"],
        LISTING,
        [("01008-BEOB.csv", a), ("01025-BEOB.csv", b)],
        failures=[("01008-BEOB.csv", failure_a), ("01025-BEOB.csv", failure_b)],
    )
    assert exit_code == 1
    assert records == []
    assert summary["health"] == "failed"
    assert summary["completeness"] == "failed"
    saved = json.loads(state.read_text(encoding="utf-8"))
    assert saved["stations"] == {}


def test_no_state_emits_full_window_without_state_file(tmp_path):
    state = tmp_path / "state.json"
    a = station_csv("01008", [("28.10.26", "10:00", "1", "3,0", "10", "1000")])
    exit_code, records, summary, _ = run_main(
        ["--no-state", "--retries", "1", "--retry-delay", "0"],
        listing_html(["01008-BEOB.csv"]),
        [("01008-BEOB.csv", a)],
    )
    assert exit_code == 0
    assert len(records) == 1
    assert summary["health"] == "healthy"
    assert not state.exists()


def test_empty_listing_fails_without_state(tmp_path):
    state = tmp_path / "state.json"
    exit_code, records, summary, calls = run_main(
        ["--state-file", str(state), "--retries", "0", "--retry-delay", "0"],
        listing_html(["content.log.bz2"]),
        [],
    )
    assert exit_code == 1
    assert records == []
    assert summary["health"] == "failed"
    assert "no station files" in summary["metrics"]["error"]
    assert len(calls) == 1
    assert not state.exists()


def test_retries_transient_503_before_succeeding(tmp_path):
    state = tmp_path / "state.json"
    a = station_csv("01008", [("28.10.26", "10:00", "1", "3,0", "10", "1000")])
    b = station_csv("01025", [("28.10.26", "10:00", "---", "4,0", "20", "990")])
    failure = urllib.error.HTTPError(
        "https://opendata.dwd.de/weather/weather_reports/poi/01025-BEOB.csv",
        503, "unavailable", {}, None,
    )
    urlopen, _ = fake_urlopen(
        LISTING,
        [("01008-BEOB.csv", a), ("01025-BEOB.csv", b)],
    )
    # Drop the failure after the first attempt on that file.
    original = urlopen

    attempts = {"count": 0}

    def flaky(request, timeout=None):
        url = request.full_url
        if url.endswith("01025-BEOB.csv"):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise failure
        return original(request, timeout=timeout)

    stdout, stderr = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=flaky), \
         mock.patch.object(MODULE, "datetime", FixedDateTime), \
         mock.patch.object(MODULE.time, "sleep", lambda *_: None), \
         mock.patch.object(sys, "stdout", stdout), \
         mock.patch.object(sys, "stderr", stderr):
        exit_code = MODULE.main(["--state-file", str(state), "--retries", "1", "--retry-delay", "0"])
    records = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert exit_code == 0
    assert len(records) == 2
    assert attempts["count"] == 2


def test_does_not_retry_404(tmp_path):
    state = tmp_path / "state.json"
    a = station_csv("01008", [("28.10.26", "10:00", "1", "3,0", "10", "1000")])
    b = station_csv("01025", [("28.10.26", "10:00", "---", "4,0", "20", "990")])
    failure = urllib.error.HTTPError(
        "https://opendata.dwd.de/weather/weather_reports/poi/01025-BEOB.csv",
        404, "gone", {}, None,
    )
    exit_code, records, summary, calls = run_main(
        ["--state-file", str(state), "--retries", "2", "--retry-delay", "0"],
        LISTING,
        [("01008-BEOB.csv", a), ("01025-BEOB.csv", b)],
        failures=[("01025-BEOB.csv", failure)],
    )
    assert exit_code == 0
    assert len(records) == 1
    station_attempts = [called for called, _ in calls if called.endswith("01025-BEOB.csv")]
    assert len(station_attempts) == 1
    assert summary["health"] == "degraded"


@pytest.mark.parametrize(
    "argv",
    [
        ["--no-state", "--state-file", "state.json"],
        ["--timeout", "0"],
        ["--retries", "-1"],
        ["--retry-delay", "-1"],
    ],
)
def test_rejects_invalid_argument_combinations(argv):
    with pytest.raises(SystemExit):
        MODULE.main(argv)
