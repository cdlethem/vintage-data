"""Behavioral tests for fetch_nws_stations_observations.py (offline, scripted transport)."""

import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
from unittest import mock

import yaml

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_nws_stations_observations.py"
SOURCE_CONFIG = pathlib.Path(__file__).parents[1] / "sources" / "nws_stations_observations.yml"
SPEC = importlib.util.spec_from_file_location("fetch_nws_stations_observations", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

STATIONS = MODULE.STATIONS


def observation(station_id, ts, **overrides):
    document = {
        "stationId": station_id,
        "stationName": f"Station {station_id}",
        "timestamp": ts,
        "textDescription": "Clear",
        "presentWeather": [
            {"intensity": None, "modifier": None, "weather": "rain", "rawString": "RA"}
        ],
        "temperature": {"unitCode": "wmoUnit:degC", "value": 20, "qualityControl": "V"},
        "dewpoint": {"unitCode": "wmoUnit:degC", "value": 12, "qualityControl": "V"},
        "windChill": None,
        "heatIndex": {"unitCode": "wmoUnit:degC", "value": None, "qualityControl": "V"},
        "maxTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": None},
        "minTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": None},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 180, "qualityControl": "V"},
        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 9.26, "qualityControl": "V"},
        "windGust": None,
        "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 101000.0, "qualityControl": "V"},
        "seaLevelPressure": None,
        "visibility": {"unitCode": "wmoUnit:m", "value": 16093.44, "qualityControl": "V"},
        "precipitationLast3Hours": {"unitCode": "wmoUnit:mm", "value": None},
        "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 62.0, "qualityControl": "V"},
        "cloudLayers": [{"base": {"unitCode": "wmoUnit:m", "value": 3000}, "amount": "CLR"}],
        "rawMessage": f"{station_id} 130625Z 18005KT 10SM CLR 20/12 A2992",
    }
    document.update(overrides)
    return document


class ScriptedTransport:
    """Serve one scripted document per station; raise for stations marked bad."""

    def __init__(self, documents, bad=()):
        self.documents = documents
        self.bad = set(bad)
        self.requests = []

    def __call__(self, url, timeout):
        self.requests.append(url)
        station = url.rsplit("/", 3)[1]
        if station in self.bad:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        if station not in self.documents:
            raise AssertionError(f"unexpected request for {station}: {url}")
        return self.documents[station]


class FakeClock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    def sleep(self, delay):
        self.value += delay
        self.sleeps.append(delay)


def run_fetcher(transport, *, state_file=None, now=None, stations=STATIONS):
    out = io.StringIO()
    err = io.StringIO()
    clock = FakeClock()
    now = now or (lambda: __import__("datetime").datetime(
        2026, 10, 13, 6, 22, tzinfo=__import__("datetime").timezone.utc
    ))
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        count = MODULE.run(
            stations=stations,
            path=pathlib.Path(state_file) if state_file else None,
            output=out,
            min_interval=1.0,
            transport=transport,
            now=now,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
    summary = {
        line.split("\t", 1)[1]: json.loads(line.split("\t", 1)[1])
        for line in err.getvalue().splitlines()
        if line.startswith("VINTAGE_RUN_SUMMARY\t")
    }
    return count, out.getvalue(), err.getvalue(), list(summary.values()), clock


def load_config():
    return yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))


class NwsStationsObservationsTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_file = pathlib.Path(self._tmp.name) / "state" / "nws_stations_observations.json"

    def write_state(self, last_observation):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "source": MODULE.SOURCE,
                    "last_observation": last_observation,
                }
            ),
            encoding="utf-8",
        )

    def documents(self, station_ts):
        return {
            station: observation(station, ts) for station, ts in station_ts.items()
        }

    def test_emits_all_stations_with_shared_timestamp_and_state_watermark(self):
        docs = self.documents(
            {station: f"2026-10-13T05:{25 + i:02d}:00+00:00" for i, station in enumerate(STATIONS)}
        )
        transport = ScriptedTransport(docs)
        count, payload, _err, summaries, clock = run_fetcher(transport, state_file=self.state_file)

        self.assertEqual(count, len(STATIONS))
        lines = [json.loads(line) for line in payload.splitlines()]
        self.assertEqual(len(lines), len(STATIONS))
        for record in lines:
            self.assertEqual(record["source"], MODULE.SOURCE)
            self.assertEqual(record["fetched_at"], "2026-10-13T06:22:00Z")
            self.assertIn("id", record)
            self.assertTrue(record["id"].startswith(f"{record['station_id']}@"))
            self.assertEqual(record["temperature_c"], 20)
            self.assertIsNone(record["wind_chill_c"])
            self.assertEqual(record["cloud_layers"][0]["amount"], "CLR")
            self.assertEqual(record["present_weather"][0]["weather"], "rain")
        self.assertEqual(len(set(r["fetched_at"] for r in lines)), 1)
        # One request per station, each against the documented latest endpoint.
        self.assertEqual(len(transport.requests), len(STATIONS))
        self.assertTrue(
            all(url.startswith(MODULE.BASE + "/stations/") and url.endswith("/observations/latest")
            for url in transport.requests)
        )
        # Paced: n-1 sleeps of exactly the minimum interval.
        self.assertEqual(clock.sleeps, [1.0] * (len(STATIONS) - 1))
        # State published atomically with one watermark per station.
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(set(state["last_observation"]), set(STATIONS))
        summary = summaries[0]
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["requests"], {"attempted": len(STATIONS), "succeeded": len(STATIONS), "failed": 0})
        self.assertEqual(summary["metrics"]["records"], len(STATIONS))
        self.assertEqual(len(summary["state_change"]), len(STATIONS))

    def test_stale_observation_is_skipped_and_state_unchanged(self):
        first_ts = {station: "2026-10-13T05:30:00+00:00" for station in STATIONS}
        transport = ScriptedTransport(self.documents(first_ts))
        count, payload, _err, summaries, _clock = run_fetcher(transport, state_file=self.state_file)
        self.assertEqual(count, len(STATIONS))
        self.assertTrue(payload)
        stored = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(stored["last_observation"][STATIONS[0]], "2026-10-13T05:30:00Z")

        # Same latest observation on the next run: nothing new, no duplicates.
        count2, payload2, _err2, summaries2, _clock2 = run_fetcher(transport, state_file=self.state_file)
        self.assertEqual(count2, 0)
        self.assertEqual(payload2, "")
        self.assertEqual(summaries2[0]["health"], "healthy")
        self.assertEqual(summaries2[0]["metrics"]["stations_stale_skipped"], len(STATIONS))
        self.assertEqual(summaries2[0]["state_change"], {})
        self.assertEqual(
            json.loads(self.state_file.read_text(encoding="utf-8")), stored
        )

    def test_newer_observation_after_stale_run_advances_watermark(self):
        old = {station: "2026-10-13T05:30:00+00:00" for station in STATIONS}
        transport = ScriptedTransport(self.documents(old))
        run_fetcher(transport, state_file=self.state_file)
        # Half the stations published a new observation, half did not.
        fresh = dict(old)
        for station in STATIONS[: len(STATIONS) // 2]:
            fresh[station] = "2026-10-13T06:30:00+00:00"
        transport2 = ScriptedTransport(self.documents(fresh))
        count, payload, _err, summaries, _clock = run_fetcher(transport2, state_file=self.state_file)
        expected_new = len(STATIONS) // 2
        self.assertEqual(count, expected_new)
        emitted = {json.loads(line)["station_id"] for line in payload.splitlines()}
        self.assertEqual(emitted, set(STATIONS[:expected_new]))
        self.assertEqual(summaries[0]["metrics"]["stations_stale_skipped"], len(STATIONS) - expected_new)
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_observation"][STATIONS[0]], "2026-10-13T06:30:00Z")
        self.assertEqual(state["last_observation"][STATIONS[-1]], "2026-10-13T05:30:00Z")

    def test_partial_failure_reports_partial_health_and_keeps_good_stations(self):
        docs = self.documents({station: "2026-10-13T05:30:00+00:00" for station in STATIONS})
        transport = ScriptedTransport(docs, bad={STATIONS[3]})
        count, payload, _err, summaries, _clock = run_fetcher(transport, state_file=self.state_file)
        self.assertEqual(count, len(STATIONS) - 1)
        emitted = {json.loads(line)["station_id"] for line in payload.splitlines()}
        self.assertNotIn(STATIONS[3], emitted)
        summary = summaries[0]
        self.assertEqual(summary["health"], "partial")
        self.assertEqual(summary["requests"]["failed"], 1)
        self.assertEqual(len(summary["stations_failed"]), 1)
        self.assertIn(STATIONS[3], summary["stations_failed"][0])

    def test_total_failure_raises_without_state(self):
        transport = ScriptedTransport({}, bad=set(STATIONS))
        with self.assertRaises(RuntimeError) as caught:
            run_fetcher(transport, state_file=self.state_file)
        self.assertIn("all 24 station requests failed", str(caught.exception))
        self.assertFalse(self.state_file.exists())

    def test_rejects_malformed_documents_as_station_failures(self):
        docs = self.documents({station: "2026-10-13T05:30:00+00:00" for station in STATIONS})
        bad_docs = dict(docs)
        bad_docs[STATIONS[0]] = observation(STATIONS[0], "2026-10-13T05:30:00+00:00", stationId="KXXX")
        bad_docs[STATIONS[1]] = observation(STATIONS[1], "2026-10-13T05:30:00+00:00", temperature="20")
        bad_docs[STATIONS[2]] = observation(
            STATIONS[2],
            "2026-10-13T05:30:00+00:00",
            windSpeed={"unitCode": "wmoUnit:ms", "value": 5},
        )
        bad_docs[STATIONS[3]] = observation(STATIONS[3], "not-a-timestamp")
        bad_docs[STATIONS[4]] = observation(
            STATIONS[4], "2026-10-13T05:30:00+00:00", presentWeather=["RA"]
        )
        transport = ScriptedTransport(bad_docs)
        count, _payload, _err, summaries, _clock = run_fetcher(transport)
        self.assertEqual(count, len(STATIONS) - 5)
        summary = summaries[0]
        self.assertEqual(summary["health"], "partial")
        self.assertEqual(summary["requests"]["failed"], 5)

    def test_invalid_state_file_fails_the_run(self):
        self.write_state({STATIONS[0]: "2026-10-13T05:30:00Z"})
        broken = json.loads(self.state_file.read_text(encoding="utf-8"))
        broken["version"] = 99
        self.state_file.write_text(json.dumps(broken), encoding="utf-8")
        docs = self.documents({station: "2026-10-13T05:30:00+00:00" for station in STATIONS})
        with self.assertRaises(ValueError) as caught:
            run_fetcher(ScriptedTransport(docs), state_file=self.state_file)
        self.assertIn("version", str(caught.exception))

    def test_rejects_bad_bounds_without_a_request(self):
        transport = ScriptedTransport({})
        for kwargs in (
            {"stations": ()},
            {"timeout": 0},
            {"min_interval": -0.5},
        ):
            with self.assertRaises(ValueError):
                MODULE.run(
                    path=None,
                    output=io.StringIO(),
                    transport=transport,
                    now=lambda: __import__("datetime").datetime(
                        2026, 10, 13, 6, 22, tzinfo=__import__("datetime").timezone.utc
                    ),
                    **kwargs,
                )
        self.assertEqual(transport.requests, [])

    def test_no_state_run_never_writes(self):
        docs = self.documents({station: "2026-10-13T05:30:00+00:00" for station in STATIONS})
        count, _payload, _err, summaries, _clock = run_fetcher(ScriptedTransport(docs))
        self.assertEqual(count, len(STATIONS))
        self.assertEqual(summaries[0]["state_change"], {})

    def test_fetch_latest_uses_documented_headers(self):
        document = observation("KOKC", "2026-10-13T05:30:00+00:00")

        def fake_urlopen(request, timeout):
            self.assertTrue(request.full_url.endswith("/stations/KOKC/observations/latest"))
            self.assertIn("vintage-data", request.get_header("User-agent"))
            self.assertEqual(request.get_header("Accept"), "application/ld+json; q=0.9, application/json")

            class Response(io.BytesIO):
                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, *exc):
                    self_inner.close()

            return Response(json.dumps(document).encode("utf-8"))

        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen):
            result = MODULE.fetch_latest(
                MODULE.BASE + "/stations/KOKC/observations/latest", 30
            )
        self.assertEqual(result["stationId"], "KOKC")

    def test_source_yaml_contract(self):
        config = load_config()
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertTrue((SCRIPT.parent / config["script"]).is_file())
        self.assertEqual(len(config["schedule"].split()), 5)
        self.assertIsInstance(config["args"], list)
        self.assertTrue(config["enabled"])
        self.assertEqual(config["sink"], "local")
        for key in ("cadence_note", "rate_limit", "licence", "attribution"):
            self.assertTrue(config.get(key))
