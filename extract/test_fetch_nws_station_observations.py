import contextlib
import importlib.util
import io
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest
from unittest import mock
import urllib.error


SCRIPT = Path(__file__).parent / "scripts" / "fetch_nws_station_observations.py"
RUN_METADATA_PATH = Path(__file__).parent.parent / "orchestration" / "include" / "run_metadata.py"
RUN_METADATA_SPEC = importlib.util.spec_from_file_location("nws_test_run_metadata", RUN_METADATA_PATH)
RUN_METADATA = importlib.util.module_from_spec(RUN_METADATA_SPEC)
assert RUN_METADATA_SPEC.loader is not None
RUN_METADATA_SPEC.loader.exec_module(RUN_METADATA)
SPEC = importlib.util.spec_from_file_location("fetch_nws_station_observations", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def feature(station_id, timestamp, temperature=5.0):
    return {
        "id": f"https://api.weather.gov/stations/{station_id}/observations/{timestamp}",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-73.98, 40.77]},
        "properties": {
            "@id": f"https://api.weather.gov/stations/{station_id}/observations/{timestamp}",
            "@type": "wx:ObservationStation",
            "station": f"https://api.weather.gov/stations/{station_id}",
            "stationId": station_id,
            "stationName": f"Station {station_id}",
            "timestamp": timestamp,
            "elevation": {"unitCode": "wmoUnit:m", "value": 27},
            "temperature": {"unitCode": "wmoUnit:degC", "value": temperature, "qualityControl": "V"},
            "dewpoint": {"unitCode": "wmoUnit:degC", "value": temperature - 4.0, "qualityControl": "V"},
            "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 64.0, "qualityControl": "V"},
            "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 9.36, "qualityControl": "V"},
            "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 300, "qualityControl": "V"},
            "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None, "qualityControl": "Z"},
            "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 102570, "qualityControl": "V"},
            "seaLevelPressure": {"unitCode": "wmoUnit:Pa", "value": 102500, "qualityControl": "V"},
            "visibility": {"unitCode": "wmoUnit:m", "value": 16090, "qualityControl": "C"},
            "precipitationLastHour": {"unitCode": "wmoUnit:mm", "value": None, "qualityControl": "Z"},
            "textDescription": "Clear",
            "presentWeather": [],
            "rawMessage": f"{station_id} 160451Z AUTO 30005KT 10SM CLR",
        },
    }


def collection(*features):
    return {"type": "FeatureCollection", "features": list(features)}


def summary_from(stderr):
    text = stderr.getvalue() if hasattr(stderr, "getvalue") else stderr
    payload, diagnostics = RUN_METADATA.extract_summary_line(text)
    assert payload is not None, f"no run-summary line in stderr: {diagnostics!r}"
    return RUN_METADATA.validate_summary(json.loads(payload))


def fresh_metrics():
    return {
        "requests": {"attempted": 0, "succeeded": 0, "failed": 0, "payload_bytes": 0},
        "partitions": {"attempted": 0, "succeeded": 0, "failed": 0, "failures": []},
    }


class FetchNwsStationObservationsTests(unittest.TestCase):
    NOW = datetime(2026, 11, 16, 6, 0, tzinfo=timezone.utc)

    def ts(self, hours_ago):
        return (self.NOW - timedelta(hours=hours_ago)).isoformat()

    def collect(self, documents, stations, state, now=None):
        def fake_urlopen(request, timeout=None):
            station = request.full_url.split("/stations/")[1].split("/")[0]
            document = documents[station]
            if isinstance(document, BaseException):
                raise document
            return JsonResponse(document)

        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen), \
             mock.patch.object(MODULE.time, "sleep"):
            metrics = fresh_metrics()
            records, next_state = MODULE.collect_observations(
                stations,
                state,
                now=now or self.NOW,
                lookback_hours=48.0,
                limit=60,
                timeout=30.0,
                request_delay=0.0,
                metrics=metrics,
            )
        return records, next_state, metrics

    def run_main(self, argv, documents):
        def fake_urlopen(request, timeout=None):
            station = request.full_url.split("/stations/")[1].split("/")[0]
            document = documents[station]
            if isinstance(document, BaseException):
                raise document
            return JsonResponse(document)

        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen), \
             mock.patch.object(MODULE.time, "sleep"):
            code = MODULE.main(list(argv) + ["--request-delay", "0"], out=out, err=err)
        return code, out.getvalue(), err.getvalue()

    def test_first_run_emits_only_window_and_sets_watermark(self):
        documents = {
            "KNYC": collection(
                feature("KNYC", self.ts(0.5)),
                feature("KNYC", self.ts(1)),
                feature("KNYC", self.ts(2)),
                feature("KNYC", self.ts(49)),   # outside the 48h first-run window
                feature("KNYC", self.ts(100)),
            )
        }
        records, next_state, metrics = self.collect(
            documents, ("KNYC",), {"version": MODULE.STATE_VERSION, "stations": {}}
        )
        self.assertEqual([record["timestamp"] for record in records], [self.ts(h) for h in (0.5, 1, 2)])
        self.assertEqual(next_state["stations"]["KNYC"], self.ts(0.5))
        self.assertEqual(metrics["requests"]["succeeded"], 1)

        record = records[0]
        self.assertEqual(record["source"], "nws_station_observations")
        self.assertEqual(record["id"], f"KNYC/{self.ts(0.5)}")
        self.assertTrue(record["fetched_at"].endswith("Z"))
        self.assertEqual(record["station_id"], "KNYC")
        self.assertEqual(record["station_name"], "Station KNYC")
        self.assertEqual(record["temperature_c"], 5.0)
        self.assertIsNone(record["wind_gust_kmh"])
        self.assertEqual(record["lat"], 40.77)
        self.assertEqual(record["lon"], -73.98)
        self.assertEqual(record["present_weather"], [])
        self.assertIn("raw_message", record)

    def test_watermark_replay_emits_only_strictly_newer(self):
        documents = {
            "KNYC": collection(
                feature("KNYC", self.ts(0.5)),
                feature("KNYC", self.ts(1)),  # equal to the stored watermark: must not repeat
                feature("KNYC", self.ts(2)),
            )
        }
        state = {"version": MODULE.STATE_VERSION, "stations": {"KNYC": self.ts(1)}}
        records, next_state, _ = self.collect(documents, ("KNYC",), state)
        self.assertEqual([record["timestamp"] for record in records], [self.ts(0.5)])
        self.assertEqual(next_state["stations"]["KNYC"], self.ts(0.5))

    def test_catch_up_is_bounded_by_lookback_floor(self):
        state = {"version": MODULE.STATE_VERSION, "stations": {"KNYC": self.ts(60)}}
        documents = {
            "KNYC": collection(
                feature("KNYC", self.ts(1)),
                feature("KNYC", self.ts(47)),
                feature("KNYC", self.ts(49)),  # older than the 48h floor: skipped on resume
                feature("KNYC", self.ts(55)),
            )
        }
        records, _, _ = self.collect(documents, ("KNYC",), state)
        self.assertEqual([record["timestamp"] for record in records], [self.ts(h) for h in (1, 47)])

    def test_malformed_feature_is_skipped_without_failing_station(self):
        documents = {
            "KNYC": collection(feature("KNYC", "not-a-timestamp"), feature("KNYC", self.ts(1)))
        }
        records, next_state, _ = self.collect(
            documents, ("KNYC",), {"version": MODULE.STATE_VERSION, "stations": {}}
        )
        self.assertEqual([record["timestamp"] for record in records], [self.ts(1)])
        self.assertEqual(next_state["stations"]["KNYC"], self.ts(1))

    def test_partial_station_failure_degrades_run(self):
        now = datetime.now(timezone.utc)
        recent = (now - timedelta(hours=1)).isoformat()
        documents = {
            "KNYC": collection(feature("KNYC", recent)),
            "KLAX": urllib.error.URLError("station unreachable"),
        }
        code, out, err = self.run_main(["--station", "KNYC", "--station", "KLAX", "--no-state"], documents)
        self.assertEqual(code, 0)
        lines = [line for line in out.splitlines() if line]
        self.assertEqual(len(lines), 1)
        summary = summary_from(err)
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["partitions"]["failed"], 1)
        self.assertEqual(summary["partitions"]["failures"][0]["partition"], "KLAX")
        self.assertEqual(summary["metrics"]["records"], 1)

    def test_all_stations_failing_fails_the_run(self):
        documents = {"KNYC": urllib.error.URLError("down"), "KLAX": urllib.error.URLError("down")}
        code, out, err = self.run_main(
            ["--station", "KNYC", "--station", "KLAX", "--no-state"], documents
        )
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        summary = summary_from(err)
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["partitions"]["failed"], 2)

    def test_second_run_with_state_emits_nothing(self):
        now = datetime.now(timezone.utc)
        documents = {
            "KNYC": collection(
                feature("KNYC", (now - timedelta(hours=1)).isoformat()),
                feature("KNYC", (now - timedelta(hours=2)).isoformat()),
            )
        }
        with self.temp_dir() as directory:
            state_file = Path(directory) / "state.json"
            code, out, err = self.run_main(
                ["--station", "KNYC", "--state-file", str(state_file)], documents
            )
            self.assertEqual(code, 0)
            self.assertEqual(len(out.splitlines()), 2)
            self.assertTrue(state_file.exists())
            saved = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(saved["version"], MODULE.STATE_VERSION)
            self.assertIn("KNYC", saved["stations"])

            code, out, err = self.run_main(
                ["--station", "KNYC", "--state-file", str(state_file)], documents
            )
            self.assertEqual(code, 0)
            self.assertEqual(out, "")
            summary = summary_from(err)
            self.assertEqual(summary["health"], "healthy")
            self.assertEqual(summary["completeness"], "complete")
            self.assertEqual(summary["metrics"]["records"], 0)

    def test_no_state_and_state_file_conflict(self):
        out, err = io.StringIO(), io.StringIO()
        code = MODULE.main(["--no-state", "--state-file", "/tmp/state.json"], out=out, err=err)
        self.assertEqual(code, 2)

    def test_state_round_trip_and_version_rejection(self):
        with self.temp_dir() as directory:
            path = Path(directory) / "state.json"
            state = {"version": MODULE.STATE_VERSION, "stations": {"KNYC": self.ts(1)}}
            MODULE.save_state(path, state)
            self.assertEqual(MODULE.load_state(path), state)
            path.write_text(json.dumps({"version": 99, "stations": {}}), encoding="utf-8")
            with self.assertRaises(ValueError):
                MODULE.load_state(path)
            path.unlink()
            self.assertEqual(MODULE.load_state(path), {"version": MODULE.STATE_VERSION, "stations": {}})

    def test_invalid_arguments_rejected(self):
        out, err = io.StringIO(), io.StringIO()
        code = MODULE.main(["--station", "nope!"], out=out, err=err)
        self.assertEqual(code, 2)
        out, err = io.StringIO(), io.StringIO()
        code = MODULE.main(["--limit", "999"], out=out, err=err)
        self.assertEqual(code, 2)

    @contextlib.contextmanager
    def temp_dir(self):
        import tempfile

        directory = tempfile.mkdtemp(prefix="nws-obs-test-")
        try:
            yield directory
        finally:
            shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
