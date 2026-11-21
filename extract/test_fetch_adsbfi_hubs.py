import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock
import urllib.error


SCRIPT = Path(__file__).parent / "scripts" / "fetch_adsbfi_hubs.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "adsbfi_hubs.yml"
SPEC = importlib.util.spec_from_file_location("fetch_adsbfi_hubs", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


AIRCRAFT = {
    "hex": "42592e",
    "type": "adsb_icao",
    "flight": "BAW123",
    "r": "G-ABCX",
    "t": "G-ABCX",
    "desc": "AIRBUS A320",
    "alt_baro": 35000,
    "gs": 472.4,
    "track": 268.1,
    "squawk": "2345",
    "emergency": "none",
    "lat": 51.41,
    "lon": -0.47,
    "seen_pos": 4.2,
    "seen": 3.1,
    "dst": 4.6,
    "dir": 342.0,
    "messages": 15234,
}


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def aircraft_with_hex(hex_code):
    aircraft = copy.deepcopy(AIRCRAFT)
    aircraft["hex"] = hex_code
    return aircraft


def snapshot(*aircraft):
    return {"ac": list(aircraft), "now": 1795239352000}


def http_error(code, reason="error"):
    return urllib.error.HTTPError(
        MODULE.BASE_URL, code, reason, {}, io.BytesIO(b"{}")
    )


def summary_of(stderr_text):
    lines = [
        line
        for line in stderr_text.splitlines()
        if line.startswith(MODULE.SUMMARY_PREFIX)
    ]
    assert len(lines) == 1, f"expected one summary line, got {lines!r}"
    return json.loads(lines[0][len(MODULE.SUMMARY_PREFIX):])


class FetchAdsbfHubsTests(unittest.TestCase):
    def fetch(self, documents, icaos=("EGLL", "KJFK"), **kwargs):
        """Run fetch_hubs with one mocked response per hub."""
        responses = [JsonResponse(doc) for doc in documents]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                with contextlib.redirect_stderr(io.StringIO()) as stderr:
                    records = []
                    hard_failure = None
                    try:
                        for record in MODULE.fetch_hubs(icaos, **kwargs):
                            records.append(record)
                    except RuntimeError as exc:
                        hard_failure = exc
        return records, urlopen, sleep, stderr.getvalue(), hard_failure

    def test_preserves_aircraft_object_and_stable_id(self):
        records, *_ = self.fetch([snapshot(AIRCRAFT)])

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "adsbfi_hubs")
        self.assertEqual(record["id"], "EGLL-42592E")
        self.assertEqual(record["hub"], "EGLL")
        self.assertEqual(record["hub_name"], "London Heathrow")
        self.assertEqual(record["radius_nm"], 50)
        self.assertEqual(record["aircraft"], AIRCRAFT)
        self.assertRegex(record["fetched_at"], r"\+00:00$")

    def test_uses_one_utc_fetch_timestamp_per_run(self):
        records, *_ = self.fetch(
            [snapshot(AIRCRAFT), snapshot(aircraft_with_hex("461E1A"))]
        )

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")

    def test_empty_aircraft_list_emits_no_records(self):
        records, *_ = self.fetch([snapshot()])
        self.assertEqual(records, [])

    def test_makes_one_request_per_hub_with_headers_and_timeout(self):
        _, urlopen, *_ = self.fetch([snapshot(), snapshot()], timeout=17)

        self.assertEqual(urlopen.call_count, 2)
        first, second = urlopen.call_args_list
        self.assertEqual(
            first.args[0].full_url,
            "https://opendata.adsb.fi/api/v3/lat/51.4700/lon/-0.4543/dist/50",
        )
        self.assertEqual(
            second.args[0].full_url,
            "https://opendata.adsb.fi/api/v3/lat/40.6413/lon/-73.7781/dist/50",
        )
        for call in first, second:
            request = call.args[0]
            self.assertEqual(request.get_header("Accept"), "application/json")
            self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
            self.assertEqual(call.kwargs, {"timeout": 17})

    def test_paces_requests_with_min_interval(self):
        _, _, sleep, _, _ = self.fetch([snapshot(), snapshot()], min_interval=1.0)

        sleep.assert_called_once_with(1.0)

    def test_invalid_parameters_fail_before_http(self):
        bad_params = (
            {"radius_nm": 0},
            {"radius_nm": MODULE.MAX_RADIUS_NM + 1},
            {"radius_nm": True},
            {"timeout": 0},
            {"timeout": MODULE.MAX_TIMEOUT + 1},
            {"timeout": 1.5},
            {"retries": -1},
            {"retries": MODULE.MAX_RETRIES + 1},
            {"retry_delay": -1},
            {"retry_delay": float("inf")},
            {"min_interval": -1},
            {"min_interval": MODULE.MAX_MIN_INTERVAL + 1},
        )
        for params in bad_params:
            with self.subTest(params=params):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, ""):
                        list(MODULE.fetch_hubs(("EGLL",), **params))
                urlopen.assert_not_called()

    def test_unknown_hub_fails_before_http(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(ValueError, "unknown hub"):
                list(MODULE.fetch_hubs(("XXXX",)))
        urlopen.assert_not_called()

    def test_rejects_malformed_documents(self):
        malformed = (
            [],
            {"type": "snapshot"},
            {"ac": "nope"},
        )
        for document in malformed:
            with self.subTest(document=document):
                records, urlopen, _, stderr, hard_failure = self.fetch(
                    [document, snapshot(AIRCRAFT)]
                )
                self.assertIsNone(hard_failure)
                self.assertEqual([r["id"] for r in records], ["KJFK-42592E"])
                self.assertIn("skipping hub EGLL", stderr)

    def test_drops_rows_without_usable_identity_and_counts_them(self):
        synthetic = aircraft_with_hex("~a7d47a")
        no_hex = dict(AIRCRAFT)
        no_hex.pop("hex")
        bad_lat = {**aircraft_with_hex("461E1A"), "lat": 91.0}
        records, _, _, stderr, hard_failure = self.fetch(
            [snapshot(AIRCRAFT, synthetic, no_hex, bad_lat)], icaos=("EGLL",)
        )

        self.assertIsNone(hard_failure)
        self.assertEqual([r["id"] for r in records], ["EGLL-42592E"])
        summary = summary_of(stderr)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["metrics"]["dropped_rows"], 3)

    def test_hub_fails_when_no_row_is_usable(self):
        unusable = (
            aircraft_with_hex("~a7d47a"),
            {**AIRCRAFT, "lat": 91.0},
            None,
        )
        records, _, _, stderr, hard_failure = self.fetch(
            [snapshot(*unusable)], icaos=("EGLL",)
        )

        self.assertEqual(records, [])
        self.assertIsNotNone(hard_failure)
        self.assertIn("no rows with a usable Mode-S hex code", stderr)

    def test_duplicate_hex_fails_the_hub(self):
        records, _, _, stderr, hard_failure = self.fetch(
            [snapshot(AIRCRAFT, aircraft_with_hex("42592E"))], icaos=("EGLL",)
        )

        self.assertEqual(records, [])
        self.assertIn("lists Mode-S hex 42592E twice", stderr)

    def test_retries_transient_http_then_recovers(self):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[http_error(502), JsonResponse(snapshot(AIRCRAFT))],
        ) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                with contextlib.redirect_stderr(io.StringIO()) as stderr:
                    records = list(MODULE.fetch_hubs(("EGLL",), retry_delay=7))

        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual([r["id"] for r in records], ["EGLL-42592E"])
        sleep.assert_called_once_with(7)
        self.assertIn("HTTP 502", stderr.getvalue())

    def test_retries_transient_connection_errors(self):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[
                urllib.error.URLError("temporary failure"),
                JsonResponse(snapshot(AIRCRAFT)),
            ],
        ) as urlopen:
            with mock.patch.object(MODULE.time, "sleep"):
                with contextlib.redirect_stderr(io.StringIO()):
                    records = list(MODULE.fetch_hubs(("EGLL",), retries=2))

        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual([r["id"] for r in records], ["EGLL-42592E"])

    def test_does_not_retry_permanent_http(self):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[http_error(404), http_error(404)],
        ) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                with contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaisesRegex(
                        RuntimeError, "all 1 adsb.fi hub requests failed"
                    ):
                        list(MODULE.fetch_hubs(("EGLL",), retries=2))

        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_oversized_response_fails_the_hub_without_retry(self):
        aircraft = aircraft_with_hex("42592e")
        aircraft["desc"] = "x" * MODULE.MAX_RESPONSE_BYTES
        oversized = json.dumps(snapshot(aircraft)).encode("utf-8")
        self.assertGreater(len(oversized), MODULE.MAX_RESPONSE_BYTES)
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[
                io.BytesIO(oversized),
                JsonResponse(snapshot(AIRCRAFT)),
            ],
        ) as urlopen:
            with mock.patch.object(MODULE.time, "sleep"):
                with contextlib.redirect_stderr(io.StringIO()) as stderr:
                    with self.assertRaisesRegex(
                        RuntimeError, "all 1 adsb.fi hub requests failed"
                    ):
                        list(MODULE.fetch_hubs(("EGLL",)))

        urlopen.assert_called_once()
        self.assertIn("exceeds", stderr.getvalue())

    def test_partial_run_reports_degraded_and_continues(self):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[
                http_error(503),
                http_error(503),
                JsonResponse(snapshot(AIRCRAFT)),
            ],
        ):
            with mock.patch.object(MODULE.time, "sleep"):
                with contextlib.redirect_stderr(io.StringIO()) as stderr:
                    records = list(
                        MODULE.fetch_hubs(("EGLL", "KJFK"), retries=1)
                    )

        self.assertEqual([r["id"] for r in records], ["KJFK-42592E"])
        summary = summary_of(stderr.getvalue())
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["records"], 1)
        self.assertEqual(
            summary["partitions"]["failures"],
            [{"hub": "EGLL", "error": "EGLL: HTTP Error 503: error"}],
        )

    def test_all_hubs_failed_is_a_hard_failure(self):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[http_error(503), http_error(503)],
        ):
            with mock.patch.object(MODULE.time, "sleep"):
                with contextlib.redirect_stderr(io.StringIO()) as stderr:
                    with self.assertRaisesRegex(
                        RuntimeError, "all 2 adsb.fi hub requests failed"
                    ):
                        list(MODULE.fetch_hubs(("EGLL", "KJFK"), retries=0))

        summary = summary_of(stderr.getvalue())
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["partitions"]["failed"], 2)

    def test_main_emits_valid_ndjson_and_summary(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[JsonResponse(snapshot(AIRCRAFT)), JsonResponse(snapshot())],
        ):
            with mock.patch.object(MODULE.time, "sleep"):
                with contextlib.redirect_stdout(stdout), \
                        contextlib.redirect_stderr(stderr):
                    exit_code = MODULE.main(
                        ["--hubs", "egll,kjfk", "--min-interval", "0"]
                    )

        self.assertEqual(exit_code, 0)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        output = json.loads(lines[0])
        self.assertEqual(output["id"], "EGLL-42592E")
        self.assertEqual(output["aircraft"], AIRCRAFT)
        summary = summary_of(stderr.getvalue())
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["metrics"]["hubs_attempted"], 2)
        self.assertEqual(summary["metrics"]["dropped_rows"], 0)

    def test_source_configuration_is_quarter_hourly_and_enabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "26-56/15 \* \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn("args: []", text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 5", text)
        self.assertIn("eight major hub snapshots are taken every 15 minutes", text)
        self.assertIn("1 request/second", text)
        self.assertIn("personal, non-commercial use only", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("https://www.adsb.fi/", text)


if __name__ == "__main__":
    unittest.main()
