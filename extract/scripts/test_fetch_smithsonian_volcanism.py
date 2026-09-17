import contextlib
import http.client
import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
import urllib.parse
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("fetch_smithsonian_volcanism.py")
SPEC = importlib.util.spec_from_file_location("fetch_smithsonian_volcanism", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
CONFIG = SCRIPT.parents[1] / "sources" / "smithsonian_volcanism.yml"


class Response(io.BytesIO):
    def __init__(self, body, status=200):
        super().__init__(body)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class AdvancingResponse(Response):
    def __init__(self, body, clock, advance):
        super().__init__(body)
        self.clock = clock
        self.advance = advance

    def read(self, size=-1):
        body = super().read(size)
        self.clock.now += self.advance
        return body


class IncompleteResponse(Response):
    def __init__(self, partial, *, prefix=b"", clock=None, advance=0.0, status=200):
        super().__init__(b"", status=status)
        self.partial = partial
        self.prefix = prefix
        self.clock = clock
        self.advance = advance

    def read(self, size=-1):
        if self.prefix:
            prefix, self.prefix = self.prefix, b""
            return prefix
        if self.clock is not None:
            self.clock.now += self.advance
        raise http.client.IncompleteRead(self.partial, len(self.partial) + 1)


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []
        self.sleep_overrun = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds + self.sleep_overrun


def fixture_document(count=25):
    features = []
    for index in range(count):
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "Activity_ID": f"activity-{index:02d}",
                    "VolcanoNumber": 300000 + index,
                    "VolcanoName": f"Fixture Volcano {index:02d}",
                    "StartDate": f"2026{9 - index // 10:02d}{28 - index % 10:02d}",
                    "EndDate": None,
                    "ContinuingEruption": "True" if index == 0 else "False",
                    "ExplosivityIndexMax": index % 6,
                    "LatitudeDecimal": 10.0 + index,
                    "LongitudeDecimal": -20.0 - index,
                },
            }
        )
    return {"type": "FeatureCollection", "numberReturned": count, "features": features}


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def run_main(urlopen_side_effect):
    stdout = io.StringIO()
    stderr = io.StringIO()
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urlopen_side_effect) as urlopen,
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        status = MODULE.main([])
    return status, stdout.getvalue(), stderr.getvalue(), urlopen


def telemetry(stderr, prefix):
    return [
        json.loads(line.removeprefix(prefix))
        for line in stderr.splitlines()
        if line.startswith(prefix)
    ]


class SmithsonianVolcanismTests(unittest.TestCase):
    def test_success_outputs_exactly_25_recent_features_and_unchanged_query(self):
        status, stdout, stderr, urlopen = run_main([response(fixture_document())])

        self.assertEqual(status, 0)
        records = [json.loads(line) for line in stdout.splitlines()]
        self.assertEqual(len(records), 25)
        self.assertEqual(records[0]["id"], "activity-00")
        self.assertEqual(records[-1]["id"], "activity-24")
        self.assertTrue(records[0]["continuing"])
        self.assertTrue(all(record["source"] == "smithsonian_volcanism" for record in records))

        request = urlopen.call_args.args[0]
        self.assertEqual(
            urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query),
            {
                "service": ["WFS"],
                "version": ["2.0.0"],
                "request": ["GetFeature"],
                "typeName": ["GVP-VOTW:E3WebApp_Eruptions1960"],
                "outputFormat": ["application/json"],
                "count": ["25"],
                "sortBy": ["StartDate D"],
            },
        )
        self.assertEqual(request.full_url.split("?", 1)[0], MODULE.BASE)
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": MODULE.REQUEST_TIMEOUT_SECONDS})

        attempts = telemetry(stderr, MODULE.ATTEMPT_PREFIX)
        summaries = telemetry(stderr, MODULE.SUMMARY_PREFIX)
        self.assertEqual(attempts[0]["records"], 25)
        self.assertGreater(attempts[0]["bytes"], 0)
        self.assertEqual(attempts[0]["failure_phase"], None)
        self.assertEqual(summaries, [mock.ANY])
        self.assertEqual(summaries[0]["completeness"], "complete")
        self.assertEqual(summaries[0]["records"], 25)

    def test_timeout_then_recovery_uses_exponential_backoff(self):
        clock = Clock()
        with (
            mock.patch.object(MODULE.time, "monotonic", side_effect=clock),
            mock.patch.object(MODULE.time, "sleep", side_effect=clock.sleep),
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                side_effect=[TimeoutError("timed out"), response(fixture_document(1))],
            ) as urlopen,
            contextlib.redirect_stderr(io.StringIO()) as stderr,
        ):
            records = MODULE.fetch_recent_eruptions(count=1)

        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(clock.sleeps, [1.0])
        attempts = telemetry(stderr.getvalue(), MODULE.ATTEMPT_PREFIX)
        self.assertEqual([item["outcome"] for item in attempts], ["timeout", "succeeded"])
        self.assertTrue(attempts[0]["retrying"])

    def test_persistent_timeout_exhaustion_publishes_nothing_and_reports_failure(self):
        status, stdout, stderr, urlopen = run_main(TimeoutError("secret host timed out"))

        self.assertEqual(status, 1)
        self.assertEqual(stdout, "")
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        summary = telemetry(stderr, MODULE.SUMMARY_PREFIX)[0]
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["records"], 0)
        self.assertEqual(summary["requests"]["attempted"], MODULE.MAX_ATTEMPTS)
        self.assertEqual(summary["metrics"]["failure_phase"], "open")
        self.assertNotIn(MODULE.BASE, stderr)
        self.assertNotIn("secret host", stderr)

    def test_transient_transport_exhaustion_is_bounded(self):
        clock = Clock()
        with (
            mock.patch.object(MODULE.time, "monotonic", side_effect=clock),
            mock.patch.object(MODULE.time, "sleep", side_effect=clock.sleep),
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                side_effect=urllib.error.URLError("connection reset"),
            ) as urlopen,
            contextlib.redirect_stderr(io.StringIO()) as stderr,
        ):
            with self.assertRaisesRegex(MODULE.SmithsonianFetchError, "exhausted all attempts") as raised:
                MODULE.fetch_recent_eruptions()

        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(clock.sleeps, [1.0, 2.0])
        self.assertEqual(raised.exception.attempts, 3)
        self.assertEqual(
            [item["outcome"] for item in telemetry(stderr.getvalue(), MODULE.ATTEMPT_PREFIX)],
            ["transport", "transport", "transport"],
        )

    def test_incomplete_read_then_recovery_discards_partial_response(self):
        clock = Clock()
        prefix = b'{"type":"FeatureCollection",'
        partial = b'"secret-response-marker":true,"features":['
        incomplete_bytes = len(prefix) + len(partial)
        complete = json.dumps(fixture_document()).encode("utf-8")
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch.object(MODULE.time, "monotonic", side_effect=clock),
            mock.patch.object(MODULE.time, "sleep", side_effect=clock.sleep),
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                side_effect=[IncompleteResponse(partial, prefix=prefix), Response(complete)],
            ) as urlopen,
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            status = MODULE.main([])

        self.assertEqual(status, 0)
        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(len(records), 25)
        self.assertEqual(records[0]["id"], "activity-00")
        self.assertEqual(records[-1]["id"], "activity-24")
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(clock.sleeps, [1.0])
        attempts = telemetry(stderr.getvalue(), MODULE.ATTEMPT_PREFIX)
        self.assertEqual([item["outcome"] for item in attempts], ["transport", "succeeded"])
        self.assertEqual(attempts[0]["failure_phase"], "read")
        self.assertEqual(attempts[0]["status"], 200)
        self.assertEqual(attempts[0]["bytes"], incomplete_bytes)
        self.assertTrue(attempts[0]["retrying"])
        self.assertEqual(attempts[1]["bytes"], len(complete))
        summary = telemetry(stderr.getvalue(), MODULE.SUMMARY_PREFIX)[0]
        self.assertEqual(summary["bytes"], incomplete_bytes + len(complete))
        self.assertEqual(summary["records"], 25)
        self.assertEqual(summary["completeness"], "complete")
        self.assertNotIn("secret-response-marker", stderr.getvalue())

    def test_persistent_incomplete_reads_exhaust_attempts_without_output(self):
        clock = Clock()
        partials = [b"secret-response-marker", b"somewhat longer", b"last partial response"]
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch.object(MODULE.time, "monotonic", side_effect=clock),
            mock.patch.object(MODULE.time, "sleep", side_effect=clock.sleep),
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                side_effect=[IncompleteResponse(partial) for partial in partials],
            ) as urlopen,
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            status = MODULE.main([])

        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(clock.sleeps, [1.0, 2.0])
        attempts = telemetry(stderr.getvalue(), MODULE.ATTEMPT_PREFIX)
        self.assertEqual([item["bytes"] for item in attempts], [len(partial) for partial in partials])
        self.assertEqual([item["failure_phase"] for item in attempts], ["read"] * 3)
        self.assertEqual([item["outcome"] for item in attempts], ["transport"] * 3)
        self.assertEqual([item["retrying"] for item in attempts], [True, True, False])
        summary = telemetry(stderr.getvalue(), MODULE.SUMMARY_PREFIX)[0]
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["records"], 0)
        self.assertEqual(summary["bytes"], sum(map(len, partials)))
        self.assertEqual(summary["requests"]["attempted"], MODULE.MAX_ATTEMPTS)
        self.assertEqual(summary["metrics"]["failure_phase"], "read")
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertNotIn("secret-response-marker", stderr.getvalue())

    def test_incomplete_read_at_aggregate_deadline_is_terminal(self):
        clock = Clock()
        partial = b"secret-response-marker at deadline"
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch.object(MODULE.time, "monotonic", side_effect=clock),
            mock.patch.object(MODULE.time, "sleep", side_effect=clock.sleep),
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=IncompleteResponse(
                    partial,
                    clock=clock,
                    advance=MODULE.REQUEST_BUDGET_SECONDS,
                ),
            ) as urlopen,
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            status = MODULE.main([])

        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(clock.sleeps, [])
        attempt = telemetry(stderr.getvalue(), MODULE.ATTEMPT_PREFIX)[0]
        self.assertEqual(attempt["bytes"], len(partial))
        self.assertEqual(attempt["failure_phase"], "read")
        self.assertEqual(attempt["outcome"], "transport")
        self.assertFalse(attempt["retrying"])
        summary = telemetry(stderr.getvalue(), MODULE.SUMMARY_PREFIX)[0]
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["records"], 0)
        self.assertEqual(summary["bytes"], len(partial))
        self.assertEqual(summary["requests"]["attempted"], 1)
        self.assertEqual(summary["metrics"]["failure_phase"], "read_deadline")
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertNotIn("secret-response-marker", stderr.getvalue())

    def test_non_transient_http_failure_is_not_retried(self):
        error = urllib.error.HTTPError(MODULE.BASE, 404, "not found", {}, None)
        status, stdout, stderr, urlopen = run_main(error)

        self.assertEqual(status, 1)
        self.assertEqual(stdout, "")
        self.assertEqual(urlopen.call_count, 1)
        attempt = telemetry(stderr, MODULE.ATTEMPT_PREFIX)[0]
        self.assertEqual(attempt["status"], 404)
        self.assertEqual(attempt["failure_phase"], "status")
        self.assertFalse(attempt["retrying"])

    def test_malformed_or_truncated_geojson_never_publishes_partial_records(self):
        cases = [
            b'{"type":"FeatureCollection","features":[',
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "numberReturned": 2,
                    "features": fixture_document(1)["features"],
                }
            ).encode("utf-8"),
        ]
        for body in cases:
            with self.subTest(body=body):
                status, stdout, stderr, urlopen = run_main([Response(body)])
                self.assertEqual(status, 1)
                self.assertEqual(stdout, "")
                self.assertEqual(urlopen.call_count, 1)
                summary = telemetry(stderr, MODULE.SUMMARY_PREFIX)[0]
                self.assertEqual(summary["records"], 0)
                self.assertEqual(summary["completeness"], "failed")
                self.assertIn(summary["metrics"]["failure_phase"], {"parse", "validate"})

    def test_deadline_exhaustion_during_response_read_is_terminal(self):
        clock = Clock()
        slow_response = AdvancingResponse(
            json.dumps(fixture_document(1)).encode("utf-8"),
            clock,
            MODULE.REQUEST_BUDGET_SECONDS + 1,
        )
        with (
            mock.patch.object(MODULE.time, "monotonic", side_effect=clock),
            mock.patch.object(MODULE.time, "sleep", side_effect=clock.sleep),
            mock.patch.object(MODULE.urllib.request, "urlopen", return_value=slow_response) as urlopen,
            contextlib.redirect_stdout(io.StringIO()) as stdout,
            contextlib.redirect_stderr(io.StringIO()) as stderr,
        ):
            status = MODULE.main([])

        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(urlopen.call_count, 1)
        summary = telemetry(stderr.getvalue(), MODULE.SUMMARY_PREFIX)[0]
        self.assertEqual(summary["metrics"]["failure_phase"], "read_deadline")
        self.assertGreater(summary["bytes"], 0)

    def test_deadline_exhaustion_during_backoff_prevents_another_attempt(self):
        clock = Clock()
        clock.sleep_overrun = 0.6
        with (
            mock.patch.object(MODULE, "REQUEST_BUDGET_SECONDS", 1.5),
            mock.patch.object(MODULE.time, "monotonic", side_effect=clock),
            mock.patch.object(MODULE.time, "sleep", side_effect=clock.sleep),
            mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=TimeoutError()) as urlopen,
            contextlib.redirect_stdout(io.StringIO()) as stdout,
            contextlib.redirect_stderr(io.StringIO()) as stderr,
        ):
            status = MODULE.main([])

        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(clock.sleeps, [1.0])
        summary = telemetry(stderr.getvalue(), MODULE.SUMMARY_PREFIX)[0]
        self.assertEqual(summary["metrics"]["failure_phase"], "backoff_deadline")

    def test_source_configuration_preserves_cadence_and_airflow_retry_bound(self):
        config = CONFIG.read_text(encoding="utf-8")
        self.assertIn('schedule: "10 6 * * *"', config)
        self.assertIn("retries: 1", config)
        self.assertIn("timeout_minutes: 10", config)
        self.assertIn("run_summary:\n  required: true", config)
        self.assertIn("120 seconds per task attempt", config)
        self.assertIn("240 seconds", config)


if __name__ == "__main__":
    unittest.main()
