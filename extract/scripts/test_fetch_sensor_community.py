import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock
import urllib.error


SCRIPT = Path(__file__).with_name("fetch_sensor_community.py")
SPEC = importlib.util.spec_from_file_location("fetch_sensor_community", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def response(document):
    stream = io.BytesIO(document)
    stream.status = 200
    return stream


def reading(identifier=1):
    return {
        "id": identifier,
        "timestamp": "2026-09-21 12:00:00",
        "location": {
            "id": 10,
            "country": "DE",
            "latitude": "52.5",
            "longitude": "13.4",
            "altitude": "40",
            "indoor": 0,
            "exact_location": 0,
        },
        "sensor": {
            "id": 20,
            "sensor_type": {"name": "SDS011", "manufacturer": "Nova Fitness"},
        },
        "sensordatavalues": [{"value_type": "P2", "value": "12.5"}],
    }


class FetchSensorCommunityFailureTests(unittest.TestCase):
    def run_failure(self, urlopen_result):
        stdout = io.StringIO()
        stderr = io.StringIO()
        patcher = (mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urlopen_result)
                   if isinstance(urlopen_result, BaseException)
                   else mock.patch.object(MODULE.urllib.request, "urlopen", return_value=urlopen_result))
        with patcher, contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = MODULE.main([])
        summaries = [line for line in stderr.getvalue().splitlines()
                     if line.startswith(MODULE.SUMMARY_PREFIX)]
        self.assertNotEqual(status, 0)
        self.assertEqual(len(summaries), 1)
        self.assertLess(len((summaries[0] + "\n").encode()), MODULE.MAX_SUMMARY_BYTES)
        summary = json.loads(summaries[0][len(MODULE.SUMMARY_PREFIX):])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["partitions"], {"attempted": 1, "succeeded": 0, "failed": 1})
        return summary, stdout.getvalue(), stderr.getvalue()

    def test_request_failure_emits_one_bounded_redacted_failed_summary(self):
        secret = "request-secret"
        error = urllib.error.URLError(
            f"https://fixture-user:fixture-password@example.invalid/?token={secret} " + "x" * 100_000
        )
        summary, stdout, stderr = self.run_failure(error)
        self.assertEqual(summary["metrics"]["failure_phase"], "request")
        self.assertEqual(stdout, "")
        self.assertNotIn(secret, stderr)
        self.assertNotIn("fixture-password", stderr)
        self.assertIn("[REDACTED]", summary["error"])

    def test_decode_failure_emits_one_bounded_failed_summary(self):
        summary, stdout, _ = self.run_failure(response(b'{"broken":'))
        self.assertEqual(summary["metrics"]["failure_phase"], "decode")
        self.assertEqual(summary["records"], 0)
        self.assertEqual(stdout, "")

    def test_response_iteration_failure_emits_one_bounded_failed_summary(self):
        document = json.dumps([reading(), "not-an-object"]).encode()
        summary, stdout, _ = self.run_failure(response(document))
        self.assertEqual(summary["metrics"]["failure_phase"], "iteration")
        self.assertEqual(summary["records"], 1)
        self.assertEqual(len(stdout.splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
