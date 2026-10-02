import importlib.util
import io
import json
import unittest
from pathlib import Path
from unittest.mock import call, patch
import urllib.error


SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_speedrun_runs.py"
SPEC = importlib.util.spec_from_file_location("fetch_speedrun_runs", SCRIPT_PATH)
speedrun = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(speedrun)


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class RawResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def http_error(code):
    return urllib.error.HTTPError(
        speedrun.BASE + "/runs", code, "error", None, None
    )


class QueueOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request.full_url, timeout))
        if not self.responses:
            raise AssertionError(f"unexpected HTTP request: {request.full_url}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, bytes):
            return RawResponse(response)
        return JsonResponse(response)


class SpeedrunRunsTests(unittest.TestCase):
    def run_get(self, responses, **params):
        opener = QueueOpener(responses)
        with patch("urllib.request.urlopen", side_effect=opener):
            with patch("time.sleep") as sleeper:
                result = speedrun._get("runs", **params)
        return opener, sleeper, result

    def test_first_attempt_success_makes_one_request(self):
        opener, sleeper, result = self.run_get([{"data": []}], max=20)
        self.assertEqual({"data": []}, result)
        self.assertEqual(1, len(opener.requests))
        self.assertEqual(45, opener.requests[0][1])
        self.assertEqual([], sleeper.call_args_list)

    def test_transient_500_is_retried(self):
        opener, sleeper, result = self.run_get(
            [http_error(500), {"data": []}], max=20
        )
        self.assertEqual({"data": []}, result)
        self.assertEqual(2, len(opener.requests))
        self.assertEqual([call(speedrun.RETRY_BASE_DELAY)], sleeper.call_args_list)

    def test_persistent_500_fails_after_bounded_attempts(self):
        opener = QueueOpener([http_error(500)] * 10)
        with patch("urllib.request.urlopen", side_effect=opener):
            with patch("time.sleep") as sleeper:
                with self.assertRaises(urllib.error.HTTPError) as raised:
                    speedrun._get("runs", max=20)
        self.assertEqual(500, raised.exception.code)
        self.assertEqual(speedrun.MAX_ATTEMPTS, len(opener.requests))
        self.assertEqual(
            [call(speedrun.RETRY_BASE_DELAY), call(speedrun.RETRY_BASE_DELAY * 2)],
            sleeper.call_args_list,
        )

    def test_non_retryable_404_fails_immediately(self):
        opener = QueueOpener([http_error(404), {"data": []}])
        with patch("urllib.request.urlopen", side_effect=opener):
            with patch("time.sleep") as sleeper:
                with self.assertRaises(urllib.error.HTTPError) as raised:
                    speedrun._get("runs", max=20)
        self.assertEqual(404, raised.exception.code)
        self.assertEqual(1, len(opener.requests))
        self.assertEqual([], sleeper.call_args_list)

    def test_connection_error_is_retried(self):
        opener, sleeper, result = self.run_get(
            [urllib.error.URLError("connection reset"), {"data": []}], max=20
        )
        self.assertEqual({"data": []}, result)
        self.assertEqual(2, len(opener.requests))
        self.assertEqual(1, sleeper.call_count)

    def test_malformed_json_is_not_retried(self):
        opener = QueueOpener([b"not json", {"data": []}])
        with patch("urllib.request.urlopen", side_effect=opener):
            with patch("time.sleep") as sleeper:
                with self.assertRaises(ValueError):
                    speedrun._get("runs", max=20)
        self.assertEqual(1, len(opener.requests))
        self.assertEqual([], sleeper.call_args_list)

    def test_request_url_carries_embedded_params(self):
        opener, sleeper, result = self.run_get(
            [{"data": []}],
            orderby="submitted",
            direction="desc",
            max=20,
            offset=0,
            embed="game,category,players,platform",
            status="verified",
        )
        url, timeout = opener.requests[0]
        self.assertEqual(45, timeout)
        self.assertTrue(url.startswith(speedrun.BASE + "/runs?"), url)
        self.assertIn("status=verified", url)
        self.assertIn("embed=game%2Ccategory%2Cplayers%2Cplatform", url)


if __name__ == "__main__":
    unittest.main()
