import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
import urllib.error
from datetime import datetime, timezone
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_uk_parliament_procedural_activity.py"
SPEC = importlib.util.spec_from_file_location("fetch_uk_parliament_procedural_activity", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = {} if headers is None else headers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def http_502(retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.ENDPOINT, 502, "Bad Gateway", headers, io.BytesIO(b"origin failure"))


def row(local_id="item-1", laying_date="2026-09-16T00:00:00Z"):
    return {
        "LocalId": local_id,
        "LayingDate": laying_date,
        "WithdrawalDate": None,
        "BusinessItemDate": [],
    }


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 9, 17, 12, 34, 56, tzinfo=timezone.utc)


class FetchUkParliamentProceduralActivityTests(unittest.TestCase):
    def test_recovers_from_502_with_bounded_retry_after_values(self):
        cases = ((None, 1), ("not-a-delay", 1), ("900", 30))
        for retry_after, expected_delay in cases:
            with self.subTest(retry_after=retry_after), mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=[http_502(retry_after), response({"value": []})]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(MODULE.collect(None, 17), [])
                self.assertEqual(sleep.call_args_list, [mock.call(expected_delay)])
                self.assertEqual(urlopen.call_count, 2)

    def test_retries_the_identical_page_after_502(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[http_502("2"), response({"value": [row()]})]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(MODULE.collect(None, 23), [row()])
        requests = [call.args[0].full_url for call in urlopen.call_args_list]
        self.assertEqual(requests, [MODULE.endpoint_url(None, 0), MODULE.endpoint_url(None, 0)])
        self.assertEqual(sleep.call_args_list, [mock.call(2)])

    def test_stops_after_finite_502_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[http_502("1")] * MODULE.MAX_502_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "retry limit exhausted"):
                MODULE.collect(None, 9)
        self.assertEqual(urlopen.call_count, MODULE.MAX_502_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_502_ATTEMPTS - 1)

    def test_stops_when_page_retry_delay_budget_is_exhausted(self):
        with mock.patch.object(MODULE, "MAX_RETRY_CUMULATIVE_DELAY_SECONDS", 40), mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[http_502("30"), http_502("30")]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "retry delay budget exhausted"):
                MODULE.collect(None, 9)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(30)])

    def test_fails_immediately_for_non_retryable_http_errors(self):
        error = urllib.error.HTTPError(MODULE.ENDPOINT, 503, "Unavailable", {}, io.BytesIO(b"down"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(
            MODULE.time, "sleep"
        ) as sleep:
            with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
                MODULE.collect(None, 9)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_rejects_invalid_json_without_retrying(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=Response(b"not json")
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "invalid JSON"):
                MODULE.collect(None, 9)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_rejects_malformed_records_without_retrying(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response({"value": [{"LocalId": "item-1"}]})
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "LayingDate"):
                MODULE.collect(None, 9)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_main_does_not_advance_state_after_an_incomplete_second_page(self):
        first_page = [row(f"item-{index:03d}") for index in range(MODULE.PAGE_SIZE)]
        original_state = {
            "version": MODULE.STATE_VERSION,
            "source": MODULE.SOURCE,
            "watermark": "2026-09-01T00:00:00Z",
            "boundary_ids": ["old-item"],
            "known": {"old-item": "old-hash"},
            "last_full_reconciliation": "2026-09-16T00:00:00Z",
        }
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            state_path.write_text(json.dumps(original_state), encoding="utf-8")
            original_contents = state_path.read_bytes()
            side_effect = [response({"value": first_page})] + [http_502("1")] * MODULE.MAX_502_ATTEMPTS
            with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=side_effect), mock.patch.object(
                MODULE.time, "sleep"
            ), mock.patch.object(sys, "argv", [str(SCRIPT), "--state-file", str(state_path)]), contextlib.redirect_stdout(
                io.StringIO()
            ):
                with self.assertRaisesRegex(RuntimeError, "retry limit exhausted"):
                    MODULE.main()
            self.assertEqual(state_path.read_bytes(), original_contents)

    def test_main_preserves_successful_output_and_state_semantics(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            standard_output = io.StringIO()
            with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"value": [row()]})), mock.patch.object(
                MODULE, "datetime", FixedDateTime
            ), mock.patch.object(sys, "argv", [str(SCRIPT), "--state-file", str(state_path)]), contextlib.redirect_stdout(
                standard_output
            ):
                MODULE.main()
            record = json.loads(standard_output.getvalue())
            self.assertEqual(record["id"], "item-1")
            self.assertEqual(record["source"], MODULE.SOURCE)
            self.assertEqual(record["fetched_at"], "2026-09-17T12:34:56+00:00")
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["watermark"], "2026-09-16T00:00:00Z")
            self.assertEqual(state["boundary_ids"], ["item-1"])
            self.assertEqual(state["last_full_reconciliation"], "2026-09-17T12:34:56+00:00")
            self.assertEqual(state["known"], {"item-1": MODULE.canonical_hash(row())})


if __name__ == "__main__":
    unittest.main()
