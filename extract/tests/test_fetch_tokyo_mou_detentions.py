import importlib.util
import io
import pathlib
import socket
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_tokyo_mou_detentions.py"
SPEC = importlib.util.spec_from_file_location("fetch_tokyo_mou_detentions", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def table_document(*, valid=True):
    headers = MODULE.EXPECTED if valid else MODULE.EXPECTED[:-1]
    header_cells = "".join(f"<th>{header}</th>" for header in ["No.", *headers])
    row = [
        "1", "9876543", "Example", "Panama", "2001", "12345", "Cargo",
        "Class", "RO", "Company", "Tokyo", "2026-09-01", "", "Deficiency",
    ]
    row_cells = "".join(f"<td>{cell}</td>" for cell in row)
    return f"<table><tr>{header_cells}</tr><tr>{row_cells}</tr></table>".encode()


class FetchDetentionsTests(unittest.TestCase):
    def fetch(self, response_or_effect, **kwargs):
        urlopen_kwargs = (
            {"return_value": response_or_effect}
            if isinstance(response_or_effect, Response)
            else {"side_effect": response_or_effect}
        )
        with mock.patch.object(MODULE.urllib.request, "urlopen", **urlopen_kwargs) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_detentions(year=2026, month=9, **kwargs))
        return records, urlopen, sleep

    def test_immediate_success_uses_one_request_and_preserves_payload(self):
        records, urlopen, sleep = self.fetch(Response(table_document()), timeout=23)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], "9876543:2026-09-01")
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, MODULE.URL)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 23)
        self.assertEqual(
            request.data,
            b"Mode=DetList&MOU=&Auth=&Src=online&Type=Auth&Month=09&Year=2026&SaveFile=",
        )
        sleep.assert_not_called()

    def test_retries_connection_timeout_then_succeeds(self):
        records, urlopen, sleep = self.fetch(
            [socket.timeout("read timed out"), Response(table_document())]
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS[0])

    def test_retries_urllib_wrapped_timeout_then_succeeds(self):
        records, urlopen, sleep = self.fetch(
            [urllib.error.URLError(TimeoutError("timed out")), Response(table_document())]
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS[0])

    def test_timeout_exhaustion_is_bounded_and_reports_attempts(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=TimeoutError("timed out")) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(TimeoutError, r"after 3 attempts \(timeout\)"):
                list(MODULE.fetch_detentions(year=2026, month=9))

        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])

    def test_malformed_table_fails_without_retries(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(table_document(valid=False))) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "missing the detention table"):
                list(MODULE.fetch_detentions(year=2026, month=9))

        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_non_timeout_transport_errors_fail_without_retries(self):
        error = urllib.error.URLError("temporary failure in name resolution")
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(urllib.error.URLError, "name resolution"):
                list(MODULE.fetch_detentions(year=2026, month=9))

        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_http_errors_fail_without_retries(self):
        error = urllib.error.HTTPError(MODULE.URL, 503, "unavailable", {}, None)
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                list(MODULE.fetch_detentions(year=2026, month=9))

        urlopen.assert_called_once()
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
