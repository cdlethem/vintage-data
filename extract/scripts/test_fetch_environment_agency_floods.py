import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock
import urllib.error

SCRIPT = Path(__file__).with_name("fetch_environment_agency_floods.py")
SPEC = importlib.util.spec_from_file_location("fetch_environment_agency_floods", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
CONFIG = SCRIPT.parents[1] / "sources" / "environment_agency_floods.yml"


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def floods_document(items=None):
    if items is None:
        items = [
            {
                "@id": "http://environment.data.gov.uk/flood-monitoring/id/floods/1",
                "floodAreaID": "011FWFCH0",
                "severity": "Flood warning",
                "severityLevel": 2,
                "message": "Flooding is possible for the River Test.",
                "timeRaised": "2026-09-16T05:00:00Z",
                "timeSeverityChanged": "2026-09-16T05:00:00Z",
                "timeMessageChanged": "2026-09-16T05:00:00Z",
                "isTidal": False,
                "eaAreaName": "Solent and South Downs",
                "eaRegionName": "South East",
            }
        ]
    return {"items": items}


class FetchEnvironmentAgencyFloodsTests(unittest.TestCase):
    def fetch(self, document=None, **overrides):
        arguments = {"limit": overrides.pop("limit", 1000), "timeout": overrides.pop("timeout", 60)}
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=response(document if document is not None else floods_document()),
        ) as urlopen:
            records = list(MODULE.fetch_warnings(**arguments))
        return records, urlopen

    def test_normalizes_flood_warning_and_preserves_source_fields(self):
        records, urlopen = self.fetch()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "environment_agency_flood_warnings")
        self.assertEqual(record["id"], "http://environment.data.gov.uk/flood-monitoring/id/floods/1")
        self.assertEqual(record["severity"], "Flood warning")
        self.assertEqual(record["floodAreaID"], "011FWFCH0")
        self.assertIn("fetched_at", record)
        urlopen.assert_called_once()
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 60})

    def test_sends_identifying_headers_for_content_negotiation(self):
        _, urlopen = self.fetch()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, MODULE.URL)
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertTrue(request.get_header("Accept-language", "").startswith("en-GB"))
        self.assertTrue(request.get_header("User-agent"))

    def test_falls_back_to_flood_area_id_when_at_id_missing(self):
        document = floods_document(
            [
                {
                    "floodAreaID": "011FWFCH1",
                    "severity": "Flood alert",
                    "severityLevel": 3,
                }
            ]
        )
        records, _ = self.fetch(document)
        self.assertEqual(records[0]["id"], "011FWFCH1")

    def test_empty_items_is_a_valid_no_warning_state(self):
        records, _ = self.fetch(floods_document([]))
        self.assertEqual(records, [])

    def test_nonpositive_limit_fails_without_an_http_call(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
            for limit in (0, -1):
                with self.subTest(limit=limit), self.assertRaisesRegex(ValueError, "limit must be positive"):
                    list(MODULE.fetch_warnings(limit=limit))
        urlopen.assert_not_called()

    def test_limit_bounds_emitted_records(self):
        document = floods_document(
            [
                {"@id": "a", "severity": "Flood alert"},
                {"@id": "b", "severity": "Flood warning"},
                {"@id": "c", "severity": "Severe flood warning"},
            ]
        )
        records, _ = self.fetch(document, limit=2)
        self.assertEqual([record["id"] for record in records], ["a", "b"])

    def test_rejects_response_missing_items(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"foo": "bar"})):
            with self.assertRaisesRegex(TypeError, "flood response is missing items"):
                list(MODULE.fetch_warnings())

    def test_rejects_warning_missing_identifiers(self):
        document = floods_document([{"severity": "Flood alert"}])
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
            with self.assertRaisesRegex(ValueError, "missing @id and floodAreaID"):
                list(MODULE.fetch_warnings())

    def test_503_recovers_after_one_delay(self):
        error = urllib.error.HTTPError(MODULE.URL, 503, "Backend fetch failed", {}, None)
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[error, response(floods_document())]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_warnings())
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], floods_document()["items"][0]["@id"])
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.SERVICE_RETRY_DELAY_SECONDS)

    def test_non_503_http_error_fails_without_retry(self):
        error = urllib.error.HTTPError(MODULE.URL, 404, "Not found", {}, None)
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(
            MODULE.time, "sleep"
        ) as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                list(MODULE.fetch_warnings())
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_main_exhausted_503_fails_with_redacted_stderr_and_no_records(self):
        error = urllib.error.HTTPError(
            MODULE.URL + "?token=private", 503, "Backend secret=private", {}, None
        )
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(
            MODULE.time, "sleep"
        ) as sleep, mock.patch("sys.argv", ["fetch_environment_agency_floods.py"]), mock.patch(
            "sys.stdout", new_callable=io.StringIO
        ) as output, mock.patch("sys.stderr", new_callable=io.StringIO) as diagnostics:
            status = MODULE.main()
        self.assertEqual(status, 1)
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(diagnostics.getvalue(), "Environment Agency flood warnings: HTTP 503\n")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.SERVICE_RETRY_DELAY_SECONDS)

    def test_main_successful_empty_response_is_not_a_failure(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(floods_document([]))
        ) as urlopen, mock.patch("sys.argv", ["fetch_environment_agency_floods.py"]), mock.patch(
            "sys.stdout", new_callable=io.StringIO
        ) as output, mock.patch("sys.stderr", new_callable=io.StringIO) as diagnostics:
            status = MODULE.main()
        self.assertEqual(status, 0)
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(diagnostics.getvalue(), "")
        urlopen.assert_called_once()

    def test_main_emits_ndjson(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(floods_document())
        ), mock.patch("sys.argv", ["fetch_environment_agency_floods.py"]):
            import contextlib

            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                MODULE.main()
        lines = [line for line in buffer.getvalue().splitlines() if line]
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["source"], "environment_agency_flood_warnings")

    def test_source_configuration_preserves_endpoint_cadence_and_schema_contract(self):
        config = CONFIG.read_text(encoding="utf-8")
        self.assertRegex(config, r"(?m)^script:\s*fetch_environment_agency_floods\.py\s*$")
        self.assertRegex(config, r'(?m)^schedule:\s*"4-59/15 \* \* \* \*"')
        self.assertRegex(config, r"(?m)^enabled:\s*true\s*$")
        self.assertRegex(config, r"(?m)^timeout_minutes:\s*10\s*$")

    def test_url_and_source_are_unchanged(self):
        self.assertEqual(MODULE.URL, "https://environment.data.gov.uk/flood-monitoring/id/floods")
        self.assertEqual(MODULE.SOURCE, "environment_agency_flood_warnings")


if __name__ == "__main__":
    unittest.main()
