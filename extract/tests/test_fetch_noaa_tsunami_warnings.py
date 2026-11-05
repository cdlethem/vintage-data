import contextlib
import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_noaa_tsunami_warnings.py"
SPEC = importlib.util.spec_from_file_location("fetch_noaa_tsunami_warnings", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

PAAQ_BODY = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">'
    "  <identifier>PAAQ-1-to0j1g</identifier>"
    "  <sender>ntwc@noaa.gov</sender>"
    "  <sent>2026-11-05T05:01:47-00:00</sent>"
    "  <status>Actual</status>"
    "  <msgType>Alert</msgType>"
    "  <source>NTWC</source>"
    "  <scope>Public</scope>"
    "  <incidents>to0j1g</incidents>"
    "  <info>"
    "    <category>Geo</category>"
    "    <event>Tsunami Information</event>"
    "    <urgency>Unknown</urgency>"
    "    <severity>Minor</severity>"
    "    <certainty>Unlikely</certainty>"
    "    <expires>2026-11-05T06:01:47-00:00</expires>"
    "    <senderName>NWS National Tsunami Warning Center Palmer AK</senderName>"
    "    <headline>This is a Tsunami Information Statement.</headline>"
    "    <description>Event details: Preliminary magnitude 4.5 (Ml) earthquake</description>"
    "    <instruction>There is NO tsunami danger from this earthquake.</instruction>"
    "    <web>http://www.tsunami.gov/events/PAAQ/2026/11/05/to0j1g/1/WEAK53/WEAK53.txt</web>"
    "    <parameter><valueName>EventLocationName</valueName>"
    "      <value>75 miles SW of Eureka, California</value></parameter>"
    "    <parameter><valueName>EventPreliminaryMagnitude</valueName><value>4.5</value></parameter>"
    "  </info>"
    "</alert>"
).encode()

PHEB_BODY = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">'
    "  <identifier>PHEB-1-26297050</identifier>"
    "  <sent>2026-10-25T02:27:01-00:00</sent>"
    "  <info><event>Tsunami Information</event><severity>Minor</severity></info>"
    "</alert>"
).encode()


def alerts_wrapper(bodies: list[bytes]) -> bytes:
    inner = b"".join(
        body.replace(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>", b"", 1) for body in bodies
    )
    return (
        b'<?xml version="1.0" encoding="UTF-8"?><alerts xmlns="urn:oasis:names:tc:emergency:cap:1.2">'
        + inner
        + b"</alerts>"
    )


def fake_bodies(paaq: bytes, phieb: bytes):
    """Route mocked urlopen calls to the per-station bodies by URL."""

    def urlopen(request, timeout=None):
        url = request.full_url
        if "PAAQCAP" in url:
            return Response(paaq)
        if "PHEBCAP" in url:
            return Response(phieb)
        raise AssertionError(f"unexpected URL {url}")

    return urlopen


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def run_main(argv, urlopen_side, tmp_path):
    stdout, stderr = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urlopen_side) as urlopen, \
            mock.patch.object(MODULE.time, "sleep") as sleep, \
            mock.patch.object(MODULE.sys, "argv", ["fetch_noaa_tsunami_warnings.py", *argv]), \
            contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        raise_error = None
        try:
            MODULE.main()
        except BaseException as error:  # noqa: BLE001 - assert on type in tests
            raise_error = error
    records = [json.loads(line) for line in stdout.getvalue().splitlines() if line.strip()]
    summary_line = next(
        (line for line in stderr.getvalue().splitlines() if line.startswith(MODULE.SUMMARY_PREFIX)),
        None,
    )
    summary = json.loads(summary_line[len(MODULE.SUMMARY_PREFIX) :]) if summary_line else None
    return records, summary, urlopen, sleep, raise_error


class ParseAlertsTests(unittest.TestCase):
    def test_single_alert_root(self):
        alerts = MODULE.parse_alerts(PAAQ_BODY, "PAAQ")
        self.assertEqual(len(alerts), 1)
        alert = alerts[0]
        self.assertEqual(alert["identifier"], "PAAQ-1-to0j1g")
        self.assertEqual(alert["sent"], "2026-11-05T05:01:47-00:00")
        self.assertEqual(alert["event"], "Tsunami Information")
        self.assertEqual(alert["severity"], "Minor")
        self.assertEqual(alert["description"], "Event details: Preliminary magnitude 4.5 (Ml) earthquake")
        self.assertEqual(alert["web"], "http://www.tsunami.gov/events/PAAQ/2026/11/05/to0j1g/1/WEAK53/WEAK53.txt")
        self.assertEqual(alert["station"], "PAAQ")
        self.assertEqual(alert["parameters"], {
            "EventLocationName": "75 miles SW of Eureka, California",
            "EventPreliminaryMagnitude": "4.5",
        })

    def test_alerts_wrapper_root(self):
        alerts = MODULE.parse_alerts(alerts_wrapper([PAAQ_BODY, PHEB_BODY]), "PHEB")
        self.assertEqual([a["identifier"] for a in alerts], ["PAAQ-1-to0j1g", "PHEB-1-26297050"])
        self.assertEqual(alerts[0]["station"], "PHEB")
        self.assertEqual(alerts[1]["parameters"], {})

    def test_missing_info_element_fails(self):
        body = (
            b'<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">'
            b"<identifier>PAAQ-1-x</identifier><sent>2026-11-05T05:01:47-00:00</sent></alert>"
        )
        with self.assertRaises(ValueError):
            MODULE.parse_alerts(body, "PAAQ")

    def test_missing_identifier_fails(self):
        body = (
            b'<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">'
            b"<sent>2026-11-05T05:01:47-00:00</sent><info><event>Tsunami Warning</event></info></alert>"
        )
        with self.assertRaisesRegex(ValueError, "identifier"):
            MODULE.parse_alerts(body, "PAAQ")

    def test_unexpected_root_fails(self):
        with self.assertRaisesRegex(ValueError, "unexpected CAP root"):
            MODULE.parse_alerts(b"<feed></feed>", "PAAQ")

    def test_invalid_xml_fails(self):
        with self.assertRaisesRegex(ValueError, "not XML"):
            MODULE.parse_alerts(b"<alert", "PAAQ")

    def test_empty_alerts_wrapper_yields_no_records(self):
        body = b'<alerts xmlns="urn:oasis:names:tc:emergency:cap:1.2"></alerts>'
        self.assertEqual(MODULE.parse_alerts(body, "PHEB"), [])


class FetchCapTests(unittest.TestCase):
    def fetch(self, side_effect, **kwargs):
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=side_effect) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            raised = None
            body = None
            try:
                body = MODULE.fetch_cap(MODULE.STATIONS["PAAQ"], 20)
            except BaseException as error:  # noqa: BLE001
                raised = error
        return body, raised, urlopen, sleep

    def test_immediate_success_makes_one_request(self):
        body, raised, urlopen, sleep = self.fetch(lambda request, timeout=None: Response(PAAQ_BODY))
        self.assertEqual(body, PAAQ_BODY)
        self.assertIsNone(raised)
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_retries_transient_503_then_succeeds(self):
        error = urllib.error.HTTPError(MODULE.STATIONS["PAAQ"], 503, "unavailable", {}, None)
        body, raised, urlopen, sleep = self.fetch([error, Response(PAAQ_BODY)])
        self.assertEqual(body, PAAQ_BODY)
        self.assertIsNone(raised)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS[0])

    def test_retries_timeout_then_succeeds(self):
        body, raised, urlopen, sleep = self.fetch([TimeoutError("timed out"), Response(PAAQ_BODY)])
        self.assertEqual(body, PAAQ_BODY)
        self.assertIsNone(raised)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS[0])

    def test_exhausted_retries_raise_with_attempt_count(self):
        error = urllib.error.HTTPError(MODULE.STATIONS["PAAQ"], 503, "unavailable", {}, None)
        body, raised, urlopen, sleep = self.fetch([error, error, error])
        self.assertIsNone(body)
        self.assertIsInstance(raised, RuntimeError)
        self.assertIn("3 attempts", str(raised))
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])

    def test_non_retriable_status_fails_without_retry(self):
        error = urllib.error.HTTPError(MODULE.STATIONS["PAAQ"], 404, "not found", {}, None)
        body, raised, urlopen, sleep = self.fetch([error])
        self.assertIsNone(body)
        self.assertIsInstance(raised, RuntimeError)
        self.assertIn("status=404", str(raised))
        urlopen.assert_called_once()
        sleep.assert_not_called()


class MainRunTests(unittest.TestCase):
    def test_first_run_emits_every_product_and_writes_state(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            state_file = pathlib.Path(directory) / "state.json"
            records, summary, urlopen, sleep, raised = run_main(
                ["--state-file", str(state_file)], fake_bodies(PAAQ_BODY, PHEB_BODY), directory
            )
            state = json.loads(state_file.read_text())
        self.assertIsNone(raised)
        self.assertEqual([r["id"] for r in records], ["PAAQ-1-to0j1g", "PHEB-1-26297050"])
        self.assertTrue(all(r["source"] == MODULE.SOURCE for r in records))
        self.assertTrue(all(r["fetched_at"] for r in records))
        self.assertEqual(summary["new_records"], 2)
        self.assertEqual(summary["tracked_ids"], 2)
        self.assertEqual(summary["stations"], {
            "PAAQ": {"alerts": 1, "new": 1},
            "PHEB": {"alerts": 1, "new": 1},
        })
        self.assertEqual(state["source"], MODULE.SOURCE)
        self.assertEqual(set(state["records"]), {"PAAQ-1-to0j1g", "PHEB-1-26297050"})

    def test_second_run_emits_nothing_and_state_is_stable(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            state_file = pathlib.Path(directory) / "state.json"
            records, summary, _, _, raised = run_main(
                ["--state-file", str(state_file)], fake_bodies(PAAQ_BODY, PHEB_BODY), directory
            )
            self.assertIsNone(raised)
            records, summary, _, _, raised = run_main(
                ["--state-file", str(state_file)], fake_bodies(PAAQ_BODY, PHEB_BODY), directory
            )
        self.assertIsNone(raised)
        self.assertEqual(records, [])
        self.assertEqual(summary["new_records"], 0)
        self.assertEqual(summary["stations"]["PAAQ"]["new"], 0)

    def test_revised_product_gets_new_record(self):
        import tempfile

        revised = PAAQ_BODY.replace(
            b"<identifier>PAAQ-1-to0j1g</identifier>", b"<identifier>PAAQ-2-to0j1g</identifier>"
        ).replace(b"<severity>Minor</severity>", b"<severity>Moderate</severity>")
        with tempfile.TemporaryDirectory() as directory:
            state_file = pathlib.Path(directory) / "state.json"
            _, _, _, _, raised = run_main(
                ["--state-file", str(state_file)], fake_bodies(PAAQ_BODY, PHEB_BODY), directory
            )
            self.assertIsNone(raised)
            records, summary, _, _, raised = run_main(
                ["--state-file", str(state_file)], fake_bodies(revised, PHEB_BODY), directory
            )
        self.assertIsNone(raised)
        self.assertEqual([r["id"] for r in records], ["PAAQ-2-to0j1g"])
        self.assertEqual(records[0]["severity"], "Moderate")
        self.assertEqual(summary["new_records"], 1)

    def test_no_state_emits_everything_and_writes_nothing(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            records, summary, _, _, raised = run_main(["--no-state"], fake_bodies(PAAQ_BODY, PHEB_BODY), directory)
        self.assertIsNone(raised)
        self.assertEqual([r["id"] for r in records], ["PAAQ-1-to0j1g", "PHEB-1-26297050"])
        self.assertEqual(summary["tracked_ids"], 0)

    def test_empty_feed_is_a_quiet_successful_run(self):
        import tempfile

        empty = b'<alerts xmlns="urn:oasis:names:tc:emergency:cap:1.2"></alerts>'
        with tempfile.TemporaryDirectory() as directory:
            records, summary, _, _, raised = run_main(
                ["--no-state"], fake_bodies(empty, empty), directory
            )
        self.assertIsNone(raised)
        self.assertEqual(records, [])
        self.assertEqual(summary["new_records"], 0)

    def test_malformed_feed_fails_without_state_write(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            state_file = pathlib.Path(directory) / "state.json"
            _, _, _, _, raised = run_main(
                ["--state-file", str(state_file)], fake_bodies(b"<bogus>", PHEB_BODY), directory
            )
        self.assertIsInstance(raised, ValueError)
        self.assertFalse(state_file.exists())


if __name__ == "__main__":
    unittest.main()
