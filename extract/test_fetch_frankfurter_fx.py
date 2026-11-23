import contextlib
import importlib.util
import io
import json
from datetime import date
from pathlib import Path
import unittest
from unittest import mock
import urllib.error
import urllib.parse


SCRIPT = Path(__file__).parent / "scripts" / "fetch_frankfurter_fx.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "frankfurter_fx.yml"
SPEC = importlib.util.spec_from_file_location("fetch_frankfurter_fx", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

TODAY = date(2026, 11, 23)

ROW = {
    "date": "2026-11-20",
    "base": "USD",
    "quote": "EUR",
    "rate": 0.90992,
}


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def rate_row(date, base, quote, rate):
    return {"date": date, "base": base, "quote": quote, "rate": rate}


def http_error(code, reason="error"):
    return urllib.error.HTTPError(
        MODULE.BASE_URL, code, reason, {}, io.BytesIO(b"{}")
    )


class FetchFrankfurterFxTests(unittest.TestCase):
    def fetch(self, document, **kwargs):
        kwargs.setdefault("today", TODAY)
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=JsonResponse(document),
            ) as urlopen,
            mock.patch.object(MODULE.time, "sleep") as sleep,
        ):
            records = list(MODULE.fetch_rates(**kwargs))
        return records, urlopen, sleep

    def test_emits_envelope_with_stable_date_base_quote_id(self):
        records, _, _ = self.fetch([ROW])

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "frankfurter_fx")
        self.assertEqual(record["id"], "2026-11-20:USD:EUR")
        self.assertEqual(record["date"], "2026-11-20")
        self.assertEqual(record["base"], "USD")
        self.assertEqual(record["quote"], "EUR")
        self.assertEqual(record["rate"], 0.90992)
        self.assertRegex(record["fetched_at"], r"\+00:00$")

    def test_uses_one_utc_fetch_timestamp_per_run(self):
        rows = [ROW, rate_row("2026-11-20", "USD", "GBP", 0.76192)]
        records, _, _ = self.fetch(rows)

        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")

    def test_makes_one_bounded_request_with_expected_query_and_user_agent(self):
        records, urlopen, _ = self.fetch(
            [ROW],
            quotes=("EUR", "GBP"),
            lookback_days=7,
            timeout=45,
        )

        self.assertEqual(len(records), 1)
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        parsed = urllib.parse.urlsplit(request.full_url)
        self.assertEqual(
            parsed.scheme + "://" + parsed.netloc + parsed.path, MODULE.BASE_URL
        )
        params = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(params["base"], ["USD"])
        self.assertEqual(params["quotes"], ["EUR,GBP"])
        # Seven complete UTC dates before 2026-11-23: 2026-11-16..2026-11-22
        self.assertEqual(params["from"], ["2026-11-16"])
        self.assertEqual(params["to"], ["2026-11-22"])
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 45})

    def test_rate_window_is_bounded_and_ends_yesterday(self):
        start, end = MODULE.rate_window(TODAY, 7)
        self.assertEqual(start, date(2026, 11, 16))
        self.assertEqual(end, date(2026, 11, 22))

        start, end = MODULE.rate_window(TODAY, 1)
        self.assertEqual((start, end), (date(2026, 11, 22), date(2026, 11, 22)))

        for bad in (0, 32, -1, True, "7"):
            with self.subTest(lookback_days=bad):
                with self.assertRaises(ValueError):
                    MODULE.rate_window(TODAY, bad)

    def test_parse_quotes_rejects_malformed_and_repeated_codes(self):
        self.assertEqual(
            MODULE.parse_quotes("EUR,GBP"),
            ("EUR", "GBP"),
        )
        for bad in ("", "EUR,,GBP", "eur", "EU", "EUR4", "EUR, EUR", "EUR,EUR"):
            with self.subTest(quotes=bad):
                with self.assertRaises(ValueError):
                    MODULE.parse_quotes(bad)

    def test_invalid_arguments_fail_before_http(self):
        cases = (
            {"base": "usd"},
            {"base": "EU"},
            {"base": ""},
            {"base": True},
            {"quotes": ()},
            {"quotes": ("EUR", "eur")},
            {"lookback_days": 0},
            {"lookback_days": MODULE.MAX_LOOKBACK_DAYS + 1},
            {"lookback_days": True},
            {"timeout": 0},
            {"timeout": MODULE.MAX_TIMEOUT + 1},
            {"timeout": True},
            {"retries": -1},
            {"retries": MODULE.MAX_RETRIES + 1},
            {"retries": True},
            {"retry_delay": -1},
            {"retry_delay": MODULE.MAX_RETRY_DELAY + 1},
            {"retry_delay": float("inf")},
            {"retry_delay": True},
        )
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaises(ValueError):
                        list(MODULE.fetch_rates(**kwargs))
                urlopen.assert_not_called()

    def test_retries_transient_429_then_succeeds(self):
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                side_effect=[
                    http_error(429, "Too Many Requests"),
                    JsonResponse([ROW]),
                ],
            ) as urlopen,
            mock.patch.object(MODULE.time, "sleep") as sleep,
        ):
            records = list(MODULE.fetch_rates(today=TODAY, retries=2, retry_delay=1.5))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], "2026-11-20:USD:EUR")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(1.5)

    def test_exhausted_retries_raise_after_bounded_attempts(self):
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                side_effect=[
                    http_error(503, "Service Unavailable"),
                    http_error(503, "Service Unavailable"),
                    http_error(503, "Service Unavailable"),
                ],
            ) as urlopen,
            mock.patch.object(MODULE.time, "sleep"),
        ):
            with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
                list(MODULE.fetch_rates(today=TODAY, retries=2))
        self.assertEqual(urlopen.call_count, 3)

    def test_non_transient_http_error_fails_immediately(self):
        with (
            mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=http_error(400, "Bad Request")
            ) as urlopen,
            mock.patch.object(MODULE.time, "sleep") as sleep,
        ):
            with self.assertRaisesRegex(RuntimeError, "HTTP 400"):
                list(MODULE.fetch_rates(today=TODAY, retries=2))
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_rejects_malformed_documents_and_rows(self):
        malformed = (
            ({"rates": [ROW]}, "not a list"),
            ([{}], "missing its ISO date"),
            ([rate_row("2026-11-20", "USD", "EUR", None)], "non-positive rate"),
            ([rate_row("2026-11-20", "usd", "EUR", 1.0)], "invalid base currency"),
            ([rate_row("2026-11-20", "USD", "EU", 1.0)], "invalid quote currency"),
            ([rate_row("2026/11/20", "USD", "EUR", 1.0)], "missing its ISO date"),
            ([rate_row("2026-11-20", "USD", "EUR", 0)], "non-positive rate"),
            ([rate_row("2026-11-20", "USD", "EUR", -1)], "non-positive rate"),
            ([rate_row("2026-11-20", "USD", "EUR", float("nan"))], "non-positive rate"),
            ([rate_row("2026-11-20", "USD", "EUR", True)], "non-positive rate"),
            (["not a row"], "non-object rate row"),
            ("not a list", "not a list"),
        )
        for document, message in malformed:
            with self.subTest(document=document):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(document)

    def test_empty_response_is_a_valid_empty_run(self):
        records, urlopen, _ = self.fetch([])

        self.assertEqual(records, [])
        urlopen.assert_called_once()

    def test_main_emits_valid_ndjson_and_run_summary(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        rows = [ROW, rate_row("2026-11-20", "USD", "GBP", 0.76192)]

        with (
            mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=JsonResponse(rows)
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            MODULE.main(["--quotes", "EUR,GBP", "--lookback-days", "7"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["id"], "2026-11-20:USD:EUR")
        self.assertEqual(json.loads(lines[1])["id"], "2026-11-20:USD:GBP")

        summary_lines = [
            line
            for line in stderr.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ]
        self.assertEqual(len(summary_lines), 1)
        summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX) :])
        self.assertEqual(summary["rows"], 2)
        self.assertEqual(summary["base"], "USD")
        self.assertEqual(summary["quotes"], ["EUR", "GBP"])
        self.assertEqual(summary["first_id"], "2026-11-20:USD:EUR")
        self.assertEqual(summary["last_id"], "2026-11-20:USD:GBP")

    def test_main_emits_summary_for_empty_run(self):
        stdout = io.StringIO()
        stderr = io.StringIO()

        with (
            mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=JsonResponse([])
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            MODULE.main([])

        self.assertEqual(stdout.getvalue(), "")
        summary_line = [
            line
            for line in stderr.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ][0]
        summary = json.loads(summary_line[len(MODULE.SUMMARY_PREFIX) :])
        self.assertEqual(summary["rows"], 0)
        self.assertIsNone(summary["first_id"])
        self.assertIsNone(summary["last_id"])

    def test_source_configuration_is_daily_and_attributed(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "23 17 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn(
            'args: ["--lookback-days", "7", "--timeout", "60", "--retries", "2", "--retry-delay", "10"]',
            text,
        )
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 10", text)
        self.assertIn("European Central Bank", text)
        self.assertIn("frankfurter_fx", text)


if __name__ == "__main__":
    unittest.main()
