import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock
import urllib.error
import urllib.request


SCRIPT = Path(__file__).parent / "scripts" / "fetch_effis_fire_stats.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "effis_fire_stats.yml"
SPEC = importlib.util.spec_from_file_location("fetch_effis_fire_stats", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


# Verified live 2026-10-25: GET /rda-stats?year=2026 (truncated to the
# regions that matter; values are the actual upstream numbers).
SAMPLE_DOCUMENT = {
    "total": {"area": "total", "sumPerForest%": 0.45, "estSumTotal": 2065149.47,
              "sumRDATotal": 1961892.0},
    "null": {"area": None, "sumPerForest%": None, "estSumTotal": 589.47,
             "sumRDATotal": 560.0},
    "EU_non": {"area": "EU_non", "sumPerForest%": 0.32, "estSumTotal": 838327.37,
               "sumRDATotal": 796411.0},
    "ME_AF": {"area": "ME_AF", "sumPerForest%": 0.58, "estSumTotal": 429268.42,
              "sumRDATotal": 407805.0},
    "EU": {"area": "EU", "sumPerForest%": 0.52, "estSumTotal": 796964.21,
           "sumRDATotal": 757116.0},
}
REGIONS = ("EU", "EU_non", "ME_AF", "null", "total")


class JsonResponse(io.BytesIO):
    def __init__(self, payload):
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
        super().__init__(body)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def http_error(code):
    return urllib.error.HTTPError(
        "https://api2.effis.emergency.copernicus.eu/rda-stats", code,
        "error", None, None,
    )


class FetchEffisFireStatsTests(unittest.TestCase):
    def fetch(self, document, year=2026, **kwargs):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            records = list(MODULE.fetch_rda_stats(year, **kwargs))
        return records, urlopen

    def test_emits_one_record_per_region_with_envelope(self):
        records, _ = self.fetch(SAMPLE_DOCUMENT)

        self.assertEqual([r["region"] for r in records], sorted(REGIONS))
        for region in REGIONS:
            record = next(r for r in records if r["region"] == region)
            self.assertEqual(record["source"], "effis_fire_stats")
            self.assertEqual(record["id"], f"2026_{region}")
            self.assertEqual(record["year"], 2026)
            self.assertRegex(record["fetched_at"], r"\+00:00$")

    def test_uses_single_utc_fetch_timestamp_per_run(self):
        records, _ = self.fetch(SAMPLE_DOCUMENT)

        self.assertGreater(len(records), 1)
        self.assertEqual({r["fetched_at"] for r in records},
                         {records[0]["fetched_at"]})

    def test_preserves_source_field_values(self):
        records, _ = self.fetch(SAMPLE_DOCUMENT)
        eu = next(r for r in records if r["region"] == "EU")

        self.assertEqual(eu["area"], "EU")
        self.assertEqual(eu["estSumTotal"], 796964.21)
        self.assertEqual(eu["sumRDATotal"], 757116.0)
        self.assertEqual(eu["sumPerForest%"], 0.52)
        unattributed = next(r for r in records if r["region"] == "null")
        self.assertIsNone(unattributed["area"])
        self.assertIsNone(unattributed["sumPerForest%"])

    def test_makes_one_bounded_request_with_configured_user_agent(self):
        records, urlopen = self.fetch(SAMPLE_DOCUMENT, timeout=17)

        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(
            request.full_url,
            f"{MODULE.BASE}/rda-stats?year=2026",
        )
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_main_defaults_to_current_utc_year(self):
        from datetime import datetime, timezone

        year = datetime.now(timezone.utc).year
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            return_value=JsonResponse(SAMPLE_DOCUMENT),
        ) as urlopen, contextlib.redirect_stdout(io.StringIO()):
            MODULE.main([])

        request = urlopen.call_args.args[0]
        self.assertIn(f"year={year}", request.full_url)

    def test_rejects_malformed_documents(self):
        malformed = (
            ([SAMPLE_DOCUMENT], "expected a JSON object"),
            ("not an object", "expected a JSON object"),
            ({}, "missing the 'total' aggregate"),
            ({**SAMPLE_DOCUMENT, "EU": [1, 2]}, "region 'EU' is not an object"),
        )
        for document, message in malformed:
            with self.subTest(document=document):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(document)

    def test_rejects_invalid_numeric_fields(self):
        for field in ("estSumTotal", "sumRDATotal"):
            for value in ("796964.21", -1.0, float("nan"), float("inf"),
                          float("-inf"), True, False, None):
                document = copy.deepcopy(SAMPLE_DOCUMENT)
                document["EU"][field] = value
                with self.subTest(field=field, value=value):
                    with self.assertRaisesRegex(
                        ValueError, "finite non-negative number"
                    ):
                        self.fetch(document)
        for value in (101.0, -0.1, "0.5", float("nan"), float("inf"), True):
            document = copy.deepcopy(SAMPLE_DOCUMENT)
            document["EU"]["sumPerForest%"] = value
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "finite percentage"):
                    self.fetch(document)

    def test_rejects_missing_numeric_fields(self):
        document = copy.deepcopy(SAMPLE_DOCUMENT)
        del document["EU"]["estSumTotal"]
        with self.assertRaisesRegex(ValueError, "estSumTotal"):
            self.fetch(document)

    def test_retries_transient_500_then_succeeds(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_error(500), JsonResponse(SAMPLE_DOCUMENT)],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_rda_stats(2026))

        self.assertEqual(len(records), len(REGIONS))
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once()
        self.assertGreaterEqual(sleep.call_args.args[0], 1.0)

    def test_retries_connection_failure_then_succeeds(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("timed out"),
                         JsonResponse(SAMPLE_DOCUMENT)],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep"):
            records = list(MODULE.fetch_rda_stats(2026))

        self.assertEqual(len(records), len(REGIONS))
        self.assertEqual(urlopen.call_count, 2)

    def test_non_retriable_404_fails_immediately(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=http_error(404),
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                list(MODULE.fetch_rda_stats(2026))

        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_exhausted_retries_raise_the_last_error(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=http_error(503),
        ) as urlopen, mock.patch.object(MODULE.time, "sleep"):
            with self.assertRaises(urllib.error.HTTPError):
                list(MODULE.fetch_rda_stats(2026))

        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)

    def test_non_json_200_body_fails_the_run(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            return_value=JsonResponse(b"<html>service moved</html>"),
        ):
            with self.assertRaisesRegex(ValueError, "non-JSON"):
                list(MODULE.fetch_rda_stats(2026))

    def test_invalid_timeout_fails_before_http(self):
        for timeout in (True, 0, -1, 1.5, MODULE.MAX_TIMEOUT + 1):
            with self.subTest(timeout=timeout):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, "timeout"):
                        list(MODULE.fetch_rda_stats(2026, timeout=timeout))
                urlopen.assert_not_called()

    def test_invalid_year_fails_before_http(self):
        for year in (1899, 10000, True, "2026"):
            with self.subTest(year=year):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, "year"):
                        list(MODULE.fetch_rda_stats(year))
                urlopen.assert_not_called()

    def test_main_emits_valid_ndjson_and_run_summary(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            return_value=JsonResponse(SAMPLE_DOCUMENT),
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                MODULE.main(["--year", "2026", "--timeout", "23"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), len(REGIONS))
        for line in lines:
            record = json.loads(line)
            self.assertEqual(record["source"], "effis_fire_stats")
            self.assertIn(record["id"],
                          {f"2026_{r}" for r in REGIONS})
        summary_line = next(
            (l for l in stderr.getvalue().splitlines()
             if l.startswith("VINTAGE_RUN_SUMMARY\t")), None
        )
        self.assertIsNotNone(summary_line)
        summary = json.loads(summary_line.split("\t", 1)[1])
        self.assertEqual(summary, {"source": "effis_fire_stats",
                                   "year": 2026, "records": len(REGIONS)})

    def test_source_configuration_is_daily_with_staggered_offset(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "29 7 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn("args: []", text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 10", text)
        self.assertIn("one bounded request per run", text)
        self.assertIn("poll daily", text)
        self.assertIn("CC BY 4.0", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("effis.jrc.ec.europa.eu", text)


if __name__ == "__main__":
    unittest.main()
