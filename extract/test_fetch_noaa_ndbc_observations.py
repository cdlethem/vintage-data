import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import unittest
import urllib.error
from unittest import mock


HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "orchestration" / "include"))

SCRIPT = HERE / "scripts" / "fetch_noaa_ndbc_observations.py"
SOURCE_CONFIG = HERE / "sources" / "noaa_ndbc_observations.yml"
SPEC = importlib.util.spec_from_file_location("fetch_noaa_ndbc_observations", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

from run_metadata import extract_summary_line, validate_summary  # noqa: E402

HEADER = (
    "#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE\n"
    "#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft\n"
)
# WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS PTDY TIDE
GOOD_ROW = "2026 11 05 05 00 200 10.0 12.0 5.9 16 9.6 268 1008.5 14.7 13.9 14.3 MM -1.7 MM"
STATIONS = ("32ST0", "46005", "KATP")


def station_file(*rows):
    return (HEADER + "\n".join(rows) + "\n").encode("utf-8")


def listing_html(*files):
    links = "".join(f'<a href="{f}">{f}</a>' for f in files)
    return f"<html><body>{links}</body></html>".encode("utf-8")


def http_error(status):
    return urllib.error.HTTPError(
        MODULE.BASE, status, "synthetic", {}, None
    )


class FakeResponse:
    def __init__(self, body: bytes):
        self._body = io.BytesIO(body)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._body.close()
        return False

    def read(self, size=-1):
        return self._body.read(size)


class FakeUrlopen:
    """Route urlopen(request, timeout=...) to canned per-URL outcomes."""

    def __init__(self, outcomes):
        # outcomes: url -> list of FakeResponse/Exception; last one repeats
        self.outcomes = {url: list(items) for url, items in outcomes.items()}
        self.calls = []

    def __call__(self, request, timeout=None):
        url = request.full_url
        self.calls.append((request, timeout))
        queue = self.outcomes[url]
        outcome = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def run_main(outcomes, argv=None):
    """Run main() against a fake urlopen; return (exit, stdout, stderr, fake)."""
    fake = FakeUrlopen(outcomes)
    stdout, stderr = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake):
        with mock.patch.object(MODULE.time, "sleep"):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = MODULE.main(["--timeout", "30"] if argv is None else argv)
    return exit_code, stdout.getvalue(), stderr.getvalue(), fake


def parse_summary(stderr):
    payload, _ = extract_summary_line(stderr)
    assert payload is not None, f"no summary line in {stderr!r}"
    summary = validate_summary(json.loads(payload))
    return summary


def full_listing(*extra_files):
    files = ("32ST0.txt", "46005.txt", "KATP.txt") if not extra_files else extra_files
    return listing_html(*files)


def fresh_stats():
    return {"requests_attempted": 0, "requests_succeeded": 0, "retries": 0}


class ListingTests(unittest.TestCase):
    def test_listing_selects_station_files_only(self):
        body = listing_html(
            "46005.txt",
            "32ST0.txt",
            "KATP.txt",
            "4403585.txt",
            "ship_obs.txt",
            "32ST0.ocean",
            "22101.drift",
            "?C=N;O=D",
            "..",
        )
        fake = FakeUrlopen({MODULE.BASE: [FakeResponse(body)]})
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake):
            with mock.patch.object(MODULE.time, "sleep"):
                stations = MODULE.list_stations(30, fresh_stats())

        self.assertEqual(stations, ["32ST0", "4403585", "46005", "KATP"])
        request, timeout = fake.calls[0]
        self.assertEqual(request.full_url, MODULE.BASE)
        self.assertEqual(request.get_header("Range"), "bytes=0-10485760")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(timeout, 30)

    def test_empty_listing_raises(self):
        body = listing_html("ship_obs.txt", "32ST0.ocean")
        fake = FakeUrlopen({MODULE.BASE: [FakeResponse(body)]})
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake):
            with mock.patch.object(MODULE.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "no station files"):
                    MODULE.list_stations(30, fresh_stats())

    def test_oversized_listing_raises(self):
        body = listing_html("46005.txt") + b"x" * (MODULE.MAX_LISTING_BYTES)
        fake = FakeUrlopen({MODULE.BASE: [FakeResponse(body)]})
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake):
            with mock.patch.object(MODULE.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "exceeds"):
                    MODULE.list_stations(30, fresh_stats())


class ParseTests(unittest.TestCase):
    def parse(self, text, **kwargs):
        now = kwargs.pop("now", None)
        from datetime import datetime, timezone

        if now is None:
            now = datetime(2026, 11, 5, 6, 0, tzinfo=timezone.utc)
        return MODULE.parse_station_head("46005", text, now)

    def test_normalizes_latest_line_with_mm_nulls(self):
        second = "2026 11 05 04 40 190 9.0 11.0 5.5 15 9.0 260 1008.4 14.8 14.0 14.4 10 -1.6 MM"
        result = self.parse(HEADER + GOOD_ROW + "\n" + second + "\n")

        self.assertEqual(result["observation_time"], "2026-11-05T05:00:00Z")
        self.assertEqual(result["wdir"], 200.0)
        self.assertEqual(result["wspd"], 10.0)
        self.assertEqual(result["gust"], 12.0)
        self.assertEqual(result["wvht"], 5.9)
        self.assertEqual(result["dpd"], 16.0)
        self.assertEqual(result["apd"], 9.6)
        self.assertEqual(result["mwd"], 268.0)
        self.assertEqual(result["pres"], 1008.5)
        self.assertEqual(result["atmp"], 14.7)
        self.assertEqual(result["wtmp"], 13.9)
        self.assertEqual(result["dewp"], 14.3)
        self.assertIsNone(result["vis"])
        self.assertEqual(result["ptdy"], -1.7)
        self.assertIsNone(result["tide"])

    def test_uses_first_data_line_not_last(self):
        older = "2026 11 05 04 00 100 5.0 6.0 3.0 10 8.0 100 1010.0 10.0 10.0 10.0 5 0.0 1.0"
        newest = "2026 11 05 05 00 200 10.0 12.0 5.9 16 9.6 268 1008.5 14.7 13.9 14.3 10 -1.7 2.0"
        result = self.parse(HEADER + newest + "\n" + older + "\n")
        self.assertEqual(result["observation_time"], "2026-11-05T05:00:00Z")
        self.assertEqual(result["wdir"], 200.0)
        self.assertEqual(result["tide"], 2.0)

    def test_accepts_boundary_values(self):
        row = (
            "2026 11 05 05 00 0 0 0 0 0 0 0 850 -60 -30 -60 0 -10 -100"
        )
        result = self.parse(HEADER + row + "\n")
        self.assertEqual(result["wdir"], 0.0)
        self.assertEqual(result["pres"], 850.0)
        self.assertEqual(result["tide"], -100.0)
        row = "2026 11 05 05 00 360 100 150 50 100 100 360 1100 60 45 50 100 10 100"
        result = self.parse(HEADER + row + "\n")
        # 360 is NDBC's "no direction" value, not out of range.
        self.assertEqual(result["wdir"], 360.0)
        self.assertEqual(result["mwd"], 360.0)
        self.assertEqual(result["wspd"], 100.0)
        self.assertEqual(result["tide"], 100.0)

    def test_rejects_out_of_range_values(self):
        base = GOOD_ROW.split()

        def row_with(field_index, token):
            tokens = base.copy()
            tokens[5 + field_index] = token
            return " ".join(tokens)

        # (field, token) pairs that must be rejected
        cases = (
            (0, "361"),  # wdir beyond a full circle
            (6, "361"),  # mwd beyond a full circle
            (1, "100.5"),  # wspd
            (1, "-0.1"),  # wspd
            (2, "150.5"),  # gust
            (3, "50.5"),  # wvht
            (7, "849.9"),  # pres
            (7, "1100.1"),  # pres
            (8, "-60.5"),  # atmp
            (9, "45.5"),  # wtmp
            (11, "100.5"),  # vis
            (12, "-10.5"),  # ptdy
            (13, "100.5"),  # tide
        )
        for field_index, token in cases:
            with self.subTest(token=token):
                with self.assertRaisesRegex(MODULE.MalformedObservation, "range"):
                    self.parse(HEADER + row_with(field_index, token) + "\n")

    def test_rejects_non_numeric_and_non_finite_tokens(self):
        base = GOOD_ROW.split()
        for token in ("abc", "NaN", "inf", "-inf", "1.2.3"):
            tokens = base.copy()
            tokens[5] = token
            with self.subTest(token=token):
                with self.assertRaisesRegex(MODULE.MalformedObservation, "finite|non-numeric"):
                    self.parse(HEADER + " ".join(tokens) + "\n")

    def test_rejects_invalid_timestamps(self):
        base = GOOD_ROW.split()

        def row_with(replacements):
            tokens = base.copy()
            for position, value in replacements:
                tokens[position] = value
            return " ".join(tokens)

        cases = (
            (((1, "13"),), "month too large"),
            (((1, "00"),), "month zero"),
            (((2, "32"),), "day beyond month length"),
            (((1, "02"), (2, "29")), "February 29 in a non-leap year"),
            (((3, "24"),), "hour too large"),
            (((4, "60"),), "minute too large"),
            (((0, "202"),), "year not four digits"),
            (((3, "5X"),), "non-numeric hour"),
        )
        for replacements, label in cases:
            with self.subTest(label=label, replacements=replacements):
                with self.assertRaisesRegex(MODULE.MalformedObservation, "timestamp|out of range|non-numeric"):
                    self.parse(HEADER + row_with(replacements) + "\n")

    def test_rejects_far_future_observation(self):
        tokens = GOOD_ROW.split()
        tokens[:5] = ("2126", "01", "01", "00", "00")
        with self.assertRaisesRegex(MODULE.MalformedObservation, "future"):
            self.parse(HEADER + " ".join(tokens) + "\n")

    def test_rejects_header_only_and_empty_files(self):
        with self.assertRaisesRegex(MODULE.MalformedObservation, "no data lines"):
            self.parse(HEADER)
        with self.assertRaisesRegex(MODULE.MalformedObservation, "no data lines"):
            self.parse("")

    def test_rejects_short_and_long_rows(self):
        short = GOOD_ROW.split()[:18]
        long = GOOD_ROW.split() + ["MM"]
        with self.assertRaisesRegex(MODULE.MalformedObservation, "18 tokens"):
            self.parse(HEADER + " ".join(short) + "\n")
        with self.assertRaisesRegex(MODULE.MalformedObservation, "20 tokens"):
            self.parse(HEADER + " ".join(long) + "\n")


class RunTests(unittest.TestCase):
    def station_outcomes(self, failures=()):
        outcomes = {MODULE.BASE: [FakeResponse(full_listing())]}
        for station_id in STATIONS:
            if station_id in failures:
                outcomes[MODULE.BASE + f"{station_id}.txt"] = [
                    http_error(failures[station_id])
                ]
            else:
                outcomes[MODULE.BASE + f"{station_id}.txt"] = [
                    FakeResponse(station_file(GOOD_ROW))
                ]
        return outcomes

    def test_healthy_run_emits_one_record_per_station(self):
        exit_code, stdout, stderr, fake = run_main(self.station_outcomes())

        self.assertEqual(exit_code, 0)
        lines = stdout.splitlines()
        self.assertEqual(len(lines), len(STATIONS))
        records = [json.loads(line) for line in lines]
        self.assertEqual([r["id"] for r in records], sorted(STATIONS))
        for record in records:
            self.assertEqual(record["source"], MODULE.SOURCE)
            self.assertRegex(record["fetched_at"], r"\+00:00$")
            self.assertEqual(record["observation_time"], "2026-11-05T05:00:00Z")
        self.assertEqual(len({r["fetched_at"] for r in records}), 1)

        summary = parse_summary(stderr)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["partitions"], {"attempted": 3, "succeeded": 3, "failed": 0, "failures": []})
        self.assertEqual(summary["requests"]["attempted"], 4)
        self.assertEqual(summary["requests"]["succeeded"], 4)
        self.assertEqual(summary["requests"]["retries"], 0)

        # The station request is a bounded head fetch with the run timeout.
        station_call = [c for c in fake.calls if c[0].full_url.endswith("46005.txt")]
        request, timeout = station_call[0]
        self.assertEqual(request.get_header("Range"), "bytes=0-4095")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(timeout, 30)

    def test_partial_failure_reports_degraded(self):
        exit_code, stdout, stderr, _ = run_main(
            self.station_outcomes(failures={"KATP": 404})
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(stdout.splitlines()), 2)
        summary = parse_summary(stderr)
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(
            summary["partitions"],
            {"attempted": 3, "succeeded": 2, "failed": 1,
             "failures": [{"station": "KATP", "error": "HTTP 404 for " + MODULE.BASE + "KATP.txt"}]},
        )

    def test_retries_retriable_status_once_and_succeeds(self):
        outcomes = self.station_outcomes()
        outcomes[MODULE.BASE + "46005.txt"] = [
            http_error(503),
            FakeResponse(station_file(GOOD_ROW)),
        ]
        exit_code, stdout, stderr, fake = run_main(outcomes)

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(stdout.splitlines()), 3)
        summary = parse_summary(stderr)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["requests"]["attempted"], 5)  # 1 listing + 4 station
        self.assertEqual(summary["requests"]["succeeded"], 4)
        self.assertEqual(summary["requests"]["retries"], 1)
        station_calls = [c for c in fake.calls if c[0].full_url.endswith("46005.txt")]
        self.assertEqual(len(station_calls), 2)

    def test_retries_transient_connection_error_once(self):
        outcomes = self.station_outcomes()
        outcomes[MODULE.BASE + "46005.txt"] = [
            ConnectionResetError("reset"),
            FakeResponse(station_file(GOOD_ROW)),
        ]
        exit_code, stdout, stderr, _ = run_main(outcomes)

        self.assertEqual(exit_code, 0)
        summary = parse_summary(stderr)
        self.assertEqual(summary["requests"]["retries"], 1)
        self.assertEqual(summary["health"], "healthy")

    def test_does_not_retry_client_error(self):
        exit_code, stdout, stderr, fake = run_main(
            self.station_outcomes(failures={"KATP": 404})
        )
        self.assertEqual(exit_code, 0)
        kapt_calls = [c for c in fake.calls if c[0].full_url.endswith("KATP.txt")]
        self.assertEqual(len(kapt_calls), 1)
        summary = parse_summary(stderr)
        self.assertEqual(summary["requests"]["retries"], 0)

    def test_does_not_retry_malformed_data(self):
        outcomes = self.station_outcomes()
        outcomes[MODULE.BASE + "KATP.txt"] = [
            FakeResponse(station_file(" ".join(GOOD_ROW.split()[:18])))
        ]
        exit_code, stdout, stderr, fake = run_main(outcomes)

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(stdout.splitlines()), 2)
        kapt_calls = [c for c in fake.calls if c[0].full_url.endswith("KATP.txt")]
        self.assertEqual(len(kapt_calls), 1)
        summary = parse_summary(stderr)
        self.assertEqual(summary["health"], "degraded")
        self.assertIn("tokens", summary["partitions"]["failures"][0]["error"])

    def test_broad_failure_reports_failed_and_exits_one(self):
        exit_code, stdout, stderr, _ = run_main(
            self.station_outcomes(
                failures={"32ST0": 503, "46005": 503, "KATP": 503}
            )
        )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout, "")
        summary = parse_summary(stderr)
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["partitions"]["failed"], 3)
        self.assertEqual(summary["requests"]["attempted"], 7)  # each retried once
        self.assertEqual(summary["requests"]["retries"], 3)

    def test_two_of_four_failures_is_degraded_not_broad(self):
        outcomes = {
            MODULE.BASE: [
                FakeResponse(
                    listing_html("32ST0.txt", "46005.txt", "KATP.txt", "99999.txt")
                )
            ]
        }
        for station_id in ("32ST0", "46005", "KATP", "99999"):
            if station_id in ("32ST0", "46005"):
                outcomes[MODULE.BASE + f"{station_id}.txt"] = [http_error(404)]
            else:
                outcomes[MODULE.BASE + f"{station_id}.txt"] = [
                    FakeResponse(station_file(GOOD_ROW))
                ]

        exit_code, stdout, stderr, _ = run_main(outcomes)

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(stdout.splitlines()), 2)
        summary = parse_summary(stderr)
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["partitions"]["attempted"], 4)
        self.assertEqual(summary["partitions"]["failed"], 2)

    def test_empty_listing_fails_run(self):
        outcomes = {MODULE.BASE: [FakeResponse(listing_html("ship_obs.txt"))]}
        exit_code, stdout, stderr, _ = run_main(outcomes)

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout, "")
        summary = parse_summary(stderr)
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")

    def test_listing_failure_after_retry_fails_run(self):
        outcomes = {MODULE.BASE: [http_error(503), http_error(503)]}
        exit_code, stdout, stderr, _ = run_main(outcomes)

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout, "")
        summary = parse_summary(stderr)
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["requests"]["attempted"], 2)
        self.assertEqual(summary["requests"]["retries"], 1)

    def test_timeout_out_of_range_rejected(self):
        for timeout in ("0", "121"):
            with self.subTest(timeout=timeout):
                with self.assertRaises(SystemExit):
                    run_main(self.station_outcomes(), argv=["--timeout", timeout])


class SourceConfigTests(unittest.TestCase):
    def test_source_configuration(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^name: noaa_ndbc_observations$')
        self.assertRegex(
            text, r"(?m)^script: fetch_noaa_ndbc_observations\.py$"
        )
        self.assertRegex(text, r'(?m)^args: \["--timeout", "30"\]$')
        self.assertRegex(text, r'(?m)^schedule: "7 \* \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertRegex(text, r"(?m)^timeout_minutes: 15$")
        self.assertRegex(text, r"(?m)^  min_minutes: 30$")
        self.assertRegex(text, r"(?m)^  max_minutes: 240$")
        self.assertIn("public domain", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("NDBC", text)


if __name__ == "__main__":
    unittest.main()
