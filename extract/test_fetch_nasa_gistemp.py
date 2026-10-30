import contextlib
import datetime as _datetime_module
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_nasa_gistemp.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "nasa_gistemp.yml"
SPEC = importlib.util.spec_from_file_location("fetch_nasa_gistemp", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


CURRENT_YEAR = 1881
AGGREGATES = ("-.18", "-.17", "***", "***", "-.12", "-.17")


class FrozenDateTime(_datetime_module.datetime):
    """datetime pinned to the fixture year so freshness checks pass."""

    @classmethod
    def now(cls, tz=None):
        return cls(CURRENT_YEAR, 6, 15, 12, 0, 0, tzinfo=tz)


def row(year, *months, aggregates=AGGREGATES):
    assert len(months) == 12
    return ",".join([year, *months, *aggregates])


def make_csv(*rows, title="Land-Ocean: Global Means"):
    return "\n".join([title, MODULE.EXPECTED_HEADER, *rows]) + "\n"


# 1880 is complete; 1881 is a partial current year with missing recent months,
# mirroring the live tables. current_year=1881 keeps the fixtures small.
BASELINE = "-.18"
Y1880 = row(
    "1880",
    "-.19", "-.25", "-.09", "-.16", "-.10", "-.21",
    "-.19", "-.10", "-.15", "-.23", "-.22", BASELINE,
)
Y1881 = row(
    "1881",
    "-.20", "-.14", ".03", ".05", ".06", "-.19",
    ".00", "-.03", "-.15", "***", "***", "***",
)
FIXTURE = make_csv(Y1880, Y1881)


class CsvResponse(io.BytesIO):
    def __init__(self, text):
        super().__init__(text.encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FetchNasaGistempTests(unittest.TestCase):
    def fetch(self, *texts, **kwargs):
        kwargs.setdefault("current_year", CURRENT_YEAR)
        responses = [CsvResponse(text) for text in texts]
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=responses,
        ) as urlopen:
            records = list(MODULE.fetch_gistemp(**kwargs))
        return records, urlopen

    def test_expands_rows_to_area_year_month_records(self):
        records, _ = self.fetch(FIXTURE, FIXTURE, FIXTURE)

        self.assertEqual(len(records), 72)
        first = records[0]
        self.assertEqual(first["source"], "nasa_gistemp")
        self.assertEqual(first["id"], "glb:1880-01")
        self.assertEqual(first["series"], "glb")
        self.assertEqual(first["year"], 1880)
        self.assertEqual(first["month"], 1)
        self.assertEqual(first["anomaly_celsius"], -0.19)
        self.assertEqual(first["anomaly_basis"], "1951-1980")
        self.assertRegex(first["fetched_at"], r"\+00:00$")

        by_id = {record["id"]: record for record in records}
        self.assertEqual(by_id["glb:1880-12"]["anomaly_celsius"], -0.18)
        self.assertIsNone(by_id["glb:1881-10"]["anomaly_celsius"])
        self.assertIsNone(by_id["sh:1881-12"]["anomaly_celsius"])
        self.assertEqual(by_id["nh:1881-09"]["anomaly_celsius"], -0.15)

    def test_uses_one_utc_fetch_timestamp_per_run(self):
        records, _ = self.fetch(FIXTURE, FIXTURE, FIXTURE)

        timestamps = {record["fetched_at"] for record in records}
        self.assertEqual(len(timestamps), 1)
        self.assertRegex(timestamps.pop(), r"\+00:00$")

    def test_makes_three_bounded_requests_with_configured_user_agent(self):
        records, urlopen = self.fetch(FIXTURE, FIXTURE, FIXTURE, timeout=17)

        self.assertEqual(len(records), 72)
        self.assertEqual(urlopen.call_count, 3)
        for call, filename in zip(
            urlopen.call_args_list,
            ("GLB.Ts+dSST.csv", "NH.Ts+dSST.csv", "SH.Ts+dSST.csv"),
        ):
            request = call.args[0]
            self.assertEqual(request.full_url, MODULE.BASE_URL + filename)
            self.assertEqual(request.get_header("Accept"), "text/csv")
            self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
            self.assertEqual(call.kwargs, {"timeout": 17})

    def test_invalid_timeout_fails_before_http(self):
        for timeout in (True, 0, -1, 1.5, MODULE.MAX_TIMEOUT + 1):
            with self.subTest(timeout=timeout):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, "timeout"):
                        list(MODULE.fetch_gistemp(timeout=timeout))
                urlopen.assert_not_called()

    def test_rejects_malformed_tables(self):
        malformed = (
            (
                "Land-Ocean: Global Means\n" + MODULE.EXPECTED_HEADER + "\n",
                "fewer than three lines",
            ),
            (
                "\n".join(
                    ["Land-Ocean: Global Means", "Year,Jan,Feb", Y1880]
                )
                + "\n",
                "unexpected header line",
            ),
            (
                make_csv(Y1880, ",".join(["1881", *("0.00" for _ in range(11)), *AGGREGATES])),
                "fields, expected",
            ),
            (
                make_csv(Y1880, row("1881", "abc", *("0.00" for _ in range(11)))),
                "malformed value",
            ),
            (
                make_csv(
                    Y1880,
                    row(
                        "1881",
                        *("-.01" for _ in range(12)),
                    ).rsplit(",", 1)[0] + ",x",
                ),
                "malformed value",
            ),
            (
                make_csv(Y1880, row("1882", *("-.01" for _ in range(12)))),
                "consecutive run",
            ),
            (
                make_csv(row("1881", *("-.01" for _ in range(12)))),
                "consecutive run",
            ),
            (
                make_csv(
                    ",".join(["1880", *("***" for _ in range(12)), *AGGREGATES])
                ),
                "no numeric anomalies",
            ),
        )
        for text, message in malformed:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(text, FIXTURE, FIXTURE)

    def test_rejects_stale_and_future_endings(self):
        with self.subTest(stale=True):
            with self.assertRaisesRegex(ValueError, "expected at least 1882"):
                self.fetch(FIXTURE, FIXTURE, FIXTURE, current_year=1883)
        with self.subTest(future=True):
            with self.assertRaisesRegex(ValueError, "future year 1881"):
                self.fetch(FIXTURE, FIXTURE, FIXTURE, current_year=1880)

    def test_main_emits_valid_ndjson_and_run_summary(self):
        stdout = io.StringIO()
        stderr = io.StringIO()

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[CsvResponse(FIXTURE) for _ in range(3)],
        ), mock.patch.object(MODULE, "datetime", FrozenDateTime):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                MODULE.main(["--timeout", "23"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 72)
        for line in lines:
            record = json.loads(line)
            self.assertEqual(record["source"], "nasa_gistemp")

        summary_lines = [
            line
            for line in stderr.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ]
        self.assertEqual(len(summary_lines), 1)
        payload = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX):])
        self.assertEqual(payload["rows"], 72)
        self.assertEqual(set(payload["areas"]), {"glb", "nh", "sh"})
        for stats in payload["areas"].values():
            self.assertEqual(stats["rows"], 24)
            self.assertEqual(stats["missing"], 3)
            self.assertEqual(stats["first"], "1880-01")
            self.assertEqual(stats["last"], "1881-09")

    def test_source_configuration_is_daily_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "46 8 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertIsNone(re.search(r"(?m)^enabled:\s*true$", text))
        self.assertIn('args: ["--timeout", "30"]', text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 5", text)
        self.assertIn("required: true", text)
        self.assertIn("1951-1980", text)
        self.assertIn("public domain", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("three bounded text/CSV requests per run", text)
        self.assertIn("new month roughly two weeks after month-end", text)
        self.assertIn("live NASA GISS source smoke", text)
        self.assertIn("read-only Airflow import checks", text)


if __name__ == "__main__":
    unittest.main()
