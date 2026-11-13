import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_spc_tornado_reports.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "spc_tornado_reports.yml"
SPEC = importlib.util.spec_from_file_location("fetch_spc_tornado_reports", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

COLUMNS = list(MODULE.EXPECTED_COLUMNS)

OLD_ROW = {
    "om": "192", "yr": "1950", "mo": "10", "dy": "01", "date": "1950-10-01",
    "time": "21:00:00", "tz": "3", "st": "OK", "stf": "40", "stn": "23",
    "mag": "1", "inj": "0", "fat": "0", "loss": "4.0", "closs": "0.0",
    "slat": "36.73", "slon": "-102.52", "elat": "36.88", "elon": "-102.3",
    "len": "15.8", "wid": "10", "ns": "1", "sn": "1", "sg": "1",
    "f1": "25", "f2": "0", "f3": "0", "f4": "0", "fc": "0",
    "edat": "", "etime": "",
}
NEW_ROW = {
    "om": "2401050556-01", "yr": "2024", "mo": "01", "dy": "05", "date": "2024-01-05",
    "time": "05:56:00", "tz": "3", "st": "TX", "stf": "48", "stn": "0",
    "mag": "0", "inj": "0", "fat": "0", "loss": "0", "closs": "0",
    "slat": "29.0583", "slon": "-95.5023", "elat": "29.0666", "elon": "-95.5002",
    "len": "0.5900", "wid": "200", "ns": "1", "sn": "1", "sg": "1",
    "f1": "39", "f2": "0", "f3": "0", "f4": "0", "fc": "0",
    "edat": "2024-01-05", "etime": "05:58:00",
}


def make_row(**overrides):
    row = copy.deepcopy(NEW_ROW)
    row.update(overrides)
    return row


def make_csv(*rows):
    lines = [",".join(COLUMNS)]
    for row in rows:
        lines.append(",".join(row[column] for column in COLUMNS))
    return "\n".join(lines)


def make_index(*years):
    links = "".join(
        f'<a href="data/1950-{year}_all_tornadoes.csv">'
        f"1950-{year}_all_tornadoes.csv</a>\n"
        for year in years
    )
    return f"<html><body>\n{links}</body></html>"


class HttpReply(io.BytesIO):
    def __init__(self, text):
        super().__init__(text.encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FetchSpcTornadoReportsTests(unittest.TestCase):
    def fetch(self, page=None, csv_text=None, **kwargs):
        if page is None:
            page = make_index(2025)
        if csv_text is None:
            csv_text = make_csv(OLD_ROW, NEW_ROW)
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[HttpReply(page), HttpReply(csv_text)],
        ) as urlopen:
            records = list(MODULE.fetch_spc_tornado_reports(**kwargs))
        return records, urlopen

    def test_preserves_source_values_verbatim_with_stable_id(self):
        records, _ = self.fetch()

        self.assertEqual(len(records), 2)
        record = records[0]
        self.assertEqual(record["source"], "spc_tornado_reports")
        self.assertRegex(record["id"], r"^[0-9a-f]{64}$")
        self.assertRegex(record["fetched_at"], r"\+00:00$")
        for column in COLUMNS:
            self.assertEqual(record[column], OLD_ROW[column])
        self.assertEqual(records[1]["om"], NEW_ROW["om"])
        self.assertEqual(records[1]["edat"], NEW_ROW["edat"])
        self.assertEqual(records[1]["etime"], NEW_ROW["etime"])

    def test_id_is_deterministic_across_runs(self):
        first, _ = self.fetch()
        second, _ = self.fetch()

        self.assertEqual(first[0]["id"], second[0]["id"])
        self.assertEqual(first[1]["id"], second[1]["id"])
        self.assertNotEqual(first[0]["id"], first[1]["id"])

    def test_uses_one_utc_fetch_timestamp_per_run(self):
        records, _ = self.fetch()

        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")

    def test_selects_newest_all_tornadoes_file_with_user_agent(self):
        _, urlopen = self.fetch(page=make_index(2024, 2025), timeout=17)

        self.assertEqual(urlopen.call_count, 2)
        index_request = urlopen.call_args_list[0].args[0]
        csv_request = urlopen.call_args_list[1].args[0]
        self.assertEqual(index_request.full_url, MODULE.INDEX_URL)
        self.assertEqual(
            csv_request.full_url,
            "https://www.spc.noaa.gov/wcm/data/1950-2025_all_tornadoes.csv",
        )
        self.assertEqual(csv_request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_index_page_without_all_tornadoes_file_fails(self):
        with self.assertRaisesRegex(
            ValueError, "no 1950-YYYY_all_tornadoes.csv file"
        ):
            self.fetch(page="<html><body>no data links</body></html>")

    def test_empty_csv_fails_before_rows(self):
        with self.assertRaisesRegex(ValueError, "no header row"):
            self.fetch(csv_text="")

    def test_rejects_csv_missing_columns(self):
        header = ",".join(column for column in COLUMNS if column != "mag")
        row = ",".join(NEW_ROW[column] for column in COLUMNS if column != "mag")
        with self.assertRaisesRegex(ValueError, "missing columns"):
            self.fetch(csv_text=header + "\n" + row)

    def test_rejects_rows_with_the_wrong_number_of_values(self):
        short = make_csv(make_row())
        lines = short.splitlines()
        lines[1] = lines[1].rsplit(",", 1)[0]  # drop etime value
        with self.assertRaisesRegex(ValueError, "wrong number of values"):
            self.fetch(csv_text="\n".join(lines))

    def test_rejects_malformed_row_values(self):
        invalid = (
            (make_row(om=""), "missing or empty"),
            (make_row(date="2024-13-05"), "impossible 'date'"),
            (make_row(date="2024-1-5"), "malformed 'date'"),
            (make_row(time="25:00:00"), "impossible 'time'"),
            (make_row(mag="6"), "impossible EF rating"),
            (make_row(mag="-10"), "impossible EF rating"),
            (make_row(mag="abc"), "non-integer 'mag'"),
            (make_row(mo="00"), "impossible month"),
            (make_row(dy="32"), "impossible day"),
            (make_row(st="TXX"), "invalid state"),
            (make_row(slat="95.0"), "impossible 'slat'"),
            (make_row(slon="-181.0"), "impossible 'slon'"),
            (make_row(len="-1.0"), "negative 'len'"),
            (make_row(loss="abc"), "non-numeric 'loss'"),
            (make_row(edat="2024-13-05"), "impossible 'edat'"),
            (make_row(etime="25:00:00"), "impossible 'etime'"),
        )
        for row, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(csv_text=make_csv(row))

    def test_invalid_timeout_fails_before_http(self):
        for timeout in (True, 0, -1, 1.5, MODULE.MAX_TIMEOUT + 1):
            with self.subTest(timeout=timeout):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, "timeout"):
                        list(MODULE.fetch_spc_tornado_reports(timeout=timeout))
                urlopen.assert_not_called()

    def test_main_emits_valid_ndjson_and_diagnostic(self):
        stdout = io.StringIO()
        stderr = io.StringIO()

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[HttpReply(make_index(2025)),
                         HttpReply(make_csv(OLD_ROW, NEW_ROW))],
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                MODULE.main(["--timeout", "11"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            record = json.loads(line)
            self.assertEqual(record["source"], "spc_tornado_reports")
            self.assertIn("fetched_at", record)
            self.assertIn("id", record)
        self.assertIn("spc_tornado_reports: 2 tornado rows", stderr.getvalue())

    def test_source_configuration_is_daily_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r"(?m)^name: spc_tornado_reports$")
        self.assertRegex(text, r"(?m)^script: fetch_spc_tornado_reports\.py$")
        self.assertRegex(text, r'(?m)^schedule: "12 6 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertIsNone(re.search(r"(?m)^enabled:\s*true$", text))
        self.assertIn('args: ["--timeout", "60"]', text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 15", text)
        self.assertIn("sink: local", text)
        self.assertIn("public domain", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("live SPC source smoke", text)
        self.assertIn("read-only Airflow import check", text)
        self.assertIn("SHA-256", text)
        self.assertIn("two bounded requests per run", text)


if __name__ == "__main__":
    unittest.main()
