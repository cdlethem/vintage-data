import contextlib
import csv
import importlib.util
import io
from datetime import datetime
from pathlib import Path
import unittest
from unittest import mock
import urllib.error


SCRIPT = Path(__file__).parents[1] / "scripts" / "validate_celestrak_socrates.py"
SPEC = importlib.util.spec_from_file_location("validate_celestrak_socrates", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

HEADERS = [
    "NORAD_CAT_ID_1", "NORAD_CAT_ID_2", "OBJECT_NAME_1", "OBJECT_NAME_2", "TCA",
    "TCA_RANGE", "TCA_RELATIVE_SPEED", "MAX_PROB", "DILUTION",
]


def rows():
    # Pair 1:2 recurs at two different TCAs; it is not a duplicate conjunction.
    return [[str(1 + index // 2), str(2 + index // 2), "OBJECT A", "OBJECT B",
             f"2026-09-27T{index // 60:02d}:{index % 60:02d}:00Z", "1.5", "12.3",
             "8.810E-02", ""] for index in range(MODULE.ROW_COUNT)]


def csv_text(data):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(HEADERS)
    writer.writerows(data)
    return output.getvalue()


def table_text(data):
    primary = ["Object Name", "NORAD Catalog Number", "Days Since Epoch", "TCA (UTC)",
               "Min Range (km)", "Relative Speed (km/s)", "Max Probability"]
    secondary = ["Object Name", "NORAD Catalog Number", "Days Since Epoch",
                 "Min Range (km)", "Relative Speed (km/s)", "Dilution Threshold"]

    def tr(kind, cells):
        return "<tr>" + "".join(f"<{kind}>{cell}</{kind}>" for cell in cells) + "</tr>"

    headings = tr("th", primary) + tr("th", secondary)
    body = ""
    for index, row in enumerate(data):
        if index == 50:
            body += headings
        display_time = row[4].replace("T", " ").replace("Z", "")
        date, clock = display_time.rsplit(" ", 1)
        hour, _, fraction = clock.partition(".")
        display_time = datetime.strptime(f"{date} {hour}", "%Y-%m-%d %H:%M:%S").strftime("%Y %b %d %H:%M:%S")
        if fraction:
            display_time += f".{fraction}"
        body += tr("td", [row[2], row[0], "0.4", display_time, row[5], row[6], row[7]])
        body += tr("td", [row[3], row[1], "0.5", row[5], row[6], "0.1"])
    return f"<html><table>{tr('th', ['Data current as of 2026 Sep 27'])}{headings}{body}{tr('th', ['Legend'])}</table></html>"


class Response:
    def __init__(self, url, body, *, status=200, content_type="text/html", headers=None):
        self.status = status
        self.url = url
        self.body = body if isinstance(body, bytes) else body.encode("utf-8")
        self.headers = {"Content-Type": content_type, "Content-Length": str(len(self.body))}
        if headers:
            self.headers.update(headers)

    def geturl(self):
        return self.url

    def read(self, size):
        return self.body[:size]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class ValidateCelestrakSocratesTests(unittest.TestCase):
    def setUp(self):
        self.data = rows()
        self.table_response = Response(MODULE.TABLE_URL, table_text(self.data))
        self.csv_response = Response(MODULE.BASELINE_URL, csv_text(self.data + [
            ["999", "888", "EXTRA", "EXTRA", "2026-09-29T00:00:00Z", "1", "2", "0", ""],
        ]), content_type="text/csv")

    def run_validator(self):
        opener = mock.Mock()
        opener.open.side_effect = [self.table_response, self.csv_response]
        with mock.patch.object(MODULE.urllib.request, "build_opener", return_value=opener):
            result = MODULE.validate()
        self.assertEqual([call.args[0].full_url for call in opener.open.call_args_list],
                         [MODULE.TABLE_URL, MODULE.BASELINE_URL])
        self.assertEqual([call.args[0].get_method() for call in opener.open.call_args_list], ["GET", "GET"])
        self.assertEqual([call.kwargs for call in opener.open.call_args_list], [{"timeout": 60}] * 2)
        return result

    def test_pinned_gets_reconcile_ids_pairs_tcas_and_conjunctions(self):
        result = self.run_validator()
        self.assertEqual(result["result"], "passed")
        self.assertEqual(result["intended_source_baseline"], MODULE.BASELINE_URL)
        self.assertEqual(result["baseline_rows"], 101)
        self.assertEqual(result["matched_ids"], 51)
        self.assertEqual(result["matched_object_pairs"], 50)
        self.assertEqual(result["matched_tcas"], 100)

    def test_ids_pairs_and_tcas_must_each_match(self):
        for field, index, replacement, dimension in (
            ("id", 0, "777", "IDs"),
            ("pair", 1, "777", "object pairs"),
            ("tca", 4, "2026-10-01T00:00:00Z", "TCAs"),
        ):
            with self.subTest(field=field):
                data = rows()
                data[0][index] = replacement
                self.table_response = Response(MODULE.TABLE_URL, table_text(data))
                with self.assertRaisesRegex(MODULE.ValidationError, dimension):
                    self.run_validator()

    def test_reassigned_tcas_fail_even_when_all_coverage_sets_match(self):
        data = rows()
        data[0][4], data[2][4] = data[2][4], data[0][4]
        self.table_response = Response(MODULE.TABLE_URL, table_text(data))
        with self.assertRaisesRegex(
            MODULE.ValidationError,
            "IDs 51/51, object pairs 50/50, TCAs 100/100, conjunctions 98/100",
        ):
            self.run_validator()

    def test_subsecond_tca_must_match_exactly(self):
        data = rows()
        data[0][4] = "2026-09-27T00:00:00.123Z"
        self.table_response = Response(MODULE.TABLE_URL, table_text(data))
        with self.assertRaisesRegex(MODULE.ValidationError, "TCAs"):
            self.run_validator()

    def test_equal_subsecond_utc_times_reconcile(self):
        data = rows()
        data[0][4] = "2026-09-27T00:00:00.123Z"
        self.table_response = Response(MODULE.TABLE_URL, table_text(data))
        self.csv_response = Response(MODULE.BASELINE_URL, csv_text(data), content_type="text/csv")
        self.assertEqual(self.run_validator()["matched_tcas"], 100)

    def test_duplicate_id_fails_even_with_100_rows(self):
        data = rows()
        data[1] = data[0][:]
        self.table_response = Response(MODULE.TABLE_URL, table_text(data))
        with self.assertRaisesRegex(MODULE.ValidationError, "duplicate conjunction ID"):
            self.run_validator()

    def test_bad_status_media_type_length_or_redirect_fails_closed(self):
        cases = (
            ("status", {"status": 503}, "HTTP status"),
            ("media type", {"content_type": "application/json"}, "Content-Type"),
            ("short body", {"headers": {"Content-Length": "999999"}}, "incomplete response"),
            ("redirect", {"url": "https://example.invalid/"}, "redirected"),
        )
        for name, changes, message in cases:
            with self.subTest(name=name):
                values = {"url": MODULE.TABLE_URL, "body": table_text(self.data)}
                values.update(changes)
                self.table_response = Response(**values)
                with self.assertRaisesRegex(MODULE.ValidationError, message):
                    self.run_validator()

    def test_missing_or_malformed_schema_and_baseline_are_rejected(self):
        data = rows()
        data[0][7] = "NaN"
        self.table_response = Response(MODULE.TABLE_URL, table_text(data))
        with self.assertRaisesRegex(MODULE.ValidationError, "MAX_PROB"):
            self.run_validator()
        self.table_response = Response(MODULE.TABLE_URL, table_text(rows()).replace("TCA (UTC)", "WRONG"))
        with self.assertRaisesRegex(MODULE.ValidationError, "tca"):
            self.run_validator()
        self.table_response = Response(MODULE.TABLE_URL, table_text(rows()))
        self.csv_response = Response(MODULE.BASELINE_URL, csv_text(rows()[:99]), content_type="text/csv")
        with self.assertRaisesRegex(MODULE.ValidationError, "fewer than 100"):
            self.run_validator()

    def test_malformed_table_row_is_not_silently_skipped(self):
        document = table_text(rows()).replace("<td>0.4</td>", "", 1)
        self.table_response = Response(MODULE.TABLE_URL, document)
        with self.assertRaisesRegex(MODULE.ValidationError, "malformed data row"):
            self.run_validator()

    def test_incomplete_pairs_repeated_headings_and_invalid_time_fail_closed(self):
        document = table_text(rows())
        for name, broken, message in (
            ("orphan primary", document.replace("<td>OBJECT B</td>", "<td>OBJECT B</td></tr><tr><td>OBJECT B</td>", 1), "malformed data row"),
            ("truncated secondary", document.replace("<td>0.5</td>", "", 1), "malformed data row"),
            ("broken repeated heading", document.replace("TCA (UTC)", "TCA", 1).replace("TCA (UTC)", "WRONG", 1), "repeated headings"),
            ("invalid time", document.replace("2026 Sep 27 00:00:00", "2026 Sep 27 99:00:00", 1), "invalid TCA"),
        ):
            with self.subTest(name=name):
                self.table_response = Response(MODULE.TABLE_URL, broken)
                with self.assertRaisesRegex(MODULE.ValidationError, message):
                    self.run_validator()

    def test_extra_csv_columns_are_not_silently_ignored(self):
        data = rows()
        document = csv_text(data).replace("8.810E-02,\r\n", "8.810E-02,,EXTRA\r\n", 1)
        self.csv_response = Response(MODULE.BASELINE_URL, document, content_type="text/csv")
        with self.assertRaisesRegex(MODULE.ValidationError, "malformed row"):
            self.run_validator()

    def test_main_reports_reconciliation_without_writing(self):
        opener = mock.Mock()
        opener.open.side_effect = [self.table_response, self.csv_response]
        output = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "build_opener", return_value=opener), \
             contextlib.redirect_stdout(output):
            self.assertEqual(MODULE.main([]), 0)
        self.assertIn('"result": "passed"', output.getvalue())
        self.assertEqual(opener.open.call_count, 2)

    def test_fixed_command_rejects_candidate_arguments_before_network(self):
        with mock.patch.object(MODULE.urllib.request, "build_opener") as build, \
             contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as failure:
                MODULE.main(["--url", "https://example.invalid", "--command", "echo"])
        self.assertEqual(failure.exception.code, 2)
        build.assert_not_called()

    def test_redirect_handler_blocks_followup_get(self):
        handler = MODULE.NoRedirect()
        self.assertIsNone(handler.redirect_request(None, None, 302, "Moved", {}, "https://example.invalid"))

    def test_network_failure_returns_nonzero_without_leaking_untrusted_url(self):
        opener = mock.Mock()
        opener.open.side_effect = urllib.error.URLError("https://example.invalid/?token=secret")
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "build_opener", return_value=opener), \
             contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main([]), 1)
        self.assertNotIn("secret", stderr.getvalue())
        self.assertIn("validation failed", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
