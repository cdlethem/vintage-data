import contextlib
import html
import importlib.util
import io
import json
import pathlib
import re
import tempfile
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_treasury_yield_curve.py"
SPEC = importlib.util.spec_from_file_location("fetch_treasury_yield_curve", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __init__(self, body, *, status=200):
        super().__init__(body)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def transient(code):
    return urllib.error.HTTPError(MODULE.BASE.format(year=2026), code,
                                  "error", {}, io.BytesIO(b"origin failure"))


def row_cells(date, ten_yr="4.19", n_a_tail=True):
    """One 26-cell row in the table's exact column order."""
    cells = [
        date,
        "N/A" if n_a_tail else "5.10",  # 20 YR (extrapolated)
        "N/A" if n_a_tail else "5.20",  # 30 YR (extrapolated)
        "N/A",                          # Extrapolation Factor
        "4.00", "4.02",                 # 6 weeks bank discount / coupon
        "4.10", "4.12",                 # 8 weeks
        "4.20", "4.22",                 # 17 weeks
        "4.30", "4.32",                 # 52 weeks
        "3.72", "3.71", "3.66", "3.65", "3.62", "3.58",
        "3.47", "3.47", "3.55", "3.74", "3.95", ten_yr,
        "4.81", "4.86",
    ]
    assert len(cells) == len(MODULE.EXPECTED_HEADER)
    return cells


def table_html(rows, header=None):
    out = ["<html><body><table><tr>"]
    for head in (header or MODULE.EXPECTED_HEADER):
        out.append(f"<th>{html.escape(head)}</th>")
    out.append("</tr>")
    for cells in rows:
        out.append("<tr>")
        for cell in cells:
            out.append(f"<td>{html.escape(cell)}</td>")
        out.append("</tr>")
    out.append("</table></body></html>")
    return "".join(out)


def run_main(urlopen_side_effect, *args):
    stdout, stderr = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           side_effect=urlopen_side_effect), \
         mock.patch.object(MODULE.time, "sleep") as sleep, \
         contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        try:
            MODULE.main(list(args))
            code = 0
        except SystemExit as exc:
            code = exc.code
    return code, stdout.getvalue(), stderr.getvalue(), sleep


def summary_of(stderr):
    lines = [line for line in stderr.splitlines()
             if line.startswith(MODULE.SUMMARY_PREFIX)]
    assert len(lines) == 1, f"expected one summary line, got: {stderr!r}"
    return json.loads(lines[0][len(MODULE.SUMMARY_PREFIX):])


class TreasuryYieldCurveTests(unittest.TestCase):
    def test_success_maps_table_rows_to_records(self):
        body = table_html([
            row_cells("11/06/2026", ten_yr="4.86"),
            row_cells("11/09/2026", ten_yr="4.88"),
            row_cells("11/10/2026", ten_yr="4.98"),
        ])
        code, out, err, _ = run_main([Response(body.encode("utf-8"))],
                                     "--no-state", "--start-year", "2026",
                                     "--end-year", "2026")
        self.assertEqual(code, 0)
        records = [json.loads(line) for line in out.splitlines()]
        self.assertEqual([r["id"] for r in records],
                         ["2026-11-06", "2026-11-09", "2026-11-10"])
        for record in records:
            self.assertEqual(record["source"], "treasury_yield_curve")
        self.assertEqual(len({r["fetched_at"] for r in records}), 1)
        first = records[0]
        self.assertIsNone(first["y20_extrapolated"])  # N/A -> null
        self.assertEqual(first["t6w_bank_discount"], 4.0)
        self.assertEqual(first["y10y"], 4.86)
        self.assertEqual(first["y30y"], 4.86)
        self.assertEqual(first["y1m"], 3.72)
        self.assertEqual(first["y20y"], 4.81)
        self.assertIsNone(first["extrapolation_factor"])
        summary = summary_of(err)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["requests"],
                         {"attempted": 1, "succeeded": 1, "failed": 0,
                          "retries": 0, "backoff_s": 0.0})
        self.assertEqual(summary["metrics"]["rows_emitted"], 3)
        self.assertEqual(summary["metrics"]["latest_source_date"], "2026-11-10")

    def test_watermark_filters_old_rows_and_advances_atomically(self):
        state_file = None
        with tempfile.TemporaryDirectory() as tmp:
            state_file = pathlib.Path(tmp) / "watermark.json"
            def old_body():
                return [Response(table_html([row_cells("11/06/2026")]).encode("utf-8"))]
            code, out, _, _ = run_main(old_body(), "--state-file", str(state_file),
                                       "--start-year", "2026", "--end-year", "2026")
            self.assertEqual(code, 0)
            self.assertEqual(len(out.splitlines()), 1)
            self.assertEqual(json.loads(state_file.read_text())["last_date"],
                             "2026-11-06")

            # Nothing new: silent run, watermark untouched.
            before = state_file.read_text()
            code, out, _, _ = run_main(old_body(), "--state-file", str(state_file),
                                       "--start-year", "2026", "--end-year", "2026")
            self.assertEqual(code, 0)
            self.assertEqual(out, "")
            self.assertEqual(state_file.read_text(), before)

            # A new business day: only the new row is emitted.
            newer = [Response(table_html([
                row_cells("11/06/2026"),
                row_cells("11/09/2026"),
                row_cells("11/10/2026", ten_yr="4.98"),
            ]).encode("utf-8"))]
            code, out, _, _ = run_main(newer, "--state-file", str(state_file),
                                       "--start-year", "2026", "--end-year", "2026")
            self.assertEqual(code, 0)
            records = [json.loads(line) for line in out.splitlines()]
            self.assertEqual([r["id"] for r in records],
                             ["2026-11-09", "2026-11-10"])
            self.assertEqual(json.loads(state_file.read_text())["last_date"],
                             "2026-11-10")

    def test_missing_table_fails_loudly(self):
        body = "<html><body><p>404 - Page Not Found</p></body></html>"
        code, out, err, _ = run_main([Response(body.encode("utf-8"))],
                                     "--no-state", "--start-year", "2026",
                                     "--end-year", "2026")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("not recognized", err)

    def test_header_drift_fails(self):
        drifted = list(MODULE.EXPECTED_HEADER)
        drifted[23] = "10Y"
        body = table_html([row_cells("11/10/2026")], header=drifted)
        code, out, err, _ = run_main([Response(body.encode("utf-8"))],
                                     "--no-state", "--start-year", "2026",
                                     "--end-year", "2026")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("not recognized", err)

    def test_duplicate_date_fails(self):
        body = table_html([row_cells("11/06/2026"), row_cells("11/06/2026")])
        code, out, err, _ = run_main([Response(body.encode("utf-8"))],
                                     "--no-state", "--start-year", "2026",
                                     "--end-year", "2026")
        self.assertEqual(code, 1)
        self.assertIn("duplicate date 2026-11-06", err)

    def test_transient_503_is_retried_then_succeeds(self):
        body = table_html([row_cells("11/10/2026")])
        code, out, err, sleep = run_main(
            [transient(503), Response(body.encode("utf-8"))],
            "--no-state", "--start-year", "2026", "--end-year", "2026",
            "--retry-delay", "5")
        self.assertEqual(code, 0)
        self.assertEqual(len(out.splitlines()), 1)
        sleep.assert_called_once_with(5.0)
        summary = summary_of(err)
        self.assertEqual(summary["requests"]["retries"], 1)
        self.assertEqual(summary["requests"]["backoff_s"], 5.0)
        self.assertEqual(summary["requests"],
                         {"attempted": 2, "succeeded": 1, "failed": 1,
                          "retries": 1, "backoff_s": 5.0})

    def test_exhausted_retries_exit_nonzero(self):
        code, out, err, sleep = run_main(
            [transient(503), transient(503), transient(503)],
            "--no-state", "--start-year", "2026", "--end-year", "2026",
            "--retries", "2", "--retry-delay", "1")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("HTTP 503", err)
        self.assertEqual(sleep.call_count, 2)

    def test_row_outside_requested_year_fails(self):
        body = table_html([row_cells("12/31/2025")])
        code, out, err, _ = run_main([Response(body.encode("utf-8"))],
                                     "--no-state", "--start-year", "2026",
                                     "--end-year", "2026")
        self.assertEqual(code, 1)
        self.assertIn("dated", err)

    def test_source_yaml_schedule_is_staggered_daily(self):
        config = (pathlib.Path(__file__).parent / "sources"
                  / "treasury_yield_curve.yml").read_text(encoding="utf-8")
        self.assertIn("name: treasury_yield_curve", config)
        self.assertIn("script: fetch_treasury_yield_curve.py", config)
        schedule = re.search(r'^schedule:\s*"([^"]+)"', config, re.M).group(1)
        minute, hour, dom, month, dow = schedule.split()
        self.assertEqual((dom, month, dow), ("*", "*", "*"))
        self.assertTrue(minute.isdigit() and 1 <= int(minute) <= 59)
        self.assertTrue(hour.isdigit() and 0 <= int(hour) <= 23)
        # Treasury publishes after the 16:00 UTC close; 17:30 UTC is safely past it.
        self.assertGreaterEqual(int(hour), 17)
        self.assertIn("licence:", config)
        self.assertIn("rate_limit:", config)
        self.assertIn("attribution_required:", config)


if __name__ == "__main__":
    unittest.main()
