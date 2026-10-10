"""Focused tests for the BoE rates fetcher.

Loads the fetcher as a module (no Airflow, no network by default) and drives
``main()`` with a mocked HTTP response, plus direct unit tests of the CSV parser
and URL builder. The run summary is validated against the repo's own
``orchestration/include/run_metadata.py`` contract so the tests double-check the
summary line the DAG runner will parse.
"""
import importlib.util
import io
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
import urllib.parse

SCRIPT = Path(__file__).parent / "scripts" / "fetch_boe_rates.py"
RUN_METADATA_PATH = Path(__file__).parent.parent / "orchestration" / "include" / "run_metadata.py"

def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module

MODULE = _load("fetch_boe_rates", SCRIPT)
RUN_METADATA = _load("boe_test_run_metadata", RUN_METADATA_PATH)

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
TODAY = date(2026, 10, 10)
CODES = list(MODULE.SERIES)
HEADER = "DATE," + ",".join(CODES)


def boe_date(d: date) -> str:
    return d.strftime("%d %b %Y")


def make_csv(rows: list[tuple[date, list[str]]], header: str = HEADER) -> str:
    lines = [header]
    for d, cells in rows:
        lines.append(boe_date(d) + "," + ",".join(cells))
    return "\n".join(lines) + "\n"


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def captured_summary(err_text: str) -> dict:
    for line in err_text.splitlines():
        if line.startswith(MODULE.SUMMARY_PREFIX):
            return json.loads(line.split("\t", 1)[1])
    raise AssertionError("no VINTAGE_RUN_SUMMARY line in stderr:\n" + err_text)


def run_main(argv, *, csv_text: str | None = None, exc: Exception | None = None,
             state_file: Path | None = None):
    """Run main() with urlopen mocked; return (exit, stdout, stderr)."""
    if state_file is not None:
        argv = list(argv) + ["--state-file", str(state_file)]

    def fake_urlopen(request, timeout=None):
        if exc is not None:
            raise exc
        return FakeResponse((csv_text or "").encode("utf-8"))

    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen):
        code = MODULE.main(argv, out=out, err=err)
    return code, out.getvalue(), err.getvalue()


class BuildUrlTests(unittest.TestCase):
    def test_url_parameters(self):
        url, start, end = MODULE.build_url(14, NOW)
        self.assertEqual(end, TODAY)
        self.assertEqual(start, TODAY - timedelta(days=13))
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        self.assertEqual(query["Dateto"], ["now"])
        self.assertEqual(query["Datefrom"], [start.strftime("%d/%b/%Y")])
        self.assertEqual(query["CSVF"], ["TN"])
        self.assertEqual(query["UsingCodes"], ["Y"])
        self.assertEqual(query["SeriesCodes"], [",".join(CODES)])
        self.assertIn(MODULE.BASE_URL, url)

    def test_window_days(self):
        _, start, end = MODULE.build_url(1, NOW)
        self.assertEqual((end - start).days, 0)


class ParseCsvTests(unittest.TestCase):
    def diag(self):
        return {"dropped_series": [], "missing_values": 0, "unparseable_values": 0,
                "bad_dates": 0, "out_of_window": 0}

    def rows(self):
        return [
            (TODAY - timedelta(days=2), ["3.75", "3.75", "121.47635635", "52863"]),
            (TODAY - timedelta(days=1), ["3.75", "", "121.47635635", ""]),
            (TODAY, ["3.75", "3.7331", "121.47635635", "48870.80555320"]),
        ]

    def test_emits_all_present_observations(self):
        records, progress = MODULE.parse_csv(make_csv(self.rows()), {}, "FA",
                                             TODAY - timedelta(days=13), TODAY, self.diag())
        self.assertEqual(len(records), 10)  # 3 dates x 4 series minus 2 blank cells
        self.assertEqual(progress["IUDBEDR"], TODAY.isoformat())

    def test_ids_and_fields(self):
        records, _ = MODULE.parse_csv(make_csv(self.rows()), {}, "FA",
                                      TODAY - timedelta(days=13), TODAY, self.diag())
        rec = next(r for r in records if r["series_code"] == "IUDBEDR" and r["date"] == TODAY.isoformat())
        self.assertEqual(rec["id"], f"IUDBEDR:{TODAY.isoformat()}")
        self.assertEqual(rec["source"], "boe_rates")
        self.assertEqual(rec["fetched_at"], "FA")
        self.assertEqual(rec["series_name"], "Bank Rate")
        self.assertEqual(rec["unit"], "percent")
        self.assertEqual(rec["value"], 3.75)

    def test_missing_cells_skipped_and_counted(self):
        diag = self.diag()
        records, _ = MODULE.parse_csv(make_csv(self.rows()), {}, "FA",
                                      TODAY - timedelta(days=13), TODAY, diag)
        ids = {(r["series_code"], r["date"]) for r in records}
        self.assertNotIn(("IUDSOIA", (TODAY - timedelta(days=1)).isoformat()), ids)
        self.assertNotIn(("IUDZLT2", (TODAY - timedelta(days=1)).isoformat()), ids)
        self.assertEqual(diag["missing_values"], 2)

    def test_watermark_filters_old_dates(self):
        wm = {code: (TODAY - timedelta(days=1)).isoformat() for code in CODES}
        records, _ = MODULE.parse_csv(make_csv(self.rows()), wm, "FA",
                                      TODAY - timedelta(days=13), TODAY, self.diag())
        self.assertEqual({r["date"] for r in records}, {TODAY.isoformat()})
        self.assertEqual(len(records), 4)

    def test_header_missing_series_drops_it(self):
        header = "DATE,IUDBEDR,IUDSOIA,IUDZOS2"  # IUDZLT2 absent
        diag = self.diag()
        records, _ = MODULE.parse_csv(make_csv(self.rows(), header=header), {}, "FA",
                                      TODAY - timedelta(days=13), TODAY, diag)
        self.assertEqual(len(diag["dropped_series"]), 1)
        self.assertEqual(diag["dropped_series"][0]["series"], "IUDZLT2")
        self.assertNotIn({r["series_code"] for r in records}, {"IUDZLT2"})

    def test_out_of_window_rows_dropped(self):
        rows = self.rows() + [(TODAY - timedelta(days=40), ["3.75", "3.75", "1", "1"])]
        diag = self.diag()
        records, _ = MODULE.parse_csv(make_csv(rows), {}, "FA",
                                      TODAY - timedelta(days=13), TODAY, diag)
        self.assertEqual(diag["out_of_window"], 1)
        self.assertNotIn((TODAY - timedelta(days=40)).isoformat(), {r["date"] for r in records})

    def test_unparseable_value_counted(self):
        rows = [(TODAY, ["3.75", "3.75", "n/a", "1"])]
        diag = self.diag()
        records, _ = MODULE.parse_csv(make_csv(rows), {}, "FA",
                                      TODAY - timedelta(days=13), TODAY, diag)
        self.assertEqual(diag["unparseable_values"], 1)
        self.assertNotIn("IUDZOS2", {r["series_code"] for r in records})

    def test_bad_date_counted(self):
        text = HEADER + "\nnot a date,3.75,3.75,1,1\n"
        diag = self.diag()
        records, _ = MODULE.parse_csv(text, {}, "FA",
                                      TODAY - timedelta(days=13), TODAY, diag)
        self.assertEqual(diag["bad_dates"], 1)
        self.assertEqual(records, [])

    def test_html_response_raises(self):
        with self.assertRaises(MODULE.BoeResponseError):
            MODULE.parse_csv("<html><body>502</body></html>", {}, "FA",
                             TODAY - timedelta(days=13), TODAY, self.diag())


class MainTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.state = Path(self._tmp.name) / "boe_state.json"

    def tearDown(self):
        self._tmp.cleanup()

    def test_success_emits_records_and_valid_summary(self):
        rows = [(TODAY - timedelta(days=1), ["3.75", "3.75", "121.47", "52863"]),
                (TODAY, ["3.75", "3.7331", "121.47", "48870.8"])]
        code, out, err = run_main(["--no-state"], csv_text=make_csv(rows))
        self.assertEqual(code, 0)
        records = [json.loads(line) for line in out.splitlines()]
        self.assertEqual(len(records), 8)
        for rec in records:
            self.assertIn("source", rec) and self.assertIn("fetched_at", rec) and self.assertIn("id", rec)
        summary = RUN_METADATA.validate_summary(captured_summary(err))
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["requests"]["succeeded"], 1)
        self.assertEqual(summary["metrics"]["records"], 8)

    def test_watermark_persists_and_filters(self):
        first = [(TODAY - timedelta(days=1), ["3.75", "3.75", "121.47", "52863"])]
        second = first + [(TODAY, ["3.75", "3.7331", "121.47", "48870.8"])]
        code, out, _ = run_main([], csv_text=make_csv(first), state_file=self.state)
        self.assertEqual(code, 0)
        self.assertTrue(self.state.exists())
        self.assertEqual(len([l for l in out.splitlines() if l]), 4)

        code, out, _ = run_main([], csv_text=make_csv(second), state_file=self.state)
        self.assertEqual(code, 0)
        new = [json.loads(l) for l in out.splitlines()]
        self.assertEqual({r["date"] for r in new}, {TODAY.isoformat()})
        self.assertEqual(len(new), 4)

    def test_no_state_writes_nothing(self):
        rows = [(TODAY, ["3.75", "3.75", "121.47", "52863"])]
        code, out, _ = run_main(["--no-state"], csv_text=make_csv(rows))
        self.assertEqual(code, 0)
        self.assertEqual(len(out.splitlines()), 4)

    def test_missing_series_is_degraded_partial(self):
        header = "DATE,IUDBEDR,IUDSOIA,IUDZOS2"
        rows = [(TODAY, ["3.75", "3.75", "121.47"])]
        code, out, err = run_main(["--no-state"], csv_text=make_csv(rows, header=header))
        self.assertEqual(code, 0)
        summary = RUN_METADATA.validate_summary(captured_summary(err))
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["partitions"]["failed"], 1)

    def test_html_response_fails(self):
        code, out, err = run_main(["--no-state"], csv_text="<html>502 Bad Gateway</html>")
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        summary = RUN_METADATA.validate_summary(captured_summary(err))
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["requests"]["failed"], 1)

    def test_network_error_fails(self):
        import urllib.error
        code, out, err = run_main(["--no-state"], exc=urllib.error.URLError("boom"))
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        summary = RUN_METADATA.validate_summary(captured_summary(err))
        self.assertEqual(summary["health"], "failed")
        self.assertIn("boom", summary["metrics"]["error"])

    def test_state_path_inside_repo_rejected(self):
        inside = SCRIPT.parent / "boe_state_forbidden.json"
        try:
            code, out, err = run_main(["--state-file", str(inside)],
                                      csv_text=make_csv([(TODAY, ["3.75", "3.75", "121.47", "1"])]))
            self.assertEqual(code, 1)
            self.assertEqual(out, "")
        finally:
            if inside.exists():
                inside.unlink()

    def test_lookback_bounds_rejected(self):
        code, _, err = run_main(["--no-state", "--lookback-days", "0"],
                                csv_text=make_csv([]))
        self.assertEqual(code, 2)
        self.assertIn("lookback-days", err)


if __name__ == "__main__":
    unittest.main()
