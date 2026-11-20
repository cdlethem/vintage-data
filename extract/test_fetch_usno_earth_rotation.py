import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_usno_earth_rotation.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "usno_earth_rotation.yml"
SPEC = importlib.util.spec_from_file_location("fetch_usno_earth_rotation", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

CONFIGURED_ARGV = ["--timeout", "30"]


class TextResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(document.encode("utf-8"))


def strict_loads(value):
    return json.loads(value, parse_constant=lambda name: (_ for _ in ()).throw(ValueError(f"non-finite JSON constant: {name}")))


def finals_line(
    *,
    yy="26", mm="11", dd="19", mjd="61363.00",
    pm_flag="I", ut1_flag="I", nut_flag="P",
    pm_x=" 0.116013 ", pm_x_err="0.000041 ",
    pm_y=" 0.330191 ", pm_y_err="0.000041  ",
    ut1="-0.0748043 ", ut1_err="0.0000168 ",
    lod="        ", lod_err="       ",
    dpsi=" -117.723 ", dpsi_err="   0.319 ",
    deps="   -8.775 ", deps_err="   0.159 ",
    bb_pm_x="          ", bb_pm_y="          ",
    bb_ut1="           ", bb_dpsi="          ", bb_deps="          ",
):
    """One finals.daily line, byte-identical to the live file (187 chars)."""
    return (
        f"{yy}{mm:>2}{dd:>2} {mjd} {pm_flag} "
        f"{pm_x}{pm_x_err}{pm_y}{pm_y_err}"
        f"{ut1_flag}{ut1}{ut1_err}{lod}{lod_err} "
        f"{nut_flag} {dpsi}{dpsi_err}{deps}{deps_err}"
        f"{bb_pm_x}{bb_pm_y}{bb_ut1}{bb_dpsi}{bb_deps} "
    )
OLD_ROW = finals_line(
    mm="8", dd="22", mjd="61274.00", nut_flag="I",
    pm_x=" 0.217528 ", pm_x_err="0.000026 ",
    pm_y=" 0.347795 ", pm_y_err="0.000026  ",
    ut1=" 0.0068596 ", ut1_err="0.0000073 ",
    lod="-0.1030 ", lod_err="0.0063 ",
    dpsi=" -123.610 ", dpsi_err="   0.074 ",
    deps="  -11.595 ", deps_err="   0.028 ",
    bb_pm_x=" 0.217545 ", bb_pm_y=" 0.347812 ",
    bb_ut1=" 0.0068540 ", bb_dpsi=" -123.531 ", bb_deps="  -11.449 ",
)
MID_ROW = finals_line(
    dd="18", mjd="61362.00",
    pm_x=" 0.116949 ", pm_x_err="0.000042 ",
    pm_y=" 0.328964 ", pm_y_err="0.000042  ",
    ut1="-0.0732199 ", ut1_err="0.0000096 ",
    lod=" 1.3567 ", lod_err="0.0097 ",
    dpsi=" -117.523 ", dpsi_err="   0.318 ",
    deps="   -8.890 ", deps_err="   0.158 ",
)
RECENT_ROW = finals_line(dd="19", mjd="61363.00")
PRED_ROW = finals_line(
    dd="20", mjd="61364.00",
    pm_flag="P", ut1_flag="P",
    pm_x=" 0.115042 ", pm_x_err="0.000615 ",
    pm_y=" 0.331358 ", pm_y_err="0.000408  ",
    ut1="-0.0765424 ", ut1_err="0.0000279 ",
    dpsi=" -118.050 ", dpsi_err="   0.320 ",
    deps="   -8.861 ",
)
DOCUMENT = "\n".join([OLD_ROW, MID_ROW, RECENT_ROW, PRED_ROW]) + "\n"


class FetchUsnoEarthRotationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_file = Path(self._tmp.name) / "state" / "usno_earth_rotation.json"

    def write_state(self, last_mjd):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(
            json.dumps({"version": 1, "source": "usno_earth_rotation", "last_mjd": last_mjd}),
            encoding="utf-8",
        )

    def run_main(self, document=DOCUMENT, argv=None):
        stdout = io.StringIO()
        stderr = io.StringIO()
        if argv is None:
            argv = list(CONFIGURED_ARGV) + ["--state-file", str(self.state_file)]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=TextResponse(document)
        ) as urlopen:
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                MODULE.main(argv)
        return stdout.getvalue(), stderr.getvalue(), urlopen

    def assert_main_rejects(self, argv, message):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=TextResponse(DOCUMENT)
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as raised:
                    MODULE.main(argv)
        self.assertEqual(raised.exception.code, 2)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn(message, stderr.getvalue())

    def test_fixture_layout_matches_parser_slicing(self):
        row = MODULE.parse_row(RECENT_ROW)
        self.assertEqual(row["yy"], "26")
        self.assertEqual(row["mm"], "11")
        self.assertEqual(row["dd"], "19")
        self.assertEqual(row["mjd"], "61363.00")
        self.assertEqual(row["pm_x"], " 0.116013 ")
        self.assertEqual(row["pm_x_err"], "0.000041 ")
        self.assertEqual(row["pm_y"], " 0.330191 ")
        self.assertEqual(row["pm_y_err"], "0.000041  ")
        self.assertEqual(row["ut1_utc"], "-0.0748043 ")
        self.assertEqual(row["ut1_utc_err"], "0.0000168 ")
        self.assertEqual(row["lod"], "        ")
        self.assertEqual(row["depsilon"], "   -8.775 ")
        self.assertEqual(row["depsilon_err"], "   0.159 ")
        self.assertEqual(row["bull_b_pm_x"], "          ")

    def test_normalizes_a_full_final_row(self):
        fetched_at = "2026-11-20T19:55:00+00:00"
        record = MODULE.normalize(MODULE.parse_row(OLD_ROW), fetched_at)
        self.assertEqual(record["source"], "usno_earth_rotation")
        self.assertEqual(record["fetched_at"], fetched_at)
        self.assertEqual(record["id"], "61274.00")
        self.assertEqual(record["date"], "2026-08-22")
        self.assertEqual(record["mjd"], 61274.0)
        self.assertEqual(record["pm_flag"], "I")
        self.assertEqual(record["pm_x"], 0.217528)
        self.assertEqual(record["pm_x_err"], 0.000026)
        self.assertEqual(record["pm_y"], 0.347795)
        self.assertEqual(record["pm_y_err"], 0.000026)
        self.assertEqual(record["ut1_flag"], "I")
        self.assertEqual(record["ut1_utc"], 0.0068596)
        self.assertEqual(record["ut1_utc_err"], 0.0000073)
        self.assertEqual(record["lod"], -0.1030)
        self.assertEqual(record["lod_err"], 0.0063)
        self.assertEqual(record["nutation_flag"], "I")
        self.assertEqual(record["dpsi"], -123.610)
        self.assertEqual(record["dpsi_err"], 0.074)
        self.assertEqual(record["depsilon"], -11.595)
        self.assertEqual(record["depsilon_err"], 0.028)
        self.assertEqual(record["bull_b_pm_x"], 0.217545)
        self.assertEqual(record["bull_b_pm_y"], 0.347812)
        self.assertEqual(record["bull_b_ut1_utc"], 0.0068540)
        self.assertEqual(record["bull_b_dpsi"], -123.531)
        self.assertEqual(record["bull_b_depsilon"], -11.449)

    def test_blank_optional_columns_normalize_to_null(self):
        fetched_at = "2026-11-20T19:55:00+00:00"
        record = MODULE.normalize(MODULE.parse_row(RECENT_ROW), fetched_at)
        self.assertIsNone(record["lod"])
        self.assertIsNone(record["lod_err"])
        self.assertEqual(record["nutation_flag"], "P")
        self.assertIsNone(record["bull_b_pm_x"])
        self.assertIsNone(record["bull_b_pm_y"])
        self.assertIsNone(record["bull_b_ut1_utc"])
        self.assertIsNone(record["bull_b_dpsi"])
        self.assertIsNone(record["bull_b_depsilon"])
        self.assertEqual(record["dpsi"], -117.723)

    def test_predictions_are_excluded_and_fetched_at_is_stamped_once(self):
        records = list(MODULE.fetch_records(DOCUMENT, 0.0, "2026-11-20T19:55:00+00:00"))
        self.assertEqual([record["id"] for record in records],
                         ["61274.00", "61362.00", "61363.00"])
        self.assertEqual(len({record["fetched_at"] for record in records}), 1)
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")

    def test_watermark_filters_rows(self):
        records = list(MODULE.fetch_records(DOCUMENT, 61361.0, "t"))
        self.assertEqual([record["id"] for record in records], ["61362.00", "61363.00"])
        self.assertEqual(list(MODULE.fetch_records(DOCUMENT, 61363.0, "t")), [])

    def test_malformed_document_fails_visibly(self):
        cases = {
            "short": "26 9 19 61363.00 I\n",
            "bad_mjd": finals_line(mjd="61363.0"),
            "bad_year": finals_line(yy="2x"),
            "bad_flag": finals_line(ut1_flag="Q"),
            "non_numeric": finals_line(ut1="N/A N/A N/A"),
            "comment": "# header line\n" + DOCUMENT,
        }
        for name, document in cases.items():
            with self.subTest(case=name):
                with self.assertRaises(ValueError):
                    list(MODULE.parse_rows(document))

    def test_first_run_backfills_all_finals_and_advances_watermark(self):
        stdout, stderr, urlopen = self.run_main()
        summary = json.loads(stderr.split(MODULE.SUMMARY_PREFIX, 1)[1])
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["records"], 3)
        records = [strict_loads(line) for line in stdout.splitlines()]
        self.assertEqual([record["id"] for record in records],
                         ["61274.00", "61362.00", "61363.00"])
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state, {"version": 1, "source": "usno_earth_rotation", "last_mjd": 61363.0})

    def test_up_to_date_run_is_a_noop(self):
        self.write_state(61363.0)
        before = self.state_file.read_bytes()
        stdout, stderr, urlopen = self.run_main()
        self.assertEqual(stdout, "")
        summary = json.loads(stderr.split(MODULE.SUMMARY_PREFIX, 1)[1])
        self.assertEqual(summary["records"], 0)
        self.assertEqual(self.state_file.read_bytes(), before)

    def test_stale_watermark_emits_only_new_rows(self):
        self.write_state(61361.0)
        stdout, stderr, urlopen = self.run_main()
        records = [strict_loads(line) for line in stdout.splitlines()]
        self.assertEqual([record["id"] for record in records], ["61362.00", "61363.00"])
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_mjd"], 61363.0)

    def test_no_state_emits_all_finals_and_writes_nothing(self):
        stdout, stderr, _ = self.run_main(argv=["--no-state"])
        records = [strict_loads(line) for line in stdout.splitlines()]
        self.assertEqual([record["id"] for record in records],
                         ["61274.00", "61362.00", "61363.00"])
        self.assertFalse(self.state_file.exists())

    def test_no_state_and_explicit_state_file_are_rejected(self):
        self.assert_main_rejects(["--no-state", "--state-file", str(self.state_file)],
                                 "cannot be combined")

    def test_non_positive_timeout_is_rejected(self):
        self.assert_main_rejects(["--timeout", "0"], "must be positive")

    def test_transport_failure_reports_summary_and_writes_no_state(self):
        argv = ["--state-file", str(self.state_file)]
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as raised:
                    MODULE.main(argv)
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(stdout.getvalue(), "")
        summary = json.loads(stderr.getvalue().split(MODULE.SUMMARY_PREFIX, 1)[1])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["records"], 0)
        self.assertIn("connection refused", summary["error"])
        self.assertFalse(self.state_file.exists())

    def test_single_bounded_request_to_usno(self):
        _, _, urlopen = self.run_main()
        self.assertEqual(urlopen.call_count, 1)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, MODULE.URL)
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)

    def test_source_config(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertIn("name: usno_earth_rotation", text)
        self.assertIn("script: fetch_usno_earth_rotation.py", text)
        self.assertIn("schedule: \"55 19 * * *\"", text)
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 5", text)
        self.assertIn("sink: local", text)
        self.assertIn("one new final row per UTC day", text)
        self.assertIn("one bounded GET per run", text)
        self.assertIn("public domain", text)
        self.assertIn("attribution_required: true", text)


if __name__ == "__main__":
    unittest.main()
