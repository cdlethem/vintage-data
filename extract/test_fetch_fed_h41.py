import importlib.util
import io
import json
import unittest
import urllib.error
import zipfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_fed_h41.py"
SPEC = importlib.util.spec_from_file_location("fetch_fed_h41", SCRIPT_PATH)
fetch_fed_h41 = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(fetch_fed_h41)

NS_MESSAGE = "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message"
NS_COMMON = "http://www.federalreserve.gov/structure/compact/common"
NS_SDMX_COMMON = "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common"
NS_H41 = "http://www.federalreserve.gov/structure/compact/H41_H41"


def series_xml(name, description, obs):
    anns = (
        f'<common:Annotation>'
        f'<common:AnnotationType>Short Description</common:AnnotationType>'
        f'<common:AnnotationText>{description}</common:AnnotationText>'
        f'</common:Annotation>'
        f'<common:Annotation>'
        f'<common:AnnotationType>Long Description</common:AnnotationType>'
        f'<common:AnnotationText>{description}</common:AnnotationText>'
        f'</common:Annotation>'
    )
    obs_xml = "\n".join(
        f'<frb:Obs OBS_STATUS="{status}" OBS_VALUE="{value}" TIME_PERIOD="{period}" />'
        for status, value, period in obs
    )
    return (
        f'<kf:Series SERIES_NAME="{name}" FREQ="19" CATEGORY="LIABCAP" '
        f'SUBCATEGORY="OFDRB" COMPONENT="DORBSC" DISTRIBUTION="TOT" '
        f'SERIESTYPE="L" UNIT="Currency" UNIT_MULT="1000000" CURRENCY="USD">'
        f'<frb:Annotations>{anns}</frb:Annotations>'
        f'{obs_xml}'
        f'</kf:Series>'
    )


def xml_bytes(body):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<m:MessageGroup xmlns:m="{NS_MESSAGE}"'
        f' xmlns:frb="{NS_COMMON}" xmlns:common="{NS_SDMX_COMMON}" xmlns:kf="{NS_H41}">'
        f'<m:Header><m:ID>H41</m:ID></m:Header>'
        f'<frb:DataSet id="H41">{body}</frb:DataSet>'
        f'</m:MessageGroup>'
    ).encode("utf-8")


def zip_bytes(xml_content):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(fetch_fed_h41.DATA_MEMBER, xml_content)
    return buf.getvalue()


# Future dates on purpose: the window must anchor to the newest period present
# in the payload, never to the wall clock.
FIXTURE = (
    series_xml(
        "RES_A.N.WW",
        "Reserves: series A",
        [
            ("A", "100", "2030-01-02"),
            ("A", "150", "2030-01-09"),
            ("A", "200.5", "2030-01-16"),
        ],
    )
    + series_xml(
        "RES_B.N.WW",
        "Reserves: series B",
        [
            ("NA", "-999999", "2030-01-09"),
            ("A", "77", "2030-01-16"),
        ],
    )
    + series_xml(
        "RES_STALE.N.WW",
        "Reserves: stale series",
        [("A", "5", "2030-01-02")],
    )
)

FIXTURE_ZIP = zip_bytes(xml_bytes(FIXTURE))
FETCHED_AT = "2030-01-17T04:38:00Z"


class ParseH41XmlTests(unittest.TestCase):
    def test_window_anchored_to_newest_period_in_payload(self):
        records = list(fetch_fed_h41.parse_h41_xml(xml_bytes(FIXTURE), weeks=2,
                                                   fetched_at=FETCHED_AT))
        got = {(r["series_name"], r["date"]) for r in records}
        # 2-week window from 2030-01-16: 2030-01-09 and 2030-01-16.
        self.assertEqual(
            got,
            {
                ("RES_A.N.WW", "2030-01-09"),
                ("RES_A.N.WW", "2030-01-16"),
                ("RES_B.N.WW", "2030-01-09"),
                ("RES_B.N.WW", "2030-01-16"),
            },
        )

    def test_series_without_recent_observations_is_excluded(self):
        records = list(fetch_fed_h41.parse_h41_xml(xml_bytes(FIXTURE), weeks=2,
                                                   fetched_at=FETCHED_AT))
        self.assertNotIn("RES_STALE.N.WW", {r["series_name"] for r in records})

    def test_na_observation_has_null_value_and_status(self):
        records = list(fetch_fed_h41.parse_h41_xml(xml_bytes(FIXTURE), weeks=2,
                                                   fetched_at=FETCHED_AT))
        na = [r for r in records if r["obs_status"] == "NA"]
        self.assertEqual(len(na), 1)
        self.assertIsNone(na[0]["value"])

    def test_record_shape_and_envelope(self):
        records = list(fetch_fed_h41.parse_h41_xml(xml_bytes(FIXTURE), weeks=2,
                                                   fetched_at=FETCHED_AT))
        rec = next(r for r in records
                   if r["series_name"] == "RES_A.N.WW" and r["date"] == "2030-01-16")
        self.assertEqual(rec["source"], "fed_h41")
        self.assertEqual(rec["fetched_at"], FETCHED_AT)
        self.assertEqual(rec["id"], "RES_A.N.WW:2030-01-16")
        self.assertEqual(rec["series_name"], "RES_A.N.WW")
        self.assertEqual(rec["description"], "Reserves: series A")
        self.assertEqual(rec["value"], 200.5)
        self.assertEqual(rec["obs_status"], "A")
        self.assertEqual(rec["unit"], "Currency")
        self.assertEqual(rec["unit_multiplier"], 1000000)
        self.assertEqual(rec["currency"], "USD")
        self.assertEqual(rec["frequency"], "19")
        self.assertEqual(rec["category"], "LIABCAP")
        self.assertEqual(rec["subcategory"], "OFDRB")
        self.assertEqual(rec["component"], "DORBSC")
        self.assertEqual(rec["distribution"], "TOT")
        self.assertEqual(rec["series_type"], "L")
        integral = next(r for r in records
                        if r["series_name"] == "RES_B.N.WW" and r["date"] == "2030-01-16")
        self.assertEqual(integral["value"], 77)
        self.assertIsInstance(integral["value"], int)

    def test_output_is_deterministic_newest_first(self):
        first = list(fetch_fed_h41.parse_h41_xml(xml_bytes(FIXTURE), weeks=2,
                                                 fetched_at=FETCHED_AT))
        second = list(fetch_fed_h41.parse_h41_xml(xml_bytes(FIXTURE), weeks=2,
                                                  fetched_at=FETCHED_AT))
        self.assertEqual(first, second)
        keys = [(r["date"], r["series_name"]) for r in first]
        self.assertEqual(keys, sorted(keys, reverse=True))

    def test_whole_window_emits_all_periods(self):
        records = list(fetch_fed_h41.parse_h41_xml(xml_bytes(FIXTURE), weeks=13,
                                                   fetched_at=FETCHED_AT))
        got = {(r["series_name"], r["date"]) for r in records}
        self.assertIn(("RES_STALE.N.WW", "2030-01-02"), got)
        self.assertEqual(len(records), 6)

    def test_weeks_bounds_are_enforced(self):
        for bad in (0, 53):
            with self.assertRaises(ValueError):
                list(fetch_fed_h41.parse_h41_xml(xml_bytes(FIXTURE), weeks=bad,
                                                 fetched_at=FETCHED_AT))

    def test_payload_without_observations_raises(self):
        with self.assertRaises(ValueError):
            list(fetch_fed_h41.parse_h41_xml(
                xml_bytes("<kf:Series SERIES_NAME=\"X\" />"), weeks=2,
                fetched_at=FETCHED_AT))

    def test_malformed_xml_raises(self):
        with self.assertRaises(Exception):
            list(fetch_fed_h41.parse_h41_xml(b"<not-xml", weeks=2,
                                             fetched_at=FETCHED_AT))


class ReadDataMemberTests(unittest.TestCase):
    def test_reads_named_member(self):
        self.assertEqual(fetch_fed_h41._read_data_member(FIXTURE_ZIP),
                         xml_bytes(FIXTURE))

    def test_missing_member_raises_key_error(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("other.xml", b"")
        with self.assertRaises(KeyError):
            fetch_fed_h41._read_data_member(buf.getvalue())

    def test_garbage_zip_raises(self):
        with self.assertRaises(zipfile.BadZipFile):
            fetch_fed_h41._read_data_member(b"not a zip")


class MainTests(unittest.TestCase):
    def run_main(self, argv, download_result=None, download_exc=None):
        out, err = io.StringIO(), io.StringIO()
        if download_exc is not None:
            side_effect = download_exc
        else:
            side_effect = lambda timeout=fetch_fed_h41.DEFAULT_TIMEOUT: download_result
        with patch.object(fetch_fed_h41, "download_zip", side_effect=side_effect), \
                redirect_stdout(out), redirect_stderr(err):
            try:
                code = fetch_fed_h41.main(argv)
            except SystemExit as exc:
                code = exc.code
            else:
                code = 0
        return code, out.getvalue(), err.getvalue()

    def test_main_emits_ndjson_envelope_records(self):
        code, out, err = self.run_main(["--weeks", "2"], download_result=FIXTURE_ZIP)
        self.assertEqual(code, 0)
        lines = [json.loads(line) for line in out.splitlines() if line]
        self.assertEqual(len(lines), 4)
        for rec in lines:
            for key in ("source", "fetched_at", "id"):
                self.assertIn(key, rec)
        self.assertIn("4 observations", err)
        self.assertIn("2 series", err)

    def test_main_exits_nonzero_when_download_fails(self):
        code, out, err = self.run_main(
            [], download_exc=urllib.error.URLError("no route"))
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("download failed", err)

    def test_main_exits_nonzero_on_corrupt_payload(self):
        code, out, err = self.run_main(["--weeks", "2"],
                                       download_result=b"not a zip")
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("download failed", err)


if __name__ == "__main__":
    unittest.main()
