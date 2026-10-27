import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone

import pytest
import yaml

HERE = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "fetch_fed_h10_fx", HERE / "scripts" / "fetch_fed_h10_fx.py"
)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)

NS_M = "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message"
NS_C = "http://www.federalreserve.gov/structure/compact/common"
NS_H = "http://www.federalreserve.gov/structure/compact/H10_H10"
PREPARED = "2026-10-26T15:27:12"
NOW = datetime(2026, 10, 27, 22, 45, tzinfo=timezone.utc)


def daily_series(fx, obs, *, currency=None, unit="Currency", name=None, freq="9", unit_mult="1"):
    return {
        "attrs": {
            "FX": fx,
            "FREQ": freq,
            "CURRENCY": fx if currency is None else currency,
            "UNIT": unit,
            "UNIT_MULT": unit_mult,
            "SERIES_NAME": name or f"RXI_N.B.{fx[:2]}",
        },
        "obs": [
            {"OBS_STATUS": status, "OBS_VALUE": value, "TIME_PERIOD": period}
            for period, value, status in obs
        ],
    }


def make_xml(series, *, test="false", prepared=PREPARED, root_tag=None):
    root = ET.Element(root_tag if root_tag is not None else f"{{{NS_M}}}MessageGroup")
    header = ET.SubElement(root, f"{{{NS_M}}}Header")
    ET.SubElement(header, f"{{{NS_M}}}ID").text = "H10"
    ET.SubElement(header, f"{{{NS_M}}}Test").text = test
    if prepared is not None:
        ET.SubElement(header, f"{{{NS_M}}}Prepared").text = prepared
    dataset = ET.SubElement(root, f"{{{NS_C}}}DataSet", id="H10")
    for series_def in series:
        element = ET.SubElement(dataset, f"{{{NS_H}}}Series", **series_def["attrs"])
        for obs in series_def["obs"]:
            ET.SubElement(element, f"{{{NS_C}}}Obs", **obs)
    return ET.tostring(root, encoding="utf-8")


def make_zip(xml_bytes, members=()):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for member in members:
            archive.writestr(member, b"")
        archive.writestr(module.DATA_MEMBER, xml_bytes)
    return buffer.getvalue()


class ScriptedDownload:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.requests = []

    def __call__(self, url, timeout):
        self.requests.append(url)
        if not self.payloads:
            raise AssertionError(f"unexpected download request {url}")
        return self.payloads.pop(0)


def run_fetcher(download, *, state_file=None, now=NOW, **overrides):
    out = io.StringIO()
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        count = module.run(
            path=state_file, output=out, download=download, now=lambda: now, **overrides
        )
    return count, out.getvalue(), err.getvalue()


def summary_from(stderr_text):
    for line in stderr_text.splitlines():
        if line.startswith(module.SUMMARY_PREFIX):
            return json.loads(line[len(module.SUMMARY_PREFIX):])
    raise AssertionError(f"no run summary in {stderr_text!r}")


def records_from(ndjson_text):
    return [json.loads(line) for line in ndjson_text.splitlines() if line]


JPY = daily_series(
    "JPY",
    [
        ("2026-10-21", "157.00", "A"),
        ("2026-10-22", "157.10", "A"),
        ("2026-10-23", "156.87", "A"),
        ("2026-10-26", "158.00", "A"),
    ],
)
EUR = daily_series(
    "EUR",
    [
        ("2026-10-21", "1.1200", "A"),
        ("2026-10-22", "NA", "ND"),
        ("2026-10-23", "1.1210", "A"),
        ("2026-10-26", "1.1199", "A"),
    ],
)
BRD = daily_series(
    "BRD",
    [
        ("2026-10-21", "121.00", "A"),
        ("2026-10-26", "121.30", "A"),
    ],
    currency="NA",
    unit="Index:_1997_Jan_100",
    name="RXI_N.B.BRD",
)
MONTHLY = daily_series("JPY_M", [("2026-09-30", "156.50", "A")], freq="129")


def baseline_zip():
    return make_zip(make_xml([JPY, EUR, BRD, MONTHLY]))


@pytest.fixture()
def state_file(tmp_path):
    return tmp_path / "state.json"


class TestBaseline:
    def test_full_history_first_run(self, state_file):
        download = ScriptedDownload([baseline_zip()])
        count, ndjson, stderr = run_fetcher(download, state_file=state_file)

        assert download.requests == [module.DATA_URL]
        assert count == 9  # JPY 4 + EUR 3 (ND skipped) + BRD 2; monthly excluded
        records = records_from(ndjson)
        assert [record["fx_code"] for record in records] == [
            "BRD", "BRD", "EUR", "EUR", "EUR", "JPY", "JPY", "JPY", "JPY",
        ]
        jpy = records[5]
        assert jpy == {
            "source": module.SOURCE,
            "fetched_at": "2026-10-27T22:45:00Z",
            "id": "fed_h10_fx|JPY|2026-10-21",
            "date": "2026-10-21",
            "fx_code": "JPY",
            "kind": "currency",
            "currency_iso": "JPY",
            "unit": "Currency",
            "value": 157.0,
            "series_name": "RXI_N.B.JP",
        }
        brd = records[0]
        assert brd["kind"] == "index"
        assert brd["currency_iso"] is None
        assert brd["unit"] == "Index:_1997_Jan_100"

        state = json.loads(state_file.read_text(encoding="utf-8"))
        assert state == {
            "version": 1,
            "source": module.SOURCE,
            "series": {"JPY": "2026-10-26", "EUR": "2026-10-26", "BRD": "2026-10-26"},
            "prepared": PREPARED,
        }
        summary = summary_from(stderr)
        assert summary["health"] == "healthy"
        assert summary["completeness"] == "complete"
        assert summary["requests"] == {"attempted": 1, "succeeded": 1}
        assert summary["state_change"] == {"JPY": "2026-10-26", "EUR": "2026-10-26", "BRD": "2026-10-26"}
        metrics = summary["metrics"]
        assert metrics["series"] == 3
        assert metrics["records"] == 9
        assert metrics["baseline"] is True
        assert metrics["unchanged"] is False
        assert metrics["nd_skipped"] == 1
        assert metrics["new_max_date"] == "2026-10-26"
        assert metrics["prepared"] == PREPARED


class TestIncremental:
    def test_new_release_re_emits_revision_window(self, state_file):
        # State from a run whose file ended 2026-10-23 with an older Prepared
        # stamp: the release moved on, so the 14-day window is re-emitted.
        state_file.write_text(json.dumps({
            "version": 1,
            "source": module.SOURCE,
            "series": {"JPY": "2026-10-23", "EUR": "2026-10-23", "BRD": "2026-10-23"},
            "prepared": "2026-10-23T15:20:44",
        }), encoding="utf-8")
        download = ScriptedDownload([baseline_zip()])
        count, ndjson, stderr = run_fetcher(download, state_file=state_file)

        records = records_from(ndjson)
        assert count == 9
        assert {record["date"] for record in records if record["fx_code"] == "JPY"} == {
            "2026-10-21", "2026-10-22", "2026-10-23", "2026-10-26",
        }
        summary = summary_from(stderr)
        assert summary["state_change"] == {"JPY": "2026-10-26", "EUR": "2026-10-26", "BRD": "2026-10-26"}
        assert summary["metrics"]["baseline"] is False
        assert summary["metrics"]["unchanged"] is False
        assert summary["metrics"]["nd_skipped"] == 1

    def test_new_series_added_by_source_gets_full_history(self, state_file):
        state_file.write_text(json.dumps({
            "version": 1,
            "source": module.SOURCE,
            "series": {"JPY": "2026-10-23"},
            "prepared": "2026-10-23T15:20:44",
        }), encoding="utf-8")
        download = ScriptedDownload([baseline_zip()])
        count, ndjson, stderr = run_fetcher(download, state_file=state_file)

        records = records_from(ndjson)
        eur = [record for record in records if record["fx_code"] == "EUR"]
        jpy = [record for record in records if record["fx_code"] == "JPY"]
        assert [record["date"] for record in eur] == [
            "2026-10-21", "2026-10-23", "2026-10-26",
        ]
        assert [record["date"] for record in jpy] == [
            "2026-10-21", "2026-10-22", "2026-10-23", "2026-10-26",
        ]
        assert count == 9
        state = json.loads(state_file.read_text(encoding="utf-8"))
        assert state["prepared"] == PREPARED
        assert state["series"]["EUR"] == "2026-10-26"
        assert summary_from(stderr)["metrics"]["baseline"] is True

    def test_restated_value_re_emitted_with_new_value(self, state_file):
        state_file.write_text(json.dumps({
            "version": 1,
            "source": module.SOURCE,
            "series": {"JPY": "2026-10-26"},
            "prepared": "2026-10-26T15:27:11",
        }), encoding="utf-8")
        restated = daily_series(
            "JPY",
            [
                ("2026-10-21", "156.50", "A"),  # restated from 157.00
                ("2026-10-22", "157.10", "A"),
                ("2026-10-23", "156.87", "A"),
                ("2026-10-26", "158.00", "A"),
            ],
        )
        download = ScriptedDownload([make_zip(make_xml([restated], prepared=PREPARED))])
        count, ndjson, stderr = run_fetcher(download, state_file=state_file)

        records = records_from(ndjson)
        by_date = {record["date"]: record["value"] for record in records}
        assert by_date["2026-10-21"] == 156.5
        assert count == 4
        summary = summary_from(stderr)
        # Watermark did not move, so no series state change is recorded.
        assert summary["state_change"] == {}
        assert summary["metrics"]["prepared"] == PREPARED

    def test_series_removed_from_release_is_dropped_from_state(self, state_file):
        state_file.write_text(json.dumps({
            "version": 1,
            "source": module.SOURCE,
            "series": {"JPY": "2026-10-23", "EUR": "2026-10-23", "GONE": "2026-10-23"},
            "prepared": "2026-10-23T15:20:44",
        }), encoding="utf-8")
        download = ScriptedDownload([make_zip(make_xml([JPY]))])
        count, _, stderr = run_fetcher(download, state_file=state_file)
        assert count == 4
        state = json.loads(state_file.read_text(encoding="utf-8"))
        assert state["series"] == {"JPY": "2026-10-26"}
        assert "no longer present" in stderr


class TestUpToDate:
    def test_same_prepared_stamp_is_a_noop(self, state_file):
        state_file.write_text(json.dumps({
            "version": 1,
            "source": module.SOURCE,
            "series": {"JPY": "2026-10-26", "EUR": "2026-10-26", "BRD": "2026-10-26"},
            "prepared": PREPARED,
        }), encoding="utf-8")
        before = state_file.read_bytes()
        download = ScriptedDownload([baseline_zip()])
        count, ndjson, stderr = run_fetcher(download, state_file=state_file)

        assert download.requests == [module.DATA_URL]  # must fetch to know it is unchanged
        assert count == 0
        assert ndjson == ""
        assert state_file.read_bytes() == before  # untouched
        summary = summary_from(stderr)
        assert summary["state_change"] == {}
        assert summary["metrics"]["unchanged"] is True
        assert summary["metrics"]["records"] == 0
        assert summary["metrics"]["new_max_date"] is None
        assert summary["metrics"]["nd_skipped"] == 0


class TestFullAndNoState:
    def test_no_state_emits_everything_without_writing_state(self):
        download = ScriptedDownload([baseline_zip()])
        count, _, stderr = run_fetcher(download)
        assert count == 9
        # No state means every series is new, i.e. a full-history baseline run.
        assert summary_from(stderr)["metrics"]["baseline"] is True

    def test_full_flag_re_emits_history_despite_state(self, state_file):
        state_file.write_text(json.dumps({
            "version": 1,
            "source": module.SOURCE,
            "series": {"JPY": "2026-10-26", "EUR": "2026-10-26", "BRD": "2026-10-26"},
            "prepared": PREPARED,
        }), encoding="utf-8")
        download = ScriptedDownload([baseline_zip()])
        count, ndjson, stderr = run_fetcher(download, state_file=state_file, full=True)
        assert count == 9
        assert summary_from(stderr)["metrics"]["baseline"] is True


class TestBounds:
    def test_revision_window_bounds(self, state_file):
        download = ScriptedDownload([baseline_zip()])
        run_fetcher(download, state_file=state_file)
        download = ScriptedDownload([baseline_zip()])
        with pytest.raises(ValueError, match="revision window"):
            run_fetcher(download, state_file=state_file, revision_days=-1)
        download = ScriptedDownload([baseline_zip()])
        with pytest.raises(ValueError, match="revision window"):
            run_fetcher(download, state_file=state_file, revision_days=module.MAX_REVISION_DAYS + 1)

    def test_timeout_bound(self, state_file):
        download = ScriptedDownload([baseline_zip()])
        with pytest.raises(ValueError, match="timeout"):
            run_fetcher(download, state_file=state_file, timeout=0)

    def test_revision_days_zero_emits_only_new_dates(self, state_file):
        state_file.write_text(json.dumps({
            "version": 1,
            "source": module.SOURCE,
            "series": {"JPY": "2026-10-23"},
            "prepared": "2026-10-23T15:20:44",
        }), encoding="utf-8")
        download = ScriptedDownload([make_zip(make_xml([JPY]))])
        count, ndjson, _ = run_fetcher(download, state_file=state_file, revision_days=0)
        records = records_from(ndjson)
        assert count == 1
        assert records[0]["date"] == "2026-10-26"


class TestValidation:
    @pytest.mark.parametrize(
        "xml_kwargs",
        [
            {"test": "true"},
            {"prepared": "garbage"},
            {"prepared": None},
        ],
        ids=["test-data", "bad-prepared", "missing-prepared"],
    )
    def test_header_rejections(self, state_file, xml_kwargs):
        download = ScriptedDownload([make_zip(make_xml([JPY], **xml_kwargs))])
        with pytest.raises(ValueError):
            run_fetcher(download, state_file=state_file)
        assert not state_file.exists()

    def test_missing_member(self, state_file):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("other.xml", b"<x/>")
        download = ScriptedDownload([buffer.getvalue()])
        with pytest.raises(ValueError, match="missing"):
            run_fetcher(download, state_file=state_file)

    def test_not_a_zip(self, state_file):
        download = ScriptedDownload([b"not a zip"])
        with pytest.raises(ValueError, match="zip"):
            run_fetcher(download, state_file=state_file)

    def test_malformed_xml(self, state_file):
        download = ScriptedDownload([make_zip(b"<broken")])
        with pytest.raises(ValueError, match="XML"):
            run_fetcher(download, state_file=state_file)

    def test_wrong_root(self, state_file):
        download = ScriptedDownload([
            make_zip(make_xml([JPY], root_tag=f"{{{NS_M}}}SomethingElse"))
        ])
        with pytest.raises(ValueError, match="MessageGroup"):
            run_fetcher(download, state_file=state_file)

    def test_missing_dataset(self, state_file):
        root = ET.Element(f"{{{NS_M}}}MessageGroup")
        header = ET.SubElement(root, f"{{{NS_M}}}Header")
        ET.SubElement(header, f"{{{NS_M}}}ID").text = "H10"
        ET.SubElement(header, f"{{{NS_M}}}Test").text = "false"
        ET.SubElement(header, f"{{{NS_M}}}Prepared").text = PREPARED
        download = ScriptedDownload([make_zip(ET.tostring(root, encoding="utf-8"))])
        with pytest.raises(ValueError, match="DataSet"):
            run_fetcher(download, state_file=state_file)

    def test_no_daily_series(self, state_file):
        download = ScriptedDownload([make_zip(make_xml([MONTHLY]))])
        with pytest.raises(ValueError, match="daily"):
            run_fetcher(download, state_file=state_file)

    @pytest.mark.parametrize(
        "obs",
        [
            [("2026-10-22", "157.00", "A"), ("2026-10-21", "157.10", "A")],
            [("2026-10-21", "157.00", "A"), ("2026-10-21", "157.10", "A")],
        ],
        ids=["out-of-order", "duplicate-date"],
    )
    def test_non_ascending_periods(self, state_file, obs):
        download = ScriptedDownload([make_zip(make_xml([daily_series("JPY", obs)]))])
        with pytest.raises(ValueError, match="ascending"):
            run_fetcher(download, state_file=state_file)

    @pytest.mark.parametrize(
        "value", ["-1.0", "1e3", "1.2.3", "NA", ""],
        ids=["negative", "exponent", "double-dot", "na", "empty"],
    )
    def test_invalid_values(self, state_file, value):
        obs = [("2026-10-21", value, "A")]
        download = ScriptedDownload([make_zip(make_xml([daily_series("JPY", obs)]))])
        with pytest.raises(ValueError, match="OBS_VALUE"):
            run_fetcher(download, state_file=state_file)

    def test_unknown_status(self, state_file):
        obs = [("2026-10-21", "157.00", "E")]
        download = ScriptedDownload([make_zip(make_xml([daily_series("JPY", obs)]))])
        with pytest.raises(ValueError, match="status"):
            run_fetcher(download, state_file=state_file)

    def test_all_nd_series_rejected(self, state_file):
        obs = [("2026-10-21", "NA", "ND"), ("2026-10-22", "NA", "ND")]
        download = ScriptedDownload([make_zip(make_xml([daily_series("JPY", obs)]))])
        with pytest.raises(ValueError, match="no usable observations"):
            run_fetcher(download, state_file=state_file)

    def test_duplicate_fx_codes(self, state_file):
        download = ScriptedDownload([
            make_zip(make_xml([JPY, daily_series("JPY", [("2026-10-25", "1", "A")])]))
        ])
        with pytest.raises(ValueError, match="duplicate"):
            run_fetcher(download, state_file=state_file)

    def test_unsupported_unit_mult(self, state_file):
        download = ScriptedDownload([
            make_zip(make_xml([daily_series("JPY", [("2026-10-21", "157.00", "A")], unit_mult="100")]))
        ])
        with pytest.raises(ValueError, match="UNIT_MULT"):
            run_fetcher(download, state_file=state_file)


class TestStateFile:
    @pytest.mark.parametrize(
        ("document", "match"),
        [
            ({"version": 2, "source": module.SOURCE, "series": {}, "prepared": None}, "version"),
            ({"version": 1, "source": "other", "series": {}, "prepared": None}, "source"),
            (
                {"version": 1, "source": module.SOURCE, "series": {}, "prepared": None, "extra": 1},
                "unexpected fields",
            ),
            (
                {"version": 1, "source": module.SOURCE, "series": {"JPY": "not-a-date"}, "prepared": None},
                "YYYY-MM-DD",
            ),
            (
                {"version": 1, "source": module.SOURCE, "series": {}, "prepared": "garbage"},
                "prepared",
            ),
            ("not a dict", "expected object"),
        ],
    )
    def test_malformed_state_rejected(self, state_file, document, match):
        state_file.write_text(json.dumps(document), encoding="utf-8")
        download = ScriptedDownload([baseline_zip()])
        with pytest.raises(ValueError, match=match):
            run_fetcher(download, state_file=state_file)

    def test_corrupt_json_rejected(self, state_file):
        state_file.write_text("{not json", encoding="utf-8")
        download = ScriptedDownload([baseline_zip()])
        with pytest.raises(ValueError, match="cannot read state"):
            run_fetcher(download, state_file=state_file)


class TestCli:
    def test_mutually_exclusive_state_flags(self, capsys):
        with pytest.raises(SystemExit):
            module.build_parser().parse_args(["--no-state", "--state-file", "x.json"])

    def test_cli_args_bounds(self, capsys):
        with pytest.raises(SystemExit):
            module.build_parser().parse_args(["--revision-days", str(module.MAX_REVISION_DAYS + 1)])
        with pytest.raises(SystemExit):
            module.build_parser().parse_args(["--timeout", "0"])
        # argparse type=int rejects non-numeric values too.
        with pytest.raises(SystemExit):
            module.build_parser().parse_args(["--revision-days", "many"])


class TestUrlPinning:
    def test_download_rejects_non_fed_url(self):
        with pytest.raises(ValueError, match="documented"):
            module.download_data("https://example.com/releases/h10/data/FRB_h10_xml.zip", 5)

    def test_download_rejects_non_data_path(self):
        with pytest.raises(ValueError, match="documented"):
            module.download_data("https://www.federalreserve.gov/releases/h10/index.html", 5)


class TestConfig:
    def test_source_yaml(self):
        config = yaml.safe_load((HERE / "sources" / "fed_h10_fx.yml").read_text(encoding="utf-8"))
        assert config["script"] == "fetch_fed_h10_fx.py"
        assert len(config["schedule"].split()) == 5
        assert config["enabled"] is True
        assert config["attribution_required"] is True
        assert "licence" in config and "attribution" in config
