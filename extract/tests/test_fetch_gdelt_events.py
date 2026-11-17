"""Behavioral tests for fetch_gdelt_events.py (offline, scripted transport)."""

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "fetch_gdelt_events.py"
SOURCE_CONFIG = Path(__file__).resolve().parent.parent / "sources" / "gdelt_events.yml"
SPEC = importlib.util.spec_from_file_location("fetch_gdelt_events", SCRIPT)
gdelt = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(gdelt)

STAMP = "20261117053000"
PREV_STAMP = "20261117051500"
URL = f"https://data.gdeltproject.org/gdeltv2/{STAMP}.export.CSV.zip"


def make_row(
    event_id: str = "1333513208",
    day: str = "20261117",
    month_year: str = "202611",
    year: str = "2026",
    date_added: str = "20261117053000",
    source_url: str = "https://example.com/article-1",
) -> str:
    fields = [""] * len(gdelt.COLUMNS)
    values = {
        "global_event_id": event_id,
        "day": day,
        "month_year": month_year,
        "year": year,
        "fraction_date": "2026-11-17T05:30:00Z",
        "actor1_code": "ACT1",
        "actor1_name": "Example Government",
        "actor1_country_code": "USA",
        "actor1_geo_type": "3",
        "actor1_geo_fullname": "New York",
        "actor1_geo_lat": "40.71",
        "actor1_geo_long": "-74.00",
        "is_root_event": "1",
        "event_code": "040",
        "event_base_code": "04",
        "event_root_code": "04",
        "quad_class": "1",
        "goldstein_scale": "1.0",
        "num_mentions": "3",
        "num_sources": "2",
        "num_articles": "2",
        "avg_tone": "-1.25",
        "date_added": date_added,
        "source_url": source_url,
    }
    for name, value in values.items():
        fields[gdelt.COLUMNS.index(name)] = value
    return "\t".join(fields)


def make_lastupdate(urls) -> str:
    lines = []
    for size, md5, url in urls:
        lines.append(f"{size} {md5} {url}")
    return "\n".join(lines) + "\n"


def make_zip(csv_text: str, member: str = f"{STAMP}.export.CSV") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, csv_text)
    return buffer.getvalue()


class ScriptedTransport:
    """Serve the scripted lastupdate.txt and zip, in request order."""

    def __init__(self, lastupdate: str, zipdata: bytes | None = None,
                 zip_raises: Exception | None = None):
        self.lastupdate = lastupdate
        self.zipdata = zipdata
        self.zip_raises = zip_raises
        self.requests: list[str] = []

    def __call__(self, request, timeout):
        url = request.full_url
        self.requests.append(url)
        if url.endswith("lastupdate.txt"):
            return _BytesResponse(self.lastupdate.encode("utf-8"))
        if self.zip_raises is not None:
            raise self.zip_raises
        assert self.zipdata is not None, "zip was requested but not scripted"
        return _BytesResponse(self.zipdata)


class _BytesResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class GdeltEventsTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_file = Path(self._tmp.name) / "state" / "gdelt_events.json"

    def write_state(self, stamp: str | None):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(
            json.dumps({"version": 1, "source": "gdelt_events", "last_file_stamp": stamp}),
            encoding="utf-8",
        )

    def run_fetcher(self, transport: ScriptedTransport, *argv: str):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(gdelt.urllib.request, "urlopen", transport):
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = gdelt.run(list(argv) or [f"--state-file={self.state_file}"])
        return code, out.getvalue(), err.getvalue(), transport

    def parsed_summary(self, stderr_text: str) -> dict:
        lines = [line for line in stderr_text.splitlines() if line.startswith(gdelt.SUMMARY_PREFIX)]
        self.assertEqual(len(lines), 1)
        return json.loads(lines[0][len(gdelt.SUMMARY_PREFIX):])

    # -- happy paths -------------------------------------------------------

    def test_first_run_ingests_newest_file(self):
        transport = ScriptedTransport(
            make_lastupdate([
                ("45123", "abc123", URL),
                ("888", "def456", URL.replace(".export.CSV.zip", ".mentions.CSV.zip")),
                ("999", "ghi789", URL.replace(".export.CSV.zip", ".gkg.csv.zip")),
            ]),
            zipdata=make_zip(make_row() + "\n" + make_row(event_id="1333513209") + "\n"),
        )
        code, out, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 0)
        records = [json.loads(line) for line in out.splitlines()]
        self.assertEqual(len(records), 2)
        for record in records:
            self.assertEqual(record["source"], "gdelt_events")
            self.assertTrue(record["fetched_at"])
            self.assertIn(record["id"], ("1333513208", "1333513209"))
        first = records[0]
        self.assertEqual(first["event_root_code"], "04")
        self.assertEqual(first["goldstein_scale"], "1.0")
        self.assertEqual(first["source_url"], "https://example.com/article-1")
        summary = self.parsed_summary(err)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["records"], 2)
        self.assertEqual(summary["metrics"]["file_stamp"], STAMP)
        self.assertEqual(summary["metrics"]["gap_minutes"], 0)
        self.assertEqual(transport.requests, [
            "https://data.gdeltproject.org/gdeltv2/lastupdate.txt", URL])
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_file_stamp"], STAMP)

    def test_picks_highest_stamp_when_lastupdate_lists_several(self):
        later = f"https://data.gdeltproject.org/gdeltv2/20261117054500.export.CSV.zip"
        transport = ScriptedTransport(
            make_lastupdate([
                ("45123", "abc123", URL),
                ("50000", "zzz999", later),
            ]),
            zipdata=make_zip(make_row(date_added="20261117054500") + "\n",
                             member="20261117054500.export.CSV"),
        )
        code, out, err, transport = self.run_fetcher(transport)
        self.assertEqual(code, 0)
        self.assertEqual(self.parsed_summary(err)["metrics"]["file_stamp"], "20261117054500")
        self.assertEqual(len(out.splitlines()), 1)

    def test_skip_when_newest_file_already_processed(self):
        self.write_state(STAMP)
        before = self.state_file.read_text(encoding="utf-8")
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]), zipdata=make_zip(make_row() + "\n"))
        code, out, err, transport = self.run_fetcher(transport)
        self.assertEqual(code, 0)
        self.assertEqual(out, "")
        summary = self.parsed_summary(err)
        self.assertEqual(summary["records"], 0)
        self.assertEqual(summary["completeness"], "complete")
        self.assertTrue(summary["metrics"]["skipped"])
        self.assertEqual(transport.requests, [
            "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"])
        self.assertEqual(self.state_file.read_text(encoding="utf-8"), before)

    def test_gap_after_outage_is_flagged_partial(self):
        self.write_state(PREV_STAMP)  # newest is 30 minutes after the checkpoint: the 05:30 file was missed
        gap_url = f"https://data.gdeltproject.org/gdeltv2/20261117054500.export.CSV.zip"
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", gap_url)]),
            zipdata=make_zip(make_row(date_added="20261117054500") + "\n",
                             member="20261117054500.export.CSV"),
        )
        code, out, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 0)
        summary = self.parsed_summary(err)
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["metrics"]["gap_minutes"], 15)
        self.assertEqual(summary["metrics"]["file_stamp"], "20261117054500")
        self.assertEqual(len(out.splitlines()), 1)

    def test_adjacent_file_has_no_gap(self):
        self.write_state(PREV_STAMP)
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]), zipdata=make_zip(make_row() + "\n"))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 0)
        summary = self.parsed_summary(err)
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["metrics"]["gap_minutes"], 0)

    def test_no_state_is_one_shot(self):
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]), zipdata=make_zip(make_row() + "\n"))
        code, out, err, _ = self.run_fetcher(transport, "--no-state")
        self.assertEqual(code, 0)
        self.assertEqual(len(out.splitlines()), 1)
        self.assertFalse(self.state_file.exists())

    def test_empty_file_is_degraded_but_advances_state(self):
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]), zipdata=make_zip(""))
        code, out, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 0)
        self.assertEqual(out, "")
        summary = self.parsed_summary(err)
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["records"], 0)
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_file_stamp"], STAMP)

    # -- malformed upstream content ----------------------------------------

    def test_row_with_wrong_field_count_fails(self):
        bad = make_row().split("\t")[:-1]
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]),
            zipdata=make_zip("\t".join(bad) + "\n"))
        code, out, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        summary = self.parsed_summary(err)
        self.assertEqual(summary["health"], "failed")
        self.assertIn("61 tab-separated fields", summary["error"])
        self.assertFalse(self.state_file.exists())

    def test_non_numeric_global_event_id_fails(self):
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]),
            zipdata=make_zip(make_row(event_id="NOTANID") + "\n"))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn("global_event_id", self.parsed_summary(err)["error"])

    def test_bad_day_field_fails(self):
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]),
            zipdata=make_zip(make_row(day="2026-11-17") + "\n"))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn("field day", self.parsed_summary(err)["error"])

    def test_non_url_source_url_fails(self):
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]),
            zipdata=make_zip(make_row(source_url="not a url") + "\n"))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn("source_url", self.parsed_summary(err)["error"])

    def test_lastupdate_without_export_line_fails(self):
        transport = ScriptedTransport(
            make_lastupdate([("888", "def456", URL.replace(".export", ".mentions"))]),
            zipdata=make_zip(""))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn("no .export.CSV.zip", self.parsed_summary(err)["error"])

    def test_lastupdate_with_foreign_host_fails(self):
        foreign = URL.replace("data.gdeltproject.org", "mirror.example.org")
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", foreign)]), zipdata=make_zip(""))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn("no .export.CSV.zip", self.parsed_summary(err)["error"])

    def test_zip_without_export_member_fails(self):
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]),
            zipdata=make_zip("a,b\n", member="other.csv"))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn(".export.CSV member", self.parsed_summary(err)["error"])

    def test_corrupt_zip_fails(self):
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]), zipdata=b"not a zip at all")
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn("not a valid zip archive", self.parsed_summary(err)["error"])

    def test_http_error_on_zip_fails(self):
        import urllib.error
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]),
            zip_raises=urllib.error.HTTPError(URL, 404, "Gone", {}, None))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn("HTTP 404", self.parsed_summary(err)["error"])
        self.assertFalse(self.state_file.exists())

    def test_row_cap_is_enforced(self):
        rows = "\n".join(
            make_row(event_id=str(10**6 + i)) for i in range(gdelt.MAX_ROWS_PER_FILE + 1))
        transport = ScriptedTransport(
            make_lastupdate([("45123", "abc123", URL)]),
            zipdata=make_zip(rows + "\n"))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn(str(gdelt.MAX_ROWS_PER_FILE), self.parsed_summary(err)["error"])
        self.assertFalse(self.state_file.exists())

    # -- state and arguments -------------------------------------------------

    def test_malformed_state_is_rejected(self):
        self.write_state(None)
        self.state_file.write_text(json.dumps({"version": 9, "source": "gdelt_events",
                                               "last_file_stamp": None}), encoding="utf-8")
        transport = ScriptedTransport(make_lastupdate([("45123", "abc123", URL)]),
                                      zipdata=make_zip(""))
        code, _, err, _ = self.run_fetcher(transport)
        self.assertEqual(code, 1)
        self.assertIn("incompatible version", self.parsed_summary(err)["error"])

    def test_no_state_and_state_file_are_mutually_exclusive(self):
        transport = ScriptedTransport(make_lastupdate([("45123", "abc123", URL)]),
                                      zipdata=make_zip(""))
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(gdelt.urllib.request, "urlopen", transport):
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit):
                    gdelt.run(["--no-state", f"--state-file={self.state_file}"])

    def test_source_config_references_this_fetcher(self):
        import yaml
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], gdelt.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertEqual(len(config["schedule"].split()), 5)
        self.assertTrue(config["enabled"])
        self.assertTrue(config.get("licence"))
        self.assertTrue(config.get("attribution"))


if __name__ == "__main__":
    unittest.main()
