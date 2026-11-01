"""Tests for fetch_uk_legislation.py."""

import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
import urllib.error
from datetime import datetime, timezone
from unittest import mock

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_uk_legislation.py"
SPEC = importlib.util.spec_from_file_location("fetch_uk_legislation", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = {} if headers is None else headers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(body):
    return Response(body)


def http_transient(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.FEED_URL, code, "error", headers, io.BytesIO(b"origin failure"))


def entry_xml(id_path, title, published, doc_type, summary=None):
    year, number = id_path.split("/")[1], id_path.split("/")[2]
    parts = [
        '<entry xmlns="http://www.w3.org/2005/Atom" '
        'xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata">',
        f"<id>http://www.legislation.gov.uk/id/{id_path}</id>",
        f"<title>{title}</title>",
        f'<link rel="self" href="http://www.legislation.gov.uk/id/{id_path}" />',
        f'<link href="http://www.legislation.gov.uk/{id_path}/made" />',
        f'<link rel="alternate" type="application/xml" '
        f'href="http://www.legislation.gov.uk/{id_path}/made/data.xml" title="XML" />',
    ]
    if published is not None:
        parts.append(f"<published>{published}</published>")
    if summary is not None:
        parts.append(f"<summary>{summary}</summary>")
    parts.append(f'<ukm:DocumentMainType Value="{doc_type}">{doc_type}</ukm:DocumentMainType>')
    parts.append(f'<ukm:Year Value="{year}">{year}</ukm:Year>')
    parts.append(f'<ukm:Number Value="{number}">{number}</ukm:Number>')
    parts.append("</entry>")
    return "".join(parts)


def feed_xml(entries, next_href=None):
    parts = [
        '<feed xmlns="http://www.w3.org/2005/Atom">',
        "<id>application/atom+xml;type=feed</id>",
        "<title>New Legislation</title>",
        f'<link rel="self" type="application/atom+xml" '
        f'href="{MODULE.FEED_URL}?sort=published&amp;results-count={MODULE.PAGE_SIZE}" />',
    ]
    if next_href is not None:
        parts.append(f'<link rel="next" type="application/atom+xml" href="{next_href.replace("&", "&amp;")}" />')
    parts.extend(entries)
    parts.append("</feed>")
    return "".join(parts).encode("utf-8")


def page_url(page):
    return f"{MODULE.FEED_URL}?sort=published&results-count={MODULE.PAGE_SIZE}&page={page}"


def record(id_path, title, published, doc_type, summary=None, created=None, isbn=None, legislation_id=None, pdf_url=None):
    type_code, year, number = id_path.split("/")
    return {
        "id": id_path,
        "legislation_id": legislation_id or id_path,
        "type": doc_type,
        "type_code": type_code,
        "year": year,
        "number": number,
        "title": title,
        "summary": summary,
        "created": created,
        "isbn": isbn,
        "published_at": published,
        "url": f"http://www.legislation.gov.uk/{id_path}/made",
        "pdf_url": pdf_url,
        "data_xml_url": f"http://www.legislation.gov.uk/{id_path}/made/data.xml",
    }


ITEM_A = record("uksi/2026/1146", "Test SI 2026", "2026-10-30T16:21:25.734273Z", "UnitedKingdomStatutoryInstrument", summary="Preamble A")
ITEM_B = record("ssi/2026/322", "Test SSI 2026", "2026-10-29T10:00:00Z", "ScottishStatutoryInstrument")
ITEM_C = record("uksi/2026/1145", "Test SI 2026b", "2026-10-20T12:00:00Z", "UnitedKingdomStatutoryInstrument")
ITEM_D = record("uki/2026/8", "Test Act 2026", "2026-11-01T06:00:00Z", "UnitedKingdomPublicGeneralAct")

ENTRY_A = entry_xml("uksi/2026/1146", "Test SI 2026", "2026-10-30T16:21:25.734273Z", "UnitedKingdomStatutoryInstrument", summary="Preamble A")
ENTRY_B = entry_xml("ssi/2026/322", "Test SSI 2026", "2026-10-29T10:00:00Z", "ScottishStatutoryInstrument")
ENTRY_C = entry_xml("uksi/2026/1145", "Test SI 2026b", "2026-10-20T12:00:00Z", "UnitedKingdomStatutoryInstrument")
ENTRY_D = entry_xml("uki/2026/8", "Test Act 2026", "2026-11-01T06:00:00Z", "UnitedKingdomPublicGeneralAct")

BAD_ID_ENTRY = (
    '<entry xmlns="http://www.w3.org/2005/Atom" '
    'xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata">'
    "<id>http://www.legislation.gov.uk/id/uksi/2026</id>"
    "<title>Bad id</title>"
    "<published>2026-10-30T16:21:25.734273Z</published>"
    '<ukm:DocumentMainType Value="UnitedKingdomStatutoryInstrument">'
    "UnitedKingdomStatutoryInstrument</ukm:DocumentMainType>"
    "</entry>"
)

class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 1, 7, 38, 0, tzinfo=timezone.utc)


def state_with(known, watermark):
    return {
        "version": MODULE.STATE_VERSION,
        "source": MODULE.SOURCE,
        "watermark": watermark,
        "known": known,
    }


class FetchUkLegislationTests(unittest.TestCase):
    def test_collect_stops_at_floor_without_fetching_the_next_page(self):
        floor = MODULE.parse_timestamp("2026-10-25T00:00:00Z", "test")
        page1 = feed_xml(
            [
                entry_xml("uksi/2026/1144", "Page one new", "2026-10-31T06:00:00Z", "UnitedKingdomStatutoryInstrument"),
                entry_xml("uksi/2026/1143", "Page one recent", "2026-10-30T06:00:00Z", "UnitedKingdomStatutoryInstrument"),
            ],
            next_href=page_url(2),
        )
        page2 = feed_xml(
            [
                entry_xml("uksi/2026/1142", "Page two above floor", "2026-10-29T06:00:00Z", "UnitedKingdomStatutoryInstrument"),
                entry_xml("uksi/2026/1141", "Page two below floor", "2026-10-20T06:00:00Z", "UnitedKingdomStatutoryInstrument"),
            ],
            next_href=page_url(3),
        )
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[response(page1), response(page2)]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            rows = list(MODULE.collect(floor, 9))
        self.assertEqual([record["id"] for record, _published in rows],
                         ["uksi/2026/1144", "uksi/2026/1143", "uksi/2026/1142"])
        self.assertEqual([call.args[0].full_url for call in urlopen.call_args_list],
                         [MODULE.first_page_url(), page_url(2)])
        sleep.assert_not_called()

    def test_collect_stops_without_next_link(self):
        floor = MODULE.parse_timestamp("2026-10-01T00:00:00Z", "test")
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(feed_xml([ENTRY_A, ENTRY_B]))
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            rows = list(MODULE.collect(floor, 9))
        self.assertEqual(len(rows), 2)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    SLIP_ID = "uksi/2026/991/pdfs/uksics_20260991_en_001.pdf"

    def _slip_entry(self, year="2026", number="991"):
        slip_url = f"http://www.legislation.gov.uk/{self.SLIP_ID}"
        return (
            '<entry xmlns="http://www.w3.org/2005/Atom" '
            'xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata">'
            f"<id>{slip_url}</id>"
            "<title>Correction Slip</title>"
            f'<link rel="self" href="{slip_url}" />'
            f'<link href="{slip_url}" />'
            "<published>2026-10-30T00:00:00+01:00</published>"
            '<ukm:DocumentMainType Value="UnitedKingdomStatutoryInstrument">'
            "UnitedKingdomStatutoryInstrument</ukm:DocumentMainType>"
            f'<ukm:Year Value="{year}">{year}</ukm:Year>'
            f'<ukm:Number Value="{number}">{number}</ukm:Number>'
            "</entry>"
        )

    def test_collect_parses_correction_slip_pdf_entries(self):
        floor = MODULE.parse_timestamp("2026-10-01T00:00:00Z", "test")
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(feed_xml([self._slip_entry()]))
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            rows = list(MODULE.collect(floor, 9))
        record_, _published = rows[0]
        slip_url = f"http://www.legislation.gov.uk/{self.SLIP_ID}"
        self.assertEqual(record_["id"], self.SLIP_ID)
        self.assertEqual(record_["legislation_id"], "uksi/2026/991")
        self.assertEqual(record_["published_at"], "2026-10-29T23:00:00Z")
        self.assertEqual(record_["url"], slip_url)
        self.assertEqual(record_["pdf_url"], slip_url)
        self.assertIsNone(record_["data_xml_url"])
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_collect_rejects_slip_with_mismatched_metadata(self):
        floor = MODULE.parse_timestamp("2026-10-01T00:00:00Z", "test")
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(feed_xml([self._slip_entry(year="2025")]))
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "does not match"):
                list(MODULE.collect(floor, 9))
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_collect_retries_the_identical_page_after_502(self):
        floor = MODULE.parse_timestamp("2026-10-01T00:00:00Z", "test")
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[http_transient(502, "2"), response(feed_xml([ENTRY_A]))],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            rows = list(MODULE.collect(floor, 9))
        self.assertEqual([record["id"] for record, _published in rows], ["uksi/2026/1146"])
        self.assertEqual([call.args[0].full_url for call in urlopen.call_args_list],
                         [MODULE.first_page_url(), MODULE.first_page_url()])
        self.assertEqual(sleep.call_args_list, [mock.call(2)])

    def test_collect_stops_after_finite_502_attempts(self):
        floor = MODULE.parse_timestamp("2026-10-01T00:00:00Z", "test")
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[http_transient(502, "1")] * MODULE.MAX_TRANSIENT_ATTEMPTS,
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "retry limit exhausted"):
                list(MODULE.collect(floor, 9))
        self.assertEqual(urlopen.call_count, MODULE.MAX_TRANSIENT_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_TRANSIENT_ATTEMPTS - 1)

    def test_collect_fails_immediately_for_non_retryable_http_errors(self):
        floor = MODULE.parse_timestamp("2026-10-01T00:00:00Z", "test")
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=http_transient(404)
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "HTTP 404"):
                list(MODULE.collect(floor, 9))
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_collect_rejects_malformed_entries_without_retrying(self):
        floor = MODULE.parse_timestamp("2026-10-01T00:00:00Z", "test")
        for entry, message in (
            (entry_xml("uksi/2026/1146", "No date", None, "UnitedKingdomStatutoryInstrument"), "published"),
            (BAD_ID_ENTRY, "legislation identifier"),
        ):
            with self.subTest(entry=entry[:80]), mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=response(feed_xml([entry]))
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                with self.assertRaisesRegex(ValueError, message):
                    list(MODULE.collect(floor, 9))
            self.assertEqual(urlopen.call_count, 1)
            sleep.assert_not_called()

    def test_collect_fails_when_pagination_is_unbounded(self):
        floor = MODULE.parse_timestamp("2026-01-01T00:00:00Z", "test")
        endless = feed_xml(
            [entry_xml("uksi/2026/1", "Loop", "2026-10-31T06:00:00Z", "UnitedKingdomStatutoryInstrument")],
            next_href=page_url(2),
        )
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=lambda *a, **k: response(endless)) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, f"exceeded {MODULE.MAX_PAGES} pages"):
                list(MODULE.collect(floor, 9))
        self.assertEqual(urlopen.call_count, MODULE.MAX_PAGES)
        sleep.assert_not_called()

    def test_main_initial_stateless_run_is_bounded_by_lookback(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(feed_xml([ENTRY_A, ENTRY_B, ENTRY_C]))
        ) as urlopen, mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
            sys, "argv", [str(SCRIPT), "--no-state"]
        ), contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()):
            MODULE.main()
        emitted = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual([entry["id"] for entry in emitted], ["uksi/2026/1146", "ssi/2026/322"])
        for entry in emitted:
            self.assertEqual(entry["source"], MODULE.SOURCE)
            self.assertEqual(entry["fetched_at"], "2026-11-01T07:38:00+00:00")
        self.assertEqual(emitted[0]["published_at"], "2026-10-30T16:21:25.734273Z")
        self.assertEqual(emitted[0]["summary"], "Preamble A")
        self.assertEqual(emitted[1]["summary"], None)
        self.assertEqual(emitted[1]["url"], "http://www.legislation.gov.uk/ssi/2026/322/made")
        self.assertEqual(urlopen.call_count, 1)

    def test_main_stateful_run_skips_known_items_and_advances_state(self):
        known = {ITEM_A["id"]: MODULE.canonical_hash(ITEM_A), ITEM_B["id"]: MODULE.canonical_hash(ITEM_B)}
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            state_path.write_text(json.dumps(state_with(known, "2026-10-29T10:00:00Z")), encoding="utf-8")
            with mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=response(feed_xml([ENTRY_A, ENTRY_B, ENTRY_C]))
            ), mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
                sys, "argv", [str(SCRIPT), "--state-file", str(state_path)]
            ), contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()):
                MODULE.main()
            emitted = [json.loads(line) for line in stdout.getvalue().splitlines()]
            self.assertEqual(emitted, [])
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["watermark"], "2026-10-30T16:21:25.734273Z")
            self.assertEqual(state["boundary_ids"], ["uksi/2026/1146"])
            self.assertEqual(state["known"], known)

    def test_main_emits_new_items_above_the_watermark(self):
        known = {ITEM_A["id"]: MODULE.canonical_hash(ITEM_A), ITEM_B["id"]: MODULE.canonical_hash(ITEM_B)}
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            state_path.write_text(json.dumps(state_with(known, "2026-10-30T16:21:25.734273Z")), encoding="utf-8")
            with mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=response(feed_xml([ENTRY_D, ENTRY_A, ENTRY_B]))
            ), mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
                sys, "argv", [str(SCRIPT), "--state-file", str(state_path)]
            ), contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()):
                MODULE.main()
            emitted = [json.loads(line) for line in stdout.getvalue().splitlines()]
            self.assertEqual([entry["id"] for entry in emitted], ["uki/2026/8"])
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["watermark"], "2026-11-01T06:00:00Z")
            self.assertEqual(state["boundary_ids"], ["uki/2026/8"])
            self.assertEqual(state["known"][ITEM_D["id"]], MODULE.canonical_hash(ITEM_D))

    def test_main_reemits_a_revised_record(self):
        revised = record(
            "uksi/2026/1146",
            "Test SI 2026",
            "2026-10-30T16:21:25.734273Z",
            "UnitedKingdomStatutoryInstrument",
            summary="Revised preamble A",
        )
        known = {ITEM_A["id"]: MODULE.canonical_hash(ITEM_A)}
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            state_path.write_text(json.dumps(state_with(known, "2026-10-29T10:00:00Z")), encoding="utf-8")
            entry = entry_xml(
                "uksi/2026/1146",
                "Test SI 2026",
                "2026-10-30T16:21:25.734273Z",
                "UnitedKingdomStatutoryInstrument",
                summary="Revised preamble A",
            )
            with mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=response(feed_xml([entry]))
            ), mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
                sys, "argv", [str(SCRIPT), "--state-file", str(state_path)]
            ), contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()):
                MODULE.main()
            emitted = [json.loads(line) for line in stdout.getvalue().splitlines()]
            self.assertEqual([entry_["id"] for entry_ in emitted], ["uksi/2026/1146"])
            self.assertEqual(emitted[0]["summary"], "Revised preamble A")
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["known"][ITEM_A["id"]], MODULE.canonical_hash(revised))

    def test_main_does_not_advance_state_after_a_page_failure(self):
        known = {ITEM_A["id"]: MODULE.canonical_hash(ITEM_A)}
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            state_path.write_text(json.dumps(state_with(known, "2026-10-29T10:00:00Z")), encoding="utf-8")
            original_contents = state_path.read_bytes()
            first_page = feed_xml([ENTRY_A, ENTRY_B], next_href=page_url(2))
            side_effect = [response(first_page)] + [http_transient(502, "1")] * MODULE.MAX_TRANSIENT_ATTEMPTS
            with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=side_effect), mock.patch.object(
                MODULE.time, "sleep"
            ), mock.patch.object(sys, "argv", [str(SCRIPT), "--state-file", str(state_path)]), contextlib.redirect_stdout(
                io.StringIO()
            ), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "retry limit exhausted"):
                    MODULE.main()
            self.assertEqual(state_path.read_bytes(), original_contents)


if __name__ == "__main__":
    unittest.main()
