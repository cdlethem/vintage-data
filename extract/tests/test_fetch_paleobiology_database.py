import contextlib
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import threading
import unittest
import urllib.parse

import yaml


SCRIPT = Path(__file__).parents[1] / "scripts" / "fetch_paleobiology_database.py"
CONFIG = Path(__file__).parents[1] / "sources" / "paleobiology_database.yml"
SPEC = importlib.util.spec_from_file_location("fetch_paleobiology_database", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
RUN_START = datetime(2026, 9, 17, 12, 34, 56, tzinfo=timezone.utc)


def occurrence(number, **fields):
    row = {
        "occurrence_no": str(number),
        "accepted_name": "Tyrannosaurus rex",
        "accepted_rank": "species",
        "lat": "47.5000",
        "lng": "-101.2500",
        "cc": "US",
        "state": "North Dakota",
        "paleolat": "44.1000",
        "paleolng": "-92.3000",
        "formation": "Hell Creek",
        "environment": "fluvial indet.",
        "reference_no": "1234",
        "created": "2026-09-14 10:00:00",
        "modified": "2026-09-17 08:00:00",
    }
    row.update(fields)
    return row


def page(rows, *, found=None, returned=None, **extra):
    document = {
        "records": rows,
        "records_found": len(rows) if found is None else found,
        "records_returned": len(rows) if returned is None else returned,
    }
    document.update(extra)
    return document


class LocalFixture:
    """Sequential loopback HTTP fixture; no external name resolution is possible."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.server = None
        self.thread = None

    def __enter__(self):
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                fixture.requests.append(self.path)
                if not fixture.responses:
                    self.send_error(500, "unexpected fixture request")
                    return
                response = fixture.responses.pop(0)
                status, content_type, body = response if isinstance(response, tuple) else (
                    200,
                    "application/json",
                    json.dumps(response).encode("utf-8"),
                )
                if isinstance(body, str):
                    body = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    @property
    def url(self):
        host, port = self.server.server_address
        return f"http://{host}:{port}/data1.2/occs/list.json"

    def __exit__(self, exc_type, exc_value, traceback):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def fetch(fixture, **overrides):
    arguments = {
        "lookback_hours": 24,
        "page_size": 2,
        "max_records": 10,
        "timeout": 5,
        "run_start": RUN_START,
        "api_url": fixture.url,
    }
    arguments.update(overrides)
    return MODULE.fetch_occurrences(**arguments)


def run_cli(url, *arguments, require_success=False):
    child = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--api-url",
            url,
            "--timeout",
            "5",
            *arguments,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=10,
        check=False,
    )
    if require_success and child.returncode != 0:
        # Preserve status and bounded, redacted stderr without reflecting arbitrary
        # upstream bodies or authenticated URLs into a test-runner diagnostic.
        diagnostic = MODULE._sanitize_diagnostic(child.stderr)[:500]
        raise AssertionError(
            f"extractor child failed: status={child.returncode} stderr={diagnostic!r}"
        )
    return child


class PaleobiologyDatabaseTests(unittest.TestCase):
    def test_multiple_pages_use_exact_offsets_fixed_window_and_stable_envelopes(self):
        first_rows = [occurrence(11), occurrence(12, accepted_name="Triceratops horridus")]
        second_rows = [occurrence(20, formation="Lance")]
        with LocalFixture(
            [page(first_rows, found=3), page(second_rows, found=3)]
        ) as fixture:
            records = fetch(fixture)

        self.assertEqual([record["id"] for record in records], ["11", "12", "20"])
        self.assertEqual([record["occurrence_no"] for record in records], [11, 12, 20])
        self.assertEqual({record["fetched_at"] for record in records}, {"2026-09-17T12:34:56+00:00"})
        self.assertEqual(records[0]["record"], first_rows[0])
        self.assertEqual(records[2]["record"]["formation"], "Lance")
        self.assertEqual(
            records[0]["query_window"],
            {
                "modified_after": "2026-09-14 12:34:56",
                "modified_before": "2026-09-17 12:34:56",
                "lookback_hours": 24,
                "overlap_hours": 48,
                "vocab": "pbdb",
                "order": "occurrence_no",
            },
        )

        queries = [
            urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
            for path in fixture.requests
        ]
        expected_common = {
            "all_records": ["1"],
            "modified_after": ["2026-09-14 12:34:56"],
            "modified_before": ["2026-09-17 12:34:56"],
            "vocab": ["pbdb"],
            "show": [MODULE.SHOW_FIELDS],
            "order": ["occurrence_no"],
            "limit": ["2"],
        }
        self.assertEqual(queries, [
            {**expected_common, "offset": ["0"]},
            {**expected_common, "offset": ["2"]},
        ])

    def test_id_is_stable_across_fetch_times_and_upstream_changes(self):
        with LocalFixture([page([occurrence(91, formation="A")])]) as first:
            first_record = fetch(first, page_size=10)[0]
        with LocalFixture([page([occurrence(91, formation="B")])]) as second:
            second_record = fetch(
                second,
                page_size=10,
                run_start=datetime(2026, 9, 18, tzinfo=timezone.utc),
            )[0]

        self.assertEqual(first_record["id"], second_record["id"])
        self.assertNotEqual(first_record["fetched_at"], second_record["fetched_at"])
        self.assertNotEqual(first_record["record"], second_record["record"])

    def test_empty_result_is_valid_and_cli_emits_no_lines(self):
        with LocalFixture([page([], found=0)]) as fixture:
            child = run_cli(
                fixture.url,
                "--lookback-hours",
                "24",
                "--page-size",
                "2",
                "--max-records",
                "10",
                require_success=True,
            )
        self.assertEqual(child.returncode, 0)
        self.assertEqual(child.stdout, "")
        self.assertEqual(child.stderr, "")

    def test_cli_emits_ndjson_envelopes(self):
        with LocalFixture([page([occurrence(7)], found=1)]) as fixture:
            child = run_cli(
                fixture.url,
                "--page-size",
                "2",
                "--max-records",
                "10",
                require_success=True,
            )
        lines = child.stdout.splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["id"], "7")

    def test_rejects_missing_or_noncanonical_identifiers(self):
        bad_rows = ({}, {"occurrence_no": ""}, {"occurrence_no": "001"}, {"occurrence_no": 0})
        for row in bad_rows:
            with self.subTest(row=row), LocalFixture([page([row])]) as fixture:
                with self.assertRaisesRegex(MODULE.ExtractorError, "canonical occurrence_no") as caught:
                    fetch(fixture)
                self.assertEqual(caught.exception.category, "missing_identifier")

    def test_rejects_duplicate_out_of_order_and_changing_pagination(self):
        cases = (
            (
                [page([occurrence(1), occurrence(2)], found=3), page([occurrence(2)], found=3)],
                "duplicate occurrence_no",
            ),
            ([page([occurrence(2), occurrence(1)], found=2)], "not in occurrence_no order"),
            (
                [page([occurrence(1), occurrence(2)], found=3), page([occurrence(3)], found=4)],
                "records_found changed",
            ),
            ([page([occurrence(1)], found=3)], "short page"),
            ([page([], found=1)], "empty page"),
        )
        for documents, message in cases:
            with self.subTest(message=message), LocalFixture(documents) as fixture:
                with self.assertRaisesRegex(MODULE.ExtractorError, message) as caught:
                    fetch(fixture)
                self.assertEqual(caught.exception.category, "pagination")

    def test_rejects_api_errors_and_every_nonempty_warning(self):
        cases = (
            (page([], errors=["invalid selector"]), "api_error"),
            (page([], warnings=["result may be incomplete"]), "api_warning"),
            (page([], warnings={"code": "legacy"}), "api_warning"),
        )
        for document, category in cases:
            with self.subTest(category=category), LocalFixture([document]) as fixture:
                with self.assertRaises(MODULE.ExtractorError) as caught:
                    fetch(fixture)
                self.assertEqual(caught.exception.category, category)

    def test_rejects_malformed_documents_and_inconsistent_row_counts(self):
        cases = (
            ((200, "application/json", b"not-json"), "malformed_json"),
            ([], "malformed_document"),
            ({}, "malformed_document"),
            ({"records": {}, "records_found": 0, "records_returned": 0}, "malformed_document"),
            (page([occurrence(1)], returned=0), "row_count"),
            (page([occurrence(1)], found=0), "row_count"),
            ({"records": [], "records_found": "0", "records_returned": 0}, "malformed_document"),
        )
        for document, category in cases:
            with self.subTest(category=category, document=document), LocalFixture([document]) as fixture:
                with self.assertRaises(MODULE.ExtractorError) as caught:
                    fetch(fixture)
                self.assertEqual(caught.exception.category, category)

    def test_oversized_json_integer_is_bounded_categorized_cli_failure(self):
        oversized_number = "9" * 10_000
        body = (
            b'{"records":[],"records_found":'
            + oversized_number.encode("ascii")
            + b',"records_returned":0}'
        )
        with LocalFixture([(200, "application/json", body)]) as fixture:
            child = run_cli(fixture.url, "--page-size", "2", "--max-records", "10")

        self.assertNotEqual(child.returncode, 0)
        self.assertEqual(child.stdout, "")
        self.assertIn("category=malformed_json", child.stderr)
        self.assertNotIn("Traceback", child.stderr)
        self.assertLessEqual(len(child.stderr), 500)

    def test_oversized_string_identifier_is_bounded_categorized_cli_failure(self):
        oversized_identifier = "9" * 10_000
        with LocalFixture([page([occurrence(oversized_identifier)])]) as fixture:
            child = run_cli(fixture.url, "--page-size", "2", "--max-records", "10")

        self.assertNotEqual(child.returncode, 0)
        self.assertEqual(child.stdout, "")
        self.assertIn("category=missing_identifier", child.stderr)
        self.assertIn("canonical occurrence_no", child.stderr)
        self.assertNotIn(oversized_identifier[:100], child.stderr)
        self.assertNotIn("Traceback", child.stderr)
        self.assertLessEqual(len(child.stderr), 500)

    def test_rejects_result_above_configured_safety_cap_before_emitting(self):
        with LocalFixture([page([occurrence(1)], found=2)]) as fixture:
            with self.assertRaisesRegex(MODULE.ExtractorError, "max_records=1") as caught:
                fetch(fixture, max_records=1, page_size=1)
        self.assertEqual(caught.exception.category, "safety_cap")

    def test_http_failures_redact_complete_authorization_values(self):
        cases = (
            ("Basic", "dXNlcjpCQVNJQy1TRUNSRVQ="),
            ("Bearer", "BEARER-SECRET"),
            ("Digest", 'username="fixture", response="DIGEST-SECRET", nonce="NONCE"'),
        )
        for scheme, credential in cases:
            secret = f"{scheme.upper()}-QUERY-SECRET"
            body = (
                "upstream failed\n"
                f"Authorization: {scheme} {credential}\n"
                f"URL=https://user:password@example.test/path?api_key={secret}\n"
                + ("x" * 5000)
            )
            with self.subTest(scheme=scheme), LocalFixture(
                [(503, "text/plain", body)]
            ) as fixture:
                child = run_cli(
                    fixture.url,
                    "--page-size",
                    "2",
                    "--max-records",
                    "10",
                )

            self.assertEqual(child.returncode, 1)
            self.assertEqual(child.stdout, "")
            self.assertIn("category=http_status", child.stderr)
            self.assertIn("status=503", child.stderr)
            self.assertIn("Authorization: [redacted]", child.stderr)
            self.assertNotIn(credential, child.stderr)
            self.assertNotIn(secret, child.stderr)
            self.assertNotIn("example.test", child.stderr)
            self.assertNotIn("user:password", child.stderr)
            self.assertNotIn("Traceback", child.stderr)
            self.assertLessEqual(len(child.stderr), 500)

    def test_subprocess_failure_report_keeps_status_with_redacted_bounded_stderr(self):
        credential = "FAILURE-REPORT-BASIC-SECRET"
        body = "Authorization: Basic " + credential + "\n" + ("x" * 5000)
        with LocalFixture([(401, "text/plain", body)]) as fixture:
            with self.assertRaises(AssertionError) as caught:
                run_cli(fixture.url, require_success=True)

        diagnostic = str(caught.exception)
        self.assertIn("status=1", diagnostic)
        self.assertIn("category=http_status", diagnostic)
        self.assertNotIn(credential, diagnostic)
        self.assertLessEqual(len(diagnostic), 550)

    def test_invalid_bounds_fail_before_http(self):
        cases = (
            ({"lookback_hours": 0}, "lookback_hours"),
            ({"lookback_hours": MODULE.MAX_LOOKBACK_HOURS + 1}, "lookback_hours"),
            ({"page_size": 0}, "page_size"),
            ({"page_size": MODULE.MAX_PAGE_SIZE + 1}, "page_size"),
            ({"max_records": 0}, "max_records"),
            ({"max_records": MODULE.HARD_MAX_RECORDS + 1}, "max_records"),
            ({"timeout": 0}, "timeout"),
            ({"timeout": MODULE.MAX_TIMEOUT + 1}, "timeout"),
        )
        for changes, message in cases:
            with self.subTest(changes=changes):
                with self.assertRaisesRegex(MODULE.ExtractorError, message):
                    MODULE.fetch_occurrences(**changes)

    def test_daily_configuration_arguments_are_accepted_and_bounded(self):
        config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], "paleobiology_database")
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertEqual(config["schedule"], "23 7 * * *")
        self.assertFalse(config["enabled"])
        self.assertEqual(config["source_scheduling"], "pending_authorized_activation")
        self.assertEqual(config["access"], "public, anonymous, keyless")

        args = MODULE.build_parser().parse_args(config["args"])
        self.assertEqual(args.lookback_hours, 24)
        self.assertEqual(args.page_size, 500)
        self.assertEqual(args.max_records, 10_000)
        self.assertEqual(args.timeout, 60)
        self.assertLessEqual(args.lookback_hours, MODULE.MAX_LOOKBACK_HOURS)
        self.assertLessEqual(args.page_size, MODULE.MAX_PAGE_SIZE)
        self.assertLessEqual(args.max_records, MODULE.HARD_MAX_RECORDS)
        self.assertLessEqual(args.timeout, MODULE.MAX_TIMEOUT)

        text = CONFIG.read_text(encoding="utf-8")
        self.assertIn("recorded live endpoint smoke", text)
        self.assertIn("production Airflow DAG", text)
        self.assertIn("manifest/warehouse-ledger validation", text)


if __name__ == "__main__":
    unittest.main()
