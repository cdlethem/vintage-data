import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import tempfile
import sys
import threading
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_medicaid_open_data.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "medicaid_open_data.yml"
RUNNER_INCLUDE = Path(__file__).parents[1] / "orchestration" / "include"
sys.path.insert(0, str(RUNNER_INCLUDE))
import extract_runner as RUNNER
import sinks as SINKS
SPEC = importlib.util.spec_from_file_location("fetch_medicaid_open_data", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

IDS = [
    "00000000-0000-4000-8000-000000000001",
    "00000000-0000-4000-8000-000000000002",
    "00000000-0000-4000-8000-000000000003",
]


class JsonResponse(io.BytesIO):
    def __init__(self, document, status=200):
        body = document if isinstance(document, bytes) else json.dumps(document).encode("utf-8")
        super().__init__(body)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def dataset(index=0, modified="2026-09-17T12:00:00+00:00", **overrides):
    record = {
        "identifier": IDS[index],
        "modified": modified,
        "title": f"Dataset {index}",
        "description": "Catalog metadata, not an analytical measure",
        "distribution": [
            {
                "title": "API",
                "accessURL": f"https://data.medicaid.gov/api/{index}",
                "format": "API",
                "nested": {"mediaType": ["application/json", "text/csv"]},
            },
            {
                "title": "CSV",
                "downloadURL": f"https://data.medicaid.gov/files/{index}.csv",
                "format": {"label": "CSV", "metadata": {"compressed": False}},
            },
        ],
        "theme": ["Medicaid", {"label": "Enrollment"}],
    }
    record.update(overrides)
    return record


def page(total, results):
    return {"total": total, "results": results, "facets": []}


class CatalogServer:
    def __init__(self, document):
        self.document = document
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                query = parse_qs(urlsplit(self.path).query)
                if (
                    urlsplit(self.path).path != "/api/1/search"
                    or query.get("fulltext") != ["Medicaid"]
                    or query.get("facets") != ["0"]
                    or int(query.get("page-size", ["0"])[0]) > 100
                ):
                    self.send_error(400)
                    return
                body = json.dumps(fixture.document).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self):
        host, port = self.server.server_address
        return f"http://{host}:{port}/api/1/search"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class FailingCommitSink(SINKS.LocalSink):
    def commit(self, meta):
        self.discard()
        raise OSError("injected sink commit failure")


class InterruptingSink(SINKS.LocalSink):
    def writer(self, source, dt, filename):
        staged = super().writer(source, dt, filename)

        class InterruptedWriter:
            def __enter__(self):
                staged.__enter__()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                return staged.__exit__(exc_type, exc_value, traceback)

            def write(self, value):
                raise KeyboardInterrupt("injected publication interruption")

        return InterruptedWriter()


class FetchMedicaidOpenDataTests(unittest.TestCase):
    def fetch(self, documents, **kwargs):
        responses = [
            document if isinstance(document, BaseException) else JsonResponse(document)
            for document in documents
        ]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            records = MODULE.fetch_catalog(**kwargs)
        return records, urlopen

    def test_request_parameters_headers_timeout_and_page_size_bound(self):
        records, urlopen = self.fetch([page(0, [])], page_size=73, timeout=19)
        self.assertEqual(records, [])
        request = urlopen.call_args.args[0]
        query = parse_qs(urlsplit(request.full_url).query)
        self.assertEqual(query["fulltext"], ["Medicaid"])
        self.assertEqual(query["facets"], ["0"])
        self.assertEqual(query["page-size"], ["73"])
        self.assertEqual(query["page"], ["1"])
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 19})

        for invalid in (0, 101, True, 1.5):
            with self.subTest(page_size=invalid):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as blocked:
                    with self.assertRaisesRegex(ValueError, "page_size"):
                        MODULE.fetch_catalog(page_size=invalid)
                blocked.assert_not_called()

    def test_complete_multi_page_retrieval_preserves_nested_metadata(self):
        first = page(3, [dataset(0), dataset(1)])
        second = page(3, [dataset(2)])
        records, urlopen = self.fetch([first, second], page_size=2)

        self.assertEqual([record["id"] for record in records], IDS)
        self.assertEqual(urlopen.call_count, 2)
        second_query = parse_qs(urlsplit(urlopen.call_args_list[1].args[0].full_url).query)
        self.assertEqual(second_query["page"], ["2"])
        self.assertEqual(records[0]["identifier"], IDS[0])
        self.assertEqual(records[0]["modified"], "2026-09-17T12:00:00+00:00")
        self.assertEqual(records[0]["distribution"], first["results"][0]["distribution"])
        self.assertEqual(records[0]["theme"], first["results"][0]["theme"])
        self.assertEqual(records[0]["source"], "medicaid_open_data")
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")
        self.assertEqual({record["fetched_at"] for record in records}, {records[0]["fetched_at"]})

    def test_empty_catalog_is_complete(self):
        records, urlopen = self.fetch([page(0, [])])
        self.assertEqual(records, [])
        urlopen.assert_called_once()

    def test_duplicate_records_are_deduplicated_when_total_reconciles(self):
        documents = [
            page(3, [dataset(0), dataset(1)]),
            page(3, [dataset(1), dataset(2)]),
        ]
        records, _ = self.fetch(documents, page_size=2)
        self.assertEqual([record["id"] for record in records], IDS)

    def test_duplicate_uuid_with_changed_watermark_is_rejected(self):
        revised_duplicate = dataset(1, modified="2026-09-18T00:00:00Z")
        documents = [
            page(3, [dataset(0), dataset(1)]),
            page(3, [revised_duplicate, dataset(2)]),
        ]
        with self.assertRaisesRegex(MODULE.CatalogError, "changed modified watermark"):
            self.fetch(documents, page_size=2)

    def test_repeated_page_premature_empty_and_short_pages_fail(self):
        cases = (
            (
                [page(3, [dataset(0), dataset(1)]), page(3, [dataset(0), dataset(1)])],
                "repeated page",
            ),
            ([page(3, [dataset(0), dataset(1)]), page(3, [])], "empty page"),
            ([page(3, [dataset(0)])], "pagination ended"),
        )
        for documents, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.IncompleteCatalogError, message):
                    self.fetch(documents, page_size=2)

    def test_changing_or_unreconciled_totals_and_bounds_fail(self):
        with self.assertRaisesRegex(MODULE.CatalogError, "total changed"):
            self.fetch(
                [page(3, [dataset(0), dataset(1)]), page(4, [dataset(2)])],
                page_size=2,
            )
        with self.assertRaisesRegex(MODULE.IncompleteCatalogError, "bounded capacity"):
            self.fetch([page(3, [])], page_size=2, max_pages=1)
        with self.assertRaisesRegex(MODULE.IncompleteCatalogError, "bounded capacity"):
            self.fetch([page(2, [dataset(0)])], page_size=1, max_pages=1)

    def test_required_uuid_modified_and_distribution_shapes_are_validated(self):
        invalid = (
            (dataset(0, identifier="not-a-uuid"), "identifier"),
            (dataset(0, modified=""), "modified"),
            (dataset(0, modified="yesterday"), "modified"),
            (dataset(0, distribution={}), "distribution must be a list"),
            (dataset(0, distribution=["CSV"]), "must be an object"),
            (dataset(0, distribution=[{"accessURL": 7}]), "accessURL"),
        )
        for record, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.CatalogError, message):
                    self.fetch([page(1, [record])])

    def test_response_record_http_and_json_failures_are_not_silenced(self):
        malformed = (
            ([], "response must be an object"),
            ({"results": []}, "total"),
            ({"total": True, "results": []}, "total"),
            ({"total": 0, "results": {}}, "results"),
            ({"total": 1, "results": [None]}, "must be an object"),
            ({"success": False, "total": 0, "results": []}, "API error"),
            ({"error": "failed", "total": 0, "results": []}, "API error"),
        )
        for document, message in malformed:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.CatalogError, message):
                    self.fetch([document])

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(b"not json"),
        ):
            with self.assertRaisesRegex(MODULE.CatalogError, "not valid JSON"):
                MODULE.fetch_catalog()

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(b'{"total":0,"results":[],"bad":NaN}'),
        ):
            with self.assertRaisesRegex(MODULE.CatalogError, "not valid JSON"):
                MODULE.fetch_catalog()

        error = urllib.error.HTTPError(MODULE.URL, 503, "unavailable", {}, io.BytesIO(b"busy"))
        with self.assertRaisesRegex(MODULE.CatalogError, "status=503"):
            self.fetch([error])

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse({"error": "busy"}, status=503),
        ):
            with self.assertRaisesRegex(MODULE.CatalogError, "status=503"):
                MODULE.fetch_catalog()

    def run_main(self, state_file, documents, extra_args=()):
        stdout = io.StringIO()
        responses = [JsonResponse(document) for document in documents]
        argv = ["--state-file", str(state_file), "--page-size", "2", *extra_args]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            with contextlib.redirect_stdout(stdout):
                MODULE.main(argv)
        return [json.loads(line) for line in stdout.getvalue().splitlines()], urlopen

    def test_first_run_unchanged_suppression_and_changed_reemission(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ, {"EXTRACT_DATA_ROOT": directory}
        ):
            state_file = Path(directory) / "state.json"
            first, _ = self.run_main(state_file, [page(2, [dataset(0), dataset(1)])])
            raw = Path(directory) / "raw" / "source=medicaid_open_data" / "dt=2026-09-17"
            raw.mkdir(parents=True)
            lines, _, _ = MODULE.serialize_records(first)
            (raw / "first.ndjson").write_text("\n".join(lines) + "\n", encoding="ascii")
            unchanged, _ = self.run_main(state_file, [page(2, [dataset(0), dataset(1)])])
            revised = dataset(1, modified="2026-09-18T01:02:03Z", title="Revised")
            changed, _ = self.run_main(state_file, [page(2, [dataset(0), revised])])
            changed_lines, _, _ = MODULE.serialize_records(changed)
            (raw / "changed.ndjson").write_text(
                "\n".join(changed_lines) + "\n", encoding="ascii"
            )
            changed_unchanged, _ = self.run_main(
                state_file, [page(2, [dataset(0), revised])]
            )

            self.assertEqual([record["id"] for record in first], IDS[:2])
            self.assertEqual(unchanged, [])
            self.assertEqual([record["id"] for record in changed], [IDS[1]])
            self.assertEqual(changed[0]["title"], "Revised")
            self.assertEqual(changed_unchanged, [])
            state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(state["watermarks"][IDS[1]], "2026-09-18T01:02:03Z")
            self.assertNotIn("pending", state)
    def test_no_state_emits_every_record_without_touching_state(self):
        stdout = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(page(1, [dataset(0)])),
        ), mock.patch.object(MODULE, "load_state") as load, mock.patch.object(
            MODULE, "save_state"
        ) as save, contextlib.redirect_stdout(stdout):
            MODULE.main(["--no-state", "--timeout", "30"])
        self.assertEqual(json.loads(stdout.getvalue())["id"], IDS[0])
        load.assert_not_called()
        save.assert_not_called()

    def test_failed_extraction_cannot_advance_existing_state_or_emit_partial_output(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            original = {
                "version": 1,
                "source": "medicaid_open_data",
                "watermarks": {IDS[0]: "2026-09-17T12:00:00+00:00"},
            }
            state_file.write_text(json.dumps(original), encoding="utf-8")
            stdout = io.StringIO()
            responses = [
                JsonResponse(page(2, [dataset(0)])),
                JsonResponse(b"malformed"),
            ]
            with mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=responses
            ), contextlib.redirect_stdout(stdout):
                with self.assertRaisesRegex(MODULE.CatalogError, "not valid JSON"):
                    MODULE.main(["--state-file", str(state_file), "--page-size", "1"])
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(json.loads(state_file.read_text(encoding="utf-8")), original)

    def test_stdout_failure_leaves_only_an_uncommitted_pending_checkpoint(self):
        class BrokenStdout(io.StringIO):
            def flush(self):
                raise OSError("landing failed")

        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            with mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=JsonResponse(page(1, [dataset(0)])),
            ), contextlib.redirect_stdout(BrokenStdout()):
                with self.assertRaisesRegex(OSError, "landing failed"):
                    MODULE.main(["--state-file", str(state_file)])
            state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(state["watermarks"], {})
            self.assertEqual(state["pending"]["watermarks"], {IDS[0]: dataset(0)["modified"]})
            self.assertFalse(MODULE.artifact_landed(state["pending"]))

    def runner_config(self, state_file):
        return {
            "name": "medicaid_open_data",
            "script": "fetch_medicaid_open_data.py",
            "args": [
                "--page-size",
                "2",
                "--max-pages",
                "100",
                "--timeout",
                "5",
                "--state-file",
                str(state_file),
            ],
            "sink": "local",
        }

    def test_real_runner_orders_checkpoint_after_raw_publication_evidence(self):
        with tempfile.TemporaryDirectory() as directory, CatalogServer(
            page(2, [dataset(0), dataset(1)])
        ) as server, mock.patch.dict(
            os.environ,
            {
                "EXTRACT_DATA_ROOT": directory,
                "MEDICAID_OPEN_DATA_URL": server.url,
            },
        ):
            root = Path(directory)
            state_file = root / "state" / "medicaid.json"
            config = self.runner_config(state_file)

            with mock.patch.object(
                RUNNER, "get_sink", return_value=FailingCommitSink(directory)
            ):
                with self.assertRaisesRegex(OSError, "sink commit failure"):
                    RUNNER.run(config)
            failed_state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(failed_state["watermarks"], {})
            self.assertEqual(set(failed_state["pending"]["watermarks"]), set(IDS[:2]))
            self.assertEqual(list(root.glob("raw/source=medicaid_open_data/dt=*/*.ndjson")), [])

            retry = RUNNER.run(config)
            self.assertEqual(retry["records"], 2)
            artifacts = list(root.glob("raw/source=medicaid_open_data/dt=*/*.ndjson"))
            self.assertEqual(len(artifacts), 1)

            unchanged = RUNNER.run(config)
            self.assertEqual(unchanged["records"], 0)
            committed = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(set(committed["watermarks"]), set(IDS[:2]))
            self.assertNotIn("pending", committed)

            server.document = page(
                2,
                [dataset(0), dataset(1, modified="2026-09-18T01:02:03Z")],
            )
            changed = RUNNER.run(config)
            self.assertEqual(changed["records"], 1)
            landed_changed = RUNNER.run(config)
            self.assertEqual(landed_changed["records"], 0)
            committed = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(committed["watermarks"][IDS[1]], "2026-09-18T01:02:03Z")

    def test_runner_interruption_discards_stage_and_replays_pending_records(self):
        with tempfile.TemporaryDirectory() as directory, CatalogServer(
            page(1, [dataset(0)])
        ) as server, mock.patch.dict(
            os.environ,
            {
                "EXTRACT_DATA_ROOT": directory,
                "MEDICAID_OPEN_DATA_URL": server.url,
            },
        ):
            root = Path(directory)
            state_file = root / "state" / "medicaid.json"
            config = self.runner_config(state_file)
            with mock.patch.object(
                RUNNER, "get_sink", return_value=InterruptingSink(directory)
            ):
                with self.assertRaisesRegex(KeyboardInterrupt, "publication interruption"):
                    RUNNER.run(config)
            pending = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(pending["watermarks"], {})
            self.assertIn("pending", pending)
            self.assertEqual(list(root.glob("raw/source=medicaid_open_data/dt=*/*.ndjson")), [])
            self.assertEqual(RUNNER.run(config)["records"], 1)

    def test_checkpoint_write_failure_prevents_output_and_retry_lands_records(self):
        with tempfile.TemporaryDirectory() as directory, CatalogServer(
            page(1, [dataset(0)])
        ) as server, mock.patch.dict(
            os.environ,
            {
                "EXTRACT_DATA_ROOT": directory,
                "MEDICAID_OPEN_DATA_URL": server.url,
            },
        ):
            root = Path(directory)
            blocked_parent = root / "blocked"
            blocked_parent.write_text("not a directory", encoding="utf-8")
            state_file = blocked_parent / "state.json"
            config = self.runner_config(state_file)
            with self.assertRaisesRegex(RuntimeError, r"exited [1-9]"):
                RUNNER.run(config)
            self.assertEqual(list(root.glob("raw/source=medicaid_open_data/dt=*/*.ndjson")), [])

            blocked_parent.unlink()
            blocked_parent.mkdir()
            self.assertEqual(RUNNER.run(config)["records"], 1)

    def test_landed_checkpoint_promotion_failure_keeps_pending_state_retryable(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ, {"EXTRACT_DATA_ROOT": directory}
        ):
            state_file = Path(directory) / "state.json"
            records = [MODULE.normalize_record(dataset(0), "2026-09-17T12:30:00+00:00")]
            lines, digest, byte_count = MODULE.serialize_records(records)
            raw = Path(directory) / "raw" / "source=medicaid_open_data" / "dt=2026-09-17"
            raw.mkdir(parents=True)
            (raw / "landed.ndjson").write_text("\n".join(lines) + "\n", encoding="ascii")
            pending = {
                "version": 1,
                "source": "medicaid_open_data",
                "watermarks": {},
                "pending": {
                    "artifact_sha256": digest,
                    "bytes": byte_count,
                    "records": 1,
                    "watermarks": {IDS[0]: dataset(0)["modified"]},
                },
            }
            MODULE.save_state(state_file, pending)
            with mock.patch.object(MODULE, "save_state", side_effect=OSError("checkpoint failed")):
                with self.assertRaisesRegex(OSError, "checkpoint failed"):
                    MODULE.reconcile_landed_state(state_file, MODULE.load_state(state_file))
            self.assertIn("pending", MODULE.load_state(state_file))
            promoted = MODULE.reconcile_landed_state(state_file, MODULE.load_state(state_file))
            self.assertEqual(promoted["watermarks"], {IDS[0]: dataset(0)["modified"]})
            self.assertNotIn("pending", MODULE.load_state(state_file))

    def test_only_catalog_endpoint_is_requested_and_main_emits_valid_ndjson(self):
        stdout = io.StringIO()
        source_record = dataset(0)
        expected_links = copy.deepcopy(source_record["distribution"])
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(page(1, [source_record])),
        ) as urlopen, contextlib.redirect_stdout(stdout):
            MODULE.main(["--no-state"])

        urlopen.assert_called_once()
        self.assertEqual(urlsplit(urlopen.call_args.args[0].full_url)._replace(query="").geturl(), MODULE.URL)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        envelope = json.loads(lines[0])
        self.assertEqual(envelope["source"], "medicaid_open_data")
        self.assertEqual(envelope["id"], IDS[0])
        self.assertEqual(envelope["distribution"], expected_links)
        self.assertNotIn(": ", lines[0])

    def test_source_configuration_matches_runner_and_acceptance_contract(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertRegex(text, r"(?m)^name: medicaid_open_data$")
        self.assertRegex(text, r"(?m)^script: fetch_medicaid_open_data.py$")
        self.assertIn('"--page-size", "100"', text)
        self.assertIn('"--timeout", "30"', text)
        self.assertRegex(text, r'(?m)^schedule: "[0-9]+ [0-9]+ \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertRegex(text, r"(?m)^sink: local$")
        self.assertIsNone(re.search(r"(?m)^enabled:\s*true$", text))
        self.assertIn("catalog metadata", text)
        self.assertIn("does not download distributions", text)
        self.assertIn("historical count of 243 is not a fixed expected total", text)
        self.assertIn("authorized network-enabled operator", text)
        self.assertIn("Independent review remains required", text)


if __name__ == "__main__":
    unittest.main()
