import contextlib
import hashlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_cisa_kev.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "cisa_kev.yml"
SPEC = importlib.util.spec_from_file_location("fetch_cisa_kev", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def row(cve_id="CVE-2026-100001", **overrides):
    record = {
        "cveID": cve_id,
        "vendorProject": "Example Corp",
        "product": "Example Product",
        "vulnerabilityName": "Example Product Cross-Site Scripting Vulnerability",
        "dateAdded": "2026-11-20",
        "shortDescription": "Example Product contains a cross-site scripting vulnerability.",
        "requiredAction": "Apply updates per vendor instructions.",
        "dueDate": "2026-11-30",
        "knownRansomwareCampaignUse": "Unknown",
        "forensicTriage": "Yes",
        "notes": "",
        "cwes": ["CWE-79"],
    }
    record.update(overrides)
    return record


def catalog(*rows, catalog_version="2026.11.24", date_released="2026-11-24T17:47:46.199Z", **overrides):
    document = {
        "title": "Known Exploited Vulnerabilities Catalog",
        "catalogVersion": catalog_version,
        "dateReleased": date_released,
        "count": len(rows),
        "vulnerabilities": list(rows),
    }
    document.update(overrides)
    return document


class JsonResponse(io.BytesIO):
    def __init__(self, payload):
        super().__init__(payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FetchCisaKevTests(unittest.TestCase):
    def run_main(self, document, *args):
        stdout = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            with contextlib.redirect_stdout(stdout):
                MODULE.main(list(args))
        return stdout.getvalue(), urlopen

    def test_first_run_emits_full_catalog_tagged_new(self):
        first = catalog(row(), row("CVE-2026-100002"))
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            output, urlopen = self.run_main(first, "--state-file", str(state))

            records = [json.loads(line) for line in output.splitlines()]
            self.assertEqual([r["id"] for r in records], ["CVE-2026-100001", "CVE-2026-100002"])
            for record in records:
                self.assertEqual(record["source"], "cisa_kev")
                self.assertEqual(record["change"], "new")
                self.assertEqual(record["catalog_version"], "2026.11.24")
                self.assertEqual(record["date_released"], "2026-11-24T17:47:46.199Z")
                self.assertRegex(record["fetched_at"], r"\+00:00$")
                self.assertEqual(record["vendorProject"], "Example Corp")
                self.assertEqual(record["cwes"], ["CWE-79"])

            saved = json.loads(state.read_text(encoding="utf-8"))
            self.assertEqual(saved["version"], 1)
            self.assertEqual(saved["source"], "cisa_kev")
            self.assertEqual(set(saved["records"]), {"CVE-2026-100001", "CVE-2026-100002"})
            urlopen.assert_called_once()

    def test_unchanged_catalog_emits_nothing_and_keeps_state(self):
        first = catalog(row())
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            self.run_main(first, "--state-file", str(state))
            saved_before = state.read_text(encoding="utf-8")

            output, urlopen = self.run_main(first, "--state-file", str(state))

            self.assertEqual(output, "")
            self.assertEqual(urlopen.call_count, 1)
            self.assertEqual(state.read_text(encoding="utf-8"), saved_before)

    def test_emits_only_added_and_updated_entries(self):
        first = catalog(
            row("CVE-2026-100001"),
            row("CVE-2026-100002"),
            row("CVE-2026-100003"),
        )
        second = catalog(
            row("CVE-2026-100001"),
            row("CVE-2026-100002", requiredAction="Apply updates or discontinue use."),
            row("CVE-2026-100004", dateAdded="2026-11-24"),
        )
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            self.run_main(first, "--state-file", str(state))
            output, _ = self.run_main(second, "--state-file", str(state))

            records = [json.loads(line) for line in output.splitlines()]
            self.assertEqual(
                [(r["id"], r["change"]) for r in records],
                [
                    ("CVE-2026-100002", "updated"),
                    ("CVE-2026-100004", "new"),
                    ("CVE-2026-100003", "removed"),
                ],
            )
            saved = json.loads(state.read_text(encoding="utf-8"))
            self.assertEqual(
                saved["records"]["CVE-2026-100002"]["requiredAction"],
                "Apply updates or discontinue use.",
            )
            self.assertNotIn("CVE-2026-100003", saved["records"])

    def test_removed_entries_are_emitted(self):
        first = catalog(row("CVE-2026-100001"), row("CVE-2026-100002"))
        second = catalog(row("CVE-2026-100001"))
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            self.run_main(first, "--state-file", str(state))
            output, _ = self.run_main(second, "--state-file", str(state))

            records = [json.loads(line) for line in output.splitlines()]
            self.assertEqual(
                [(r["id"], r["change"]) for r in records],
                [("CVE-2026-100002", "removed")],
            )
            self.assertEqual(records[0]["cveID"], "CVE-2026-100002")
    def test_no_state_emits_full_catalog_without_writing_state(self):
        document = catalog(row(), row("CVE-2026-100002"))
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            output, _ = self.run_main(document, "--no-state")

            self.assertEqual(len(output.splitlines()), 2)
            self.assertFalse(state.exists())

    def test_no_state_ignores_existing_state_file(self):
        first = catalog(row("CVE-2026-100001"), row("CVE-2026-100002"))
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            self.run_main(first, "--state-file", str(state))
            saved_before = state.read_text(encoding="utf-8")
            output, _ = self.run_main(first, "--no-state")

            self.assertEqual(len(output.splitlines()), 2)
            self.assertEqual(state.read_text(encoding="utf-8"), saved_before)

    def test_no_state_and_state_file_are_mutually_exclusive(self):
        with self.assertRaises(SystemExit):
            MODULE.main(["--no-state", "--state-file", "/tmp/kev-state.json"])

    def test_nonpositive_timeout_is_rejected_before_http(self):
        for timeout in ("0", "-5"):
            with self.subTest(timeout=timeout):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaises(SystemExit):
                        MODULE.main(["--timeout", timeout])
                    urlopen.assert_not_called()

    def test_makes_one_bounded_request_with_user_agent(self):
        output, urlopen = self.run_main(catalog(), "--timeout", "17")

        self.assertEqual(output, "")
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, MODULE.URL)
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_rejects_malformed_documents(self):
        for document, message in (
            ({}, "missing vulnerabilities"),
            ({"vulnerabilities": {}}, "missing vulnerabilities"),
            ({"vulnerabilities": "nope"}, "missing vulnerabilities"),
            (catalog(row(), count=3), "count mismatch"),
        ):
            with self.subTest(document=document):
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.validated_catalog(document)

        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=JsonResponse(b"[]")):
            with self.assertRaisesRegex(ValueError, "not an object"):
                MODULE.fetch_catalog(timeout=30)

    def test_rejects_invalid_catalog_metadata(self):
        for key in ("catalogVersion", "dateReleased"):
            document = catalog(row())
            document[key] = None
            with self.subTest(key=key):
                with self.assertRaisesRegex(ValueError, key):
                    MODULE.validated_catalog(document)
    def test_rejects_rows_missing_or_malformed_fields(self):
        for field in sorted(MODULE.REQUIRED_ROW_FIELDS):
            broken = row()
            del broken[field]
            with self.subTest(field=field, case="missing"):
                with self.assertRaisesRegex(ValueError, "missing fields"):
                    MODULE.validated_catalog(catalog(broken))
            with self.subTest(field=field, case="empty"):
                with self.assertRaisesRegex(ValueError, "invalid fields"):
                    MODULE.validated_catalog(catalog(row(**{field: ""})))

        for cve_id in (None, ""):
            with self.subTest(cve_id=cve_id):
                with self.assertRaisesRegex(ValueError, "invalid fields"):
                    MODULE.validated_catalog(catalog(row(**{"cveID": cve_id})))

        for cve_id in ("CVE-2026-1", "VULN-2026-1", "CVE-2026-100001 "):
            with self.subTest(cve_id=cve_id):
                with self.assertRaisesRegex(ValueError, "invalid cveID"):
                    MODULE.validated_catalog(catalog(row(**{"cveID": cve_id})))

        for date in (None, "2026-11-20T00:00:00Z", "2026/11/20", "2026-13-20", "2026-02-30"):
            for field in ("dateAdded", "dueDate"):
                with self.subTest(field=field, date=date):
                    message = "invalid fields" if date is None else "invalid " + field
                    with self.assertRaisesRegex(ValueError, message):
                        MODULE.validated_catalog(catalog(row(**{field: date})))

    def test_optional_fields_may_be_absent_or_empty(self):
        stripped = row()
        for field in sorted(MODULE.OPTIONAL_ROW_FIELDS | {"cwes"}):
            del stripped[field]
        _, _, indexed = MODULE.validated_catalog(catalog(stripped))
        self.assertIn("CVE-2026-100001", indexed)

        for field in sorted(MODULE.OPTIONAL_ROW_FIELDS):
            _, _, _ = MODULE.validated_catalog(catalog(row(**{field: ""})))

    def test_rejects_non_string_optional_fields_and_bad_cwes(self):
        for override in ({"forensicTriage": 3}, {"notes": 7}, {"cwes": "CWE-79"}, {"cwes": ["CWE-79", 1]}):
            with self.subTest(override=override):
                if "cwes" in override:
                    with self.assertRaisesRegex(ValueError, "invalid cwes"):
                        MODULE.validated_catalog(catalog(row(**override)))
                else:
                    with self.assertRaisesRegex(ValueError, "invalid fields"):
                        MODULE.validated_catalog(catalog(row(**override)))

    def test_rejects_duplicate_cve_ids(self):
        with self.assertRaisesRegex(ValueError, "duplicate cveID"):
            MODULE.validated_catalog(catalog(row(), row()))

    def test_rejects_non_json_response_body(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=JsonResponse(b"<html>not json</html>")):
            with self.assertRaisesRegex(ValueError, "not JSON"):
                MODULE.fetch_catalog(timeout=30)

    def test_http_error_surfaces_status_and_body(self):
        import urllib.error

        error = urllib.error.HTTPError(
            MODULE.URL, 403, "forbidden", {"Content-Type": "text/plain"}, io.BytesIO(b"blocked")
        )
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "status=403"):
                MODULE.fetch_catalog(timeout=30)

    def test_state_written_only_after_output_flushed(self):
        class BreakingStdout(io.StringIO):
            def write(self, text):
                raise OSError("disk full")

        document = catalog(row())
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            with mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=JsonResponse(document),
            ), contextlib.redirect_stdout(BreakingStdout()):
                with self.assertRaises(OSError):
                    MODULE.main(["--state-file", str(state), "--timeout", "30"])
            self.assertFalse(state.exists())

    def test_snapshot_sha256_matches_downloaded_bytes(self):
        document = catalog(row())
        body = json.dumps(document).encode("utf-8")
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            output, _ = self.run_main(document, "--state-file", str(state))
            record = json.loads(output.splitlines()[0])
            saved = json.loads(state.read_text(encoding="utf-8"))
            expected = hashlib.sha256(body).hexdigest()
            self.assertEqual(record["snapshot_sha256"], expected)
            self.assertEqual(saved["snapshot_sha256"], expected)

    def test_corrupted_state_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            for content in (
                "not json",
                "[]",
                json.dumps({"version": 2, "source": "cisa_kev", "records": {}}),
                json.dumps({"version": 1, "source": "cisa_kev", "records": {"CVE-1-1": "not an object"}}),
            ):
                state.write_text(content, encoding="utf-8")
                with self.subTest(content=content[:24]):
                    with self.assertRaises(ValueError):
                        MODULE.load_state(state)

    def test_source_configuration_is_daily_and_enabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "41 18 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertRegex(text, r"(?m)^script: fetch_cisa_kev\.py$")
        self.assertIn("args: []", text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 10", text)
        self.assertIn("sink: local", text)
        self.assertIn("CC0 1.0 Universal", text)
        self.assertIn("attribution_required: false", text)
        self.assertIn("one bounded ~1.9 MB request per day", text)
        self.assertIn("keyless", text)


if __name__ == "__main__":
    unittest.main()
