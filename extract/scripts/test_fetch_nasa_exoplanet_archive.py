import contextlib
from datetime import datetime
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest import mock
import urllib.error
import urllib.parse


SCRIPT = Path(__file__).with_name("fetch_nasa_exoplanet_archive.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "nasa_exoplanet_archive.yml"
REPO_ROOT = SCRIPT.parents[2]
EXTRACT_RUNNER = REPO_ROOT / "orchestration" / "include" / "extract_runner.py"
SPEC = importlib.util.spec_from_file_location("fetch_nasa_exoplanet_archive", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def planet(pl_name="Kepler-22 b", hostname="Kepler-22", pl_rade=2.1):
    return {"pl_name": pl_name, "hostname": hostname, "pl_rade": pl_rade}


class JsonResponse(io.BytesIO):
    def __init__(self, document, *, headers=None, status=200, raw=False):
        body = document if raw else json.dumps(document).encode("utf-8")
        super().__init__(body)
        self.headers = headers or {}
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class InProcessPopen:
    """Minimal Popen fixture that exercises the configured script invocation."""

    expected_command = None
    return_document = None
    urlopen = None

    def __init__(self, command, *, cwd, stdout, stderr, text):
        del stdout
        if command != self.expected_command:
            raise AssertionError(f"unexpected configured command: {command!r}")
        if Path(cwd) != SCRIPT.parent or text is not True:
            raise AssertionError("extract runner used unexpected process options")
        output = io.StringIO()
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=JsonResponse(self.return_document),
            ) as urlopen,
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(stderr),
        ):
            self.returncode = MODULE.main(command[2:])
        type(self).urlopen = urlopen
        output.seek(0)
        self.stdout = output

    def wait(self):
        return self.returncode

    def kill(self):
        self.returncode = -9


def source_config():
    """Parse the scalar fields used by the runner without adding a YAML dependency."""
    values = {}
    for line in SOURCE_CONFIG.read_text(encoding="utf-8").splitlines():
        if not line or line[0].isspace() or line.startswith("#") or ":" not in line:
            continue
        key, raw = line.split(":", 1)
        raw = raw.split("  #", 1)[0].strip()
        if raw == "false":
            value = False
        elif raw == "true":
            value = True
        elif raw.startswith("[") or raw.startswith('"'):
            value = json.loads(raw)
        elif raw.isdigit():
            value = int(raw)
        else:
            value = raw
        values[key] = value
    return values


def load_extract_runner():
    include_dir = EXTRACT_RUNNER.parent
    sys.path.insert(0, str(include_dir))
    try:
        spec = importlib.util.spec_from_file_location(
            "nasa_exoplanet_archive_extract_runner", EXTRACT_RUNNER
        )
        runner = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(runner)
        return runner
    finally:
        sys.path.remove(str(include_dir))


class FetchNasaExoplanetArchiveTests(unittest.TestCase):
    def fetch(self, document, *, timeout=17):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            records = MODULE.fetch_planets(timeout=timeout)
        return records, urlopen

    def test_constructs_one_fixed_public_synchronous_tap_request(self):
        records, urlopen = self.fetch([planet()])

        self.assertEqual(len(records), 1)
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        parsed = urllib.parse.urlsplit(request.full_url)
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "exoplanetarchive.ipac.caltech.edu")
        self.assertEqual(parsed.path, "/TAP/sync")
        self.assertIsNone(request.data)
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertTrue(request.get_header("User-agent"))
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

        parameters = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
        self.assertEqual(parameters, {"format": ["json"], "query": [MODULE.QUERY]})
        self.assertEqual(
            MODULE.QUERY,
            "select top 10000 pl_name, hostname, pl_rade from pscomppars "
            "order by pl_name asc, hostname asc",
        )

    def test_serializes_ndjson_with_utc_timestamp_and_preserved_null_radius(self):
        stdout = io.StringIO()
        rows = [planet(), planet("TRAPPIST-1 e", "TRAPPIST-1", None)]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(rows)
        ):
            with contextlib.redirect_stdout(stdout):
                status = MODULE.main(["--timeout", "23"])

        self.assertEqual(status, 0)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        records = [json.loads(line) for line in lines]
        self.assertEqual(records[0]["source"], MODULE.SOURCE)
        self.assertEqual(records[0]["id"], "Kepler-22 b")
        self.assertEqual(records[0]["hostname"], "Kepler-22")
        self.assertEqual(records[0]["pl_rade"], 2.1)
        self.assertIsNone(records[1]["pl_rade"])
        self.assertEqual(len({record["fetched_at"] for record in records}), 1)
        fetched_at = datetime.fromisoformat(records[0]["fetched_at"])
        self.assertIsNotNone(fetched_at.tzinfo)
        self.assertEqual(fetched_at.utcoffset().total_seconds(), 0)
        self.assertNotIn(": ", lines[0])

    def test_identity_is_stable_across_fetch_time_and_mutable_radius(self):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[JsonResponse([planet(pl_rade=2.1)]), JsonResponse([planet(pl_rade=2.2)])],
        ), mock.patch.object(
            MODULE,
            "_utc_now",
            side_effect=["2026-09-17T10:00:00+00:00", "2026-09-18T10:00:00+00:00"],
        ):
            first = MODULE.fetch_planets()
            second = MODULE.fetch_planets()

        self.assertEqual(first[0]["id"], second[0]["id"])
        self.assertNotEqual(first[0]["fetched_at"], second[0]["fetched_at"])
        self.assertNotEqual(first[0]["pl_rade"], second[0]["pl_rade"])

    def test_complete_response_is_validated_before_main_emits_anything(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        rows = [planet(), {"pl_name": "bad", "hostname": "host"}]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(rows)
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                status = MODULE.main([])

        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("invalid schema at row 1", stderr.getvalue())
        self.assertLessEqual(len(stderr.getvalue()), 550)

    def test_rejects_invalid_envelopes_empty_and_over_cap_responses(self):
        invalid = (
            ({}, "must be an array"),
            ([], "unexpectedly contained no planets"),
            ([planet()] * (MODULE.ROW_LIMIT + 1), "row cap exceeded"),
        )
        for document, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.ArchiveError, message):
                    self.fetch(document)

    def test_rejects_malformed_rows_duplicate_identity_and_schema_drift(self):
        invalid = (
            ([None], "expected an object"),
            ([planet(pl_name="")], "pl_name"),
            ([planet(hostname=" host")], "hostname"),
            ([planet(pl_rade="2.1")], "pl_rade"),
            ([planet(pl_rade=-1)], "nonnegative"),
            ([dict(planet(), discoverymethod="Transit")], "expected exactly"),
            ([planet(), planet()], "duplicate planet identity"),
        )
        for document, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.ArchiveError, message):
                    self.fetch(document)
        with self.assertRaisesRegex(MODULE.ArchiveError, "finite"):
            MODULE.parse_response(
                [planet(pl_rade=float("inf"))], "2026-09-17T10:00:00+00:00"
            )

    def test_rejects_non_json_oversize_and_incomplete_responses(self):
        responses = (
            (JsonResponse(b"not-json", raw=True), "non-JSON"),
            (
                JsonResponse(b"[]", headers={"Content-Length": "100"}, raw=True),
                "incomplete HTTP response",
            ),
            (
                JsonResponse(
                    b"[]",
                    headers={"Content-Length": str(MODULE.MAX_RESPONSE_BYTES + 1)},
                    raw=True,
                ),
                "safety limit",
            ),
        )
        for response, message in responses:
            with self.subTest(message=message):
                with mock.patch.object(
                    MODULE.urllib.request, "urlopen", return_value=response
                ):
                    with self.assertRaisesRegex(MODULE.ArchiveError, message):
                        MODULE.fetch_planets()

    def test_reports_http_timeout_and_network_failures_without_urls(self):
        failures = (
            (urllib.error.HTTPError(MODULE.URL, 503, "unavailable", {}, None), "HTTP error 503"),
            (socket.timeout("timed out"), "timeout after 60 seconds"),
            (urllib.error.URLError("offline"), "network error"),
        )
        for failure, category in failures:
            with self.subTest(category=category):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with mock.patch.object(
                    MODULE.urllib.request, "urlopen", side_effect=failure
                ):
                    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                        status = MODULE.main([])
                self.assertEqual(status, 1)
                self.assertEqual(stdout.getvalue(), "")
                self.assertIn(category, stderr.getvalue())
                self.assertNotIn(MODULE.URL, stderr.getvalue())

    def test_invalid_timeout_fails_before_http(self):
        for value in (0, MODULE.MAX_TIMEOUT + 1, True):
            with self.subTest(value=value):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, "timeout"):
                        MODULE.fetch_planets(timeout=value)
                urlopen.assert_not_called()

    def test_daily_configuration_is_disabled_bounded_and_documents_external_gates(self):
        config = source_config()
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertEqual(config["args"], ["--timeout", "60"])
        self.assertEqual(config["schedule"], "37 6 * * *")
        self.assertFalse(config["enabled"])
        self.assertEqual(config["sink"], "local")
        self.assertLessEqual(config["timeout_minutes"] * 60, 600)
        configured = MODULE.build_parser().parse_args(config["args"])
        self.assertEqual(configured.timeout, MODULE.DEFAULT_TIMEOUT)
        self.assertIn("not necessarily the complete", text)
        self.assertIn("anonymous public endpoint", text)
        self.assertIn("NASA Exoplanet Archive", text)
        self.assertIn("activation and completion remain blocked", text)
        self.assertIn("raw/source=nasa_exoplanet_archive", text)

    def test_configured_runner_lands_normal_raw_source_with_mocked_network(self):
        config = source_config()
        runner = load_extract_runner()
        rows = [planet(), planet("TRAPPIST-1 e", "TRAPPIST-1", None)]
        InProcessPopen.expected_command = [
            sys.executable,
            str(SCRIPT),
            *config["args"],
        ]
        InProcessPopen.return_document = rows

        with tempfile.TemporaryDirectory() as directory:
            with (
                mock.patch.object(runner.subprocess, "Popen", InProcessPopen),
                mock.patch.dict(os.environ, {"EXTRACT_DATA_ROOT": directory}),
            ):
                manifest = runner.run(config)

            output = Path(manifest["path"])
            self.assertEqual(output.parents[1].name, "source=nasa_exoplanet_archive")
            records = [
                json.loads(line)
                for line in output.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(manifest["status"], "success")
            self.assertEqual(manifest["records"], 2)
            self.assertEqual({record["source"] for record in records}, {MODULE.SOURCE})
            self.assertEqual({record["id"] for record in records}, {"Kepler-22 b", "TRAPPIST-1 e"})
            self.assertIsNone(records[1]["pl_rade"])

        self.assertIsNotNone(InProcessPopen.urlopen)
        InProcessPopen.urlopen.assert_called_once()


if __name__ == "__main__":
    unittest.main()
