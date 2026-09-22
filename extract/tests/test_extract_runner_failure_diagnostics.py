import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).parents[2]
INCLUDE_DIR = REPO_ROOT / "orchestration" / "include"
RUNNER_PATH = INCLUDE_DIR / "extract_runner.py"


def load_runner():
    sys.path.insert(0, str(INCLUDE_DIR))
    try:
        spec = importlib.util.spec_from_file_location("failure_diagnostics_runner", RUNNER_PATH)
        runner = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(runner)
        return runner
    finally:
        sys.path.remove(str(INCLUDE_DIR))


class FailingPopen:
    stderr_text = ""
    returncode = 17

    def __init__(self, command, *, cwd, stdout, stderr, text):
        del command, cwd, stdout, text
        stderr.write(type(self).stderr_text)
        self.stdout = io.StringIO("")
        self._returncode = type(self).returncode

    def wait(self):
        return self._returncode

    def kill(self):
        self._returncode = -9


class ExplodingPopen(FailingPopen):
    def __init__(self, command, *, cwd, stdout, stderr, text):
        super().__init__(command, cwd=cwd, stdout=stdout, stderr=stderr, text=text)

        def records():
            raise ValueError("authorization=iterator-secret")
            yield ""

        self.stdout = records()


class InvalidSummaryPopen(FailingPopen):
    """Exits 0 after emitting a summary with a contract-invalid value."""

    returncode = 0

    def __init__(self, command, *, cwd, stdout, stderr, text):
        del command, cwd, stdout, text
        stderr.write("ordinary log line token=summary-secret\n")
        stderr.write('VINTAGE_RUN_SUMMARY\t{"health": "exploded", "requests": {"attempted": 1, "succeeded": 1}}\n')
        self.stdout = iter([
            json.dumps({"source": "invalid_summary_fixture",
                        "fetched_at": "2026-09-21T00:00:00Z", "id": 1}) + "\n"
        ])
        self._returncode = type(self).returncode


class ExtractRunnerFailureDiagnosticsTests(unittest.TestCase):
    def test_child_failure_persists_bounded_redacted_serialized_diagnostic(self):
        runner = load_runner()
        secret = "top-secret-token"
        FailingPopen.stderr_text = (
            "prelude\n"
            + "x" * (runner.FAILURE_STDERR_MAX_CHARS + 200)
            + "\nAuthorization: Bearer " + secret + "\n"
            "request https://alice:" + secret + "@example.test/path?access_token=" + secret + "\n"
            'config {"token": "' + secret + '"}\n'
            "token=" + secret + "\n"
            "final useful child stderr\n"
        )
        config = {
            "name": "diagnostic_fixture",
            "script": "forced_failure.py",
            "sink": "local",
            "args": ["--api-key=" + secret],
        }

        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(runner.subprocess, "Popen", FailingPopen),
            mock.patch.dict("os.environ", {"EXTRACT_DATA_ROOT": directory}),
        ):
            with self.assertRaises(RuntimeError) as raised:
                runner.run(config)

            failure_files = list(Path(directory).rglob("*.fail.json"))
            self.assertEqual(len(failure_files), 1)
            manifest = json.loads(failure_files[0].read_text(encoding="utf-8"))

        diagnostic = json.loads(manifest["error"])
        self.assertEqual(diagnostic["exception_type"], "RuntimeError")
        self.assertEqual(diagnostic["exit_status"], 17)
        self.assertIn("final useful child stderr", diagnostic["stderr"])
        self.assertIn("[truncated]", diagnostic["stderr"])
        self.assertLessEqual(len(diagnostic["stderr"]), runner.FAILURE_STDERR_MAX_CHARS)
        self.assertLessEqual(len(manifest["error"].encode("utf-8")), runner.FAILURE_DIAGNOSTIC_MAX_BYTES)
        self.assertIn("RuntimeError", diagnostic["traceback"])
        self.assertEqual(json.loads(str(raised.exception)), diagnostic)
        self.assertNotIn(secret, manifest["error"])
        self.assertNotIn(secret, json.dumps(manifest))
        self.assertIn("[REDACTED]", manifest["error"])

    def test_iterator_exception_is_materialized_instead_of_lost(self):
        runner = load_runner()
        ExplodingPopen.stderr_text = "child stderr token=child-secret\n"
        config = {"name": "iterator_fixture", "script": "broken.py", "sink": "local"}

        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(runner.subprocess, "Popen", ExplodingPopen),
            mock.patch.dict("os.environ", {"EXTRACT_DATA_ROOT": directory}),
        ):
            with self.assertRaises(RuntimeError) as raised:
                runner.run(config)
            manifest = json.loads(next(Path(directory).rglob("*.fail.json")).read_text())

        diagnostic = json.loads(manifest["error"])
        self.assertEqual(diagnostic["exception_type"], "ValueError")
        self.assertEqual(diagnostic["exit_status"], -9)
        self.assertEqual(diagnostic["message"], "authorization=[REDACTED]")
        self.assertIn("ValueError: authorization=[REDACTED]", diagnostic["traceback"])
        self.assertEqual(diagnostic["stderr"], "child stderr token=[REDACTED]")
        self.assertEqual(json.loads(str(raised.exception)), diagnostic)

    def test_diagnostic_captures_exception_type_message_and_traceback(self):
        runner = load_runner()

        try:
            raise ValueError("password=hunter2")
        except ValueError as exc:
            diagnostic = json.loads(runner._failure_diagnostic(exc, "stderr", None))

        self.assertEqual(diagnostic["exception_type"], "ValueError")
        self.assertEqual(diagnostic["exit_status"], None)
        self.assertEqual(diagnostic["message"], "password=[REDACTED]")
        self.assertIn("ValueError: password=[REDACTED]", diagnostic["traceback"])

    def test_redaction_covers_structured_and_split_forms(self):
        runner = load_runner()
        secret = "top-secret"

        # Quoted object-member forms.
        self.assertNotIn(secret, runner._redact('{"token": "top-secret"}'))
        self.assertNotIn(secret, runner._redact("{'api_key': 'top-secret'}"))
        self.assertNotIn(secret, runner._redact('{"Authorization": "Bearer top-secret"}'))
        # Inline option=value form.
        self.assertNotIn(secret, runner._redact("--api-key=" + secret))
        # Split option/value argv pairs.
        self.assertEqual(runner._redact_args(["--api-key", secret]),
                         ["--api-key", "[REDACTED]"])
        self.assertEqual(runner._redact_args(["--auth-token", secret, "--verbose"]),
                         ["--auth-token", "[REDACTED]", "--verbose"])
        self.assertEqual(runner._redact_args(["--api-key", "--verbose"]),
                         ["--api-key", "--verbose"])
        self.assertEqual(runner._redact_args(["-t", secret]), ["-t", secret])

    def test_manifest_raised_error_and_logs_hide_split_secret(self):
        runner = load_runner()
        secret = "split-secret-value"
        FailingPopen.stderr_text = "child saw token=" + secret + "\n"
        config = {
            "name": "split_fixture",
            "script": "forced_failure.py",
            "sink": "local",
            "args": ["--api-key", secret],
        }
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(runner.subprocess, "Popen", FailingPopen),
            mock.patch.dict("os.environ", {"EXTRACT_DATA_ROOT": directory}),
            self.assertLogs(runner.log.name, level="INFO") as captured,
        ):
            with self.assertRaises(RuntimeError) as raised:
                runner.run(config)
            manifest = json.loads(next(Path(directory).rglob("*.fail.json")).read_text())
        self.assertIn("[REDACTED]", json.dumps(manifest))
        self.assertNotIn(secret, json.dumps(manifest))
        self.assertNotIn(secret, str(raised.exception))
        self.assertNotIn(secret, "\n".join(captured.output))

    def test_contract_invalid_summary_routes_through_failure_manifest(self):
        runner = load_runner()
        config = {"name": "invalid_summary_fixture", "script": "bad_summary.py", "sink": "local"}
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(runner.subprocess, "Popen", InvalidSummaryPopen),
            mock.patch.dict("os.environ", {"EXTRACT_DATA_ROOT": directory}),
        ):
            with self.assertRaises(RuntimeError) as raised:
                runner.run(config)
            manifest = json.loads(next(Path(directory).rglob("*.fail.json")).read_text())

        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["exit_code"], 0)
        diagnostic = json.loads(manifest["error"])
        self.assertEqual(diagnostic["exception_type"], "RunSummaryError")
        self.assertEqual(diagnostic["exit_status"], 0)
        self.assertIn("ordinary log line token=[REDACTED]", diagnostic["stderr"])
        self.assertEqual(json.loads(str(raised.exception)), diagnostic)
        # The staged records must not be published.
        self.assertEqual(list(Path(directory).rglob("*.meta.json")), [])
        self.assertEqual(list(Path(directory).rglob("*.ndjson")), [])


if __name__ == "__main__":
    unittest.main()
