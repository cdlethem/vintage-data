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


class ExtractRunnerFailureDiagnosticsTests(unittest.TestCase):
    def test_child_failure_persists_bounded_redacted_serialized_diagnostic(self):
        runner = load_runner()
        secret = "top-secret-token"
        FailingPopen.stderr_text = (
            "prelude\n"
            + "x" * (runner.FAILURE_STDERR_MAX_CHARS + 200)
            + "\nAuthorization: Bearer " + secret + "\n"
            "request https://alice:" + secret + "@example.test/path?access_token=" + secret + "\n"
            "token=" + secret + "\n"
            "final useful child stderr\n"
        )
        config = {
            "name": "diagnostic_fixture",
            "script": "forced_failure.py",
            "sink": "local",
            "args": ["--api-key=" + secret],
        }

        with tempfile.TemporaryDirectory() as directory:
            with (
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

        with tempfile.TemporaryDirectory() as directory:
            with (
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


if __name__ == "__main__":
    unittest.main()
