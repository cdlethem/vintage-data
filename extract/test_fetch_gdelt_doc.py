from __future__ import annotations

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import urllib.error

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = Path(__file__).parent / "scripts" / "fetch_gdelt_doc.py"
SPEC = importlib.util.spec_from_file_location("fetch_gdelt_doc", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class RawResponse(io.BytesIO):
    def __init__(self, data: bytes, status: int = 200):
        super().__init__(data)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def http_error(code: int, msg: str = "error", headers=None) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://api.gdeltproject.org/api/v2/doc/doc", code, msg, headers or {}, None)


def invoke(responses, state_file: str, pre_state: dict | None = None):
    """Run main() with urlopen patched; return (exit_code, stdout_lines, summary, calls)."""
    if pre_state is not None:
        Path(state_file).write_text(json.dumps(pre_state) + "\n")
    calls = []

    def fake_urlopen(req, timeout=None):
        calls.append(req.full_url)
        response = responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", fake_urlopen), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = MODULE.main(["--state-file", state_file])
    stdout = [line for line in out.getvalue().splitlines() if line.strip()]
    summary = None
    for line in err.getvalue().splitlines():
        if line.startswith(MODULE.PREFIX):
            summary = json.loads(line[len(MODULE.PREFIX):])
    return code, stdout, summary, calls


def fresh_state() -> dict:
    return {"consecutive_429": 0, "next_probe_at": None, "last_status": None,
            "last_attempt_at": None, "last_success_at": None, "retry_count": 0, "cooldown_s": 0}


class GdeltDocTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_file = str(Path(self._tmp.name) / "state.json")

    def test_200_empty_object_opens_circuit_without_failing(self):
        code, stdout, summary, calls = invoke([RawResponse(b"{}")], self.state_file)
        self.assertEqual(0, code)
        self.assertEqual([], stdout)
        self.assertEqual(1, len(calls))
        self.assertEqual("open_circuit", summary["health"])
        self.assertEqual(1, summary["requests"]["attempted"])
        self.assertEqual(200, summary["metrics"]["status"])
        self.assertEqual("no_articles", summary["metrics"]["response"])
        self.assertEqual(3600, summary["metrics"]["cooldown_s"])
        state = json.loads(Path(self.state_file).read_text())
        self.assertEqual(1, state["consecutive_429"])
        self.assertIsNotNone(state["next_probe_at"])

    def test_empty_object_backoff_escalates_with_consecutive_strikes(self):
        pre = fresh_state()
        pre["consecutive_429"] = 2
        code, _, summary, _ = invoke([RawResponse(b"{}")], self.state_file, pre)
        self.assertEqual(0, code)
        self.assertEqual(14400, summary["metrics"]["cooldown_s"])
        state = json.loads(Path(self.state_file).read_text())
        self.assertEqual(3, state["consecutive_429"])

    def test_429_still_opens_circuit_with_retry_after_floor(self):
        code, _, summary, _ = invoke([http_error(429, "Too Many Requests", {"Retry-After": "600"})], self.state_file)
        self.assertEqual(0, code)
        self.assertEqual("open_circuit", summary["health"])
        self.assertEqual(429, summary["metrics"]["status"])
        self.assertEqual(3600, summary["metrics"]["cooldown_s"])

    def test_http_500_still_fails_the_run(self):
        code, _, summary, _ = invoke([http_error(500, "Internal Server Error")], self.state_file)
        self.assertEqual(1, code)
        self.assertEqual("failed", summary["health"])
        self.assertIn("500", summary["error"])

    def test_articles_list_still_emits_records(self):
        payload = {"articles": [{"url": "https://x.example/a", "seendate": "20261017T010000",
                                 "title": "T", "domain": "x.example", "language": "English",
                                 "sourcecountry": "US"}]}
        code, stdout, summary, _ = invoke([RawResponse(json.dumps(payload).encode("utf-8"))], self.state_file)
        self.assertEqual(0, code)
        record = json.loads(stdout[0])
        self.assertEqual(("gdelt_doc", "https://x.example/a"), (record["source"], record["id"]))
        self.assertEqual("healthy", summary["health"])
        self.assertEqual(1, summary["records"])

    def test_empty_articles_list_is_degraded_not_failed(self):
        code, stdout, summary, _ = invoke([RawResponse(b'{"articles": []}')], self.state_file)
        self.assertEqual(0, code)
        self.assertEqual([], stdout)
        self.assertEqual("degraded", summary["health"])
        self.assertEqual(0, summary["records"])

    def test_open_circuit_suppresses_probe_until_due(self):
        pre = fresh_state()
        pre["next_probe_at"] = "2999-01-01T00:00:00+00:00"
        code, _, summary, calls = invoke([], self.state_file, pre)
        self.assertEqual(0, code)
        self.assertEqual("open_circuit", summary["health"])
        self.assertEqual(0, summary["requests"]["attempted"])
        self.assertEqual([], calls)


if __name__ == "__main__":
    unittest.main()
