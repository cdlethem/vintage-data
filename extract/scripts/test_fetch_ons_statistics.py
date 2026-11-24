#!/usr/bin/env python3
"""Offline behavioral tests for the ONS statistics basket extractor."""
import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
from unittest.mock import MagicMock, patch

SCRIPT = pathlib.Path(__file__).with_name("fetch_ons_statistics.py")
SPEC = importlib.util.spec_from_file_location("fetch_ons_statistics", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)

EDITION_URL = f"{module.API_BASE}/datasets/testset/editions/time-series"
VERSION_URL = f"{module.API_BASE}/datasets/testset/editions/time-series/versions/1"
CSV_URL = f"{module.DOWNLOAD_BASE}/testset/editions/time-series/versions/1.csv"

EDITION = {"edition": "time-series", "links": {"latest_version": {"id": "1"}}}
VERSION = {"state": "published", "dimensions": [{"id": "time"}, {"id": "geo"}]}
CSV_V1 = "v0,time,Time,geo,Geo\n1.5,2024,Jan-24,K1,UK\n2.5,2025,Jan-25,K2,UK\n"


class FakeResponse:
    def __init__(self, body: bytes):
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


def make_urlopen(routes: dict):
    calls: list[str] = []

    def fake_urlopen(request, timeout=None):
        url = request.full_url
        calls.append(url)
        outcomes = routes[url]
        outcome = outcomes.pop(0) if len(outcomes) > 1 else outcomes[0]
        if isinstance(outcome, Exception):
            raise outcome
        return FakeResponse(outcome)

    return fake_urlopen, calls


def http_error(code: int, url: str = "x"):
    import urllib.error

    return urllib.error.HTTPError(url, code, f"HTTP {code}", {}, io.BytesIO(b""))


class OnsStatisticsTests(unittest.TestCase):
    def setUp(self):
        self.original_datasets = module.DATASETS
        module.DATASETS = ("testset",)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.state = pathlib.Path(self.directory.name) / "state.json"
        self.sleep_mock = MagicMock()

    def tearDown(self):
        module.DATASETS = self.original_datasets

    def run_extractor(self, routes, *argv):
        stdout, stderr = io.StringIO(), io.StringIO()
        fake_urlopen, calls = make_urlopen(routes)
        with (
            patch.object(module.urllib.request, "urlopen", side_effect=fake_urlopen),
            patch.object(module.time, "sleep", self.sleep_mock),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            result = module.main(list(argv))
        lines = stderr.getvalue().splitlines()
        summary = None
        for line in lines:
            if line.startswith(module.SUMMARY_PREFIX):
                summary = json.loads(line[len(module.SUMMARY_PREFIX):])
        return result, stdout.getvalue().splitlines(), summary, calls

    def v1_routes(self):
        return {
            EDITION_URL: [json.dumps(EDITION).encode()],
            VERSION_URL: [json.dumps(VERSION).encode()],
            CSV_URL: [CSV_V1.encode()],
        }

    def test_first_run_emits_all_rows_and_saves_state(self):
        result, output, summary, calls = self.run_extractor(self.v1_routes(), "--state-file", str(self.state))
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 2)
        record = json.loads(output[0])
        self.assertEqual(record["id"], "testset|2024|K1")
        self.assertEqual(record["dataset"], "testset")
        self.assertEqual(record["version"], 1)
        self.assertEqual(record["source"], module.SOURCE)
        self.assertEqual(record["dimensions"], {"time": "2024", "geo": "K1"})
        self.assertEqual(record["dimension_labels"], {"time": "Jan-24", "geo": "UK"})
        self.assertEqual(record["values"], {"v0": "1.5"})
        self.assertTrue(record["fetched_at"])
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["records"], 2)
        self.assertEqual(summary["datasets"]["testset"], {"status": "ok", "version": 1, "rows": 2, "new": 2})
        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertEqual(state["emitted"]["testset"], ["testset|2024|K1", "testset|2025|K2"])

    def test_rerun_with_same_version_is_quiet(self):
        self.run_extractor(self.v1_routes(), "--state-file", str(self.state))
        result, output, summary, _ = self.run_extractor(self.v1_routes(), "--state-file", str(self.state))
        self.assertEqual(result, 0)
        self.assertEqual(output, [])
        self.assertEqual(summary["records"], 0)
        self.assertEqual(summary["datasets"]["testset"]["new"], 0)
        self.assertEqual(summary["datasets"]["testset"]["rows"], 2)

    def test_new_version_emits_only_new_rows_not_revisions(self):
        self.run_extractor(self.v1_routes(), "--state-file", str(self.state))
        csv_v2 = "v0,time,Time,geo,Geo\n9.9,2024,Jan-24,K1,UK\n2.5,2025,Jan-25,K2,UK\n3.5,2025,Jan-25,K1,UK\n"
        edition = {"edition": "time-series", "links": {"latest_version": {"id": "2"}}}
        routes = {
            EDITION_URL: [json.dumps(edition).encode()],
            f"{module.API_BASE}/datasets/testset/editions/time-series/versions/2": [json.dumps(VERSION).encode()],
            f"{module.DOWNLOAD_BASE}/testset/editions/time-series/versions/2.csv": [csv_v2.encode()],
        }
        result, output, summary, _ = self.run_extractor(routes, "--state-file", str(self.state))
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 1)
        record = json.loads(output[0])
        self.assertEqual(record["id"], "testset|2025|K1")
        self.assertEqual(record["version"], 2)
        self.assertEqual(summary["records"], 1)
        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertEqual(
            state["emitted"]["testset"],
            ["testset|2024|K1", "testset|2025|K1", "testset|2025|K2"],
        )

    def test_editions_items_wrapper_is_accepted(self):
        routes = self.v1_routes()
        routes[EDITION_URL] = [json.dumps({"items": [EDITION], "count": 1, "total_count": 1}).encode()]
        result, output, summary, _ = self.run_extractor(routes, "--no-state")
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 2)
        self.assertEqual(summary["health"], "healthy")

    def test_header_mismatch_fails_the_run(self):
        bad_csv = "v0,when,When,geo,Geo\n1.5,2024,Jan-24,K1,UK\n"
        routes = self.v1_routes()
        routes[CSV_URL] = [bad_csv.encode()]
        result, output, summary, _ = self.run_extractor(routes, "--state-file", str(self.state))
        self.assertEqual(result, 1)
        self.assertEqual(output, [])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertIn("does not match", summary["datasets"]["testset"]["error"])
        self.assertFalse(self.state.exists())

    def test_404_csv_fails_the_run(self):
        import urllib.error

        routes = self.v1_routes()
        routes[CSV_URL] = [urllib.error.HTTPError(CSV_URL, 404, "Not Found", {}, io.BytesIO(b"error code: 1015"))]
        result, output, summary, _ = self.run_extractor(routes, "--state-file", str(self.state))
        self.assertEqual(result, 1)
        self.assertEqual(output, [])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["datasets"]["testset"]["status"], "failed")
        self.assertIn("status=404", summary["datasets"]["testset"]["error"])

    def test_transient_503_is_retried(self):
        routes = self.v1_routes()
        routes[EDITION_URL] = [http_error(503, EDITION_URL), json.dumps(EDITION).encode()]
        result, output, summary, calls = self.run_extractor(routes, "--no-state")
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 2)
        self.assertEqual(self.sleep_mock.call_count, 1)
        self.assertEqual(calls.count(EDITION_URL), 2)

    def test_no_state_does_not_write_a_state_file(self):
        result, output, summary, _ = self.run_extractor(self.v1_routes(), "--no-state")
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 2)
        self.assertFalse(self.state.exists())

    def test_partial_basket_failure_is_degraded(self):
        module.DATASETS = ("testset", "brokenset")
        routes = self.v1_routes()
        routes[f"{module.API_BASE}/datasets/brokenset/editions/time-series"] = [json.dumps(EDITION).encode()]
        routes[f"{module.API_BASE}/datasets/brokenset/editions/time-series/versions/1"] = [json.dumps(VERSION).encode()]
        routes[f"{module.DOWNLOAD_BASE}/brokenset/editions/time-series/versions/1.csv"] = [
            http_error(500, "brokenset csv")
        ]
        result, output, summary, _ = self.run_extractor(routes, "--state-file", str(self.state))
        self.assertEqual(result, 1)
        self.assertEqual(len(output), 2)
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["datasets"]["testset"]["status"], "ok")
        self.assertEqual(summary["datasets"]["brokenset"]["status"], "failed")
        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertEqual(set(state["emitted"]), {"testset"})

    def test_malformed_state_fails(self):
        self.state.write_text(json.dumps({"version": 99, "source": module.SOURCE}), encoding="utf-8")
        result, output, summary, _ = self.run_extractor(self.v1_routes(), "--state-file", str(self.state))
        self.assertEqual(result, 1)
        self.assertEqual(output, [])
        self.assertEqual(summary["health"], "failed")
        self.assertIn("incompatible", summary["error"])


if __name__ == "__main__":
    unittest.main()
