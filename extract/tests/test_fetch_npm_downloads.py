import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from datetime import datetime, timezone
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_npm_downloads.py"
SPEC = importlib.util.spec_from_file_location("fetch_npm_downloads", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

TODAY = datetime(2026, 11, 9, 5, 40, tzinfo=timezone.utc)
DAYS = ("2026-11-06", "2026-11-07", "2026-11-08")


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def http_error(code):
    return urllib.error.HTTPError("https://api.npmjs.org/downloads", code, "error", {}, None)


def bulk_document(null_package=None):
    doc = {}
    for i, pkg in enumerate(MODULE.PACKAGES):
        if pkg == null_package:
            doc[pkg] = None
            continue
        doc[pkg] = {
            "package": pkg,
            "start": DAYS[0],
            "end": DAYS[-1],
            "downloads": [{"downloads": 1000 + i + n, "day": day} for n, day in enumerate(DAYS)],
        }
    return doc


class FetchNpmDownloadsTests(unittest.TestCase):
    def run_main(self, side_effect):
        stdout, stderr = io.StringIO(), io.StringIO()
        fake_now = classmethod(lambda cls, tz=None: TODAY)
        with mock.patch.object(MODULE, "datetime", type("FakeDatetime", (), {"now": fake_now})), mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=side_effect
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep, mock.patch(
            "sys.stdout", stdout
        ), mock.patch("sys.stderr", stderr):
            code = MODULE.main([])
        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        summary = {}
        for line in stderr.getvalue().splitlines():
            if line.startswith(MODULE.SUMMARY_PREFIX):
                summary = json.loads(line[len(MODULE.SUMMARY_PREFIX):])
        return code, records, summary, urlopen, sleep

    def test_success_emits_one_record_per_package_day(self):
        code, records, summary, urlopen, sleep = self.run_main([Response(json.dumps(bulk_document()).encode())])
        self.assertEqual(code, 0)
        self.assertEqual(len(records), len(MODULE.PACKAGES) * len(DAYS))
        for record in records:
            self.assertEqual(record["source"], MODULE.SOURCE)
            self.assertEqual(record["fetched_at"], TODAY.isoformat())
            self.assertEqual(record["id"], f"{record['package']}:{record['day']}")
            self.assertIsInstance(record["downloads"], int)
            self.assertIn(record["package"], MODULE.PACKAGES)
            self.assertIn(record["day"], DAYS)
        # sorted by (package, day)
        self.assertEqual(records, sorted(records, key=lambda r: (r["package"], r["day"])))
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["records"], len(records))
        sleep.assert_not_called()
        self.assertEqual(urlopen.call_count, 1)

    def test_single_bulk_request_covers_full_window_and_packages(self):
        _, _, _, urlopen, _ = self.run_main([Response(json.dumps(bulk_document()).encode())])
        url = urlopen.call_args_list[0].args[0].full_url
        self.assertTrue(url.startswith("https://api.npmjs.org/downloads/range/2026-11-06:2026-11-08/"))
        requested = url.rsplit("/", 1)[1]
        self.assertEqual(requested.split(","), list(MODULE.PACKAGES))

    def test_retries_503_then_succeeds(self):
        code, records, summary, _, sleep = self.run_main([http_error(503), Response(json.dumps(bulk_document()).encode())])
        self.assertEqual(code, 0)
        self.assertEqual(len(records), len(MODULE.PACKAGES) * len(DAYS))
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS[0])

    def test_retries_read_timeout_then_succeeds(self):
        code, records, _, _, sleep = self.run_main([TimeoutError("timed out"), Response(json.dumps(bulk_document()).encode())])
        self.assertEqual(code, 0)
        self.assertEqual(len(records), len(MODULE.PACKAGES) * len(DAYS))
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS[0])

    def test_retries_429_with_backoff(self):
        code, _, _, _, sleep = self.run_main(
            [http_error(429), http_error(429), Response(json.dumps(bulk_document()).encode())]
        )
        self.assertEqual(code, 0)
        self.assertEqual(sleep.call_args_list, [mock.call(1.0), mock.call(2.0)])

    def test_404_fails_without_retry(self):
        code, records, summary, _, sleep = self.run_main([http_error(404), http_error(404)])
        self.assertEqual(code, 1)
        self.assertEqual(records, [])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        sleep.assert_not_called()

    def test_stale_package_fails_loudly_without_retry(self):
        code, records, summary, _, sleep = self.run_main([Response(json.dumps(bulk_document(null_package="react")).encode())])
        self.assertEqual(code, 1)
        self.assertEqual(records, [])
        self.assertEqual(summary["health"], "failed")
        self.assertIn("'react'", summary["error"])
        sleep.assert_not_called()

    def test_malformed_row_fails_without_retry(self):
        doc = bulk_document()
        doc["react"]["downloads"][0]["downloads"] = "lots"
        code, records, summary, _, sleep = self.run_main([Response(json.dumps(doc).encode())])
        self.assertEqual(code, 1)
        self.assertEqual(records, [])
        self.assertEqual(summary["health"], "failed")
        self.assertIn("'react'", summary["error"])
        sleep.assert_not_called()

    def test_exhausted_retries_fail_bounded(self):
        code, records, summary, urlopen, sleep = self.run_main([http_error(503), http_error(503), http_error(503)])
        self.assertEqual(code, 1)
        self.assertEqual(records, [])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_args_list, [mock.call(1.0), mock.call(2.0)])


if __name__ == "__main__":
    unittest.main()
