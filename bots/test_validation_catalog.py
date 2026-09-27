from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest

import validation_catalog


class PublicSmokeCatalogTest(unittest.TestCase):
    def run_extractor(self, code, *requirements):
        return subprocess.run(
            [sys.executable, "-c", validation_catalog._EXTRACTOR_ENVELOPE,
             *requirements, "", "", "", "", sys.executable, "-c", code],
            capture_output=True, text=True, timeout=10,
        )

    def test_envelope_rejects_non_string_open_library_author(self):
        record = {"source": "open_library", "id": "change-1", "kind": "add-book", "author": {"key": "/people/example"}}
        result = self.run_extractor(
            "print(" + repr(json.dumps(record)) + ")",
            "open_library", "open_library", "author", "kind", "add-book",
        )
        self.assertNotEqual(0, result.returncode)

    def test_envelope_accepts_integer_stable_identifiers(self):
        record = {"source": "sensor_community", "id": 123456, "country": "DE"}
        result = self.run_extractor(
            "print(" + repr(json.dumps(record)) + ")",
            "sensor_community", "sensor_community", "country", "", "",
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("123456", json.loads(result.stdout)["id"])

    def test_envelope_rejects_an_unterminated_line_at_the_byte_bound(self):
        result = self.run_extractor(
            "import sys; sys.stdout.buffer.write(b'x' * 65537)",
            "arxiv", "arxiv", "title", "", "",
        )
        self.assertNotEqual(0, result.returncode)

    def test_envelope_returns_bounded_extractor_stderr_on_nonzero_exit(self):
        marker = "source request failed"
        result = self.run_extractor(
            "import sys; sys.stderr.write(" + repr("x" * 10000 + marker) + "); sys.exit(7)",
            "arxiv", "arxiv", "title", "", "",
        )
        self.assertNotEqual(0, result.returncode)
        self.assertIn(marker, result.stderr)
        self.assertLessEqual(len(result.stderr.encode()), 2100)


    def test_fixed_csv_reference_requires_matching_candidate_events_and_pinned_coverage(self):
        csv_body = (
            b"NORAD_CAT_ID_1,NORAD_CAT_ID_2,TCA\n"
            b"1,2,2026-09-27T00:00:00\n3,4,2026-09-27T01:00:00\n"
        )

        class Reference(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(csv_body)))
                self.end_headers()
                self.wfile.write(csv_body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Reference)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}/source.csv"

            def compare(records, baseline="2"):
                script = "import json; [print(json.dumps(row)) for row in " + repr(records) + "]"
                return subprocess.run(
                    [sys.executable, "-c", validation_catalog._EXTRACTOR_ENVELOPE,
                     "events", "events", "", "", "", url, "200", baseline,
                     "NORAD_CAT_ID_1,NORAD_CAT_ID_2,TCA", sys.executable, "-c", script],
                    capture_output=True, text=True, timeout=10,
                    env={**os.environ, "NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"},
                )

            rows = [
                {"source": "events", "id": "1:2:2026-09-27T00:00:00"},
                {"source": "events", "id": "3:4:2026-09-27T01:00:00"},
            ]
            matched = compare(rows)
            self.assertEqual(0, matched.returncode, matched.stderr)
            self.assertEqual(2, json.loads(matched.stdout.splitlines()[-1])["matched_count"])
            self.assertIn("coverage differs", compare(rows[:1]).stderr)
            self.assertIn("coverage differs", compare([rows[0], {**rows[1], "id": "other"}]).stderr)
            self.assertIn("pinned coverage baseline", compare(rows, baseline="3").stderr)
        finally:
            server.shutdown()
            thread.join(timeout=10)
            server.server_close()

if __name__ == "__main__":
    unittest.main()
