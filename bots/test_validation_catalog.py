from __future__ import annotations

import json
import subprocess
import sys
import unittest

import validation_catalog


class PublicSmokeCatalogTest(unittest.TestCase):
    def run_extractor(self, code, *requirements):
        return subprocess.run(
            [sys.executable, "-c", validation_catalog._EXTRACTOR_ENVELOPE,
             *requirements, sys.executable, "-c", code],
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


if __name__ == "__main__":
    unittest.main()
