from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from pathlib import Path

from airflow.providers.vintage.bot_dashboard.legacy_import import (
    LegacyInputError,
    _Root,
    _build_report,
    _safe_manifest_path,
    _summary,
)


class LegacyImportTest(unittest.TestCase):
    def test_safe_root_rejects_relative_escape_and_symlink_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "manager").mkdir(mode=0o700)
            report = root / "manager" / "run.json"
            report.write_text("{}", encoding="utf-8")
            report.chmod(0o600)
            with _Root(str(root), label="runs") as owned:
                self.assertEqual(b"{}", owned.read("manager/run.json", suffixes={".json"}, cap=100)[0])
                for unsafe in ("../escape.json", "/absolute.json", "manager\\run.json", "manager/../run.json"):
                    with self.subTest(unsafe=unsafe), self.assertRaises(LegacyInputError):
                        owned.read(unsafe, suffixes={".json"}, cap=100)
            link = root / "manager" / "link.json"
            link.symlink_to(report)
            with _Root(str(root), label="runs") as owned:
                with self.assertRaises(LegacyInputError):
                    owned.read("manager/link.json", suffixes={".json"}, cap=100)

    def test_report_identity_and_timestamp_are_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source_discovery" / "run.json"
            path.parent.mkdir()
            body = b'{"status":"skipped"}'
            path.write_bytes(body)
            path.chmod(0o600)
            item = {"relative_path": "source_discovery/20260907T120000Z-run.json", "body": body, "metadata": path.stat(), "sha256": hashlib.sha256(body).hexdigest(), "byte_count": len(body)}
            first = _build_report(item)
            second = _build_report(item)
            self.assertEqual(first, second)
            envelope, outcome, details = first
            self.assertEqual("skipped", outcome)
            self.assertEqual("legacy_skipped", envelope["reason_code"])
            self.assertIsNone(envelope["payload"], "legacy skipped reports must carry no bot payload")
            self.assertFalse(details["body_text"])

    def test_manifest_paths_are_relative_and_never_allow_roots(self):
        for value in ("../patch.patch", "/tmp/patch.patch", "reviews\\patch.patch", "", ".", "a/../b"):
            with self.subTest(value=value), self.assertRaises((LegacyInputError, ValueError)):
                _safe_manifest_path(value)
        self.assertEqual("patches/change.patch", _safe_manifest_path("patches/change.patch"))

    def test_summary_hash_and_pending_items_are_stable(self):
        item = {"kind": "review_patch", "relative_path": "patches/x.patch", "sha256": "a" * 64, "byte_count": 4}
        summary = _summary([item], {})
        self.assertEqual(1, summary["patch_count"])
        self.assertEqual(1, summary["pending_count"])
        self.assertEqual(summary, _summary([item], {}))


if __name__ == "__main__":
    unittest.main()
