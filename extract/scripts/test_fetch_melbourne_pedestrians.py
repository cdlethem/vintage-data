#!/usr/bin/env python3
"""Offline behavioral tests for the Melbourne pedestrian snapshot extractor."""
import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch


SCRIPT = pathlib.Path(__file__).with_name("fetch_melbourne_pedestrians.py")
SPEC = importlib.util.spec_from_file_location("fetch_melbourne_pedestrians", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def row(minutes_ago=1):
    return {
        "recordid": "synthetic-1",
        "fields": {
            "sensing_datetime": (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat(),
            "location_id": 42,
            "total_of_directions": 7,
        },
    }


class MelbourneSummaryTests(unittest.TestCase):
    def run_extractor(self, document, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            patch.object(module.urllib.request, "urlopen", return_value=Response(json.dumps(document).encode())),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            result = module.main(list(args))
        return result, stdout.getvalue().splitlines(), stderr.getvalue().splitlines()

    def assert_summary(self, lines, health, completeness, records):
        self.assertEqual(len(lines), 1, lines)
        self.assertTrue(lines[0].startswith(module.SUMMARY_PREFIX), lines)
        summary = json.loads(lines[0][len(module.SUMMARY_PREFIX):])
        self.assertEqual((summary["health"], summary["completeness"], summary["records"]),
                         (health, completeness, records))
        return summary

    def test_success_emits_one_complete_summary_and_advances_watermark(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, output, errors = self.run_extractor([row()], "--state-file", str(state))
            self.assertEqual(result, 0)
            self.assertEqual(len(output), 1)
            record = json.loads(output[0])
            self.assertEqual(record["id"], "synthetic-1")
            self.assertEqual(record["source"], module.SOURCE)
            summary = self.assert_summary(errors, "healthy", "complete", 1)
            self.assertEqual(summary["coverage"]["watermark_after"], record["sensing_datetime"])
            self.assertEqual(json.loads(state.read_text())["sensing_datetime"], record["sensing_datetime"])

    def test_bad_download_fails_with_one_summary_and_no_state_change(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, output, errors = self.run_extractor([row(), {"recordid": "incomplete", "fields": {}}],
                                                         "--state-file", str(state))
            self.assertEqual(result, 1)
            self.assertEqual(output, [])
            self.assert_summary(errors, "failed", "failed", 0)
            self.assertFalse(state.exists())

    def test_unreadable_state_fails_with_one_summary_and_no_download(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            state.write_text("not json")
            with patch.object(module, "_download", side_effect=AssertionError("unexpected download")) as download:
                result, output, errors = self.run_extractor([], "--state-file", str(state))
            self.assertEqual(result, 1)
            self.assertEqual(output, [])
            self.assert_summary(errors, "failed", "failed", 0)
            download.assert_not_called()
            self.assertEqual(state.read_text(), "not json")

    def test_failed_watermark_save_never_emits_complete_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            with patch.object(module.os, "replace", side_effect=OSError("state unavailable")):
                result, output, errors = self.run_extractor([row()], "--state-file", str(state))
            self.assertEqual(result, 1)
            self.assertEqual(len(output), 1)
            self.assert_summary(errors, "failed", "failed", 1)
            self.assertFalse(state.exists())

    def test_interruption_does_not_report_success_or_advance_state(self):
        def interrupted(*args):
            yield {"source": module.SOURCE, "id": "synthetic-1", "fetched_at": "2026-09-28T00:00:00Z"}
            raise KeyboardInterrupt

        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            stdout, stderr = io.StringIO(), io.StringIO()
            with (
                patch.object(module, "fetch_counts", side_effect=interrupted),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    module.main(["--state-file", str(state)])
            self.assertEqual(len(stdout.getvalue().splitlines()), 1)
            self.assertEqual(stderr.getvalue(), "")
            self.assertFalse(state.exists())


if __name__ == "__main__":
    unittest.main()
