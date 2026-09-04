import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import digest
from croniter import CroniterError


NOW = datetime(2026, 9, 4, 12, 0, 30, tzinfo=timezone.utc)


class CronGapMinutesTest(unittest.TestCase):
    def test_representative_schedules(self):
        cases = {
            "0-59/15 * * * *": 15,
            "30 1,9,17 * * *": 480,
            "0 8 * * 1": 10_080,
            "5 5 1 * *": 44_640,
        }
        for schedule, expected in cases.items():
            with self.subTest(schedule=schedule):
                self.assertEqual(digest.cron_gap_minutes(schedule, NOW), expected)

    def test_rejects_malformed_schedules(self):
        for schedule in (None, "not a cron", "* * * * * *", "x x x x x"):
            with self.subTest(schedule=schedule):
                with self.assertRaises((ValueError, CroniterError)):
                    digest.cron_gap_minutes(schedule, NOW)


class SourceHealthTest(unittest.TestCase):
    def test_schedule_controls_staleness_without_type_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_root = Path(tmp)
            manifest_dir = data_root / "raw" / "source=uncovered" / "dt=2026-09-04"
            manifest_dir.mkdir(parents=True)
            manifest = {
                "started_at": (NOW - timedelta(minutes=41)).isoformat(),
                "records": 10,
                "bytes": 100,
            }
            (manifest_dir / "run.meta.json").write_text(json.dumps(manifest))
            window_start = NOW - timedelta(hours=24)

            with mock.patch.object(digest, "DATA_ROOT", data_root):
                fast = digest.check_source(
                    "uncovered",
                    {"schedule": "0-59/15 * * * *"},
                    window_start,
                    now=NOW,
                )
                hourly = digest.check_source(
                    "uncovered",
                    {"schedule": "0 * * * *"},
                    window_start,
                    now=NOW,
                )

            self.assertEqual(fast["expected_gap_min"], 15)
            self.assertIs(fast["stale"], True)
            self.assertEqual(digest.classify(fast), "PROBLEM")
            self.assertEqual(hourly["expected_gap_min"], 60)
            self.assertIs(hourly["stale"], False)

    def test_invalid_schedule_is_isolated_and_disabled_sources_are_ok(self):
        with tempfile.TemporaryDirectory() as tmp:
            window_start = NOW - timedelta(hours=24)
            with mock.patch.object(digest, "DATA_ROOT", Path(tmp)):
                invalid = digest.check_source(
                    "invalid",
                    {"schedule": "bad", "enabled": "true"},
                    window_start,
                    now=NOW,
                )
                valid = digest.check_source(
                    "valid",
                    {"schedule": "*/30 * * * *", "enabled": "true"},
                    window_start,
                    now=NOW,
                )
                disabled = digest.check_source(
                    "disabled",
                    {"schedule": "bad", "enabled": "false"},
                    window_start,
                    now=NOW,
                )

            self.assertIn("schedule_error", invalid)
            self.assertEqual(digest.classify(invalid), "PROBLEM")
            self.assertEqual(valid["expected_gap_min"], 30)
            self.assertNotIn("schedule_error", valid)
            self.assertEqual(digest.classify(disabled), "OK")


class ConfigurationCoverageTest(unittest.TestCase):
    def test_every_source_schedule_has_a_positive_gap(self):
        configs = sorted(digest.SOURCES_DIR.glob("*.yml"))
        self.assertTrue(configs)
        for path in configs:
            cfg = digest.read_yml(path)
            with self.subTest(source=cfg.get("name", path.stem)):
                self.assertGreater(digest.cron_gap_minutes(cfg["schedule"], NOW), 0)

    def test_source_types_contains_no_cadence_overrides(self):
        metadata = json.loads((digest.HERE / "source_types.json").read_text())
        duplicates = [
            name
            for name, value in metadata.items()
            if isinstance(value, dict) and "expected_gap_minutes" in value
        ]
        self.assertEqual(duplicates, [])


if __name__ == "__main__":
    unittest.main()
