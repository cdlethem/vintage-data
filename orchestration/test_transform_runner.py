"""Focused tests for transform schedules, selectors, locking, and failures."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

INCLUDE = pathlib.Path(__file__).resolve().parent / "include"
sys.path.insert(0, str(INCLUDE))

import transform_runner


class TransformConfigTest(unittest.TestCase):
    def test_exact_job_schedules_and_timeouts(self):
        jobs = transform_runner.load_jobs()
        self.assertEqual(
            {
                name: (cfg["schedule"], cfg["timeout_minutes"], cfg["lock_wait_seconds"])
                for name, cfg in jobs.items()
            },
            {
                "twice_hourly": ("7,37 * * * *", 25, 300),
                "hourly": ("17 * * * *", 50, 300),
                "daily": ("47 3 * * *", 240, 300),
            },
        )

    def test_invalid_name_cron_and_duplicate_schedule_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "jobs.yml"
            path.write_text(
                "jobs:\n"
                "  - {name: twice_hourly, schedule: '17 * * * *', timeout_minutes: 1, lock_wait_seconds: 1}\n"
                "  - {name: hourly, schedule: '17 * * * *', timeout_minutes: 1, lock_wait_seconds: 1}\n"
                "  - {name: weekly, schedule: 'bad cron', timeout_minutes: 1, lock_wait_seconds: 1}\n"
            )
            with self.assertRaises(ValueError):
                transform_runner.load_jobs(path)


class TransformRunnerTest(unittest.TestCase):
    def setUp(self):
        # Unit tests must not inherit this machine's production publication state.
        env = mock.patch.dict(os.environ, {"LIGHTDASH_ENABLED": "0"})
        env.start()
        self.addCleanup(env.stop)

    def test_enabled_publication_captures_isolated_build(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(
            os.environ, {"LIGHTDASH_ENABLED": "1", "LIGHTDASH_STATE_ROOT": tmp}
        ), mock.patch.object(transform_runner, "_run_command") as command, mock.patch.object(
            transform_runner.subprocess, "run"
        ) as capture:
            capture.return_value.stdout = '{"batch_id": "fixture"}'
            result = transform_runner.run("daily", lock_path=pathlib.Path(tmp) / "dbt.lock")
            commands = [call.args[0] for call in command.call_args_list]
            target = commands[0][commands[0].index("--target-path") + 1]
            self.assertTrue(pathlib.Path(target).is_relative_to(tmp))
            self.assertEqual(commands[1][-1], str(pathlib.Path(target) / "manifest.json"))
            self.assertEqual(commands[2][commands[2].index("--target-path") + 1], target)
            self.assertEqual(capture.call_args.args[0][-2:], ["capture", commands[1][-1]])
            self.assertEqual(result["publication"], {"batch_id": "fixture"})

    def test_publisher_retry_does_not_rebuild(self):
        with mock.patch.object(transform_runner.subprocess, "run") as run:
            run.return_value.stdout = '{"published": ["fct_demo"]}'
            result = transform_runner.publish_marts({"publication": {"batch_id": "a" * 64}})
            self.assertEqual(result["published"], ["fct_demo"])
            self.assertEqual(run.call_args.args[0][-2:], ["publish", "a" * 64])

    def test_disabled_publication_does_not_invoke_subprocess(self):
        with mock.patch.object(transform_runner.subprocess, "run") as run:
            self.assertEqual(transform_runner.publish_marts({"publication": None}), {"status": "disabled"})
            run.assert_not_called()

    def test_each_job_builds_its_ancestor_tag_selector(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock = pathlib.Path(tmp) / "dbt.lock"
            for name in sorted(transform_runner.EXPECTED_JOBS):
                with self.subTest(name=name), mock.patch.object(
                    transform_runner, "_run_command"
                ) as run_command:
                    result = transform_runner.run(name, lock_path=lock)
                    commands = [call.args[0] for call in run_command.call_args_list]
                    self.assertEqual(commands[0][-3:], ["parse", "--target", "prod"])
                    self.assertEqual(commands[1][0].split("/")[-1], "validate_project")
                    self.assertEqual(commands[2][-2:], ["--select", f"+tag:{name}"])
                    self.assertEqual(result["status"], "ok")

    def test_lock_timeout_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "dbt.lock"
            with transform_runner.exclusive_lock(path, 1):
                with self.assertRaises(TimeoutError):
                    with transform_runner.exclusive_lock(path, 0.01):
                        self.fail("second lock should not be acquired")

    def test_failed_command_propagates(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            transform_runner,
            "_run_command",
            side_effect=subprocess.CalledProcessError(1, ["dbt", "parse"]),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                transform_runner.run(
                    "hourly", lock_path=pathlib.Path(tmp) / "dbt.lock"
                )


if __name__ == "__main__":
    unittest.main()
