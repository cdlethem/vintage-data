"""Focused tests for transform schedules, selectors, locking, and failures."""

from __future__ import annotations

import fcntl
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

INCLUDE = pathlib.Path(__file__).resolve().parent / "include"
sys.path.insert(0, str(INCLUDE))

import transform_runner


def lock_is_held(path: pathlib.Path) -> bool:
    """A separate open file description proves whether the flock is taken."""
    with path.open("a+") as probe:
        try:
            fcntl.flock(probe.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(probe.fileno(), fcntl.LOCK_UN)
        return False


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
        self.create_project = mock.patch.object(
            transform_runner,
            "_create_isolated_project",
            return_value={"project_dir": "fixture", "selected_models": ["fct_demo"]},
        )
        self.create_project.start()
        self.addCleanup(self.create_project.stop)
        self.write_scope = mock.patch.object(transform_runner, "_write_vintage_scope")
        self.write_scope.start()
        self.addCleanup(self.write_scope.stop)
        runtime_environment = mock.patch.object(transform_runner, "load_runtime_environment")
        runtime_environment.start()
        self.addCleanup(runtime_environment.stop)


    def test_runtime_requires_a_family(self):
        with self.assertRaises(TypeError):
            transform_runner.run("hourly")


    def test_publication_streams_inside_the_held_transform_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock = pathlib.Path(tmp) / "dbt.lock"
            observed = {}

            def worker(command, *, environment=None, lock_fd=None):
                observed["command"] = command
                observed["lock_fd"] = lock_fd
                observed["held"] = lock_is_held(lock)
                return subprocess.CompletedProcess(command, 0, '{"batch_id": "%s", "published": ["fct_demo"]}' % ("a" * 64), "")

            with mock.patch.dict(os.environ, {"LIGHTDASH_ENABLED": "1", "LIGHTDASH_STATE_ROOT": tmp}), \
                    mock.patch.object(transform_runner, "_run_command"), \
                    mock.patch.object(transform_runner, "_run_worker", side_effect=worker):
                result = transform_runner.run("hourly", family="demo", lock_path=lock)
            self.assertEqual(observed["command"][-2], "publish")
            self.assertTrue(observed["command"][-1].endswith("target/manifest.json"))
            self.assertIsInstance(observed["lock_fd"], int)
            self.assertTrue(observed["held"])
            self.assertEqual(result["publication"]["published"], ["fct_demo"])
            self.assertFalse(lock_is_held(lock))

    def test_cancelled_publication_reaps_its_process_group_and_releases_nothing_early(self):
        script = (
            "import os,sys,pathlib,time;"
            "pathlib.Path(sys.argv[1]).write_text(f'{os.getpid()} {os.fstat(int(sys.argv[2])).st_ino}');"
            "time.sleep(120)"
        )
        with tempfile.TemporaryDirectory() as tmp:
            lock = pathlib.Path(tmp) / "dbt.lock"
            marker = pathlib.Path(tmp) / "child.txt"

            def interrupt(*args, **kwargs):
                while not (marker.exists() and marker.read_text()):
                    time.sleep(0.01)
                raise KeyboardInterrupt

            with transform_runner.exclusive_lock(lock, 1) as lock_fd:
                with mock.patch.object(subprocess.Popen, "communicate", side_effect=interrupt):
                    with self.assertRaises(KeyboardInterrupt):
                        transform_runner._run_worker(
                            [sys.executable, "-c", script, str(marker), str(lock_fd)], lock_fd=lock_fd)
                pid, inode = marker.read_text().split()
                # The child really held the transform lock's own description.
                self.assertEqual(int(inode), lock.stat().st_ino)
                self.assertTrue(lock_is_held(lock))
                with self.assertRaises(ProcessLookupError):
                    os.kill(int(pid), 0)
            self.assertFalse(lock_is_held(lock))

    def test_each_job_builds_its_ancestor_tag_selector(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock = pathlib.Path(tmp) / "dbt.lock"
            for name in sorted(transform_runner.EXPECTED_JOBS):
                with self.subTest(name=name), mock.patch.object(
                    transform_runner, "_run_command"
                ) as run_command:
                    result = transform_runner.run(
                        name, family="demo", lock_path=lock
                    )
                    commands = [call.args[0] for call in run_command.call_args_list]
                    self.assertEqual(commands[0][:3], [str(transform_runner.TRANSFORM_ROOT / "bin" / "dbt"), "parse", "--target"])
                    self.assertEqual(commands[1][0].split("/")[-1], "validate_project")
                    self.assertEqual(commands[2][-2:], ["--select", f"+tag:{name}"])
                    self.assertEqual(result["status"], "ok")

    def test_sync_uses_the_published_batch_without_rebuilding(self):
        with mock.patch.object(transform_runner.subprocess, "run") as run:
            run.return_value.stdout = '{"status": "unchanged"}'
            result = transform_runner.sync_lightdash(
                {"publication": {"batch_id": "a" * 64, "published": ["fct_demo"], "stale": []}})
            self.assertEqual(result, {"status": "unchanged"})
            self.assertEqual(run.call_args.args[0][-2:], ["sync", "a" * 64])

    def test_sync_rejects_a_stale_publication(self):
        with mock.patch.object(transform_runner.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "stale"):
                transform_runner.sync_lightdash(
                    {"publication": {"batch_id": "a" * 64, "published": [], "stale": ["fct_demo"]}})
            run.assert_not_called()

    def test_disabled_sync_does_not_invoke_subprocess(self):
        with mock.patch.object(transform_runner.subprocess, "run") as run:
            self.assertEqual(transform_runner.sync_lightdash({"publication": None}), {"status": "disabled"})
            run.assert_not_called()

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
                    "hourly", family="demo", lock_path=pathlib.Path(tmp) / "dbt.lock"
                )


if __name__ == "__main__":
    unittest.main()
