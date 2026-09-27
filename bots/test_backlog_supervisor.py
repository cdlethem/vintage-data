import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import backlog_supervisor as supervisor


class BacklogSupervisorTest(unittest.TestCase):
    def test_probe_surfaces_model_configuration_blocker(self):
        session = MagicMock()
        session.scalars.return_value.all.return_value = []
        session.scalar.side_effect = [0, None]
        with patch("airflow.utils.session.create_session") as factory, patch(
            "airflow.providers.vintage.bot_dashboard.autopilot.status",
            return_value={"enabled": True, "last_decision": None, "last_error": None,
                          "model_problem": "Connect a model provider and map executive in Model settings before starting"},
        ):
            factory.return_value.__enter__.return_value = session
            observation = supervisor.snapshot()
        self.assertEqual("Connect a model provider and map executive in Model settings before starting",
                         observation["executive_error"])

    def test_scheduling_stall_is_flagged_only_when_enabled_and_stale(self):
        from datetime import datetime, timedelta, timezone
        session = MagicMock()
        session.scalars.return_value.all.return_value = []
        fresh = datetime.now(timezone.utc) - timedelta(seconds=30)
        stale = datetime.now(timezone.utc) - timedelta(minutes=20)
        cases = [(True, fresh, False), (True, stale, True), (True, None, True), (False, stale, False)]
        for enabled, last_run_at, expected in cases:
            session.scalar.side_effect = [0, last_run_at]
            with self.subTest(enabled=enabled, last_run_at=last_run_at), patch(
                "airflow.utils.session.create_session"
            ) as factory, patch(
                "airflow.providers.vintage.bot_dashboard.autopilot.status",
                return_value={"enabled": enabled, "last_decision": None, "last_error": None, "model_problem": None},
            ):
                factory.return_value.__enter__.return_value = session
                observation = supervisor.snapshot()
            self.assertEqual(expected, observation["executive_scheduling_stalled"])

    def test_prompt_calls_out_a_stalled_executive_and_forbids_git_checkout(self):
        message = supervisor.prompt({"executive_scheduling_stalled": True, "open_count": 1})
        self.assertIn("bot__executive has not started a new DagRun", message)
        self.assertIn("git checkout", message)
        self.assertNotIn("URGENT", supervisor.prompt({"executive_scheduling_stalled": False, "open_count": 1}))

    def test_completion_requires_successful_empty_probe_and_no_execution(self):
        self.assertTrue(supervisor.drained({"open_count": 0, "active_executions": 0}))
        for observation in [{}, {"probe_error": "OperationalError"},
                            {"open_count": 1, "active_executions": 0},
                            {"open_count": 0, "active_executions": 1}]:
            self.assertFalse(supervisor.drained(observation))

    def test_queue_deduplication_is_scoped_to_thread_and_supervision(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.sqlite"
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE queued_items (thread_id TEXT,payload_json TEXT)")
                db.executemany("INSERT INTO queued_items VALUES (?,?)", [
                    ("target", json.dumps({"message": "unrelated work"})),
                    ("other", json.dumps({"message": supervisor.MARKER + " scheduled check]"})),
                ])
            self.assertFalse(supervisor.pending("target", path))
            with sqlite3.connect(path) as db:
                db.execute("INSERT INTO queued_items VALUES (?,?)", ("target", supervisor.MARKER))
            self.assertTrue(supervisor.pending("target", path))

    def test_pending_check_does_not_pile_up_or_hide_new_arrivals(self):
        state = {"enabled": True, "thread_id": "target", "baseline_ids": ["one", "two"]}
        observation = {"open_count": 2, "active_executions": 1, "open_ids": ["two", "new"], "observed_at": "now"}
        with patch.object(supervisor, "snapshot", return_value=observation), patch.object(supervisor, "pending", return_value=True), patch.object(supervisor.subprocess, "run") as run:
            supervisor.check(state, enqueue=True)
        run.assert_not_called()
        self.assertEqual(1, state["baseline_remaining"])
        self.assertTrue(state["enabled"])

    def test_database_failure_queues_investigation_without_leaking_or_stopping(self):
        state = {"enabled": True, "thread_id": "target"}
        with patch.object(supervisor, "snapshot", side_effect=RuntimeError("secret-database-url")), patch.object(supervisor, "pending", return_value=False), patch.object(supervisor.shutil, "which", return_value="/bin/codex"), patch.object(supervisor.subprocess, "run") as run:
            run.return_value.returncode = 0
            supervisor.check(state, enqueue=True)
        argv = run.call_args.args[0]
        self.assertEqual(["/bin/codex", "queue", "--thread", "target", "--message"], argv[:5])
        self.assertNotIn("secret-database-url", argv[-1])
        self.assertTrue(state["enabled"])
        self.assertEqual("RuntimeError", state["last_check"]["probe_error"])

    def test_empty_queue_marks_completion_and_bounds_history(self):
        state = {"enabled": True, "thread_id": "target", "history": [{}] * 144}
        with patch.object(supervisor, "snapshot", return_value={"open_count": 0, "active_executions": 0, "open_ids": [], "observed_at": "now"}):
            supervisor.check(state, enqueue=False)
        self.assertFalse(state["enabled"])
        self.assertEqual("now", state["completed_at"])
        self.assertEqual(144, len(state["history"]))

    def test_authorized_tuning_continues_from_empty_baseline_once(self):
        state = {"enabled": True, "thread_id": "target", "concurrency_experiment": {
            "enabled": True, "phase": "draining"}}
        empty = {"open_count": 0, "active_executions": 0, "open_ids": [], "observed_at": "first"}
        with patch.object(supervisor, "snapshot", return_value=empty):
            supervisor.check(state, enqueue=False)
        self.assertTrue(state["enabled"])
        self.assertEqual("baseline", state["concurrency_experiment"]["phase"])
        empty["observed_at"] = "second"
        with patch.object(supervisor, "snapshot", return_value=empty):
            supervisor.check(state, enqueue=False)
        self.assertEqual("first", state["concurrency_experiment"]["baseline_started_at"])
        state["concurrency_experiment"]["enabled"] = False
        with patch.object(supervisor, "snapshot", return_value=empty):
            supervisor.check(state, enqueue=False)
        self.assertFalse(state["enabled"])

    def test_tuning_cannot_claim_baseline_from_failed_probe(self):
        state = {"enabled": True, "thread_id": "target", "concurrency_experiment": {
            "enabled": True, "phase": "draining"}}
        with patch.object(supervisor, "snapshot", side_effect=RuntimeError("private")):
            supervisor.check(state, enqueue=False)
        self.assertEqual("draining", state["concurrency_experiment"]["phase"])


if __name__ == "__main__":
    unittest.main()
