from __future__ import annotations

import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from airflow.providers.vintage.bot_dashboard.db_manager import BotDashboardDBManager
from airflow.providers.vintage.bot_dashboard.models import Event, Revision, RunReport, Task, metadata


class MigrationTest(unittest.TestCase):
    def _config(self, database: Path) -> Config:
        config = Config(BotDashboardDBManager.alembic_file)
        config.set_main_option("script_location", BotDashboardDBManager.migration_dir)
        config.set_main_option("sqlalchemy.url", f"sqlite:///{database}")
        return config

    def test_workflow_hardening_preserves_history_remaps_status_and_is_replayable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "dashboard.sqlite"
            config = self._config(database)
            command.upgrade(config, "0001_bot_dashboard")
            engine = create_engine(f"sqlite:///{database}")
            now = datetime.now(timezone.utc)
            task_id = uuid.uuid4()
            with Session(engine) as session:
                task = Task(id=task_id, source="manager", recommendation_key="legacy-task", title="Legacy", category="other", state="proposed", priority=1, planned_resolution="Inspect")
                session.add(task)
                session.flush()
                session.add(Revision(task_id=task_id, revision_number=1, title="Legacy", action="Inspect"))
                session.add(Event(task_id=task_id, sequence=1, event_type="created", actor_kind="system", actor_id="migration", to_state="proposed", payload={}, fingerprint="a" * 64))
                for try_number, status in ((1, "ok"), (2, "busy")):
                    session.add(RunReport(id=uuid.uuid4(), dag_id="bot__legacy", run_id="old-run", task_id="run", map_index=-1, try_number=try_number, bot_name="legacy", report_format="json", status=status, outcome="failed", retry_class="terminal", reason_code="legacy_import", started_at=now, finished_at=now, deadline_at=now, duration_ms=0, context_sha256="0" * 64, context_byte_count=0, context_build_ms=0, attempts_json=[], body_json=None, sha256=f"{try_number:064x}", byte_count=0, created_at=now, expires_at=now))
                session.commit()
            command.upgrade(config, "head")
            command.upgrade(config, "head")
            with Session(engine) as session:
                self.assertIsNotNone(session.get(Task, task_id))
                self.assertIsNotNone(session.scalar(select(Revision).where(Revision.task_id == task_id)))
                self.assertIsNotNone(session.scalar(select(Event).where(Event.task_id == task_id)))
                reports = session.scalars(select(RunReport).where(RunReport.run_id == "old-run").order_by(RunReport.try_number)).all()
                self.assertEqual(["succeeded", "capacity_unavailable"], [row.outcome for row in reports])
                self.assertEqual(["legacy_succeeded", "legacy_capacity"], [row.reason_code for row in reports])
                self.assertEqual([1, 2], [row.try_number for row in reports])
                self.assertEqual(2, len({row.try_number for row in reports}))
                tables = set(inspect(engine).get_table_names())
                self.assertIn("bot_dashboard_run_budget_claim", tables)
                self.assertIn("bot_dashboard_artifact", tables)
                self.assertIn("bot_dashboard_legacy_import", tables)
                columns = {column["name"] for column in inspect(engine).get_columns("bot_dashboard_execution")}
                self.assertTrue({"base_sha", "verification_manifest", "terminal_reason_code", "merged_at"} <= columns)
                report_columns = {column["name"] for column in inspect(engine).get_columns("bot_dashboard_run_report")}
                self.assertTrue({"budget_claim_id", "outcome", "try_number", "deadline_at"} <= report_columns)
                uniques = {
                    tuple(item["column_names"])
                    for item in inspect(engine).get_unique_constraints("bot_dashboard_run_report")
                }
                self.assertIn(
                    ("dag_id", "run_id", "task_id", "map_index", "try_number"), uniques
                )
                self.assertNotIn(("dag_id", "run_id", "task_id", "map_index"), uniques)
                session.add(
                    RunReport(
                        id=uuid.uuid4(), dag_id="bot__legacy", run_id="old-run", task_id="run",
                        map_index=-1, try_number=3, bot_name="legacy", report_format="json",
                        status="failed", outcome="failed", retry_class="terminal", reason_code="later_try",
                        started_at=now, finished_at=now, deadline_at=now, duration_ms=0,
                        context_sha256="0" * 64, context_byte_count=0, context_build_ms=0,
                        attempts_json=[], body_json=None, sha256="3" * 64, byte_count=0,
                        created_at=now, expires_at=now,
                    )
                )
                session.commit()
                session.add(
                    RunReport(
                        id=uuid.uuid4(), dag_id="bot__legacy", run_id="old-run", task_id="run",
                        map_index=-1, try_number=3, bot_name="legacy", report_format="json",
                        status="failed", outcome="failed", retry_class="terminal", reason_code="duplicate_try",
                        started_at=now, finished_at=now, deadline_at=now, duration_ms=0,
                        context_sha256="0" * 64, context_byte_count=0, context_build_ms=0,
                        attempts_json=[], body_json=None, sha256="4" * 64, byte_count=0,
                        created_at=now, expires_at=now,
                    )
                )
                with self.assertRaises(IntegrityError):
                    session.commit()
                session.rollback()
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
