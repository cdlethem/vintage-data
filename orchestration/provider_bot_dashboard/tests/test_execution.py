from __future__ import annotations

import unittest
import uuid
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from airflow.providers.vintage.bot_dashboard.execution import executor_preconditions, publish_execution_change
from airflow.providers.vintage.bot_dashboard.models import Execution, Policy, Revision, metadata
from airflow.providers.vintage.bot_dashboard.service import (
    PreconditionFailed,
    assign_task,
    create_manual_task,
    start_task,
)


class AdmissionTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)
        with Session(self.engine) as session:
            session.add(Policy(category="*", mode="manual"))
            created = create_manual_task(session, title="Change code", category="other", priority=1, planned_resolution="Change one file", actor_id="u1", actor_name="User")
            assigned = assign_task(session, created["id"], version=1, actor_id="u1", actor_name="User", kind="bot", profile="junior")
            session.commit()
            self.task = assigned

    def tearDown(self):
        self.engine.dispose()

    def test_start_rejects_unverified_bot_admission_then_is_idempotent(self):
        with Session(self.engine) as session:
            with self.assertRaises(PreconditionFailed):
                start_task(session, self.task["id"], version=2, actor_id="u1", idempotency_key="client-key-1")
            revision = session.scalar(select(Revision).where(Revision.task_id == uuid.UUID(self.task["id"])))
            revision.verification_commands = [["python", "-m", "compileall"]]
            revision.allowed_path_globs = ["src/**"]
            revision.suggested_executor = "junior"
            first = start_task(session, self.task["id"], version=2, actor_id="u1", idempotency_key="client-key-1")
            replay = start_task(session, self.task["id"], version=3, actor_id="u1", idempotency_key="client-key-1")
            session.commit()
            self.assertEqual(first["admission"], replay["admission"])
            self.assertNotIn("execution_id", first["admission"])
            self.assertEqual(1, session.query(Execution).count())
            self.assertEqual("accepted", first["task"]["state"])

    def test_unconfigured_executor_fails_closed(self):
        with patch("airflow.providers.vintage.bot_dashboard.execution.conf.getboolean", return_value=True), patch.dict("os.environ", {}, clear=True):
            codes = executor_preconditions()
        self.assertIn("confinement_unverified", codes)
        self.assertIn("sandbox_launcher_missing", codes)
        self.assertIn("egress_policy_unverified", codes)


    def test_no_change_persists_manifest_without_git_publication(self):
        with Session(self.engine) as session:
            revision = session.scalar(select(Revision).where(Revision.task_id == uuid.UUID(self.task["id"])))
            revision.verification_commands = [["python", "-m", "compileall"]]
            revision.allowed_path_globs = ["src/**"]
            revision.suggested_executor = "junior"
            start_task(session, self.task["id"], version=2, actor_id="u1", idempotency_key="no-change-1")
            row = session.scalar(select(Execution).where(Execution.task_id == uuid.UUID(self.task["id"])))
            row.claimed_run_id = "executor-run"
            manifest = {"manifest_version": 1, "task_id": self.task["id"], "execution_id": row.execution_id, "revision": 1, "base_sha": "a" * 40, "changed_paths": [], "patch_sha256": "b" * 64, "patch_bytes": 0, "checks": []}
            result = publish_execution_change(session, dag_id="bot__task_executor", run_id="executor-run", executor_result={"status": "no_change", "verification_manifest": manifest, "report_sha256": "c" * 64})
            session.commit()
            self.assertEqual({"status": "no_change"}, result)
            self.assertEqual(manifest, session.scalar(select(Execution).where(Execution.execution_id == row.execution_id)).verification_manifest)
if __name__ == "__main__":
    unittest.main()
