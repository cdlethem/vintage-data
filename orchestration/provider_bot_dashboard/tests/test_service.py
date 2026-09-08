from __future__ import annotations

import unittest
import uuid
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from airflow.providers.vintage.bot_dashboard.models import Event, Execution, Policy, Revision, Task, metadata
from airflow.providers.vintage.bot_dashboard.maintenance import sync_provider
from airflow.providers.vintage.bot_dashboard.service import (
    DomainError,
    PreconditionFailed,
    add_event_items,
    create_manual_task,
    reconcile_manager,
    transition_task,
)


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)
        with Session(self.engine) as session:
            session.add(Policy(category="*", mode="manual"))
            session.commit()

    def tearDown(self):
        self.engine.dispose()

    def test_manual_lifecycle_requires_human_note_and_evidence(self):
        with Session(self.engine) as session:
            task = create_manual_task(session, title="Verify queue", category="other", priority=1, planned_resolution="Run the behavioral check", actor_id="u1", actor_name="User")
            session.commit()
            progressed = transition_task(session, task["id"], version=1, actor_id="u1", to_state="in_progress")
            session.commit()
            with self.assertRaises(PreconditionFailed):
                transition_task(session, task["id"], version=progressed["version"], actor_id="u1", to_state="completed")
            session.rollback()
            add_event_items(session, task["id"], version=progressed["version"], actor_id="u1", event_type="comment_added", items=["Completion note"])
            add_event_items(session, task["id"], version=progressed["version"] + 1, actor_id="u1", event_type="evidence_added", items=[{"label": "Run", "url": "/dags/check"}])
            completed = transition_task(session, task["id"], version=progressed["version"] + 2, actor_id="u1", to_state="completed")
            session.commit()
            self.assertEqual("completed", completed["state"])
            task_row = session.scalar(select(Task).where(Task.id == uuid.UUID(task["id"])))
            sequences = session.scalars(select(Event.sequence).where(Event.task_id == task_row.id).order_by(Event.sequence)).all()
            self.assertEqual(list(range(1, len(sequences) + 1)), sequences)

    def test_manager_v3_policy_reconciliation_is_idempotent_and_manual_by_default(self):
        report = {
            "schema_version": 3,
            "agent": "manager",
            "status": "degraded_evidence",
            "report_date": "2026-09-07",
            "executive_summary": "Queue check.",
            "plan": [{"recommendation_key": "repair-queue", "title": "Repair queue", "priority": 1, "category": "reliability", "action": "Repair it", "why_now": "It failed", "expected_benefit": "Reliable work", "resources": "One engineer", "risk": "Regression", "rollback": "Revert", "verification": "Exercise queue", "approval": "pending", "suggested_executor": "junior", "resurface": None}],
            "deferred": [{"item": "old", "reason": "stale", "revisit_when": "next run"}],
            "approvals_required": ["repair-queue"],
            "input_freshness": [],
        }
        with Session(self.engine) as session:
            first = reconcile_manager(session, report, dag_id="bot__manager", run_id="run-1")
            second = reconcile_manager(session, report, dag_id="bot__manager", run_id="run-1")
            session.commit()
            task = session.scalar(select(Task))
            self.assertEqual({"created": 1, "revised": 0, "resurfaced": 0}, first)
            self.assertEqual({"created": 0, "revised": 0, "resurfaced": 0}, second)
            self.assertEqual("proposed", task.state)
            self.assertEqual(1, session.query(Revision).filter(Revision.task_id == task.id).count())

    def test_illegal_transition_is_rejected(self):
        with Session(self.engine) as session:
            task = create_manual_task(session, title="Queue", category="other", priority=1, planned_resolution="Inspect", actor_id="u1", actor_name="User")
            with self.assertRaises(DomainError):
                transition_task(session, task["id"], version=1, actor_id="u1", to_state="ready")


    def test_maintenance_merges_once_and_emits_deterministic_follow_up(self):
        class Provider:
            def read_change(self, _number):
                return {"provider": "github", "number": 7, "url": "https://example/pr/7", "state": "merged", "draft": False, "head_sha": "a" * 40, "head_ref": "bot/task", "base_ref": "main", "author_id": "svc"}

        with Session(self.engine) as session:
            task = create_manual_task(session, title="Merge me", category="other", priority=1, planned_resolution="Do it", actor_id="u1", actor_name="User")
            task_row = session.scalar(select(Task).where(Task.id == uuid.UUID(task["id"])))
            task_row.state = "in_review"
            revision = session.scalar(select(Revision).where(Revision.task_id == task_row.id))
            revision.follow_up_bots = ["source_vetting"]
            session.add(Execution(execution_id="e" * 64, task_id=task_row.id, sequence=1, revision=1, idempotency_key="maintenance-1", target_run_id="run", profile="junior", reviewer_required=True, provider="github", repository="org/repo", target_branch="main", branch="bot/task", pr_number=7, service_account_id="svc", trusted_head_sha="a" * 40))
            session.commit()
            with patch("airflow.providers.vintage.bot_dashboard.maintenance.get_provider", return_value=Provider()):
                first = sync_provider(session)
                session.commit()
                second = sync_provider(session)
            self.assertEqual(1, first["checked"])
            self.assertEqual(1, first["changed"])
            self.assertEqual(1, len(first["follow_up_bots"]))
            self.assertEqual(first["follow_up_bots"][0]["trigger_run_id"], f"follow_up__{task_row.id}__1__source_vetting")
            self.assertEqual(0, second["checked"])
if __name__ == "__main__":
    unittest.main()
