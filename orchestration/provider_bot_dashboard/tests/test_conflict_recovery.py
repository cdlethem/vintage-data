from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from airflow.providers.vintage.bot_dashboard.conflict_recovery import admit_conflict_repair
from airflow.providers.vintage.bot_dashboard.models import Event, Execution, ValidationGate, metadata
from airflow.providers.vintage.bot_dashboard.service import PreconditionFailed
from airflow.providers.vintage.bot_dashboard.revision_seed import get_seed
import test_execution_retry as fixtures


class ConflictRecoveryTest(unittest.TestCase):
    setUp = fixtures.ExecutionRetryTest.setUp
    tearDown = fixtures.ExecutionRetryTest.tearDown
    fixture = fixtures.ExecutionRetryTest.fixture

    def _published_candidate(self, session):
        task, row = self.fixture(session)
        task.state = "ready"
        task.blocked_from_state = None
        row.terminal_at = None
        row.dispatch_state = "running"
        row.stage = "reviewed"
        row.base_sha = "a" * 40
        row.source_artifact_sha256 = "b" * 64
        row.patch_sha256 = "c" * 64
        row.executor_report_sha256 = "d" * 64
        row.review_report_sha256 = "e" * 64
        row.verification_manifest = {"changed_paths": ["src/repair.py"], "old": "evidence"}
        row.provider = "github"
        row.repository = "owner/repo"
        row.target_branch = "main"
        row.branch = "bot-dashboard/candidate"
        row.pr_number = 132
        row.pr_url = "https://example.test/pull/132"
        row.service_account_id = "bot"
        row.trusted_head_sha = "f" * 40
        row.review_verdict = "approved"
        row.provider_state = {
            "review_verdict": "approved",
            "review_failure_kind": "format_failed",
            "review_repair": {"scope": "src/**"},
        }
        gate = ValidationGate(
            task_id=task.id, gate_key="candidate-check", stage="merge",
            recipe="manual", owner="operator", required_capability="operator",
            subject=row.trusted_head_sha, dependencies=[], status="passed",
            evidence={"subject": row.trusted_head_sha, "result": "old approval"},
            recheck_condition="before merge", required=True, recipe_args={"keep": "recipe"},
            lease_id="lease", lease_run_id="gate-run", attempt=2, last_error="old error",
        )
        session.add(gate)
        session.flush()
        return task, row, gate

    def _provider(self, head="f" * 40):
        provider = Mock()
        provider.read_change.return_value = {
            "provider": "github", "number": 132, "head_ref": "bot-dashboard/candidate",
            "base_ref": "main", "author_id": "bot", "head_sha": head,
            "state": "open", "draft": True, "mergeability": "conflicting",
            "base_sha": "a" * 40,
        }
        provider.read_base_identity.return_value = "1" * 40
        return provider

    def _admit(self, session, task, *, provider=None, key="conflict-132"):
        provider = provider or self._provider()
        config = SimpleNamespace(provider="github", project="owner/repo", base_branch="main")
        with patch("airflow.providers.vintage.bot_dashboard.conflict_recovery.load_repository_config", return_value=config), patch(
            "airflow.providers.vintage.bot_dashboard.conflict_recovery.get_provider", return_value=provider,
        ), patch("airflow.providers.vintage.bot_dashboard.conflict_recovery.require_executor_preconditions"), patch(
            "airflow.providers.vintage.bot_dashboard.conflict_recovery.model_for_role",
            return_value={"provider_id": "fixture", "model": "small", "base_url": "https://model.example/v1"},
        ), patch("airflow.providers.vintage.bot_dashboard.artifacts.read_artifact"):
            return admit_conflict_repair(
                session, str(task.id), version=task.version, actor_id="executive",
                actor_kind="system", idempotency_key=key,
            )

    def test_repair_rebases_on_fresh_base_preserves_evidence_and_invalidates_approval(self):
        with Session(self.engine) as session:
            task, row, gate = self._published_candidate(session)
            old_execution = row.execution_id
            old_run = row.target_run_id
            admitted = self._admit(session, task)
            repair = session.scalar(select(Execution).where(Execution.execution_id == admitted["execution_id"]))
            session.commit()
            self.assertEqual("pending", admitted["dispatch_state"])
            self.assertNotEqual(old_execution, repair.execution_id)
            self.assertNotEqual(old_run, repair.target_run_id)
            self.assertEqual(row.sequence + 1, repair.sequence)
            self.assertEqual("1" * 40, repair.base_sha)
            self.assertIsNone(repair.source_artifact_sha256)
            self.assertIsNone(repair.patch_sha256)
            self.assertIsNone(repair.review_verdict)
            self.assertIsNone(repair.pr_number)
            self.assertEqual("terminal", row.dispatch_state)
            self.assertTrue(repair.reviewer_required)
            self.assertEqual("superseded_merge_conflict", row.terminal_reason_code)
            self.assertEqual(132, row.pr_number)
            self.assertEqual("approved", row.review_verdict)
            self.assertEqual("accepted", task.state)
            self.assertEqual("pending_candidate", gate.subject)
            self.assertEqual("pending", gate.status)
            self.assertIsNone(gate.evidence)
            self.assertIsNone(gate.lease_id)
            self.assertIsNone(gate.lease_run_id)
            self.assertEqual(0, gate.attempt)
            self.assertIsNone(gate.last_error)
            self.assertEqual({"keep": "recipe"}, gate.recipe_args)
            event = session.scalar(select(Event).where(Event.event_type == "merge_conflict_repair_admitted"))
            seed = event.payload["revision_seed"]
            self.assertEqual("c" * 64, seed["patch_sha256"])
            self.assertEqual("b" * 64, seed["source_artifact_sha256"])
            self.assertEqual("d" * 64, seed["executor_report_sha256"])
            self.assertEqual("e" * 64, seed["review_report_sha256"])
            self.assertEqual("c" * 64, get_seed(session, repair)["patch_sha256"])
            self.assertEqual("format_failed", seed["review_failure_kind"])
            self.assertEqual("1" * 40, seed["repair_base_sha"])
            old_gate = event.payload["invalidated_candidate_gates"][0]
            self.assertEqual("passed", old_gate["status"])
            self.assertEqual({"subject": "f" * 40, "result": "old approval"}, old_gate["evidence"])

    def test_repair_requires_exact_dirty_head_and_current_scope(self):
        with Session(self.engine) as session:
            task, row, _ = self._published_candidate(session)
            with self.assertRaises(PreconditionFailed):
                self._admit(session, task, provider=self._provider(head="0" * 40))
            self.assertEqual("f" * 40, row.trusted_head_sha)
            self.assertEqual(132, row.pr_number)
            task.revisions[-1].allowed_path_globs = []
            with self.assertRaises(PreconditionFailed):
                self._admit(session, task)
            self.assertEqual("ready", task.state)

    def test_repair_replay_returns_the_same_admission_without_reobserving(self):
        with Session(self.engine) as session:
            task, row, _ = self._published_candidate(session)
            first = self._admit(session, task)
            provider = Mock()
            second = self._admit(session, task, provider=provider)
            self.assertEqual(first["execution_id"], second["execution_id"])
            self.assertEqual(first["sequence"], second["sequence"])
            provider.read_change.assert_not_called()


if __name__ == "__main__":
    unittest.main()
