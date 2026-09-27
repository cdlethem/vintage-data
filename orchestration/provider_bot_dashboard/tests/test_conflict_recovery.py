from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from airflow.providers.vintage.bot_dashboard.conflict_recovery import (
    admit_conflict_repair,
    conflict_repair_eligibility,
)
from airflow.providers.vintage.bot_dashboard.models import Event, Execution, ValidationGate, metadata, utcnow
from airflow.providers.vintage.bot_dashboard.service import PreconditionFailed, _event
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

    def _seeded_failure(self, session, *, seed_branch=None):
        task, source = self.fixture(session)
        source.execution_id = "d" * 64
        source.terminal_reason_code = "superseded_merge_conflict"
        source.terminal_at = source.terminal_at or utcnow()
        source.dispatch_state = "terminal"
        source.stage = "superseded_conflict_repair"
        source.provider = "github"
        source.repository = "owner/repo"
        source.target_branch = "main"
        source.branch = f"bot-dashboard/{task.id}/{source.sequence}-r{source.revision}"
        source.pr_number = 111
        source.pr_url = "https://example.test/pull/111"
        source.service_account_id = "bot"
        source.trusted_head_sha = "f" * 40
        task.state = "blocked"
        task.blocked_from_state = "in_progress"
        seed = {
            "base_sha": "a" * 40, "source_artifact_sha256": "b" * 64,
            "patch_sha256": "c" * 64, "provider": "github",
            "repository": "owner/repo", "target_branch": "main", "revision": source.revision,
            "execution_id": source.execution_id,
            "pr_number": 111, "pr_url": "https://example.test/pull/111",
            "trusted_head_sha": "f" * 40,
            "verification_manifest": {"changed_paths": ["src/repair.py"]},
        }
        if seed_branch is not None:
            seed["branch"] = seed_branch
        failed = Execution(
            execution_id="e" * 64, task_id=task.id, sequence=source.sequence + 1,
            revision=source.revision, idempotency_key="seed-apply-failure",
            target_run_id=f"task__{task.id}__{source.sequence + 1}__r{source.revision}",
            profile="senior", reviewer_required=True, dispatch_state="terminal",
            stage="terminal", terminal_at=utcnow(),
            terminal_reason_code="unresolved_revision_seed_conflict",
        )
        session.add(failed)
        _event(
            session, task, "execution_admitted", "system", "fixture",
            payload={"sequence": source.sequence, "execution_id": source.execution_id,
                     "revision_seed": seed},
        )
        _event(
            session, task, "revision_seed_recovered", "system", "fixture",
            payload={"sequence": failed.sequence, "execution_id": failed.execution_id,
                     "revision_seed": seed},
        )
        session.flush()
        return task, source, failed, seed

    def _provider(self, head="f" * 40, number=132, branch="bot-dashboard/candidate"):
        provider = Mock()
        provider.read_change.return_value = {
            "provider": "github", "number": number, "head_ref": branch,
            "base_ref": "main", "author_id": "bot", "head_sha": head,
            "state": "open", "draft": False, "mergeability": "conflicting",
            "base_sha": "a" * 40,
        }
        provider.read_base_identity.return_value = "1" * 40
        return provider

    def _admit(self, session, task, *, provider=None, key="conflict-132"):
        provider = provider or self._provider()
        config = SimpleNamespace(
            provider="github", project="owner/repo", base_branch="main",
            service_account_id="bot",
        )
        with patch("airflow.providers.vintage.bot_dashboard.conflict_recovery.load_repository_config", return_value=config), patch(
            "airflow.providers.vintage.bot_dashboard.conflict_recovery.get_provider", return_value=provider,
        ), patch("airflow.providers.vintage.bot_dashboard.conflict_recovery.require_executor_preconditions"), patch(
            "airflow.providers.vintage.bot_dashboard.conflict_recovery.model_for_role",
            return_value={"provider_id": "fixture", "model": "small", "base_url": "https://model.example/v1"},
        ), patch("airflow.providers.vintage.bot_dashboard.artifacts.read_artifact") as read_artifact:
            self.artifact_reads = read_artifact
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

    def test_terminal_seed_recovery_revalidates_old_pr_and_preserves_failed_execution(self):
        with Session(self.engine) as session:
            task, source, failed, seed = self._seeded_failure(session)
            eligibility = conflict_repair_eligibility(session, str(task.id))
            self.assertEqual(
                {"recoverable": True, "mode": "terminal_seed",
                 "execution_id": failed.execution_id, "seed_execution_id": "d" * 64,
                 "pr_number": 111, "requires_provider_revalidation": True},
                eligibility,
            )
            with self.assertRaises(PreconditionFailed):
                self._admit(
                    session, task,
                    provider=self._provider(
                        head="0" * 40, number=111, branch=source.branch,
                    ),
                    key="seeded-stale-head",
                )
            self.assertEqual("unresolved_revision_seed_conflict", failed.terminal_reason_code)
            provider = self._provider(number=111, branch=source.branch)
            admitted = self._admit(
                session, task, provider=provider, key="seeded-conflict",
            )
            repair = session.scalar(select(Execution).where(
                Execution.execution_id == admitted["execution_id"],
            ))
            self.assertEqual("unresolved_revision_seed_conflict", failed.terminal_reason_code)
            self.assertEqual("terminal", failed.dispatch_state)
            self.assertEqual(failed.sequence + 1, repair.sequence)
            self.assertEqual("1" * 40, repair.base_sha)
            self.assertEqual("github", repair.provider)
            self.assertEqual("owner/repo", repair.repository)
            self.assertIsNone(repair.pr_number)
            event = session.scalar(select(Event).where(
                Event.event_type == "merge_conflict_repair_admitted",
            ))
            self.assertEqual("terminal_seed", event.payload["mode"])
            self.assertEqual(failed.execution_id, event.payload["repaired_execution_id"])
            self.assertEqual(seed["execution_id"], event.payload["seed_execution_id"])
            provider.read_change.assert_called_once_with(111)
            self.assertEqual(2, self.artifact_reads.call_count)

    def test_in_progress_seed_apply_failure_follows_immutable_prior_seed_chain(self):
        with Session(self.engine) as session:
            task, source, failed, seed = self._seeded_failure(session)
            retry = Execution(
                execution_id="f" * 64, task_id=task.id, sequence=failed.sequence + 1,
                revision=1, idempotency_key="bare-retry",
                target_run_id=f"task__{task.id}__{failed.sequence + 1}__r1",
                profile="senior", reviewer_required=True, dispatch_state="terminal",
                stage="terminal", terminal_at=utcnow(),
                terminal_reason_code="revision_seed_apply_failed",
            )
            session.add(retry)
            task.state = "in_progress"
            task.blocked_from_state = None
            session.flush()
            eligibility = conflict_repair_eligibility(session, str(task.id))
            self.assertTrue(eligibility["recoverable"])
            self.assertEqual("terminal_seed", eligibility["mode"])
            self.assertEqual(retry.execution_id, eligibility["execution_id"])
            self.assertEqual(seed["execution_id"], eligibility["seed_execution_id"])
            provider = self._provider(number=111, branch=source.branch)
            admitted = self._admit(
                session, task, provider=provider, key="chain-seed-conflict",
            )
            repair = session.scalar(select(Execution).where(
                Execution.execution_id == admitted["execution_id"],
            ))
            self.assertEqual(retry.sequence + 1, repair.sequence)
            self.assertEqual("revision_seed_apply_failed", retry.terminal_reason_code)
            event = session.scalar(select(Event).where(
                Event.event_type == "merge_conflict_repair_admitted",
            ))
            self.assertEqual(retry.execution_id, event.payload["repaired_execution_id"])
            self.assertEqual(seed["execution_id"], event.payload["seed_execution_id"])

    def test_terminal_seed_recovery_preserves_recorded_branch_identity(self):
        with Session(self.engine) as session:
            branch = "bot-dashboard/recorded-seed"
            task, source, _, _ = self._seeded_failure(session, seed_branch=branch)
            source.branch = "bot-dashboard/different-source"
            provider = self._provider(number=111, branch=branch)
            self._admit(session, task, provider=provider, key="recorded-seed-branch")
            provider.read_change.assert_called_once_with(111)


    def test_legacy_seed_recovery_rejects_missing_or_mismatched_source_provenance(self):
        for case in ("missing_source", "mismatched_revision", "missing_branch"):
            with self.subTest(case=case), Session(self.engine) as session:
                task, source, _, _ = self._seeded_failure(session)
                if case == "missing_source":
                    session.delete(source)
                elif case == "mismatched_revision":
                    source.revision += 1
                else:
                    source.branch = None
                session.flush()
                provider = self._provider(number=111)
                with self.assertRaises(PreconditionFailed):
                    self._admit(session, task, provider=provider, key=f"invalid-{case}")
                provider.read_change.assert_not_called()

if __name__ == "__main__":
    unittest.main()
