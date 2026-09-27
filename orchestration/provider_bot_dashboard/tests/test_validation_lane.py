from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
import uuid
import unittest
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from airflow.providers.vintage.bot_dashboard.models import Artifact, Event, Execution, Task, ValidationGate, metadata, utcnow
from airflow.providers.vintage.bot_dashboard.service import PreconditionFailed, create_manual_task
from airflow.providers.vintage.bot_dashboard import validation_lane


SUBJECT = "a" * 40


class ValidationLaneTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)
        self.session = Session(self.engine)
        self.task = create_manual_task(
            self.session, title="Validation lane", category="other", priority=1,
            planned_resolution="Run the admitted validation", actor_id="user", actor_name="User",
        )

    def tearDown(self):
        self.session.close()
        self.engine.dispose()
    def gate(self, key="extractor", *, dependencies=None):
        row = ValidationGate(
            task_id=uuid.UUID(self.task["id"]), gate_key=key, stage="merge", recipe="dag_inspection",
            owner="validation-service", required_capability="candidate-tree-readonly", subject=SUBJECT,
            dependencies=dependencies or [], recipe_args={"command_id": "inspect", "dag_id": "source", "expected_tasks": ["extract"]},
            recheck_condition="candidate head changes", required=True,
        )
        self.session.add(row)
        self.session.flush()
        return row

    def _claim(self):
        return validation_lane.claim_pending(self.session, limit=20, runner_id="validation.runner", autopilot_enabled=True)

    def test_dependency_exact_subject_and_lifecycle_are_enforced(self):
        first = self.gate()
        second = self.gate("warehouse", dependencies=["extractor"])
        snapshot = {"head_sha": SUBJECT, "base_sha": "b" * 40, "source_artifact_sha256": "c" * 64,
                    "patch_artifact_sha256": "d" * 64, "patch_sha256": "d" * 64}
        with patch.object(validation_lane, "validate_admission"), patch.object(validation_lane, "_candidate_snapshot", return_value=snapshot):
            claimed = self._claim()
            self.assertEqual([str(first.id)], [item["id"] for item in claimed])
            active = validation_lane.start(
                self.session, gate_id=str(first.id), version=claimed[0]["version"], lease_id=claimed[0]["lease_id"],
                lease_run_id=claimed[0]["lease_run_id"], subject=SUBJECT, autopilot_enabled=True,
            )
            with self.assertRaises(PreconditionFailed):
                validation_lane.finish(
                    self.session, gate_id=str(first.id), version=active["version"], lease_id=claimed[0]["lease_id"],
                    lease_run_id=claimed[0]["lease_run_id"], subject="b" * 40, status="passed",
                    evidence={"observation": "wrong head"}, autopilot_enabled=True,
                )
            validation_lane.finish(
                self.session, gate_id=str(first.id), version=active["version"], lease_id=claimed[0]["lease_id"],
                lease_run_id=claimed[0]["lease_run_id"], subject=SUBJECT, status="passed",
                evidence={"observation": "exact head", "sha256": "e" * 64}, autopilot_enabled=True,
            )
            claimed = self._claim()
            self.assertEqual([str(second.id)], [item["id"] for item in claimed])

    def test_invalid_gate_is_failed_without_blocking_the_next_candidate(self):
        invalid = self.gate("invalid")
        valid = self.gate("valid")
        snapshot = {"head_sha": SUBJECT, "base_sha": "b" * 40, "source_artifact_sha256": "c" * 64,
                    "patch_artifact_sha256": "d" * 64, "patch_sha256": "d" * 64}
        with patch.object(validation_lane, "validate_admission", side_effect=[
                validation_lane.ValidationRecipeError("missing capability"), None,
        ]), patch.object(validation_lane, "_candidate_snapshot", return_value=snapshot):
            claimed = validation_lane.claim_pending(
                self.session, limit=1, runner_id="validation.runner", autopilot_enabled=True,
            )
        self.assertEqual([str(valid.id)], [item["id"] for item in claimed])
        self.assertEqual("failed", invalid.status)
        self.assertEqual("validation_admission_PreconditionFailed", invalid.last_error)

    def test_expired_lease_recovers_only_when_autopilot_is_enabled(self):
        row = self.gate()
        snapshot = {"head_sha": SUBJECT, "base_sha": "b" * 40, "source_artifact_sha256": "c" * 64,
                    "patch_artifact_sha256": "d" * 64, "patch_sha256": "d" * 64}
        with patch.object(validation_lane, "validate_admission"), patch.object(validation_lane, "_candidate_snapshot", return_value=snapshot):
            claimed = self._claim()
        row.lease_expires_at = utcnow() - timedelta(seconds=1)
        self.assertEqual(0, validation_lane.recover_expired(self.session, limit=10, autopilot_enabled=False))
        self.assertEqual("leased", row.status)
        self.assertEqual(1, validation_lane.recover_expired(self.session, limit=10, autopilot_enabled=True))
        self.assertEqual("pending", row.status)
        self.assertIsNone(row.lease_id)

    def test_future_candidate_waits_for_publication_without_consuming_an_attempt(self):
        row = self.gate()
        row.subject = "pending_candidate"
        self.session.flush()
        self.assertEqual([], self._claim())
        self.assertEqual(("pending", 0), (row.status, row.attempt))
        row.subject = SUBJECT
        snapshot = {"head_sha": SUBJECT, "base_sha": "b" * 40,
                    "source_artifact_sha256": "c" * 64, "patch_artifact_sha256": "d" * 64,
                    "patch_sha256": "d" * 64}
        with patch.object(validation_lane, "validate_admission"), patch.object(
                validation_lane, "_candidate_snapshot", return_value=snapshot):
            self.assertEqual([str(row.id)], [item["id"] for item in self._claim()])

    def test_failed_runner_backoff_preserves_evidence_and_stops_after_three_attempts(self):
        row = self.gate()
        row.status = "failed"; row.attempt = 2
        observed = {"label": "Validation runner failure", "reason_code": "source unavailable"}
        row.evidence = observed
        row.updated_at = utcnow() - timedelta(minutes=10)
        self.session.flush()
        self.assertEqual(0, validation_lane.recover_expired(self.session, limit=10, autopilot_enabled=True))
        row.updated_at = utcnow() - timedelta(minutes=16)
        self.assertEqual(1, validation_lane.recover_expired(self.session, limit=10, autopilot_enabled=True))
        self.assertEqual("pending", row.status)
        event = self.session.scalar(select(Event).where(Event.event_type == "validation_gate_retry_scheduled"))
        self.assertEqual(observed, event.payload["previous_evidence"])
        row.status = "leased"; row.attempt = 3
        row.lease_expires_at = utcnow() - timedelta(seconds=1)
        self.assertEqual(1, validation_lane.recover_expired(self.session, limit=10, autopilot_enabled=True))
        self.assertEqual("failed", row.status)
        self.assertEqual("validation_retry_exhausted", row.last_error)
        self.assertEqual(0, validation_lane.recover_expired(self.session, limit=10, autopilot_enabled=True))

    def test_completed_implementation_can_record_later_live_validation(self):
        row = self.gate()
        row.stage = "completion"
        task = self.session.get(Task, uuid.UUID(self.task["id"]))
        task.state = "completed"
        task.completed_at = utcnow()
        manifest = {
            "manifest_version": 1, "task_id": str(task.id), "execution_id": "e" * 64,
            "revision": 1, "base_sha": "b" * 40, "changed_paths": ["extract/source.py"],
            "patch_sha256": "d" * 64, "patch_bytes": 12, "checks": [],
        }
        self.session.add(Execution(
            task_id=task.id, execution_id="e" * 64, sequence=1, revision=1,
            idempotency_key="checked", target_run_id="checked", profile="senior",
            terminal_at=utcnow(), terminal_reason_code="merged", merged_at=utcnow(),
            trusted_head_sha=SUBJECT, base_sha="b" * 40,
            source_artifact_sha256="c" * 64, patch_sha256="d" * 64,
            provider="github", repository="org/repo", branch="bot/task",
            target_branch="main", service_account_id="42", pr_number=17,
            provider_state={"state": "merged", "head_sha": SUBJECT},
            verification_manifest=manifest,
        ))
        for kind, sha in (("source", "c" * 64), ("patch", "d" * 64)):
            self.session.add(Artifact(
                kind=kind, sha256=sha, media_type="application/octet-stream",
                byte_count=12, relative_path=f"test/{kind}", expires_at=utcnow() + timedelta(days=1),
            ))
        self.session.flush()
        provider = Mock()
        observed = {"provider": "github", "number": 17, "head_ref": "bot/task",
                    "base_ref": "main", "author_id": "42", "head_sha": SUBJECT, "state": "open"}
        provider.read_change.return_value = observed
        config = SimpleNamespace(provider="github", project="org/repo")
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.load_repository_config", return_value=config), \
                patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider), \
                patch.object(validation_lane, "validate_admission"):
            with self.assertRaisesRegex(PreconditionFailed, "trusted head changed"):
                validation_lane._candidate_snapshot(self.session, row)
            provider.read_change.return_value = {**observed, "state": "merged"}
            claimed = self._claim()
            self.assertEqual([str(row.id)], [item["id"] for item in claimed])
            running = validation_lane.start(
                self.session, gate_id=str(row.id), version=claimed[0]["version"],
                lease_id=claimed[0]["lease_id"], lease_run_id=claimed[0]["lease_run_id"],
                subject=SUBJECT, autopilot_enabled=True,
            )
            validation_lane.finish(
                self.session, gate_id=str(row.id), version=running["version"],
                lease_id=claimed[0]["lease_id"], lease_run_id=claimed[0]["lease_run_id"],
                subject=SUBJECT, status="passed",
                evidence={"observation": "Merged source emitted expected records"},
                autopilot_enabled=True,
            )
        self.assertEqual("completed", task.state)
        self.assertEqual("passed", row.status)
        event = self.session.scalar(select(Event).where(Event.event_type == "validation_gate_recorded"))
        self.assertEqual("passed", event.payload["status"])


if __name__ == "__main__":
    unittest.main()
