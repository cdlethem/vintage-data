from __future__ import annotations

import json
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from airflow.models.variable import Variable
from airflow.providers.vintage.bot_dashboard import autopilot as ap
from airflow.providers.vintage.bot_dashboard.models import Event, Execution, Policy, Revision, Task, metadata
from airflow.providers.vintage.bot_dashboard.service import Conflict, PreconditionFailed, create_manual_task, patch_task


class AutopilotTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)
        Variable.__table__.create(self.engine)
        self.session = Session(self.engine)
        self.session.add(Policy(category="*", mode="manual"))
        self.session.commit()
        self.mapping = patch.object(ap, "_model", return_value={"model": "openai-codex/gpt-6-astra", "provider_id": "gateway", "base_url": "https://model.example/v1"})
        self.mapping.start()

    def tearDown(self):
        self.mapping.stop()
        self.session.close()
        self.engine.dispose()

    def task(self, state="proposed"):
        value = create_manual_task(self.session, title="Verify source cadence", category="cadence", priority=1,
                                  planned_resolution="Verify cadence against the source evidence", actor_id="owner", actor_name="Owner")
        task = self.session.get(Task, uuid.UUID(value["id"]))
        task.state = state
        self.session.commit()
        return task

    def enable(self):
        current = ap.status(self.session)
        ap.set_enabled(self.session, ap.Toggle(enabled=True, version=current["version"]), "owner")
        self.session.commit()

    def decide(self, claim, action, **kwargs):
        result = ap.decide(self.session, ap.Decision(lease_id=claim["lease_id"], action=action,
            rationale="Current source evidence supports this next step and preserves independent review.", **kwargs))
        self.session.commit()
        return result

    def test_off_by_default_and_toggle_versions(self):
        self.task()
        self.assertEqual("off", ap.claim(self.session)["status"])
        self.enable()
        with self.assertRaises(Conflict):
            ap.set_enabled(self.session, ap.Toggle(enabled=False, version=0), "owner")

    def test_unassigned_ticket_requires_assignment_before_start(self):
        task = self.task("accepted")
        self.enable()
        claim = ap.claim(self.session)
        self.assertIn("assign", claim["actions"])
        self.assertNotIn("start", claim["actions"])
        with patch.object(ap, "model_for_role") as model:
            with self.assertRaisesRegex(PreconditionFailed, "Assign an allowed bot profile"):
                ap._perform(self.session, task, SimpleNamespace(action="start"), {})
            model.assert_not_called()
        self.decide(claim, "assign", profile="senior")
        detail = ap._snapshot(self.session, str(task.id))
        self.assertIn("start", ap._actions(detail))
        self.assertEqual([], self.session.scalars(select(Execution)).all())

    def test_human_assignment_never_offers_model_execution(self):
        task = self.task("accepted")
        task.assignee_kind = "human"
        task.assignee_profile = None
        self.session.commit()
        for state in ("accepted", "blocked", "in_review"):
            detail = ap._snapshot(self.session, str(task.id))
            detail["state"] = state
            self.assertNotIn("start", ap._actions(detail))
            self.assertNotIn("revise", ap._actions(detail))

    def test_review_retry_does_not_offer_revision_without_a_new_plan(self):
        task = self.task("in_review")
        task.assignee_kind = "bot"
        task.assignee_profile = "senior"
        self.session.commit()
        execution = self.execution(task, review_verdict="unable_to_review")
        self.enable()
        claim = ap.claim(self.session)
        self.assertIn("configure", claim["actions"])
        self.assertNotIn("revise", claim["actions"])
        with self.assertRaisesRegex(ap.service.DomainError, "not available"):
            self.decide(claim, "revise")
        self.assertEqual(1, execution.revision)
        self.assertEqual("unable_to_review", execution.review_verdict)
        patch_task(self.session, str(task.id), version=task.version, actor_id="owner",
                   changes={"planned_resolution": "Address a newly documented implementation finding"})
        self.session.commit()
        self.assertIn("revise", ap._actions(ap._snapshot(self.session, str(task.id))))
        self.assertEqual(1, execution.revision)
        self.assertEqual("unable_to_review", execution.review_verdict)

    def test_blocked_start_requires_retryable_unpublished_execution(self):
        task = self.task("blocked")
        task.assignee_kind = "bot"
        task.assignee_profile = "senior"
        task.blocked_from_state = "in_progress"
        self.session.commit()
        self.assertNotIn("start", ap._actions(ap._snapshot(self.session, str(task.id))))
        from airflow.providers.vintage.bot_dashboard.models import utcnow
        execution = self.execution(task, stage="terminal", dispatch_state="terminal",
                                   terminal_at=utcnow(), terminal_reason_code="execution_blocked",
                                   pr_number=None, pr_url=None)
        self.assertIn("start", ap._actions(ap._snapshot(self.session, str(task.id))))
        execution.pr_number = 17
        self.session.commit()
        self.assertNotIn("start", ap._actions(ap._snapshot(self.session, str(task.id))))
        execution.pr_number = None
        execution.terminal_reason_code = "no_change"
        self.session.commit()
        self.assertNotIn("start", ap._actions(ap._snapshot(self.session, str(task.id))))

    def test_exhausted_plan_offers_configuration_before_another_retry(self):
        task = self.task("accepted")
        task.assignee_kind = "bot"
        task.assignee_profile = "senior"
        self.session.commit()
        from airflow.providers.vintage.bot_dashboard.models import utcnow
        for sequence in range(1, 4):
            self.execution(task, sequence=sequence, execution_id=str(sequence) * 64,
                           idempotency_key=f"attempt-{sequence}", stage="terminal",
                           dispatch_state="terminal", terminal_at=utcnow(),
                           terminal_reason_code="execution_blocked", pr_number=None, pr_url=None)
        actions = ap._actions(ap._snapshot(self.session, str(task.id)))
        self.assertIn("configure", actions)
        self.assertNotIn("start", actions)
        patch_task(self.session, str(task.id), version=task.version, actor_id="owner",
                   changes={"planned_resolution": "Resolve the diagnosed environment mismatch"})
        self.session.commit()
        self.assertIn("start", ap._actions(ap._snapshot(self.session, str(task.id))))

    def test_claim_replay_recovers_lost_response_without_a_second_decision(self):
        self.task(); self.enable()
        owner = {"dag_id": "bot__executive", "run_id": "run-one", "task_id": "run", "map_index": -1}
        with patch.object(ap, "_owner_failed", return_value=False):
            first = ap.claim(self.session, owner); self.session.commit()
            again = ap.claim(self.session, owner)
            other = ap.claim(self.session, {**owner, "run_id": "run-two"})
        self.assertEqual(first["lease_id"], again["lease_id"])
        self.assertEqual("busy", other["status"])

    def test_unmapped_model_pauses_claims_without_changing_human_controls(self):
        task = self.task(); self.enable()
        with patch.object(ap, "_model", side_effect=PreconditionFailed(
                "Connect a model provider and map executive in Model settings before starting")):
            for _ in range(2):
                self.assertEqual({"status": "model_unavailable"}, ap.claim(self.session))
                self.session.commit()
            state = ap.status(self.session)
            self.assertTrue(state["enabled"])
            self.assertIsNotNone(state["last_checked_at"])
            self.assertIn("map executive", state["model_problem"])
            self.assertEqual(0, state["active_decisions"])
            self.assertEqual("proposed", task.state)
        # Normal claims resume once the mapping is restored through normal settings.
        self.assertEqual("claimed", ap.claim(self.session)["status"])

    def test_any_mapped_model_is_accepted_for_the_executive_role(self):
        from airflow.models.connection import Connection
        Connection.__table__.create(self.engine)
        self.session.add(Connection(conn_id="bot_dashboard_model_gateway", conn_type="generic",
                                    extra=json.dumps({"name": "Gateway", "base_url": "https://model.example/v1"})))
        self.session.add(Variable(key="bot_dashboard_model_assignments",
                                  val=json.dumps([{"role": "executive", "provider_id": "gateway",
                                                   "model": "anthropic/claude-sonnet-4"}])))
        self.session.commit()
        self.task(); self.enable()
        self.mapping.stop()
        try:
            self.assertEqual("anthropic/claude-sonnet-4", ap._model(self.session)["model"])
            self.assertIsNone(ap.status(self.session)["model_problem"])
            self.assertEqual("claimed", ap.claim(self.session)["status"])
        finally:
            self.mapping.start()

    def test_failed_owner_is_recovered_without_waiting_for_lease_expiry(self):
        first = self.task(); second = self.task(); self.enable()
        owner = {"dag_id": "bot__executive", "run_id": "run-one", "task_id": "run", "map_index": -1}
        claim = ap.claim(self.session, owner); self.session.commit()
        with patch.object(ap, "_owner_failed", return_value=True):
            resumed = ap.claim(self.session, {**owner, "run_id": "run-two"})
        self.assertEqual("claimed", resumed["status"])
        self.assertNotEqual(claim["task"]["id"], resumed["task"]["id"])
        self.assertEqual("proposed", first.state)
        self.assertEqual("proposed", second.state)

    def test_finishing_work_has_priority_but_new_work_gets_aging_cycles(self):
        from datetime import datetime, timezone
        proposed = self.task(); ready = self.task("ready"); self.enable()
        with patch.object(ap, "utcnow", return_value=datetime(2026, 9, 17, 16, 1, tzinfo=timezone.utc)):
            value = ap.claim(self.session)
        self.assertEqual(str(ready.id), value["task"]["id"])
        self.session.rollback()
        with patch.object(ap, "utcnow", return_value=datetime(2026, 9, 17, 16, 5, tzinfo=timezone.utc)):
            value = ap.claim(self.session)
        self.assertEqual(str(proposed.id), value["task"]["id"])

    def test_approved_review_precedes_an_unresolved_review(self):
        from datetime import datetime, timezone
        unresolved = self.task("in_review"); self.execution(unresolved, review_verdict="changes_requested")
        approved = self.task("in_review")
        self.execution(approved, execution_id="f" * 64, idempotency_key="approved-execution", target_run_id="approved-run", pr_number=18)
        self.enable()
        with patch.object(ap, "utcnow", return_value=datetime(2026, 9, 17, 16, 1, tzinfo=timezone.utc)):
            result = ap.claim(self.session)
        self.assertEqual(str(approved.id), result["task"]["id"])

    def test_prepared_ticket_gets_next_decision_without_automatic_admission(self):
        from datetime import datetime, timezone
        from airflow.providers.vintage.bot_dashboard.service import _event
        older = self.task("accepted")
        prepared = self.task("accepted")
        prepared.assignee_kind = "bot"; prepared.assignee_profile = "senior"
        _event(self.session, prepared, "executive_decision", "system", "executive",
               payload={"action": "configure", "result": "applied"})
        self.session.commit(); self.enable()
        with patch.object(ap, "utcnow", return_value=datetime(2026, 9, 17, 16, 1, tzinfo=timezone.utc)):
            result = ap.claim(self.session)
        self.assertEqual(str(prepared.id), result["task"]["id"])
        self.assertEqual("accepted", prepared.state)
        self.assertEqual([], self.session.scalars(select(Execution)).all())
        self.session.rollback()
        with patch.object(ap, "utcnow", return_value=datetime(2026, 9, 17, 16, 5, tzinfo=timezone.utc)):
            result = ap.claim(self.session)
        self.assertEqual(str(older.id), result["task"]["id"])
        self.session.rollback()
        _event(self.session, prepared, "executive_decision", "system", "executive",
               payload={"action": "configure", "result": "applied", "result_version": prepared.version,
                        "revisit_at": "2026-09-17T16:15:00+00:00"})
        self.session.commit()
        with patch.object(ap, "utcnow", return_value=datetime(2026, 9, 17, 16, 6, tzinfo=timezone.utc)):
            result = ap.claim(self.session)
        self.assertEqual(str(older.id), result["task"]["id"])

    def test_parallel_leases_are_distinct_replayable_and_independently_released(self):
        self.task(); self.task(); self.task(); self.enable()
        owners = [{"dag_id": "bot__executive", "run_id": "batch", "task_id": "run", "map_index": i} for i in range(3)]
        with patch.object(ap, "_concurrency", return_value=2), patch.object(ap, "_owner_failed", return_value=False):
            first = ap.claim(self.session, owners[0]); self.session.commit()
            second = ap.claim(self.session, owners[1]); self.session.commit()
            self.assertNotEqual(first["task"]["id"], second["task"]["id"])
            self.assertEqual(first["lease_id"], ap.claim(self.session, owners[0])["lease_id"])
            self.assertEqual("busy", ap.claim(self.session, owners[2])["status"])
            self.assertEqual(2, ap.status(self.session)["active_decisions"])
            self.decide(first, "accept")
            self.assertEqual(second["lease_id"], ap.claim(self.session, owners[1])["lease_id"])
            self.assertEqual("already_applied", self.decide(first, "accept")["status"])
            ap.failed(self.session, ap.FailedDecision(lease_id=second["lease_id"])); self.session.commit()
            self.assertEqual(0, ap.status(self.session)["active_decisions"])

    def test_parallel_failure_does_not_revoke_another_decision(self):
        self.task(); self.task(); self.enable()
        with patch.object(ap, "_concurrency", return_value=2):
            first = ap.claim(self.session); self.session.commit()
            second = ap.claim(self.session); self.session.commit()
        ap.failed(self.session, ap.FailedDecision(lease_id=first["lease_id"])); self.session.commit()
        self.assertEqual([second["task"]["id"]], ap.status(self.session)["task_ids"])
        self.assertEqual("applied", self.decide(second, "accept")["status"])

    def test_failed_mapped_owner_is_detected_before_sibling_dag_finishes(self):
        session = Mock()
        session.scalar.side_effect = ["failed"]
        owner = {"dag_id": "bot__executive", "run_id": "batch", "task_id": "run", "map_index": 3}
        self.assertTrue(ap._owner_failed(session, owner))
        self.assertEqual(1, session.scalar.call_count)
        params = session.scalar.call_args.args[0].compile().params
        self.assertIn(3, params.values())
        session.scalar.side_effect = ["running", "running"]
        self.assertFalse(ap._owner_failed(session, owner))

    def test_lower_limit_and_toggle_preserve_ticket_isolation(self):
        self.task(); self.task(); self.task(); self.enable()
        with patch.object(ap, "_concurrency", return_value=2):
            first = ap.claim(self.session); self.session.commit()
            second = ap.claim(self.session); self.session.commit()
        with patch.object(ap, "_concurrency", return_value=1):
            self.assertEqual("busy", ap.claim(self.session)["status"])
            self.decide(first, "accept")
            self.assertEqual("busy", ap.claim(self.session)["status"])
        ap.set_enabled(self.session, ap.Toggle(enabled=False, version=1), "owner"); self.session.commit()
        with self.assertRaises(Conflict): self.decide(second, "accept")
        self.assertEqual(0, ap.status(self.session)["active_decisions"])

    def test_legacy_lease_survives_upgrade_and_expiry_releases_only_expired_ticket(self):
        from datetime import timedelta
        first_task = self.task(); self.task(); self.enable()
        first = ap.claim(self.session); self.session.commit()
        row, state = ap._control(self.session)
        state["lease"] = state.pop("leases")[0]
        ap._save(self.session, row, state); self.session.commit()
        with patch.object(ap, "_concurrency", return_value=2):
            second = ap.claim(self.session); self.session.commit()
        self.assertNotEqual(first["task"]["id"], second["task"]["id"])
        row, state = ap._control(self.session)
        state["leases"][0]["expires_at"] = (ap.utcnow() - timedelta(minutes=1)).isoformat()
        ap._save(self.session, row, state); self.session.commit()
        with patch.object(ap, "_concurrency", return_value=2):
            replacement = ap.claim(self.session); self.session.commit()
        self.assertEqual(first["task"]["id"], replacement["task"]["id"])
        with self.assertRaises(Conflict): self.decide(first, "accept")
        self.decide(second, "accept")
        self.assertEqual(1, ap.status(self.session)["active_decisions"])

    def test_switch_off_revokes_pending_decision_without_changing_ticket(self):
        task = self.task()
        self.enable()
        claim = ap.claim(self.session)
        self.session.commit()
        ap.set_enabled(self.session, ap.Toggle(enabled=False, version=1), "owner")
        self.session.commit()
        with self.assertRaises(Conflict):
            self.decide(claim, "accept")
        self.assertEqual("proposed", task.state)

    def test_accept_is_separate_from_start_audited_and_idempotent(self):
        task = self.task()
        self.enable()
        claim = ap.claim(self.session)
        self.assertEqual("busy", ap.claim(self.session)["status"])
        self.session.commit()
        self.assertEqual("applied", self.decide(claim, "accept")["status"])
        self.assertEqual("accepted", task.state)
        self.assertEqual([], self.session.scalars(select(Execution)).all())
        self.assertEqual("already_applied", self.decide(claim, "accept")["status"])
        audit = self.session.scalar(select(Event).where(Event.event_type == "executive_decision"))
        self.assertEqual("openai-codex/gpt-6-astra", audit.payload["model"])
        changed = self.session.scalar(select(Event).where(Event.event_type == "state_changed"))
        self.assertEqual(("system", "executive"), (changed.actor_kind, changed.actor_id))

    def test_human_edit_invalidates_decision(self):
        task = self.task()
        self.enable()
        claim = ap.claim(self.session)
        self.session.commit()
        patch_task(self.session, str(task.id), version=task.version, actor_id="owner", changes={"planned_resolution": "Updated requirements"})
        self.session.commit()
        with self.assertRaises(Conflict): self.decide(claim, "accept")
        self.assertEqual("proposed", task.state)

    def execution(self, task, **changes):
        values = dict(execution_id="e" * 64, task_id=task.id, sequence=1, revision=1, idempotency_key="test-execution",
            target_run_id="test-run", profile="senior", reviewer_required=True, stage="reviewed", dispatch_state="running",
            provider="github", repository="org/repo", target_branch="main", branch="bot/task", service_account_id="42",
            pr_number=17, pr_url="https://github.com/org/repo/pull/17", trusted_head_sha="a" * 40, review_verdict="approved")
        row = Execution(**(values | changes)); self.session.add(row); self.session.commit(); return row

    def provider(self, **changes):
        provider = Mock()
        provider.config = SimpleNamespace(project="org/repo", provider="github")
        observation = dict(provider="github", number=17, head_sha="a" * 40, head_ref="bot/task", base_ref="main", author_id="42", state="open", draft=False)
        provider.read_change.return_value = observation | changes
        return provider

    def test_execution_evidence_change_rejects_old_decision(self):
        task = self.task("ready"); execution = self.execution(task)
        self.enable(); claim = ap.claim(self.session); self.session.commit()
        execution.review_verdict = "changes_requested"; self.session.commit()
        with self.assertRaises(Conflict): self.decide(claim, "merge")

    def test_drift_cannot_merge_and_reason_is_preserved(self):
        task = self.task("ready"); self.execution(task)
        self.enable(); claim = ap.claim(self.session); self.session.commit()
        provider = self.provider(head_sha="b" * 40)
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider):
            result = self.decide(claim, "merge")
        self.assertEqual("deferred", result["status"])
        provider.merge_change.assert_not_called()
        self.assertIn("changed", result["error"])
        self.assertEqual("ready", task.state)

    def test_merge_is_sha_pinned_and_does_not_skip_completion(self):
        task = self.task("ready"); self.execution(task)
        self.enable(); claim = ap.claim(self.session); self.session.commit()
        provider = self.provider()
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider):
            self.assertEqual("applied", self.decide(claim, "merge")["status"])
        provider.merge_change.assert_called_once_with(17, "a" * 40)
        provider.upsert_comment.assert_called_once()
        comment = provider.upsert_comment.call_args.args[2]
        self.assertIn("### Executive decision", comment)
        self.assertNotIn("Astra", comment)
        self.assertEqual("ready", task.state)

    def test_completion_waits_for_observed_merge(self):
        task = self.task("ready"); self.execution(task)
        self.enable(); claim = ap.claim(self.session); self.session.commit()
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=self.provider()):
            self.assertEqual("deferred", self.decide(claim, "complete")["status"])
        self.assertEqual("ready", task.state)

    def test_verified_no_change_completion_keeps_note_and_evidence(self):
        task = self.task("ready")
        from airflow.providers.vintage.bot_dashboard.models import utcnow
        self.execution(task, pr_number=None, pr_url=None, terminal_reason_code="no_change", terminal_at=utcnow(), stage="terminal")
        self.enable(); claim = ap.claim(self.session); self.session.commit()
        self.assertEqual("applied", self.decide(claim, "complete")["status"])
        self.assertEqual("completed", task.state)
        for kind in ["comment_added", "evidence_added"]:
            row = self.session.scalar(select(Event).where(Event.event_type == kind))
            self.assertEqual(("system", "executive"), (row.actor_kind, row.actor_id))

    def test_model_failure_cools_down_and_leaves_ticket_untouched(self):
        task = self.task(); self.enable(); claim = ap.claim(self.session); self.session.commit()
        ap.failed(self.session, ap.FailedDecision(lease_id=claim["lease_id"])); self.session.commit()
        self.assertEqual("idle", ap.claim(self.session)["status"])
        self.assertEqual("proposed", task.state)
        self.assertIn("executive", ap.status(self.session)["last_error"])

    def test_conflict_releases_stale_decision_and_requires_fresh_claim_after_one_minute(self):
        from datetime import timedelta
        task = self.task(); self.enable(); old = ap.claim(self.session); self.session.commit()
        task.version += 1
        task.planned_resolution = "Changed evidence requires reassessment."
        self.session.commit()
        with self.assertRaises(Conflict):
            self.decide(old, "accept")
        now = ap.utcnow()
        with patch.object(ap, "utcnow", return_value=now):
            ap.failed(self.session, ap.FailedDecision(lease_id=old["lease_id"], reason_code="decision_context_changed"))
            self.session.commit()
            self.assertEqual("idle", ap.claim(self.session)["status"])
        self.assertEqual("proposed", task.state)
        with patch.object(ap, "utcnow", return_value=now + timedelta(seconds=61)):
            fresh = ap.claim(self.session); self.session.commit()
        self.assertNotEqual(old["lease_id"], fresh["lease_id"])
        self.assertEqual(task.planned_resolution, fresh["task"]["planned_resolution"])
        with self.assertRaises(Conflict):
            self.decide(old, "accept")
        self.assertEqual("applied", self.decide(fresh, "accept")["status"])

    def test_rate_limit_keeps_fifteen_minute_backoff(self):
        from datetime import datetime, timedelta
        self.task(); self.enable(); claim = ap.claim(self.session); self.session.commit()
        now = ap.utcnow()
        with patch.object(ap, "utcnow", return_value=now):
            ap.failed(self.session, ap.FailedDecision(lease_id=claim["lease_id"], reason_code="model_rate_limited"))
            self.session.commit()
        event = self.session.scalar(select(Event).where(Event.event_type == "executive_error"))
        self.assertEqual(now + timedelta(minutes=15), datetime.fromisoformat(event.payload["revisit_at"]))
        with patch.object(ap, "utcnow", return_value=now + timedelta(seconds=61)):
            self.assertEqual("idle", ap.claim(self.session)["status"])

    def test_human_work_still_requires_evidence_before_ready(self):
        task = self.task("in_progress"); self.enable(); claim = ap.claim(self.session); self.session.commit()
        self.assertEqual("deferred", self.decide(claim, "ready")["status"])
        self.assertEqual("in_progress", task.state)
        from airflow.providers.vintage.bot_dashboard.service import add_event_items
        add_event_items(self.session, str(task.id), version=task.version, actor_id="owner", event_type="evidence_added",
                        items=[{"label": "Verified source cadence", "url": "https://example.com/report"}])
        self.session.commit()
        claim = ap.claim(self.session); self.session.commit()
        self.assertEqual("applied", self.decide(claim, "ready")["status"])
        self.assertEqual("ready", task.state)

    def test_unmapped_model_cannot_enable(self):
        self.mapping.stop()
        try:
            with self.assertRaises(PreconditionFailed):
                ap.set_enabled(self.session, ap.Toggle(enabled=True, version=0), "owner")
            self.assertFalse(ap.status(self.session)["enabled"])
        finally:
            self.mapping.start()

    def test_untrusted_model_output_cannot_disable_review_or_change_state_directly(self):
        task = self.task("accepted"); self.enable(); claim = ap.claim(self.session); self.session.commit()
        result = self.decide(claim, "configure", changes={"version": task.version, "reviewer_required": False})
        self.assertEqual("deferred", result["status"])
        self.assertTrue(task.reviewer_required)
        with self.assertRaises(ValueError):
            ap.Decision(lease_id="a" * 64, action="shell", rationale="Please bypass checks")


if __name__ == "__main__": unittest.main()
