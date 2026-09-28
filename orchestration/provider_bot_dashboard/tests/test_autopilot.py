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
from airflow.providers.vintage.bot_dashboard.models import Event, Execution, Policy, Revision, Task, ValidationGate, metadata, utcnow
from airflow.providers.vintage.bot_dashboard.git_provider import GitProviderError
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

    def test_assigned_accepted_ticket_does_not_repeat_assignment(self):
        task = self.task("accepted")
        task.assignee_kind = "bot"
        task.assignee_profile = "staff"
        self.session.commit()
        actions = ap._actions(ap._snapshot(self.session, str(task.id)))
        self.assertNotIn("assign", actions)
        self.assertIn("start", actions)

    def test_assignment_and_configuration_do_not_cycle_after_a_rejected_start(self):
        from datetime import timedelta

        task = self.task("accepted")
        self.enable()
        first = ap.claim(self.session); self.session.commit()
        self.assertEqual("applied", self.decide(first, "configure", changes=ap.PatchTask(
            version=task.version, planned_resolution="Verify the current source and publish reviewed evidence."))["status"])
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=2)):
            second = ap.claim(self.session); self.session.commit()
        self.assertEqual(["assign", "dismiss"], second["actions"])
        self.decide(second, "assign", profile="senior")
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=2)):
            third = ap.claim(self.session); self.session.commit()
        self.assertIn("start", third["actions"])
        self.assertNotIn("configure", third["actions"])
        self.assertNotIn("assign", third["actions"])
        with patch.object(ap, "_perform", side_effect=PreconditionFailed("Bot lacks execution permission")):
            self.assertEqual("deferred", self.decide(third, "start")["status"])
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(hours=2)):
            self.assertEqual("idle", ap.claim(self.session)["status"])
        self.assertEqual([], self.session.scalars(select(Execution)).all())

        # A changed requirement, not elapsed time or executive audit noise,
        # permits a fresh plan decision on the existing ticket.
        patch_task(self.session, str(task.id), version=task.version, actor_id="owner",
                   changes={"planned_resolution": "Use a newly authorized runner for current source verification."})
        self.session.commit()
        wake = ap.claim(self.session)
        self.assertEqual(str(task.id), wake["task"]["id"])
        self.assertIn("configure", wake["actions"])
        self.assertIn("start", wake["actions"])

    def test_unchanged_configuration_remains_suppressed_after_model_failure(self):
        from datetime import timedelta

        task = self.task("accepted")
        task.assignee_kind = "bot"; task.assignee_profile = "senior"
        self.session.commit(); self.enable()
        first = ap.claim(self.session); self.session.commit()
        self.decide(first, "configure", changes=ap.PatchTask(
            version=task.version, planned_resolution="Validate the exact reviewed source response."))
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=2)):
            second = ap.claim(self.session); self.session.commit()
        self.assertNotIn("configure", second["actions"])
        ap.failed(self.session, ap.FailedDecision(lease_id=second["lease_id"])); self.session.commit()
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=16)):
            retry = ap.claim(self.session)
        self.assertIn("start", retry["actions"])
        self.assertNotIn("configure", retry["actions"])

    def test_transient_provider_failure_retries_after_cooldown(self):
        from datetime import timedelta
        import httpx

        task = self.task("accepted")
        task.assignee_kind = "bot"; task.assignee_profile = "senior"
        self.session.commit(); self.enable()
        first = ap.claim(self.session); self.session.commit()
        with patch.object(ap, "_perform", side_effect=httpx.ConnectError("Connection reset")):
            self.assertEqual("deferred", self.decide(first, "start")["status"])
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=2)):
            self.assertEqual("idle", ap.claim(self.session)["status"])
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=61)):
            retry = ap.claim(self.session)
        self.assertEqual(str(task.id), retry["task"]["id"])
        self.assertIn("start", retry["actions"])

    def test_human_owned_accepted_ticket_waits_without_executive_reassignment(self):
        task = self.task("accepted")
        task.assignee_kind = "human"
        task.assignee_profile = None
        self.session.commit()
        self.enable()
        self.assertEqual("idle", ap.claim(self.session)["status"])

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

    def test_blocked_unusable_review_offers_same_head_retry(self):
        task = self.task("blocked")
        task.blocked_from_state = "in_review"
        task.assignee_kind = "bot"; task.assignee_profile = "senior"
        self.execution(task, review_verdict="unable_to_review",
                       provider_state={"review_verdict": "unable_to_review"})
        actions = ap._actions(ap._snapshot(self.session, str(task.id)))
        self.assertIn("retry_review", actions)
        self.assertNotIn("start", actions)

    def test_small_review_repair_is_scoped_and_high_risk_repairs_fail_closed(self):
        task = self.task("in_review")
        task.assignee_kind = "bot"; task.assignee_profile = "senior"
        revision = self.session.scalar(select(Revision).where(Revision.task_id == task.id))
        revision.allowed_path_globs = ["src/**", "tests/**"]
        revision.verification_commands = [["python3", "-m", "unittest"]]
        execution = self.execution(task, review_verdict="changes_requested", provider_state={
            "review_verdict": "changes_requested",
            "review_repair": {"instructions": "Handle null input before parsing.",
                              "paths": ["src/job.py"],
                              "check_expectations": ["The admitted unit test passes."]},
        })
        changes = ap._review_repair_changes(self.session, task, execution)
        self.assertIn("Handle null input", changes["planned_resolution"])
        execution.provider_state["review_repair"]["paths"] = ["outside/job.py"]
        with self.assertRaisesRegex(PreconditionFailed, "exceed"):
            ap._review_repair_changes(self.session, task, execution)
        execution.provider_state["review_repair"] = {
            "instructions": "Apply a production schema migration.", "paths": ["src/job.py"],
            "check_expectations": ["Migration passes."]}
        with self.assertRaisesRegex(PreconditionFailed, "high risk"):
            ap._review_repair_changes(self.session, task, execution)

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

    def test_superseded_pr_does_not_block_unpublished_repair_retry(self):
        task = self.task("blocked")
        task.assignee_kind = "bot"
        task.assignee_profile = "senior"
        task.blocked_from_state = "in_progress"
        self.execution(
            task,
            sequence=1,
            stage="superseded_conflict_repair",
            dispatch_state="terminal",
            terminal_at=utcnow(),
            terminal_reason_code="superseded_merge_conflict",
            pr_number=111,
            pr_url="https://github.com/org/repo/pull/111",
        )
        self.execution(
            task,
            sequence=2,
            execution_id="f" * 64,
            idempotency_key="repair-attempt",
            target_run_id="repair-run",
            stage="terminal",
            dispatch_state="terminal",
            terminal_at=utcnow(),
            terminal_reason_code="unresolved_revision_seed_conflict",
            pr_number=None,
            pr_url=None,
        )
        self.assertIn("start", ap._actions(ap._snapshot(self.session, str(task.id))))

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

    def test_retryable_blocked_execution_is_prioritized_for_resolution(self):
        from datetime import datetime, timezone
        from airflow.providers.vintage.bot_dashboard.models import utcnow
        self.task("ready")
        blocked = self.task("blocked")
        blocked.assignee_kind = "bot"
        blocked.assignee_profile = "senior"
        blocked.blocked_from_state = "in_progress"
        self.execution(
            blocked,
            stage="terminal",
            dispatch_state="terminal",
            terminal_at=utcnow(),
            terminal_reason_code="unresolved_revision_seed_conflict",
            pr_number=None,
            pr_url=None,
        )
        self.enable()
        with patch.object(ap, "utcnow", return_value=datetime(2026, 9, 17, 16, 1, tzinfo=timezone.utc)):
            result = ap.claim(self.session)
        self.assertEqual(str(blocked.id), result["task"]["id"])
        self.assertIn("start", result["actions"])

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

    def test_preexisting_wait_does_not_park_ticket_after_wait_is_removed(self):
        task = self.task()
        self.enable()
        detail = ap._snapshot(self.session, str(task.id))
        ap.service._event(self.session, task, "executive_decision", "system", "executive", payload={
            "action": "wait", "result": "applied", "result_version": task.version,
            "result_context_sha256": ap._digest(detail),
        })
        self.session.commit()
        claim = ap.claim(self.session)
        self.assertEqual(str(task.id), claim["task"]["id"])
        self.assertNotIn("wait", claim["actions"])
        with self.assertRaises(ValueError):
            ap.Decision(lease_id=claim["lease_id"], action="wait", rationale="The source is not tested yet.")

    def test_autopilot_never_offers_human_blocking(self):
        task = self.task()
        for state in ("proposed", "accepted", "in_progress", "in_review", "ready", "blocked"):
            task.state = state
            if state == "blocked":
                task.blocked_from_state = "in_review"
            self.session.commit()
            self.assertNotIn("block", ap._actions(ap._snapshot(self.session, str(task.id))))

    def test_merge_conflict_has_one_owned_resolution(self):
        task = self.task("blocked")
        task.blocked_from_state = "in_review"
        task.assignee_kind = "bot"
        task.assignee_profile = "senior"
        self.execution(task, provider_state={
            "state": "open", "draft": True, "head_sha": "a" * 40,
            "mergeability": "conflicting",
        })
        patch_task(self.session, str(task.id), version=task.version, actor_id="executive",
                   actor_kind="system", changes={"planned_resolution": "Use the latest accepted scope for repair"})
        self.session.commit()
        self.assertEqual(["repair_conflict"], ap._actions(ap._snapshot(self.session, str(task.id))))
        self.enable()
        claim = ap.claim(self.session)
        self.assertEqual(str(task.id), claim["task"]["id"])
        self.assertEqual(["repair_conflict"], claim["actions"])

    def test_applied_configuration_does_not_invite_a_second_identical_edit(self):
        from datetime import timedelta
        task = self.task("accepted")
        self.enable()
        claim = ap.claim(self.session)
        self.session.commit()
        self.assertEqual("applied", self.decide(claim, "configure", changes=ap.PatchTask(
            version=task.version, planned_resolution="Run the bounded source verification and report the result."))["status"])
        now = ap.utcnow() + timedelta(minutes=2)
        with patch.object(ap, "utcnow", return_value=now):
            next_claim = ap.claim(self.session)
        self.assertEqual(str(task.id), next_claim["task"]["id"])
        self.assertNotIn("configure", next_claim["actions"])
        self.assertIn("assign", next_claim["actions"])

    def test_failed_validation_evidence_reopens_plan_without_a_ticket_edit(self):
        from datetime import timedelta

        task = self.task("ready")
        self.execution(task)
        gate = ValidationGate(task_id=task.id, gate_key="source-response", stage="merge",
            recipe="public_source_smoke", owner="bot", required_capability="public-network-readonly",
            subject="a" * 40, recheck_condition="Verify the actual observed source response",
            required=True)
        self.session.add(gate); self.session.commit(); self.enable()
        first = ap.claim(self.session); self.session.commit()
        self.assertNotIn("merge", first["actions"])
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=self.provider()):
            self.decide(first, "configure", changes=ap.PatchTask(
                version=task.version, planned_resolution="Run the recorded source response check."))
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=2)):
            self.assertEqual("idle", ap.claim(self.session)["status"])
        gate.status = "failed"
        gate.evidence = {"observation": "Source response does not match expected contract"}
        self.session.commit()
        claim = ap.claim(self.session)
        self.assertEqual(str(task.id), claim["task"]["id"])
        self.assertIn("configure", claim["actions"])
        self.assertNotIn("merge", claim["actions"])

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

    def test_behind_bot_pr_refreshes_same_head_without_merging_until_checks_rerun(self):
        task = self.task("ready")
        task.assignee_kind = "bot"
        task.assignee_profile = "senior"
        row = self.execution(task, base_sha="b" * 40,
                             verification_manifest={"changed_paths": ["src/candidate.py"]})
        self.enable()
        claim = ap.claim(self.session); self.session.commit()
        provider = self.provider()
        provider.read_base_identity.return_value = "c" * 40
        provider.refresh_change.return_value = provider.read_change.return_value | {"head_sha": "d" * 40}
        provider.refreshed_change_preserves_paths.return_value = True
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider):
            self.assertEqual("applied", self.decide(claim, "merge")["status"])
        provider.refresh_change.assert_called_once_with(17, "a" * 40)
        provider.refreshed_change_preserves_paths.assert_called_once_with(
            "a" * 40, "d" * 40, ["src/candidate.py"],
        )
        provider.merge_change.assert_not_called()
        self.assertEqual("d" * 40, row.trusted_head_sha)
        self.assertEqual("c" * 40, row.provider_state["refreshed_base_sha"])
        self.assertTrue(self.session.scalars(select(Event).where(
            Event.task_id == task.id, Event.event_type == "bot_pr_base_refreshed",
        )).all())

        provider.read_change.return_value = provider.refresh_change.return_value
        from datetime import timedelta
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=2)):
            next_claim = ap.claim(self.session); self.session.commit()
        provider.merge_change.side_effect = GitProviderError("required checks pending")
        self.assertIn("lease_id", next_claim, next_claim)
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider):
            self.assertEqual("deferred", self.decide(next_claim, "merge")["status"])
        provider.merge_change.side_effect = None
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=2)):
            checked_claim = ap.claim(self.session); self.session.commit()
        self.assertIn("merge", checked_claim["actions"])
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider):
            self.assertEqual("applied", self.decide(checked_claim, "merge")["status"])
        self.assertEqual([(17, "d" * 40)] * 2, [
            call.args for call in provider.merge_change.call_args_list
        ])
        provider.refresh_change.assert_called_once()

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

    def test_advance_promotes_and_merges_an_approved_exact_head(self):
        task = self.task("in_review"); self.execution(task); self.enable()
        claim = ap.claim(self.session); self.session.commit()
        self.assertIn("advance", claim["actions"])
        provider = self.provider()
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider):
            self.assertEqual("applied", self.decide(claim, "advance")["status"])
        provider.merge_change.assert_called_once_with(17, "a" * 40)
        self.assertEqual("ready", task.state)

    def test_completion_is_not_offered_before_observed_merge(self):
        task = self.task("ready"); self.execution(task)
        self.enable(); claim = ap.claim(self.session)
        self.assertIn("merge", claim["actions"])
        self.assertNotIn("complete", claim["actions"])

    def test_ready_parent_with_active_prerequisite_does_not_reconsider_merge_or_rewrite_plan(self):
        task = self.task("ready")
        self.execution(task)
        child = self.task("in_progress")
        self.execution(child, execution_id="f" * 64, stage="executing",
                       pr_number=None, pr_url=None)
        child.related_task_id = task.id
        ap.service._event(self.session, task, "follow_up_ticket_linked", "system", "source_scheduling",
                          payload={"task_id": str(child.id), "title": child.title})
        self.session.commit()
        self.enable()
        self.assertEqual("idle", ap.claim(self.session)["status"])
        child.state = "completed"
        child.completed_at = utcnow()
        self.session.commit()
        claim = ap.claim(self.session)
        self.assertEqual(str(task.id), claim["task"]["id"])
        self.assertIn("merge", claim["actions"])

    def test_bot_pr_merges_without_waiting_for_linked_child(self):
        task = self.task("ready")
        task.assignee_kind = "bot"
        task.assignee_profile = "senior"
        self.execution(task, review_verdict="changes_requested")
        child = self.task("in_progress")
        child.related_task_id = task.id
        self.session.commit()
        actions = ap._actions(ap._snapshot(self.session, str(task.id)))
        self.assertIn("merge", actions)
        self.assertNotIn("configure", actions)

    def test_blocked_child_keeps_merge_closed_but_failed_gate_can_reopen_parent_plan(self):
        from datetime import timedelta

        parent = self.task("ready")
        self.execution(parent)
        child = self.task("blocked")
        child.related_task_id = parent.id
        ap.service._event(self.session, parent, "follow_up_ticket_linked", "system", "source_scheduling",
                          payload={"task_id": str(child.id), "title": child.title})
        gate = ValidationGate(task_id=parent.id, gate_key="linked-validation", stage="merge",
            recipe="public_source_smoke", owner="bot", required_capability="public-network-readonly",
            subject="a" * 40, recheck_condition="Retry after a source check failure", required=True)
        self.session.add(gate); self.session.commit(); self.enable()
        first = ap.claim(self.session); self.session.commit()
        self.assertEqual(str(parent.id), first["task"]["id"])
        self.assertIn("configure", first["actions"])
        self.assertNotIn("merge", first["actions"])
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=self.provider()):
            self.decide(first, "configure", changes=ap.PatchTask(
                version=parent.version, planned_resolution="Resolve the linked validator failure."))
        with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=2)):
            next_claim = ap.claim(self.session)
        self.assertNotEqual(str(parent.id), next_claim.get("task", {}).get("id"))
        self.session.rollback()
        gate.status = "failed"
        gate.evidence = {"observation": "Linked validator observed a source contract mismatch"}
        self.session.commit()
        # Do not let the unrelated every-fifth-minute age cycle select the child
        # before the ready parent when asserting the parent's gate-driven wake.
        now = ap.utcnow()
        with patch.object(ap, "utcnow", return_value=now + timedelta(minutes=(1 - now.minute) % 5)):
            wake = ap.claim(self.session)
        self.assertEqual(str(parent.id), wake["task"]["id"])
        self.assertIn("configure", wake["actions"])
        self.assertNotIn("merge", wake["actions"])

    def test_executive_reuses_one_pr_comment_per_ticket(self):
        from datetime import timedelta
        task = self.task("ready")
        self.execution(task)
        self.enable()
        provider = self.provider()
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider):
            first = ap.claim(self.session); self.session.commit()
            self.decide(first, "configure", changes=ap.PatchTask(
                version=task.version, planned_resolution="Verify the public source before merging."))
            with patch.object(ap, "utcnow", return_value=ap.utcnow() + timedelta(minutes=2)):
                second = ap.claim(self.session); self.session.commit()
            self.decide(second, "merge")
        markers = [call.args[1] for call in provider.upsert_comment.call_args_list]
        self.assertEqual(2, len(markers))
        self.assertEqual(markers[0], markers[1])
        self.assertIn(str(task.id), markers[0])

    def test_bot_pr_advances_to_merge_despite_rejected_review_and_failed_gate(self):
        task = self.task("in_review")
        task.assignee_kind = "bot"
        task.assignee_profile = "senior"
        self.execution(task, review_verdict="changes_requested", provider_state={
            "state": "open", "head_sha": "a" * 40, "draft": False,
            "review_verdict": "changes_requested",
        })
        self.session.add(ValidationGate(
            task_id=task.id, gate_key="live-check", stage="merge",
            recipe="trusted_workflow_check", owner="validation-service",
            required_capability="github-actions-readonly", subject="a" * 40,
            recheck_condition="Next provider event", status="failed", required=True,
        ))
        self.session.commit()
        self.enable()
        claim = ap.claim(self.session)
        self.session.commit()
        self.assertEqual(["ready", "advance"], claim["actions"])
        provider = self.provider()
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider):
            self.assertEqual("applied", self.decide(claim, "advance")["status"])
        provider.merge_change.assert_called_once_with(17, "a" * 40)
        self.assertEqual("ready", task.state)

    def test_premerge_validation_remains_required_for_reviewed_change(self):
        task = self.task("ready")
        self.execution(task)
        gate = ValidationGate(task_id=task.id, gate_key="source-response", stage="merge",
            recipe="manual", owner="operator", required_capability="public-network-readonly",
            subject="a" * 40, recheck_condition="Verify the current source response",
            required=True)
        self.session.add(gate); self.session.commit()
        actions = ap._actions(ap._snapshot(self.session, str(task.id)))
        self.assertNotIn("merge", actions)
        self.assertNotIn("advance", actions)
        self.assertNotIn("complete", actions)
        gate.status = "passed"; self.session.commit()
        self.assertIn("merge", ap._actions(ap._snapshot(self.session, str(task.id))))

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

    def test_stale_no_change_needs_new_human_implementation_evidence(self):
        task = self.task("ready")
        self.execution(task, pr_number=None, pr_url=None,
                       terminal_reason_code="no_change", terminal_at=utcnow(), stage="terminal")
        patch_task(self.session, str(task.id), version=task.version, actor_id="owner",
                   changes={"planned_resolution": "Repair the revision handoff under trusted runner policy"})
        self.session.commit()
        self.assertNotIn("complete", ap._actions(ap._snapshot(self.session, str(task.id))))
        ap.service.add_event_items(self.session, str(task.id), version=task.version, actor_id="owner",
            event_type="evidence_added", items=[{"label": "Trusted repair merged",
                                                  "observation": "Parent-owned code was reviewed and merged"}])
        self.session.commit()
        self.enable(); claim = ap.claim(self.session); self.session.commit()
        self.assertEqual(["complete"], claim["actions"])
        self.assertEqual("applied", self.decide(claim, "complete")["status"])
        self.assertEqual("completed", task.state)
        evidence = self.session.scalars(select(Event).where(
            Event.task_id == task.id, Event.event_type == "evidence_added").order_by(Event.sequence.desc())).first()
        self.assertEqual("Human implementation evidence", evidence.payload["items"][0]["label"])

    def test_reviewed_merge_completes_with_live_gate_still_pending(self):
        task = self.task("ready")
        self.execution(task, merged_at=utcnow(), provider_state={
            "state": "merged", "head_sha": "a" * 40, "draft": False,
        })
        gate = ValidationGate(task_id=task.id, gate_key="live-source", stage="completion",
            recipe="manual", owner="operator", required_capability="public-network-readonly",
            subject="a" * 40, recheck_condition="Run merged extractor against its live source",
            required=True)
        self.session.add(gate); self.session.commit()
        self.enable(); claim = ap.claim(self.session); self.session.commit()
        self.assertEqual(["complete"], claim["actions"])
        provider = self.provider(state="merged")
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=provider):
            self.assertEqual("applied", self.decide(claim, "complete")["status"])
        self.assertEqual("completed", task.state)
        self.assertEqual("pending", gate.status)
        evidence = self.session.scalar(select(Event).where(Event.task_id == task.id, Event.event_type == "evidence_added"))
        self.assertEqual("unverified", evidence.payload["items"][0]["production_validation"])
        self.assertEqual(["live-source"], evidence.payload["items"][0]["pending_gate_keys"])

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
            self.assertEqual("capacity_wait", ap.claim(self.session)["status"])
        with patch.object(ap, "utcnow", return_value=now + timedelta(minutes=16)):
            probe = ap.claim(self.session)
            self.assertEqual("claimed", probe["status"])
            self.session.commit()
            self.assertEqual("capacity_probe", ap.claim(self.session)["status"])
            self.assertTrue(ap.status(self.session)["capacity_probe_active"])
            self.decide(probe, "accept")
            self.assertIsNone(ap.status(self.session)["capacity_recheck_at"])

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
