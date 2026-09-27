import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session
from airflow.models.variable import Variable
from airflow.providers.vintage.bot_dashboard import autopilot as ap, planning, follow_up, service
from airflow.providers.vintage.bot_dashboard.models import metadata, Task, Revision, Execution, Event, Policy, utcnow


class PlanningTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)
        Variable.__table__.create(self.engine)
        self.s = Session(self.engine)
        self.s.add(Policy(category="*", mode="manual"))
        value = service.create_manual_task(self.s, title="Repair food recall charts", category="other",
                    priority=1, planned_resolution="Repair existing files; a distinct analysis ticket is required.",
                    actor_id="owner", actor_name="Owner")
        self.task = self.s.get(Task, uuid.UUID(value["id"]))
        self.task.state = "blocked"; self.task.blocked_from_state = "in_review"
        self.revision = self.s.scalar(select(Revision).where(Revision.task_id == self.task.id))
        self.revision.follow_up_bots = ["analytics_engineer"]
        self.revision.resource_keys = ["analytics:food", "source:food"]
        self.execution = Execution(execution_id="e"*64, task_id=self.task.id, sequence=1, revision=1,
            idempotency_key="execution", target_run_id="run", profile="senior", stage="reviewed",
            pr_number=17, trusted_head_sha="a"*40, review_verdict="changes_requested",
            provider_state={"state": "open", "head_sha": "a"*40})
        self.s.add(self.execution); self.s.commit()
        self.mapping = patch.object(ap, "_model", return_value={"model": "gpt-6-astra", "provider_id": "gateway"})
        self.mapping.start()

    def tearDown(self):
        self.mapping.stop(); self.s.close(); self.engine.dispose()

    def request(self):
        planning.request(self.s, self.task, "analytics_engineer", "Plan the distinct analysis work required by independent review.")
        self.s.commit()
        return self.s.scalar(select(Event).where(Event.event_type == "planning_requested"))

    def test_executive_request_is_audited_without_accepting_or_merging_parent(self):
        ap.set_enabled(self.s, ap.Toggle(enabled=True, version=0), "owner"); self.s.commit()
        claim = ap.claim(self.s); self.s.commit()
        self.assertIn("request_follow_up", claim["actions"])
        self.assertEqual(["analytics_engineer"], claim["task"]["follow_up_options"])
        with patch("airflow.providers.vintage.bot_dashboard.git_provider.get_provider", return_value=Mock()):
            result = ap.decide(self.s, ap.Decision(lease_id=claim["lease_id"], action="request_follow_up",
                specialist="analytics_engineer", rationale="Plan the distinct analysis ticket required before merge."))
        self.s.commit()
        self.assertEqual("applied", result["status"])
        self.assertEqual("blocked", self.task.state)
        self.assertEqual("changes_requested", self.execution.review_verdict)
        self.assertEqual([], planning.options(self.s, self.task, self.execution, self.revision))
        with self.assertRaises(service.PreconditionFailed):
            self.request()

    def test_unconfigured_route_or_changed_head_cannot_request_planning(self):
        with self.assertRaises(service.PreconditionFailed):
            planning.request(self.s, self.task, "data_analyst", "Not configured")
        self.execution.provider_state = {"state": "open", "head_sha": "b"*40}
        with self.assertRaises(service.PreconditionFailed):
            self.request()

    def test_context_requires_exact_executive_request_and_revision(self):
        e = self.request()
        identity = {"dag_id": "bot__analytics_engineer", "run_id": e.payload["run_id"]}
        conf = {"planning_request_id": e.id}
        args = (self.s, identity, conf, self.task, self.execution, self.revision, "analytics_engineer")
        self.assertEqual(e.payload["request"], planning.validate(*args))
        self.revision.revision_number += 1
        with self.assertRaises(service.PreconditionFailed):
            planning.validate(*args)
        self.revision.revision_number -= 1
        conf["planning_request_id"] = True
        with self.assertRaises(service.PreconditionFailed):
            planning.validate(*args)

    def test_failed_unpublished_execution_can_request_internal_dependency_planning(self):
        self.task.blocked_from_state = "in_progress"
        self.execution.stage = "terminal"
        self.execution.dispatch_state = "terminal"
        self.execution.admission_kind = "executor"
        self.execution.terminal_at = utcnow()
        self.execution.terminal_reason_code = "execution_blocked"
        self.execution.pr_number = None
        self.execution.pr_url = None
        self.execution.trusted_head_sha = None
        self.execution.review_verdict = None
        self.execution.provider_state = {}
        self.execution.base_sha = "b" * 40
        self.s.commit()
        self.assertEqual(["analytics_engineer"], planning.options(
            self.s, self.task, self.execution, self.revision
        ))
        event = self.request()
        self.assertEqual("b" * 40, event.payload["head_sha"])
        identity = {"dag_id": "bot__analytics_engineer", "run_id": event.payload["run_id"]}
        conf = {"planning_request_id": event.id}
        self.assertIn("distinct analysis work", planning.validate(
            self.s, identity, conf, self.task, self.execution, self.revision, "analytics_engineer"
        ))

    def test_dispatch_is_idempotent_and_uses_only_recorded_identity(self):
        event = self.request()
        session = Mock()
        session.scalars.return_value.all.return_value = [event]
        session.scalar.side_effect = [None, self.revision]
        jobs = planning.pending(session)
        self.assertEqual(1, len(jobs))
        self.assertEqual(event.id, jobs[0]["conf"]["planning_request_id"])
        self.assertEqual(event.payload["run_id"], jobs[0]["trigger_run_id"])
        self.assertTrue(jobs[0]["skip_when_already_exists"])
        from airflow.providers.vintage.bot_dashboard.api_models import MaintenanceResponse
        response = dict(failed_dispatches=0, expired_leases=0, pruned_reports=0, pruned_artifacts=0,
                        provider=dict(checked=0, changed=0, errors=0, follow_up_bots=[]), follow_up_bots=jobs)
        self.assertEqual(jobs, MaintenanceResponse.model_validate(response).model_dump()["follow_up_bots"])
        session.scalar.side_effect = [123]
        self.assertEqual([], planning.pending(session))

    def test_child_requires_approval_and_does_not_overwrite_same_resource_parent(self):
        # An automatic policy must not silently accept this separately requested ticket.
        self.s.get(Policy, "*").mode = "auto_accept"; self.s.commit()
        self.task.source_bot = "analytics_engineer"; self.s.commit()
        report = SimpleNamespace(id=uuid.uuid4(), sha256="f"*64, report_schema="analytics_engineer_v2",
            bot_name="analytics_engineer", dag_id="bot__analytics_engineer", run_id="planning-run",
            task_id="run", map_index=-1, started_at=utcnow())
        proposal = dict(recommendation_key="food-analysis", title="Add recall trends", category="other", priority=1,
            planned_resolution="Plan the distinct missing trends", why_now="Reviewer requested a linked ticket",
            expected_benefit="Track analysis work", risk="Interpretation", rollback="Revert", suggested_executor="senior",
            evidence=[], verification_commands=[["python", "check.py"]], allowed_path_globs=["transform/models/food.yml"],
            resource_keys=self.revision.resource_keys, follow_up_bots=["data_analyst"])
        before = self.task.planned_resolution
        before_version = self.task.version
        with patch.object(follow_up, "record", return_value=self.task.id):
            result = service.reconcile_recommendations(self.s, report, {"plans": [{"task_proposal": proposal}]})
            self.s.commit()
            self.assertEqual(1, result["created"])
            replay = service.reconcile_recommendations(self.s, report, {"plans": [{"task_proposal": proposal}]})
            self.s.commit()
        self.assertEqual(0, replay["created"])
        child = self.s.scalar(select(Task).where(Task.related_task_id == self.task.id))
        self.assertEqual("proposed", child.state)
        self.assertTrue(child.reviewer_required)
        self.assertEqual("blocked", self.task.state)
        self.assertEqual(before, self.task.planned_resolution)
        self.assertEqual(before_version + 1, self.task.version)
        self.assertEqual(1, self.s.scalar(select(func.count()).select_from(Event).where(Event.event_type == "follow_up_ticket_linked")))

    def test_executive_context_retains_linked_child_after_decision_noise(self):
        value = service.create_manual_task(
            self.s, title="Add missing recall trends", category="other", priority=1,
            planned_resolution="Implement the separately reviewed analytics work.",
            actor_id="owner", actor_name="Owner",
        )
        child = self.s.get(Task, uuid.UUID(value["id"]))
        child.related_task_id = self.task.id
        child.state = "completed"
        child.completed_at = utcnow()
        service._event(self.s, self.task, "follow_up_ticket_linked", "system", "analytics_engineer",
                       payload={"task_id": str(child.id), "title": child.title})
        for index in range(30):
            service._event(self.s, self.task, "executive_decision", "system", "executive",
                           payload={"action": "wait", "result": "applied", "ordinal": index})
        self.s.commit()

        snapshot = ap._snapshot(self.s, str(self.task.id))
        self.assertEqual([{"task_id": str(child.id), "title": child.title, "state": "completed",
                           "completed_at": child.completed_at.isoformat()}], snapshot["linked_follow_ups"])
        self.assertTrue(any(event["event_type"] == "follow_up_ticket_linked" for event in snapshot["events"]))
        self.assertEqual(5, len(snapshot["executive_history"]))

    def test_decision_shape_cannot_hide_specialist_on_other_actions(self):
        with self.assertRaises(ValueError):
            ap.Decision(lease_id="a"*64, action="merge", specialist="analytics_engineer", rationale="Still requires independent approval")
        with self.assertRaises(ValueError):
            ap.Decision(lease_id="a"*64, action="request_follow_up", rationale="Missing the selected specialist")
