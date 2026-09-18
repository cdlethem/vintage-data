import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch
from airflow.providers.vintage.bot_dashboard import follow_up, service
from airflow.providers.vintage.bot_dashboard.models import utcnow
from airflow.providers.vintage.bot_dashboard.report_schemas import SourceSchedulingV2, AnalyticsEngineerV2, DataAnalystV1


class FollowUpTest(unittest.TestCase):
    def fixtures(self, bot="source_scheduling"):
        task_id = uuid.uuid4()
        identity = {"dag_id": "bot__" + bot, "run_id": f"follow_up__{task_id}__2__{bot}"}
        task = SimpleNamespace(id=task_id, title="GitLab metadata", state="ready", planned_resolution="Retain live acceptance before completion.")
        execution = SimpleNamespace(sequence=2, execution_id="a"*64, merged_at=utcnow(),
            provider_state={"state": "merged", "head_sha": "b"*40}, trusted_head_sha="b"*40,
            pr_number=41, review_verdict="approved")
        revision = SimpleNamespace(follow_up_bots=[bot], resource_keys=["source:gitlab"],
            allowed_path_globs=["extract/sources/gitlab.yml"], verification_commands=[["python", "test.py"]], evidence=[])
        run = SimpleNamespace(conf={"task_id": str(task_id), "execution_id": execution.execution_id})
        session = Mock()
        session.scalar.side_effect = [run, execution, revision]
        session.get.return_value = task
        return session, identity, task, execution, revision

    def test_context_targets_the_requested_ticket_and_preserves_gates(self):
        session, identity, task, execution, _ = self.fixtures()
        result = follow_up.context(session, identity)
        self.assertEqual(str(task.id), result["selected"]["parent_task_id"])
        self.assertEqual(task.planned_resolution, result["selected"]["planned_resolution"])
        self.assertEqual(execution.trusted_head_sha, result["selected"]["head_sha"])
        self.assertEqual(1, result["pending_count"])

    def test_untrusted_head_and_wrong_routing_fail_closed(self):
        for broken in ["head", "run", "route", "merge"]:
            session, identity, _, execution, revision = self.fixtures()
            if broken == "head": execution.provider_state["head_sha"] = "c"*40
            if broken == "run": identity["run_id"] += "_other"
            if broken == "route": revision.follow_up_bots = []
            if broken == "merge": execution.merged_at = None
            with self.subTest(broken=broken), self.assertRaises(service.PreconditionFailed):
                follow_up.context(session, identity)

    def test_bounded_recheck_keeps_the_same_parent_and_execution(self):
        session, identity, task, execution, _ = self.fixtures()
        identity["run_id"] += "__recheck__20260917194500"
        value = follow_up.context(session, identity)
        self.assertEqual(execution.execution_id, value["selected"]["execution_id"])
        self.assertEqual(str(task.id), value["selected"]["parent_task_id"])

    def test_analytics_routes_keep_parent_and_require_explicit_authorization(self):
        for bot in ("analytics_engineer", "data_analyst"):
            with self.subTest(bot=bot):
                session, identity, task, _, _ = self.fixtures(bot)
                result = follow_up.context(session, identity)
                self.assertEqual(str(task.id), result["selected"]["parent_task_id"])
                self.assertEqual(1, result["gap_backlog_count"])
                session, identity, _, _, revision = self.fixtures(bot)
                revision.follow_up_bots = ["source_scheduling"]
                with self.assertRaises(service.PreconditionFailed):
                    follow_up.context(session, identity)

    def test_record_rejects_cross_specialist_identity(self):
        report = SimpleNamespace(bot_name="data_analyst", dag_id="bot__analytics_engineer",
                                 run_id="follow_up__parent__1__analytics_engineer")
        with self.assertRaises(service.PreconditionFailed):
            follow_up.record(Mock(), report, {})

    def test_requested_planning_context_does_not_claim_an_unmerged_pr_is_merged(self):
        session, identity, task, execution, revision = self.fixtures("analytics_engineer")
        revision.revision_number = 3
        execution.merged_at = None
        execution.provider_state["state"] = "open"
        execution.review_verdict = "changes_requested"
        task.state = "blocked"
        identity["run_id"] = f"planning__{task.id}__2__r3__analytics_engineer"
        event = SimpleNamespace(id=17, task_id=task.id, event_type="planning_requested",
            actor_id="executive", actor_kind="system", payload={"bot": "analytics_engineer",
            "execution_id": execution.execution_id, "head_sha": execution.trusted_head_sha,
            "revision_number": 3, "run_id": identity["run_id"], "request": "Plan the distinct analysis ticket."})
        session.scalar.side_effect = [SimpleNamespace(conf={"task_id": str(task.id),
            "execution_id": execution.execution_id, "planning_request_id": 17}), execution, revision]
        session.get.side_effect = [task, event]
        result = follow_up.context(session, identity)
        self.assertIsNone(result["selected"]["merged_at"])
        self.assertEqual("requested_planning_follow_up", result["selected"]["work_kind"])
        self.assertEqual(event.payload["request"], result["selected"]["planning_request"])

    def test_summary_is_attached_once_without_changing_task_decisions(self):
        session, identity, task, execution, _ = self.fixtures()
        session.scalars.return_value.all.return_value = []
        report = SimpleNamespace(id=uuid.uuid4(), bot_name="source_scheduling", **identity, sha256="d"*64)
        payload = {"status": "degraded_evidence", "plans": [], "summary": "The required live validation path is unavailable."}
        with patch.object(follow_up, "context", return_value={"selected": {"parent_task_id": str(task.id), "execution_id": execution.execution_id, "head_sha": execution.trusted_head_sha}}), patch.object(service, "_locked_task", return_value=task), patch.object(service, "_event") as event:
            follow_up.record(session, report, payload)
            event.assert_called_once()
            self.assertEqual("specialist_follow_up", event.call_args.args[2])
            session.scalars.return_value.all.return_value = [SimpleNamespace(payload={"report_id": str(report.id)})]
            follow_up.record(session, report, payload)
            event.assert_called_once()
        self.assertEqual("ready", task.state)

    def test_observation_only_report_cannot_claim_healthy_completion(self):
        value = dict(schema_version=2, agent="source_scheduling", plans=[], summary="Missing authorized live validation.")
        self.assertEqual([], SourceSchedulingV2(**value, status="degraded_evidence").plans)
        with self.assertRaises(ValueError):
            SourceSchedulingV2(**value, status="ok")

    def test_analytics_observations_do_not_require_invented_work_or_queries(self):
        engineer = dict(schema_version=2, agent="analytics_engineer", datasets=[], decisions=[],
                        plans=[], summary="Warehouse evidence is unavailable.")
        analyst = dict(schema_version=1, agent="data_analyst", family="gitlab", queries=[],
                       analyses=[], plans=[], summary="No authorized warehouse evidence is available.")
        for model, value in ((AnalyticsEngineerV2, engineer), (DataAnalystV1, analyst)):
            self.assertEqual([], model(**value, status="degraded_evidence").plans)
            with self.assertRaises(ValueError):
                model(**value, status="ok")
        # Validator must reject an otherwise parsed proposal lacking real analysis.
        # model_construct isolates this check from the unrelated proposal field schema.
        report = DataAnalystV1.model_construct(**{**analyst, "plans": [object()], "status": "degraded_evidence"})
        with self.assertRaisesRegex(ValueError, "executed queries and measured analyses"):
            report.proposals_require_measured_analysis()
