"""Queue behavior: stable matching, scope preservation, and auditable resets."""
import json
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from airflow.models.dagrun import DagRun
from airflow.providers.vintage.bot_dashboard.models import Event, Execution, Policy, Revision, Task, metadata, utcnow
from airflow.providers.vintage.bot_dashboard.report_schemas import validate_named_report
from airflow.providers.vintage.bot_dashboard.service import (
    PreconditionFailed, _recommendation_identity, queue_summary, reconcile_recommendations, reset_queue,
)


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)
        DagRun.__table__.create(self.engine)
        self.session = Session(self.engine)
        self.session.add(Policy(category="*", mode="manual"))
        self.session.commit()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def propose(self, key="holiday-repair", title="Repair holiday snapshots", *, resources=None, bot="analytics_engineer", started=None):
        proposal = dict(recommendation_key=key, title=title, category="other", priority=1,
            planned_resolution=title, why_now="Snapshots are incomplete", expected_benefit="Complete coverage",
            risk="Low", rollback="Revert", suggested_executor="junior", evidence=[],
            verification_commands=[["python", "check.py"]], allowed_path_globs=["transform/**"],
            resource_keys=resources or ["source:nager_date"], follow_up_bots=[])
        report = SimpleNamespace(id=uuid.uuid4(), report_schema="analytics_engineer_v2", bot_name=bot,
            dag_id=f"bot__{bot}", run_id=str(uuid.uuid4()), task_id="run", map_index=-1,
            started_at=started or utcnow())
        result = reconcile_recommendations(self.session, report, {"plans": [{"task_proposal": proposal}]})
        self.session.commit()
        return result

    def test_changed_key_and_title_revise_same_open_resource(self):
        self.propose()
        result = self.propose("new-key", "Repair and model the holiday data")
        self.assertEqual(0, result["created"])
        self.assertEqual(1, result["revised"])
        self.assertEqual(1, self.session.scalar(select(func.count()).select_from(Task)))
        self.assertEqual(2, self.session.scalar(select(func.count()).select_from(Revision)))

    def test_holiday_proposals_with_different_model_names_are_one_task(self):
        common = ["analytics:nager_date", "source:nager_date", "visualization:nager_date"]
        self.propose(resources=common + ["model:fct_nager_date_holiday"])
        result = self.propose("holiday-observations", "Repair and model holiday snapshots", resources=common + ["model:fct_nager_date_holiday_observation"])
        self.assertEqual({"created": 0, "revised": 1, "delegated": 0}, result)
        self.assertEqual(1, self.session.scalar(select(func.count()).select_from(Task)))

    def test_model_alias_matching_preserves_different_scope(self):
        from airflow.providers.vintage.bot_dashboard.service import _same_work_resources
        original = {"source:nager_date", "analytics:nager_date", "model:holidays"}
        for different in [
            {"source:sumo", "analytics:sumo", "model:matches"},
            {"source:nager_date", "visualization:nager_date", "model:other"},
            {"source:nager_date", "analytics:nager_date", "model:other", "model:aggregate"},
            {"source:nager_date", "analytics:nager_date"},
        ]:
            with self.subTest(resources=different):
                self.assertFalse(_same_work_resources(original, different))
        self.assertFalse(_same_work_resources({"source:x", "model:a"}, {"source:x", "model:b"}))

    def test_different_resources_or_specialist_work_remain_separate(self):
        self.propose()
        self.propose("bike", "Model bike stations", resources=["source:bike"])
        self.propose("holiday-trends", "Add holiday trends", bot="data_analyst")
        self.assertEqual(3, self.session.scalar(select(func.count()).select_from(Task)))

    def test_accepted_scope_is_not_overwritten(self):
        self.propose()
        task = self.session.scalar(select(Task))
        task.state = "accepted"
        task.planned_resolution = "Human-approved scope"
        self.session.commit()
        result = self.propose("other-key", "Wider scope")
        self.assertEqual(0, result["created"])
        self.assertEqual(0, result["revised"])
        self.session.refresh(task)
        self.assertEqual("Human-approved scope", task.planned_resolution)

    def source_run(self, run_id, started, ended, *, state="failed"):
        self.session.execute(DagRun.__table__.insert().values(
            dag_id="extract__daily", run_id=run_id, run_type="scheduled",
            run_after=started, start_date=started, end_date=ended, state=state,
            log_template_id=None,
        ))
        self.session.commit()

    def triage(self, reference, *, key="source-repair", title="Repair failing extraction", resources=None,
               run_id=None, started=None, evidence_kind="failure_occurrence"):
        proposal = dict(
            recommendation_key=key, title=title, category="reliability", priority=1,
            planned_resolution=title, why_now="Extraction failed", expected_benefit="Restore the feed",
            risk="Missing data", rollback="Revert", suggested_executor="senior",
            evidence=[{"kind": evidence_kind, "reference": reference, "summary": "Failed source run"}],
            verification_commands=[["python", "check.py"]], allowed_path_globs=["dags/**"],
            resource_keys=resources or ["source:daily"], follow_up_bots=[], reviewer_required=True,
        )
        report = SimpleNamespace(
            id=uuid.uuid4(), report_schema="failure_triage_v2", bot_name="failure_triage",
            dag_id="bot__failure_triage", run_id=run_id or str(uuid.uuid4()),
            task_id="run", map_index=-1, started_at=started or utcnow(),
        )
        payload = {"schema_version": 2, "agent": "failure_triage", "status": "ok",
                   "failures": [{"fingerprint": "f" * 64, "action": "task_proposed",
                                 "reason": "Failed extraction", "task_proposal": proposal}],
                   "remaining_unreviewed": 0, "summary": "Failed extraction"}
        result = reconcile_recommendations(self.session, report, validate_named_report("failure_triage_v2", payload))
        self.session.commit()
        return result, report, payload

    def test_post_merge_failure_creates_one_related_proposal_and_preserves_ready_scope(self):
        merged = utcnow() - timedelta(hours=2)
        earlier = merged - timedelta(hours=1)
        initial = f"component=extract__daily; run_id=old-run; occurred_at={earlier.isoformat()}"
        self.source_run("old-run", earlier - timedelta(minutes=2), earlier)
        self.triage(initial, started=merged - timedelta(minutes=30))
        original = self.session.scalar(select(Task))
        original.state = "ready"
        original.planned_resolution = "Approved repair scope"
        original_version = original.version
        self.session.add(Execution(
            execution_id="e" * 64, task_id=original.id, sequence=1, revision=1,
            idempotency_key="merged-repair", target_run_id="executor-run", profile="senior",
            merged_at=merged, terminal_at=merged, stage="terminal", dispatch_state="terminal",
        ))
        self.session.commit()
        self.session.get(Policy, "*").mode = "auto_accept"
        self.session.commit()
        fresh = merged + timedelta(minutes=20)
        reference = f"component=extract__daily; run_id=new-run; occurred_at={fresh.isoformat()}"
        self.source_run("new-run", fresh - timedelta(minutes=2), fresh)
        result, report, payload = self.triage(reference, key="renamed-repair",
            title="Investigate extraction after merged repair", started=fresh + timedelta(minutes=5))
        self.assertEqual(1, result["created"])
        child = self.session.scalar(select(Task).where(Task.related_task_id == original.id))
        self.assertIsNotNone(child)
        self.assertEqual("proposed", child.state)
        self.assertEqual("Investigate extraction after merged repair", child.planned_resolution)
        self.assertEqual(reference, self.session.scalar(
            select(Revision.evidence).where(Revision.task_id == child.id)
        )[0]["reference"])
        self.assertEqual("ready", original.state)
        self.assertEqual("Approved repair scope", original.planned_resolution)
        self.assertEqual(original_version, original.version)
        replay = reconcile_recommendations(self.session, report, payload)
        self.session.commit()
        self.assertEqual({"created": 0, "revised": 0, "delegated": 0}, replay)
        second, _, _ = self.triage(reference, key="third-name",
            started=fresh + timedelta(minutes=10))
        self.assertEqual({"created": 0, "revised": 0, "delegated": 0}, second)
        self.assertEqual(2, self.session.scalar(select(func.count()).select_from(Task)))
        self.assertEqual(original.id, child.related_task_id)
        stale, _, _ = self.triage(initial, key="historical-rewording",
            title="Stale historical failure", started=fresh + timedelta(minutes=15))
        self.assertEqual({"created": 0, "revised": 0, "delegated": 0}, stale)
        self.assertEqual("Investigate extraction after merged repair", child.planned_resolution)
        self.assertEqual(1, self.session.scalar(
            select(func.count()).select_from(Revision).where(Revision.task_id == child.id)
        ))
        later = fresh + timedelta(minutes=16)
        later_reference = f"component=extract__daily; run_id=later-run; occurred_at={later.isoformat()}"
        self.source_run("later-run", later - timedelta(minutes=2), later)
        additional, _, _ = self.triage(later_reference, key="later-key",
            title="Diagnose the newer extraction failure", started=later + timedelta(minutes=2))
        self.assertEqual({"created": 0, "revised": 1, "delegated": 0}, additional)
        self.assertEqual(2, self.session.scalar(
            select(func.count()).select_from(Revision).where(Revision.task_id == child.id)
        ))
        self.assertEqual("Diagnose the newer extraction failure", child.planned_resolution)
        other = Task(
            source="specialist", source_bot="failure_triage",
            recommendation_key=_recommendation_identity("failure_triage",
                {"category": "reliability", "recommendation_key": "other-parent"}, None),
            title="Earlier independent repair", category="reliability", state="ready",
            priority=1, planned_resolution="Do not alter second admitted scope",
        )
        self.session.add(other)
        self.session.flush()
        self.session.add(Revision(task_id=other.id, revision_number=1,
            title=other.title, resource_keys=["source:daily"]))
        self.session.add(Execution(
            execution_id="b" * 64, task_id=other.id, sequence=1, revision=1,
            idempotency_key="other-merge", target_run_id="other-executor", profile="senior",
            merged_at=merged, terminal_at=merged, stage="terminal", dispatch_state="terminal",
        ))
        self.session.commit()
        duplicate, _, _ = self.triage(reference, key="other-parent",
            started=fresh + timedelta(minutes=25))
        self.assertEqual(0, duplicate["created"])
        self.assertEqual(3, self.session.scalar(select(func.count()).select_from(Task)))
        self.assertEqual("Do not alter second admitted scope", other.planned_resolution)
        self.assertEqual("Diagnose the newer extraction failure", child.planned_resolution)
        self.assertEqual(2, self.session.scalar(
            select(func.count()).select_from(Revision).where(Revision.task_id == child.id)
        ))

    def test_post_merge_airflow_failure_log_creates_repair_without_duplicate_revision(self):
        merged = utcnow() - timedelta(hours=2)
        old = merged - timedelta(minutes=10)
        old_reference = f"component=extract__daily; run_id=old-run; occurred_at={old.isoformat()}"
        self.source_run("old-run", old - timedelta(minutes=2), old)
        self.triage(old_reference, started=merged - timedelta(minutes=5))
        original = self.session.scalar(select(Task))
        original.state = "ready"
        original.planned_resolution = "Preserve merged implementation"
        self.session.add(Execution(
            execution_id="c" * 64, task_id=original.id, sequence=1, revision=1,
            idempotency_key="merged-source", target_run_id="executor-run", profile="senior",
            merged_at=merged, terminal_at=merged, stage="terminal", dispatch_state="terminal",
        ))
        self.session.commit()
        failed = merged + timedelta(minutes=15)
        reference = f"component=extract__daily; run_id=new-run; occurred_at={failed.isoformat()}"
        self.source_run("new-run", failed - timedelta(minutes=2), failed)
        result, _, _ = self.triage(reference, key="new-failure", started=failed + timedelta(minutes=5),
                                   evidence_kind="airflow_failure_log")
        self.assertEqual(1, result["created"])
        child = self.session.scalar(select(Task).where(Task.related_task_id == original.id))
        self.assertIsNotNone(child)
        self.assertEqual("proposed", child.state)
        self.assertEqual("Preserve merged implementation", original.planned_resolution)
        repeat, _, _ = self.triage(reference, key="renamed-failure", started=failed + timedelta(minutes=10))
        self.assertEqual({"created": 0, "revised": 0, "delegated": 0}, repeat)
        self.assertEqual(1, self.session.scalar(select(func.count()).select_from(Revision).where(
            Revision.task_id == child.id
        )))

    def test_historical_failure_does_not_reopen_completed_fix(self):
        merged = utcnow() - timedelta(hours=2)
        before = merged - timedelta(minutes=20)
        reference = f"component=extract__daily; run_id=old-run; occurred_at={before.isoformat()}"
        self.source_run("old-run", before - timedelta(minutes=2), before)
        self.triage(reference, started=merged - timedelta(minutes=10))
        original = self.session.scalar(select(Task))
        original.state = "completed"
        original.planned_resolution = "Merged original scope"
        self.session.add(Execution(
            execution_id="a" * 64, task_id=original.id, sequence=1, revision=1,
            idempotency_key="merged-repair", target_run_id="executor-run", profile="senior",
            merged_at=merged, terminal_at=merged, stage="terminal", dispatch_state="terminal",
        ))
        self.session.commit()
        result, _, _ = self.triage(reference, key="changed-key",
            started=merged + timedelta(minutes=10))
        self.assertEqual({"created": 0, "revised": 0, "delegated": 0}, result)
        self.assertEqual(1, self.session.scalar(select(func.count()).select_from(Task)))
        self.assertEqual("Merged original scope", original.planned_resolution)
        forged = f"component=extract__daily; run_id=not-in-airflow; occurred_at={(merged + timedelta(minutes=5)).isoformat()}"
        missing, _, _ = self.triage(forged, key="changed-key",
            started=merged + timedelta(minutes=10))
        self.assertEqual(0, missing["created"])
        self.source_run("successful-run", merged + timedelta(minutes=2),
            merged + timedelta(minutes=5), state="success")
        nonfailed = f"component=extract__daily; run_id=successful-run; occurred_at={(merged + timedelta(minutes=5)).isoformat()}"
        healthy, _, _ = self.triage(nonfailed, key="changed-key",
            started=merged + timedelta(minutes=10))
        self.assertEqual(0, healthy["created"])
        self.source_run("pre-merge-run", merged - timedelta(minutes=5),
            merged + timedelta(minutes=5))
        premerge = f"component=extract__daily; run_id=pre-merge-run; occurred_at={(merged + timedelta(minutes=5)).isoformat()}"
        old_code, _, _ = self.triage(premerge, key="changed-key",
            started=merged + timedelta(minutes=10))
        self.assertEqual(0, old_code["created"])
        self.source_run("future-run", merged + timedelta(minutes=12),
            merged + timedelta(minutes=20))
        not_yet_observed = f"component=extract__daily; run_id=future-run; occurred_at={(merged + timedelta(minutes=20)).isoformat()}"
        early, _, _ = self.triage(not_yet_observed, key="changed-key",
            started=merged + timedelta(minutes=10))
        self.assertEqual(0, early["created"])
        self.assertEqual(1, self.session.scalar(select(func.count()).select_from(Task)))
        new_reference = f"component=extract__daily; run_id=new-run; occurred_at={(merged + timedelta(minutes=15)).isoformat()}"
        self.source_run("new-run", merged + timedelta(minutes=12),
            merged + timedelta(minutes=15))
        result, _, _ = self.triage(new_reference, key="changed-key",
            started=merged + timedelta(minutes=20))
        self.assertEqual(1, result["created"])
        self.assertEqual(original.id, self.session.scalar(
            select(Task.related_task_id).where(Task.state == "proposed")
        ))

    def test_in_progress_failure_triage_cannot_change_admitted_scope(self):
        now = utcnow()
        old = f"component=extract__daily; run_id=old-run; occurred_at={(now - timedelta(hours=1)).isoformat()}"
        self.triage(old, started=now - timedelta(minutes=40))
        original = self.session.scalar(select(Task))
        original.state = "in_progress"
        original.planned_resolution = "Admitted repair scope"
        self.session.commit()
        fresh = f"component=extract__daily; run_id=new-run; occurred_at={now.isoformat()}"
        result, _, _ = self.triage(fresh, key="changed-key", started=now + timedelta(minutes=5))
        self.assertEqual({"created": 0, "revised": 0, "delegated": 0}, result)
        self.assertEqual("Admitted repair scope", original.planned_resolution)
        self.assertEqual(1, self.session.scalar(select(func.count()).select_from(Task)))

    def test_reset_preserves_history_and_requires_fresh_evidence(self):
        self.propose()
        before = utcnow() - timedelta(hours=1)
        preview = reset_queue(self.session, actor_id="operator", reason="Fresh assessment")
        self.assertEqual(1, preview["count"])
        self.assertEqual(1, queue_summary(self.session)["attention_count"])
        reset_queue(self.session, actor_id="operator", reason="Fresh assessment", apply=True)
        self.session.commit()
        self.assertEqual(0, queue_summary(self.session)["attention_count"])
        self.assertEqual(1, self.session.scalar(select(func.count()).select_from(Revision)))
        self.assertEqual(1, self.session.scalar(select(func.count()).select_from(Event).where(Event.event_type == "queue_reset")))
        self.assertEqual(0, self.propose("late-old-report", started=before)["created"])
        self.assertEqual(1, self.propose(started=utcnow() + timedelta(seconds=1))["created"])
        self.assertEqual(2, self.session.scalar(select(func.count()).select_from(Task)))

    def test_reset_refuses_in_flight_work(self):
        self.propose()
        task = self.session.scalar(select(Task))
        task.state = "in_progress"
        self.session.commit()
        with self.assertRaises(PreconditionFailed):
            reset_queue(self.session, actor_id="operator", reason="Fresh assessment", apply=True)
        self.assertEqual("in_progress", task.state)

    def test_new_issue_can_target_a_previously_completed_resource(self):
        self.propose()
        task = self.session.scalar(select(Task))
        task.state = "completed"
        self.session.commit()
        result = self.propose("new-upstream-change", "Adapt holidays to a changed upstream schema")
        self.assertEqual(1, result["created"])
        self.assertEqual(2, self.session.scalar(select(func.count()).select_from(Task)))

    def test_accepted_bot_work_is_active_not_awaiting_a_human_decision(self):
        self.propose()
        task = self.session.scalar(select(Task))
        task.state = "accepted"
        task.assignee_kind = "bot"
        task.assignee_profile = "junior"
        self.session.commit()
        summary = queue_summary(self.session)
        self.assertEqual(0, summary["attention_count"])
        self.assertEqual(1, summary["active_count"])

    def test_summary_is_not_limited_to_five_actions(self):
        for i in range(8):
            self.propose(str(i), f"Task {i}", resources=[f"source:{i}"])
        self.assertEqual(8, queue_summary(self.session)["attention_count"])

    def test_bundles_use_current_origin_and_keep_airflow_base_path(self):
        from airflow.providers.vintage.bot_dashboard import plugin
        with patch.object(plugin, "_enabled", return_value=True), patch.object(plugin.conf, "get", return_value="https://remote.example/airflow"):
            apps = plugin._react_apps()
        self.assertEqual(2, len(apps))
        self.assertTrue(all(app["bundle_url"].startswith("/airflow/bot-dashboard/static/") for app in apps))

    def test_packaged_activity_bundle_includes_model_settings(self):
        root = Path(__file__).parents[1] / "src/airflow/providers/vintage/bot_dashboard/static"
        manifest = json.loads((root / "asset-manifest.json").read_text("utf-8"))
        self.assertIn(b"Models & connections", (root / manifest["app"]).read_bytes())
