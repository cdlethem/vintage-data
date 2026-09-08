"""Queue behavior: stable matching, scope preservation, and auditable resets."""
import unittest
import uuid
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from airflow.providers.vintage.bot_dashboard.models import Event, Policy, Revision, Task, metadata, utcnow
from airflow.providers.vintage.bot_dashboard.service import (
    PreconditionFailed, queue_summary, reconcile_recommendations, reset_queue,
)


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)
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
