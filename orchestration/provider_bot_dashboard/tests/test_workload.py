import unittest
from datetime import timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from airflow.providers.vintage.bot_dashboard import workload
from airflow.providers.vintage.bot_dashboard.models import Task, metadata, utcnow


class WorkloadTest(unittest.TestCase):
    def evaluate(self, **changes):
        return workload.evaluate(**{**dict(open_count=0, created_24h=0, completed_24h=0,
                                           completed_7d=0, new_sources_24h=0), **changes})

    def test_cold_start_is_bounded_and_pressure_resumes_automatically(self):
        self.assertTrue(self.evaluate()["allowed"])
        self.assertEqual(1, self.evaluate()["source_budget_24h"])
        self.assertFalse(self.evaluate(created_24h=1, open_count=1)["allowed"])
        self.assertEqual("open_work_limit", self.evaluate(open_count=8)["reason_code"])
        self.assertTrue(self.evaluate(open_count=7, completed_24h=10, completed_7d=20)["allowed"])

    def test_measured_budget_reserves_headroom_and_caps_source_intake(self):
        self.assertFalse(self.evaluate(created_24h=8, completed_24h=10)["allowed"])
        value = self.evaluate(completed_7d=100, completed_24h=20, new_sources_24h=2)
        self.assertEqual(2, value["source_budget_24h"])
        self.assertEqual("source_budget_used", value["reason_code"])

    def test_blocked_work_counts_and_dismissals_never_buy_capacity(self):
        engine = create_engine("sqlite:///:memory:")
        metadata.create_all(engine)
        now = utcnow()
        with Session(engine) as session:
            for i in range(9):
                session.add(Task(source="manual", recommendation_key=str(i), title="Work", category="new_source",
                    state="blocked" if i < 8 else "dismissed", priority=1,
                    created_at=now - timedelta(days=2), dismissed_at=now if i == 8 else None))
            session.commit()
            before = workload.status(session)
            self.assertEqual(8, before["open_count"])
            self.assertEqual(0, before["completed_24h"])
            self.assertFalse(before["allowed"])
            task = session.query(Task).filter_by(recommendation_key="0").one()
            task.state = "completed"; task.completed_at = now
            session.commit()
            after = workload.status(session)
            self.assertEqual(1, after["completed_24h"])
            self.assertEqual(7, after["open_count"])
            self.assertTrue(after["allowed"])
        engine.dispose()
