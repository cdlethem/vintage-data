from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import Session

from airflow.providers.vintage.bot_dashboard.db_manager import BotDashboardDBManager
from airflow.providers.vintage.bot_dashboard.models import RunReport, metadata
from airflow.providers.vintage.bot_dashboard.projection import persist_run_envelope
from airflow.providers.vintage.bot_dashboard.service import SpendCapReached, claim_run_budget, usage_summary


class UsageAccountingTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)
        self.now = datetime(2026, 9, 7, 12, tzinfo=timezone.utc)

    def tearDown(self):
        self.engine.dispose()

    def _envelope(self, run_id="r1", *, usage_total=None, attempts=None, model="gpt-5.6-luna"):
        stamp = self.now.isoformat()
        return {
            "envelope_version": 1,
            "identity": {"bot": "analytics_engineer", "dag_id": "bot__analytics_engineer", "run_id": run_id, "task_id": "run", "map_index": -1, "try_number": 1},
            "timing": {"started_at": stamp, "finished_at": stamp, "deadline_at": stamp, "duration_ms": 0},
            "outcome": "failed",
            "retry_class": "terminal",
            "reason_code": "provider_failed",
            "failure": {"class": "Provider", "code": "failed", "fingerprint": "1" * 64, "detail": "failed"},
            "selected_model": model,
            "attempts": attempts or [],
            "usage_total": usage_total,
            "context": {"sha256": "0" * 64, "byte_count": 0, "build_ms": 0},
            "payload_schema": None,
            "payload": None,
        }

    @staticmethod
    def _usage(input_tokens, output_tokens, cached, cost, *, requests=1, pricing_id="2026-09-08"):
        return {"schema": "usage.v1", "input_tokens": input_tokens, "output_tokens": output_tokens, "cached_input_tokens": cached, "cache_write_tokens": 0, "reasoning_tokens": 0, "total_tokens": input_tokens + output_tokens, "requests": requests, "cost_micro_usd": cost, "cost_source": "price_book", "pricing_id": pricing_id}

    def test_usage_persists_explicit_and_attempt_fallback(self):
        total = self._usage(103792, 12375, 1069056, 57028, requests=30)
        stamp = self.now.isoformat()
        attempt = {"ordinal": 1, "alias": "luna", "provider": "command", "started_at": stamp, "finished_at": stamp, "duration_ms": 1, "outcome": "failed", "reason_code": "provider_failed", "fallback_used": False, "usage": self._usage(2, 3, 4, 5)}
        with Session(self.engine) as session:
            persist_run_envelope(session, self._envelope(usage_total=total))
            persist_run_envelope(session, self._envelope("fallback", attempts=[attempt]))
            session.commit()
            explicit = session.scalar(select(RunReport).where(RunReport.run_id == "r1"))
            fallback = session.scalar(select(RunReport).where(RunReport.run_id == "fallback"))
        self.assertEqual((103792, 12375, 1069056, 30, 57028), (explicit.input_tokens, explicit.output_tokens, explicit.cached_input_tokens, explicit.model_requests, explicit.cost_micro_usd))
        self.assertEqual((2, 3, 4, 1, 5), (fallback.input_tokens, fallback.output_tokens, fallback.cached_input_tokens, fallback.model_requests, fallback.cost_micro_usd))

    def test_usage_summary_groups_and_excludes_legacy(self):
        with Session(self.engine) as session:
            for index, (bot, model, cost) in enumerate((("a", "m1", 30), ("b", "m2", 20), ("a", "m1", 10)), 1):
                session.add(RunReport(dag_id=f"bot__{bot}", run_id=f"r{index}", task_id="run", map_index=-1, try_number=1, bot_name=bot, report_format="json", model=model, status="ok", outcome="succeeded", retry_class="none", reason_code="ok", started_at=self.now, finished_at=self.now, deadline_at=self.now, duration_ms=0, context_sha256="0"*64, context_byte_count=0, context_build_ms=0, attempts_json=[], input_tokens=1, output_tokens=2, total_tokens=3, cached_input_tokens=4, cache_write_tokens=0, reasoning_tokens=0, model_requests=1, cost_micro_usd=cost, cost_source="price_book", sha256=f"{index:064x}", byte_count=1, created_at=self.now, expires_at=self.now))
            session.add(RunReport(dag_id="legacy__import", run_id="old", task_id="run", map_index=-1, try_number=1, bot_name="legacy", report_format="json", model="old", status="ok", outcome="succeeded", retry_class="none", reason_code="ok", started_at=self.now, finished_at=self.now, deadline_at=self.now, duration_ms=0, context_sha256="0"*64, context_byte_count=0, context_build_ms=0, attempts_json=[], input_tokens=99, output_tokens=99, total_tokens=198, model_requests=1, cost_micro_usd=999, cost_source="price_book", sha256="9"*64, byte_count=1, created_at=self.now, expires_at=self.now))
            session.commit()
            result = usage_summary(session, days=30, now=self.now)
        self.assertEqual(60, result["totals"]["cost_micro_usd"])
        self.assertEqual(["m1", "m2"], [item["model"] for item in result["by_model"]])
        self.assertEqual(["a", "b"], [item["bot"] for item in result["by_bot"]])
        self.assertEqual(1, len(result["by_day"]))

    def test_spend_cap_blocks_new_but_allows_replay(self):
        with patch("airflow.providers.vintage.bot_dashboard.service.conf.getfloat", return_value=0.001), patch("airflow.providers.vintage.bot_dashboard.service.utcnow", return_value=self.now):
            with Session(self.engine) as session:
                claim_run_budget(session, dag_id="bot__a", run_id="spent", task_id="run", map_index=-1, configured_total_seconds=60)
                session.add(RunReport(dag_id="bot__a", run_id="spent", task_id="run", map_index=-1, try_number=1, bot_name="a", report_format="json", status="ok", outcome="succeeded", retry_class="none", reason_code="ok", started_at=self.now, finished_at=self.now, deadline_at=self.now, duration_ms=0, context_sha256="0"*64, context_byte_count=0, context_build_ms=0, attempts_json=[], model_requests=1, cost_micro_usd=1000, cost_source="price_book", sha256="8"*64, byte_count=1, created_at=self.now, expires_at=self.now))
                session.commit()
                with self.assertRaises(SpendCapReached):
                    claim_run_budget(session, dag_id="bot__new", run_id="new", task_id="run", map_index=-1, configured_total_seconds=60)
                self.assertIsNotNone(claim_run_budget(session, dag_id="bot__a", run_id="spent", task_id="run", map_index=-1, configured_total_seconds=60))

    def test_migration_0004_is_replayable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "dashboard.sqlite"
            config = Config(BotDashboardDBManager.alembic_file)
            config.set_main_option("script_location", BotDashboardDBManager.migration_dir)
            config.set_main_option("sqlalchemy.url", f"sqlite:///{database}")
            command.upgrade(config, "0003_workflow_hardening")
            engine = create_engine(f"sqlite:///{database}")
            command.upgrade(config, "0004_usage_accounting")
            command.upgrade(config, "0004_usage_accounting")
            columns = {column["name"] for column in inspect(engine).get_columns("bot_dashboard_run_report")}
            self.assertTrue({"cached_input_tokens", "cache_write_tokens", "reasoning_tokens", "model_requests", "cost_micro_usd", "cost_source", "pricing_id"}.issubset(columns))
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
