from __future__ import annotations

import copy
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from airflow.providers.vintage.bot_dashboard.artifacts import put_artifact
from airflow.providers.vintage.bot_dashboard.models import RunBudgetClaim, RunReport, metadata
from airflow.providers.vintage.bot_dashboard.projection import persist_run_envelope
from airflow.providers.vintage.bot_dashboard.service import claim_run_budget


class ProjectionTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    @staticmethod
    def envelope(*, outcome="skipped", reason="gate_no_work", bot="source_discovery", run_id="r1"):
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        failure = None
        retry = "none"
        if outcome == "capacity_unavailable":
            reason, retry = "provider_busy", "capacity"
        elif outcome == "timed_out":
            reason, retry = "deadline_exceeded", "terminal"
            failure = {"class": "ProviderTimeout", "code": "timeout", "fingerprint": "1" * 64, "detail": "deadline"}
        elif outcome == "failed":
            reason, retry = "provider_failed", "terminal"
            failure = {"class": "ProviderTerminal", "code": "invalid", "fingerprint": "2" * 64, "detail": "terminal"}
        return {
            "envelope_version": 1,
            "identity": {"bot": bot, "dag_id": f"bot__{bot}", "run_id": run_id, "task_id": "run", "map_index": -1, "try_number": 1},
            "timing": {"started_at": now, "finished_at": now, "deadline_at": now, "duration_ms": 0},
            "outcome": outcome,
            "retry_class": retry,
            "reason_code": reason,
            "failure": failure,
            "selected_model": None,
            "attempts": [],
            "context": {"sha256": "0" * 64, "byte_count": 0, "build_ms": 0},
            "payload_schema": None,
            "payload": None,
        }

    def test_budget_claim_is_immutable_and_report_links_to_it(self):
        with Session(self.engine) as session:
            first = claim_run_budget(session, dag_id="bot__source_discovery", run_id="r1", task_id="run", map_index=-1, configured_total_seconds=60)
            replay = claim_run_budget(session, dag_id="bot__source_discovery", run_id="r1", task_id="run", map_index=-1, configured_total_seconds=60)
            self.assertEqual(first["configured_total_seconds"], replay["configured_total_seconds"])
            self.assertEqual(datetime.fromisoformat(first["deadline_at"].replace("Z", "+00:00")).replace(tzinfo=None), datetime.fromisoformat(replay["deadline_at"].replace("Z", "+00:00")).replace(tzinfo=None))
            with self.assertRaises(Exception):
                claim_run_budget(session, dag_id="bot__source_discovery", run_id="r1", task_id="run", map_index=-1, configured_total_seconds=61)
            projection = persist_run_envelope(session, self.envelope())
            replay_projection = persist_run_envelope(session, self.envelope())
            session.commit()
            self.assertEqual(projection, replay_projection)
            report = session.scalar(select(RunReport))
            claim = session.scalar(select(RunBudgetClaim))
            self.assertEqual(claim.id, report.budget_claim_id)
            self.assertEqual("skipped", report.outcome)
            self.assertIsNone(report.body_json)

    def test_non_success_outcomes_need_no_payload_and_have_retention(self):
        expected = {"skipped": 7, "capacity_unavailable": 7, "timed_out": 365, "failed": 365}
        with Session(self.engine) as session:
            for index, (outcome, days) in enumerate(expected.items(), 1):
                value = self.envelope(outcome=outcome, run_id=f"r{index}")
                persist_run_envelope(session, value)
                row = session.scalar(select(RunReport).where(RunReport.run_id == f"r{index}"))
                self.assertIsNone(row.body_json)
                age = (row.expires_at - row.created_at).total_seconds() / 86400
                self.assertAlmostEqual(days, age, delta=0.01)
            session.commit()

    def test_failure_metadata_including_bounded_detail_is_persisted(self):
        value = self.envelope(outcome="timed_out", run_id="timeout")
        value["failure"]["detail"] = "command provider deadline expired"
        with Session(self.engine) as session:
            persist_run_envelope(session, value)
            session.commit()
            row = session.scalar(select(RunReport).where(RunReport.run_id == "timeout"))
        self.assertEqual("ProviderTimeout", row.failure_class)
        self.assertEqual("timeout", row.failure_code)
        self.assertEqual("1" * 64, row.failure_fingerprint)
        self.assertEqual("command provider deadline expired", row.failure_detail)
        self.assertLessEqual(len(row.failure_detail), 8192)

    def test_attempt_token_counters_survive_secret_masking(self):
        """Airflow's masker treats any key containing "token" as sensitive."""
        value = self.envelope(outcome="failed", run_id="tokens")
        now = value["timing"]["started_at"]
        value["selected_model"] = "primary"
        value["attempts"] = [
            {
                "ordinal": 1,
                "alias": "primary",
                "provider": "command",
                "started_at": now,
                "finished_at": now,
                "duration_ms": 12,
                "outcome": "failed",
                "reason_code": "provider_failed",
                "fallback_used": False,
                "input_tokens": 20,
                "output_tokens": 22,
                "total_tokens": 42,
            }
        ]
        with Session(self.engine) as session:
            persist_run_envelope(session, value)
            session.commit()
            row = session.scalar(select(RunReport).where(RunReport.run_id == "tokens"))
        self.assertEqual((20, 22, 42), (row.input_tokens, row.output_tokens, row.total_tokens))
        self.assertEqual(12, row.provider_duration_ms)
        self.assertEqual([20], [attempt["input_tokens"] for attempt in row.attempts_json])

    def test_usage_rollup_persists_and_emits_tagged_metrics(self):
        value = self.envelope(outcome="failed", run_id="usage")
        value["selected_model"] = "gpt-5.6-luna"
        value["usage_total"] = {
            "schema": "usage.v1",
            "input_tokens": 103792,
            "output_tokens": 12375,
            "cached_input_tokens": 1069056,
            "cache_write_tokens": 0,
            "reasoning_tokens": 0,
            "total_tokens": 116167,
            "requests": 30,
            "cost_micro_usd": 57028,
            "cost_source": "price_book",
            "pricing_id": "2026-09-08",
        }
        with patch("airflow.providers.vintage.bot_dashboard.projection.stats") as metrics:
            with Session(self.engine) as session:
                persist_run_envelope(session, value)
                session.commit()
                row = session.scalar(select(RunReport).where(RunReport.run_id == "usage"))
        self.assertEqual((103792, 12375, 1069056, 30, 57028), (row.input_tokens, row.output_tokens, row.cached_input_tokens, row.model_requests, row.cost_micro_usd))
        metrics.gauge.assert_any_call(
            "bot_dashboard.run.usage.cost_micro_usd",
            57028,
            tags={"bot": "source_discovery", "model": "gpt-5.6-luna"},
        )

    def test_changed_replay_body_is_rejected(self):
        value = self.envelope()
        with Session(self.engine) as session:
            persist_run_envelope(session, value)
            changed = copy.deepcopy(value)
            changed["reason_code"] = "different_reason"
            with self.assertRaises(ValueError):
                persist_run_envelope(session, changed)


    def test_artifact_replay_is_content_addressed_and_typed(self):
        # Configure through the environment: patching the shared conf object
        # intercepts unrelated Airflow lookups made on the same thread.
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"AIRFLOW__BOT_DASHBOARD__ARTIFACT_ROOT": directory}
        ):
            with Session(self.engine) as session:
                first = put_artifact(session, kind="patch", content=b"diff")
                replay = put_artifact(session, kind="patch", content=b"diff", expected_sha256=first["sha256"])
                self.assertEqual(first["sha256"], replay["sha256"])
                self.assertEqual(first["media_type"], replay["media_type"])
                self.assertEqual(first["byte_count"], replay["byte_count"])
                with self.assertRaises(ValueError):
                    put_artifact(session, kind="source", content=b"diff", expected_sha256=first["sha256"])
                self.assertEqual("text/x-diff", first["media_type"])
                self.assertTrue((Path(directory) / first["sha256"][:2] / first["sha256"]).exists())

if __name__ == "__main__":
    unittest.main()
