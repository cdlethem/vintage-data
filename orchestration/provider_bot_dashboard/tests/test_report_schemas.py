from __future__ import annotations

import copy
import unittest
from datetime import datetime, timezone

from pydantic import ValidationError

from airflow.providers.vintage.bot_dashboard.report_schemas import (
    validate_named_report,
    validate_run_envelope,
)


class ReportSchemaTest(unittest.TestCase):
    def test_named_v2_contracts_are_strict(self):
        value = {
            "schema_version": 2,
            "agent": "source_discovery",
            "status": "ok",
            "searches": ["vintage data"],
            "proposals": [],
            "dismissed": [],
            "user_escalations": [],
            "summary": "No new sources.",
        }
        self.assertEqual(value, validate_named_report("source_discovery_v2", value))
        unknown = copy.deepcopy(value)
        unknown["unexpected"] = True
        with self.assertRaises(ValidationError):
            validate_named_report("source_discovery_v2", unknown)

    def test_manager_v3_cross_field_invariants(self):
        value = {
            "schema_version": 3,
            "agent": "manager",
            "status": "degraded_evidence",
            "report_date": "2026-09-07",
            "executive_summary": "Evidence is incomplete.",
            "plan": [],
            "deferred": [{"item": "queue", "reason": "stale", "revisit_when": "next run"}],
            "approvals_required": [],
            "input_freshness": {"as_of": datetime.now(timezone.utc).isoformat(), "freshness_ok": False, "stale_agents": [], "missing_agents": [], "failed_agents": [], "max_age_seconds": 60},
        }
        self.assertEqual(3, validate_named_report("manager_v3", value)["schema_version"])
        mismatch = copy.deepcopy(value)
        mismatch["status"] = "ok"
        with self.assertRaises(ValidationError):
            validate_named_report("manager_v3", mismatch)

    def test_data_analyst_v1_requires_measured_evidence_and_full_trend_coverage(self):
        proposal = {
            "recommendation_key": "analysis-demo-trends",
            "title": "Add time series to the demo family dashboard",
            "category": "other",
            "priority": 40,
            "planned_resolution": "Add the reviewed analysis block and a second analytic metric.",
            "why_now": "The dashboard ranks one category and shows no change over time.",
            "expected_benefit": "Trends become readable at day and month grain.",
            "risk": "A wrong grain renders an unreadable chart.",
            "rollback": "Revert the family YAML and regenerate content.",
            "verification_commands": [["visualization/bin/viz", "check-analysis",
                                       "transform/models/marts/demo/_demo_models.yml"]],
            "allowed_path_globs": ["transform/models/marts/demo/_demo_models.yml"],
            "resource_keys": ["analysis:demo"],
            "follow_up_bots": ["data_analyst"],
            "suggested_executor": "senior",
            "reviewer_required": True,
            "evidence": [{"kind": "query", "reference": "fct_demo", "summary": "36 populated months."}],
        }
        value = {
            "schema_version": 1,
            "agent": "data_analyst",
            "status": "ok",
            "family": "demo",
            "queries": ["select date_trunc('month', happened_at), count(*) from fct_demo group by 1"],
            "analyses": [{
                "model": "fct_demo",
                "rows": 1000,
                "event_time_column": "happened_at",
                "event_time_span": "2023-08-01..2026-07-01, 36 months",
                "collection_time_column": "observed_at",
                "dimensions": [{"column": "category", "distinct_values": 14, "null_share": 0.0,
                                "top_values": ["a", "b"]}],
                "metrics_added": ["reported"],
                "trends": [
                    {"slug": "volume", "kind": "total", "metric": "entities", "time_column": "happened_at",
                     "grain": "MONTH", "top_n": 0, "observed_points": 36,
                     "reading": "Distinct entities per event month; absent months are absent, not zero."},
                    {"slug": "by-category", "kind": "breakdown", "metric": "entities",
                     "time_column": "happened_at", "grain": "MONTH", "breakdown": "category", "top_n": 6,
                     "observed_points": 36, "reading": "Largest six categories; remainder excluded."},
                ],
                "findings": ["May 2025 held 1,841 distinct entities across 353 streets."],
            }],
            "plans": [{"source": "demo", "steps": ["profile", "query", "specify"], "task_proposal": proposal}],
            "summary": "Demo family gains monthly event-time trends.",
        }
        self.assertEqual("demo", validate_named_report("data_analyst_v1", value)["family"])
        collection_only = copy.deepcopy(value)
        for trend in collection_only["analyses"][0]["trends"]:
            trend["time_column"] = "observed_at"
        with self.assertRaises(ValidationError):
            validate_named_report("data_analyst_v1", collection_only)
        no_dimension = copy.deepcopy(value)
        no_dimension["analyses"][0]["trends"] = value["analyses"][0]["trends"][:1]
        with self.assertRaises(ValidationError):
            validate_named_report("data_analyst_v1", no_dimension)
        unbounded = copy.deepcopy(value)
        unbounded["analyses"][0]["trends"][1]["top_n"] = 0
        with self.assertRaises(ValidationError):
            validate_named_report("data_analyst_v1", unbounded)

    def test_unknown_schema_and_deep_or_oversized_output_fail(self):
        with self.assertRaises(ValueError):
            validate_named_report("untrusted", {})
        deep: object = "leaf"
        for _ in range(22):
            deep = {"child": deep}
        with self.assertRaises(ValueError):
            validate_named_report("source_discovery_v2", deep)

    def test_run_envelope_requires_typed_identity_and_timing(self):
        now = datetime.now(timezone.utc).isoformat()
        envelope = {
            "envelope_version": 1,
            "identity": {"bot": "source_discovery", "dag_id": "bot__source_discovery", "run_id": "r1", "task_id": "run", "map_index": -1, "try_number": 1},
            "timing": {"started_at": now, "finished_at": now, "deadline_at": now, "duration_ms": 0},
            "outcome": "skipped",
            "retry_class": "none",
            "reason_code": "gate_no_work",
            "failure": None,
            "selected_model": None,
            "attempts": [],
            "context": {"sha256": "0" * 64, "byte_count": 0, "build_ms": 0},
            "payload_schema": None,
            "payload": None,
        }
        self.assertEqual("skipped", validate_run_envelope(envelope)["outcome"])
        bad = copy.deepcopy(envelope)
        bad["identity"]["try_number"] = 0
        with self.assertRaises(ValidationError):
            validate_run_envelope(bad)


if __name__ == "__main__":
    unittest.main()
