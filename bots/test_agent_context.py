from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from bots import agent_context


NOW = dt.datetime.now(dt.timezone.utc)


def report(payload, *, report_id="r", finished=None, outcome="succeeded"):
    return {"report_id": report_id, "payload": payload, "outcome": outcome, "finished_at": finished or NOW.isoformat()}


class FakeClient:
    def __init__(self, *, failures=None, health=None, manager=None):
        self.failures = failures or []
        self.health = health or []
        self.manager = manager or {"backlog": []}

    def airflow_failures(self, **kwargs):
        return {"items": self.failures, "remaining_after_batch": 0}

    def query_runs(self, *args, **kwargs):
        return {"items": self.health}

    def manager_context(self, **kwargs):
        return self.manager


class DiscoveryAndQueueBoundsTest(unittest.TestCase):
    def test_discovery_normalizes_and_caps_queries_and_summaries_and_backpressures_at_five(self):
        proposals = [{"slug": f"s{i}", "title": "title", "domain": "Example.COM", "viability": "medium"} for i in range(110)]
        searches = [f"  Query {i}  " for i in range(60)]
        rows = [report({"searches": searches, "proposals": proposals})]
        with mock.patch.object(agent_context, "_source_configs", return_value=[]), \
             mock.patch.object(agent_context, "_payload_rows", side_effect=lambda name, **kwargs: rows if name == "source_discovery" else []), \
             mock.patch.object(agent_context, "_seed_count", return_value=0):
            context = agent_context.discovery_context()
        self.assertEqual(len(context["recent_queries"]), 50)
        self.assertEqual(context["recent_queries"][0], "query 0")
        self.assertEqual(len(context["proposal_summaries"]), 100)
        self.assertEqual(context["total_count"], 110)
        self.assertEqual(context["returned_count"], 100)

        self.assertEqual(context["context_schema_version"], 1)
        self.assertIn("as_of", context)
        five = [{"slug": f"pending-{i}", "title": "x", "domain": "x", "viability": "low"} for i in range(5)]
        with mock.patch.object(agent_context, "_source_configs", return_value=[]), \
             mock.patch.object(agent_context, "_payload_rows", side_effect=lambda name, **kwargs: [report({"proposals": five})] if name == "source_discovery" else []), \
             mock.patch.object(agent_context, "_seed_count", return_value=0):
            pressured = agent_context.discovery_context()
        self.assertEqual(pressured["vetting_pending_count"], 5)
        self.assertEqual(pressured["backpressure_limit"], 5)
        self.assertTrue(pressured["backpressure"])

    def test_vetting_and_scheduling_select_one_deterministic_item(self):
        proposals = [
            {"slug": "z", "title": "z", "domain": "x", "viability": "high"},
            {"slug": "a", "title": "a", "domain": "x", "viability": "high"},
            {"slug": "low", "title": "low", "domain": "x", "viability": "low"},
        ]
        discovery = [report({"proposals": proposals}, report_id="new", finished="2026-01-02T00:00:00+00:00"), report({"proposals": [{"slug": "z", "title": "z", "domain": "x", "viability": "high"}]}, report_id="old", finished="2026-01-01T00:00:00+00:00")]
        with mock.patch.object(agent_context, "_source_configs", return_value=[]), \
             mock.patch.object(agent_context, "_payload_rows", side_effect=lambda name, **kwargs: discovery if name == "source_discovery" else []):
            selected = agent_context.vetting_context()
        self.assertEqual(selected["returned_count"], 3)
        self.assertEqual(selected["selected"]["slug"], "a")
        self.assertEqual(selected["context_schema_version"], 1)
        self.assertIn("as_of", selected)

        decisions = [
            {"slug": "z", "decision": "recommended", "task_proposal": {"recommendation_key": "z", "resource_keys": ["extract/z.py", "extract/sources/z.yml"]}},
            {"slug": "a", "decision": "recommended", "task_proposal": {"recommendation_key": "a", "resource_keys": ["extract/a.py", "extract/sources/a.yml"]}},
        ]
        with mock.patch.object(agent_context, "_source_configs", return_value=[]), \
             mock.patch.object(agent_context, "_payload_rows", return_value=[report({"decisions": decisions})]):
            scheduling = agent_context.scheduling_context()
        self.assertEqual(scheduling["returned_count"], 2)
        self.assertEqual(scheduling["context_schema_version"], 1)
        self.assertEqual(scheduling["selected"]["slug"], "a")
        self.assertEqual(sum(item is not None for item in [scheduling["selected"]]), 1)


class CadenceBoundsTest(unittest.TestCase):
    def test_cadence_deduplicates_caps_audit_and_decision_text_and_skips_only_when_fresh(self):
        healthy = {"status": "OK", "stale": False, "failed_runs": 0, "consecutive_failures": 0, "zero_record_runs": 0, "held_runs": 0, "records_last": 100, "extract_health": "healthy"}
        digest = {"sources": [{"source": f"s{i}", **healthy} for i in range(20)] + [{"source": "s0", **healthy}]}
        audit = [report({}, report_id="audit", finished=(NOW - dt.timedelta(hours=1)).isoformat())]
        with mock.patch.object(agent_context, "_run_capture", return_value=digest), \
             mock.patch.object(agent_context, "_source_configs", return_value=[]), \
             mock.patch.object(agent_context, "_payload_rows", return_value=audit):
            context = agent_context.cadence_context(audit_size=50)
        self.assertEqual(len({row["source"] for row in context["rotating_sources"]}), 12)
        self.assertEqual(context["audit_size"], 12)
        self.assertLessEqual(len(context["cadence_decision"].splitlines()), 40)
        self.assertLessEqual(len(context["cadence_decision"].encode()), 12_000)
        self.assertFalse(context["review_required"])
        self.assertEqual(context["context_schema_version"], 1)
        self.assertIn("as_of", context)

        flagged = {"sources": [{"source": "s0", "status": "PROBLEM", "stale": True, "failed_runs": 2, "extract_health": "failing"}]}
        with mock.patch.object(agent_context, "_run_capture", return_value=flagged), \
             mock.patch.object(agent_context, "_source_configs", return_value=[]), \
             mock.patch.object(agent_context, "_payload_rows", return_value=audit):
            self.assertTrue(agent_context.cadence_context()["review_required"])


class FailureAndAnalyticsBoundsTest(unittest.TestCase):
    def test_failures_group_normalized_ids_separate_component_and_code_and_cap_evidence(self):
        items = []
        for i in range(12):
            items.append({"dag_id": f"etl_{i}", "task_id": "load", "run_id": f"run-{i}", "ended_at": f"2026-01-01T00:00:{i:02d}+00:00", "exception_type": "ValueError", "error_code": "bad_input", "detail": "x" * 5000, "sink": f"sink-{i}"})
        for i in range(15):
            items.append({"dag_id": "etl_same", "task_id": "load", "run_id": f"same-{i}", "ended_at": f"2026-01-01T00:00:{i:02d}+00:00", "exception_type": "ValueError", "error_code": "bad_input", "detail": "same detail", "sink": f"marker-{i}"})
        items.append({"dag_id": "etl_same", "task_id": "load", "run_id": "different-code", "ended_at": "2026-01-01T00:00:00+00:00", "exception_type": "ValueError", "error_code": "other_code", "detail": "same detail", "sink": "other"})
        client = FakeClient(failures=items)
        with mock.patch.object(agent_context, "_control_client", return_value=client):
            context = agent_context.failure_context(batch_size=50)
        self.assertEqual(context["failure_count"], 14)
        self.assertLessEqual(len(context["selected_failures"]), 10)
        same = next(group for group in context["selected_failures"] if group["component"] == "etl_same")
        self.assertEqual(same["count"], 15)
        self.assertLessEqual(len(same["occurrences"]), 10)
        self.assertLessEqual(len(same["sink_markers"]), 20)
        self.assertEqual(context["context_schema_version"], 1)
        self.assertLessEqual(len(same["tail"].encode()), 3500)
        self.assertEqual(len(same["fingerprint"]), 64)
        self.assertTrue(all(len(group["fingerprint"]) == 64 for group in context["selected_failures"]))
        self.assertTrue(context["bot_dags_excluded"])

    def test_analytics_selects_zero_or_one_source_and_fails_on_warehouse_or_manifest(self):
        sync = {"ok": True, "sources": [{"name": "b", "issues": []}, {"name": "a", "issues": ["drift"]}]}
        manifest = {"nodes": {"model.a": {"sources": [], "path": "models/a.sql"}}}
        client = FakeClient()
        with mock.patch.object(agent_context, "_control_client", return_value=client), \
             mock.patch.object(agent_context, "_payloads", return_value=[]):
            context = agent_context.analytics_context(sync_result=sync, manifest=manifest, visualization_report={"errors": []})
        self.assertEqual(context["returned_count"], 1)
        self.assertEqual(context["context_schema_version"], 1)
        self.assertEqual(context["selected"]["name"], "a")
        with mock.patch.object(agent_context, "_control_client", return_value=client), \
             mock.patch.object(agent_context, "_payloads", return_value=[]):
            empty = agent_context.analytics_context(sync_result={"ok": True, "sources": []}, manifest=manifest, visualization_report={"errors": []})
        self.assertIsNone(empty["selected"])
        self.assertEqual(empty["returned_count"], 0)

        with self.assertRaises(agent_context.ContextError) as caught:
            agent_context.analytics_context(sync_result={"ok": False, "error": "warehouse down"}, manifest=manifest, visualization_report={"errors": []})
        self.assertEqual(caught.exception.code, "warehouse_sync_failed")
        with self.assertRaises(agent_context.ContextError) as caught:
            agent_context.analytics_context(sync_result=sync, manifest={"bad": True}, visualization_report={"errors": []})
        self.assertEqual(caught.exception.code, "manifest_invalid")


class ContextContractTest(unittest.TestCase):
    def test_context_metadata_and_encoded_bounds_are_present(self):
        base = agent_context._base("test")
        self.assertEqual(base["context_schema_version"], 1)
        self.assertIn("as_of", base)
        self.assertLessEqual(agent_context._encoded_size(base), agent_context.CONTEXT_LIMITS["source_discovery"])

    def test_dashboard_error_is_context_error_not_healthy_empty_queue(self):
        class Down:
            code = "dashboard_unavailable"
        with mock.patch.object(agent_context, "_control_client", side_effect=Down()):
            with self.assertRaises(agent_context.ContextError) as caught:
                agent_context._reports("source_discovery")
        self.assertEqual(caught.exception.code, "dashboard_unavailable")


if __name__ == "__main__":
    unittest.main()
