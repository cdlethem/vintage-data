from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.providers.vintage.bot_dashboard import api
from airflow.providers.vintage.bot_dashboard import __version__ as provider_version
from airflow.providers.vintage.bot_dashboard.models import metadata


class InternalApiTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        metadata.create_all(self.engine)
        self.user = SimpleNamespace(get_name=lambda: "bot-worker", get_id=lambda: "worker-id")
        dependency = next(item.dependency for item in SessionDep.__metadata__)
        def session_override():
            with Session(self.engine) as session:
                yield session
                session.commit()
        api.app.dependency_overrides[api.get_user] = lambda: self.user
        api.app.dependency_overrides[api.auth.dependencies[1].dependency] = lambda: None
        api.app.dependency_overrides[dependency] = session_override
        api.app.dependency_overrides[api._schema_guard] = lambda: None
        self.client = TestClient(api.app)

    def tearDown(self):
        api.app.dependency_overrides.clear()
        self.engine.dispose()

    @property
    def headers(self):
        return {"authorization": "Bearer test-token"}

    def test_health_reports_provider_version_without_raising(self):
        """Health is the operator's only readiness probe; it must never 500."""
        response = self.client.get("/api/health")
        self.assertEqual(200, response.status_code, response.text)
        body = response.json()
        self.assertEqual(provider_version, body["provider_version"])
        self.assertIn(body["status"], {"ready", "not_ready"})
        self.assertIsInstance(body["codes"], list)

    def test_internal_identity_forbids_cookie_wrong_user_and_non_bearer(self):
        with patch.dict(os.environ, {"AIRFLOW__BOT_DASHBOARD__INTERNAL_USER": "bot-worker"}):
            self.assertIn(self.client.post("/api/internal/runs/claim-budget", headers={"cookie": "session=x"}, json={}).status_code, (401, 403))
            self.assertIn(self.client.post("/api/internal/runs/claim-budget", headers={}, json={}).status_code, (401, 403))
            api.app.dependency_overrides[api.get_user] = lambda: SimpleNamespace(get_name=lambda: "browser", get_id=lambda: "browser")
            self.assertEqual(403, self.client.post("/api/internal/runs/claim-budget", headers=self.headers, json={}).status_code)

    def test_budget_replay_is_idempotent_and_changed_timeout_is_rejected(self):
        body = {"identity": {"dag_id": "bot__source_discovery", "run_id": "r1", "task_id": "run", "map_index": -1}, "configured_total_seconds": 60}
        with patch.dict(os.environ, {"AIRFLOW__BOT_DASHBOARD__INTERNAL_USER": "bot-worker"}):
            first = self.client.post("/api/internal/runs/claim-budget", headers=self.headers, json=body)
            replay = self.client.post("/api/internal/runs/claim-budget", headers=self.headers, json=body)
            changed = {**body, "configured_total_seconds": 61}
            rejected = self.client.post("/api/internal/runs/claim-budget", headers=self.headers, json=changed)
        self.assertEqual(200, first.status_code)
        self.assertEqual(first.json()["configured_total_seconds"], replay.json()["configured_total_seconds"])
        self.assertEqual(first.json()["deadline_at"].replace("+00:00", ""), replay.json()["deadline_at"].replace("+00:00", ""))
        self.assertEqual(409, rejected.status_code)
        self.assertEqual("logical run budget cannot change", rejected.json()["detail"])

    def test_malformed_and_oversized_internal_bodies_are_rejected(self):
        malformed = self.client.post("/api/internal/runs/claim-budget", headers={**self.headers, "content-type": "application/json"}, content=b"not-json")
        oversized = self.client.post("/api/internal/runs/claim-budget", headers={**self.headers, "content-type": "application/json"}, content=b"x" * (api.MAX_BODY + 1))
        self.assertEqual(422, malformed.status_code)
        self.assertEqual(413, oversized.status_code)

    def test_schema_unready_requests_fail_closed(self):
        api.app.dependency_overrides.pop(api._schema_guard, None)
        with patch("airflow.providers.vintage.bot_dashboard.api.BotDashboardDBManager.check_migration", return_value=False):
            response = self.client.post("/api/internal/runs/claim-budget", headers=self.headers, json={})
        self.assertEqual(503, response.status_code)
        self.assertEqual("migration_required", response.json()["detail"]["code"])


    def test_usage_route_returns_bounded_summary_and_no_store(self):
        summary = {
            "days": 30,
            "currency": "USD",
            "generated_at": "2026-09-07T00:00:00+00:00",
            "totals": {
                "requests": 1, "input_tokens": 10, "output_tokens": 2,
                "cached_input_tokens": 8, "cache_write_tokens": 0, "reasoning_tokens": 0,
                "total_tokens": 12, "cost_micro_usd": 3, "runs": 1,
                "priced_runs": 1, "unpriced_runs": 0,
            },
            "by_model": [],
            "by_bot": [],
            "by_day": [],
            "cap": {"daily_spend_cap_micro_usd": None, "spent_today_micro_usd": 0, "exceeded": False},
        }
        with patch("airflow.providers.vintage.bot_dashboard.api.usage_summary", return_value=summary):
            response = self.client.get("/api/usage?days=30", headers=self.headers)
        self.assertEqual(200, response.status_code, response.text)
        self.assertEqual("no-store", response.headers["cache-control"])
        self.assertEqual(summary, response.json())
        self.assertNotIn("secret", response.text.lower())
        self.assertNotIn("credential", response.text.lower())
        self.assertNotIn("prompt", response.text.lower())
        self.assertNotIn("context", response.text.lower())
        self.assertNotIn("path", response.text.lower())

    def test_usage_route_rejects_days_outside_contract(self):
        for days in ("0", "401"):
            response = self.client.get(f"/api/usage?days={days}", headers=self.headers)
            self.assertEqual(422, response.status_code, response.text)

if __name__ == "__main__":
    unittest.main()
