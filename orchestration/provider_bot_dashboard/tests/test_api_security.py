from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from pydantic import ValidationError

from airflow.providers.vintage.bot_dashboard.api import app, _new_csrf, _valid_csrf
from airflow.providers.vintage.bot_dashboard.api_models import Evidence


class ApiSecurityTest(unittest.TestCase):
    def test_csrf_token_is_256_bit_signed_and_expires(self):
        with patch(
            "airflow.providers.vintage.bot_dashboard.api._csrf_secret",
            return_value=b"test-secret",
        ):
            token = _new_csrf()
            self.assertEqual(64, token.split(".")[1].__len__())
            self.assertTrue(_valid_csrf(token))
            self.assertFalse(_valid_csrf(token + "tampered"))

    def test_csrf_cookie_covers_api_at_root_and_subpath(self):
        from fastapi import Response
        from airflow.providers.vintage.bot_dashboard.api import capabilities
        for base, expected in [("https://example.test", "/bot-dashboard"), ("https://example.test/airflow/", "/airflow/bot-dashboard")]:
            response = Response()
            with patch("airflow.providers.vintage.bot_dashboard.api._base_url", return_value=base), patch("airflow.providers.vintage.bot_dashboard.api._new_csrf", return_value="fixture"), patch("airflow.providers.vintage.bot_dashboard.api._setting_bool", return_value=False), patch("airflow.providers.vintage.bot_dashboard.api.get_auth_manager"):
                capabilities(response, None)
            self.assertIn(f"Path={expected};", response.headers["set-cookie"])

    def test_evidence_rejects_active_and_credentialed_urls(self):
        rejected = [
            "javascript:alert(1)",
            "data:text/html,payload",
            "//evil.example/path",
            "https://user:secret@example.com/path",
            "https://example.com/path#fragment",
            "/safe\x00path",
        ]
        for value in rejected:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                Evidence(label="unsafe", url=value)
        self.assertEqual(
            "https://example.com/evidence",
            Evidence(label="safe", url="https://example.com/evidence").url,
        )
        self.assertEqual("/dags/example", Evidence(label="safe", url="/dags/example").url)

    def test_unsafe_request_rejects_origin_before_body_and_sets_headers(self):
        client = TestClient(app)
        with patch(
            "airflow.providers.vintage.bot_dashboard.api._base_url",
            return_value="http://testserver",
        ):
            response = client.post("/api/missing", content=b"not-json")
        self.assertEqual(403, response.status_code)
        self.assertEqual("no-store", response.headers["cache-control"])
        self.assertEqual("nosniff", response.headers["x-content-type-options"])
        self.assertIn("x-correlation-id", response.headers)


if __name__ == "__main__":
    unittest.main()
