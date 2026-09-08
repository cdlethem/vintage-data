from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
import unittest
from unittest import mock

import httpx

from bots import provider_dashboard


class FakeHTTP:
    def __init__(self, auth_responses, request_responses):
        self.auth_responses = list(auth_responses)
        self.request_responses = list(request_responses)
        self.auth_calls = []
        self.requests = []

    def post(self, url, **kwargs):
        self.auth_calls.append((url, kwargs))
        return self.auth_responses.pop(0)

    class Client:
        def __init__(self, owner, **kwargs):
            self.owner = owner
            self.kwargs = kwargs
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def request(self, method, url, **kwargs):
            self.owner.requests.append((method, url, self.kwargs, kwargs))
            value = self.owner.request_responses.pop(0)
            return value() if callable(value) else value


def response(status, *, json=None, content=None, headers=None):
    request = httpx.Request("GET", "http://dashboard.test")
    if json is not None:
        return httpx.Response(status, json=json, headers=headers, request=request)
    return httpx.Response(status, content=content or b"", headers=headers, request=request)


class ProviderDashboardClientTest(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, {
            "AIRFLOW__API__BASE_URL": "http://dashboard.test",
            "BOT_DASHBOARD_API_USERNAME": "bot-worker",
            "BOT_DASHBOARD_API_PASSWORD": "super-secret-password",
        }, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)

    def client(self, fake, *, deadline=None):
        return provider_dashboard.DashboardClient(deadline_at=deadline)

    def patch_http(self, fake):
        return mock.patch.object(provider_dashboard.httpx, "post", side_effect=fake.post), mock.patch.object(provider_dashboard.httpx, "Client", side_effect=lambda **kwargs: fake.Client(fake, **kwargs))

    def test_requests_are_bearer_only_under_internal_prefix(self):
        fake = FakeHTTP([response(200, json={"access_token": "token-1"})], [response(200, json={"items": []})])
        post, client = self.patch_http(fake)
        with post, client:
            value = self.client(fake).query_runs(["source_discovery"])
        self.assertEqual(value, {"items": []})
        self.assertEqual(fake.requests[0][0:2], ("POST", "http://dashboard.test/bot-dashboard/api/internal/runs/query"))
        headers = fake.requests[0][2]["headers"]
        self.assertNotIn("Cookie", headers)
        self.assertEqual(fake.auth_calls[0][0], "http://dashboard.test/auth/token")

    def test_401_reauthenticates_once_then_fails(self):
        fake = FakeHTTP([response(200, json={"access_token": "one"}), response(200, json={"access_token": "two"})], [response(401, json={}), response(401, json={})])
        post, client = self.patch_http(fake)
        with post, client:
            with self.assertRaises(provider_dashboard.ControlPlaneError) as caught:
                self.client(fake).manager_context()
        self.assertEqual(caught.exception.code, "api_status_401")
        self.assertEqual(len(fake.auth_calls), 2)
        self.assertEqual(len(fake.requests), 2)

    def test_5xx_is_transient_and_4xx_is_terminal(self):
        for status, retry in ((503, "transient"), (429, "terminal"), (400, "terminal")):
            with self.subTest(status=status):
                fake = FakeHTTP([response(200, json={"access_token": "token"})], [response(status, json={})])
                post, client = self.patch_http(fake)
                with post, client:
                    with self.assertRaises(provider_dashboard.ControlPlaneError) as caught:
                        self.client(fake).manager_context()
                self.assertEqual(caught.exception.retry_class, retry)

    def test_request_response_bounds_and_deadline_timeouts(self):
        oversized = FakeHTTP([response(200, json={"access_token": "token"})], [])
        post, client = self.patch_http(oversized)
        with post, client:
            with self.assertRaises(provider_dashboard.ControlPlaneError) as caught:
                self.client(oversized).submit_run({"x": "a" * provider_dashboard._MAX_BODY})
        self.assertEqual(caught.exception.code, "request_too_large")

        fake = FakeHTTP([response(200, json={"access_token": "token"})], [response(200, json={"ok": True})])
        deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=2)
        post, client = self.patch_http(fake)
        with post, client:
            self.client(fake, deadline=deadline).submit_run({"x": "small"})
        timeout = fake.requests[0][2]["timeout"]
        self.assertLessEqual(timeout.read, 2.1)
        self.assertLessEqual(timeout.connect, 2.1)

        fake = FakeHTTP([response(200, json={"access_token": "token"})], [response(200, content=b"x" * (provider_dashboard._MAX_BODY + 1))])
        post, client = self.patch_http(fake)
        with post, client:
            with self.assertRaises(provider_dashboard.ControlPlaneError) as caught:
                self.client(fake).manager_context()
        self.assertEqual(caught.exception.code, "response_too_large")


    def test_chunked_artifact_download_checks_digest_and_size(self):
        content = b"artifact contents" * 100
        digest = hashlib.sha256(content).hexdigest()
        chunks = [content[:100], content[100:]]
        def chunk_response():
            chunk = chunks.pop(0)
            return response(200, content=chunk, headers={"x-artifact-bytes": str(len(content))})
        fake = FakeHTTP([response(200, json={"access_token": "token"})], [chunk_response, chunk_response])
        post, client = self.patch_http(fake)
        with post, client:
            self.assertEqual(self.client(fake).get_artifact(digest), content)
        self.assertEqual([entry[3]["params"]["offset"] for entry in fake.requests], [0, 100])

        wrong_digest = "0" * 64
        fake = FakeHTTP([response(200, json={"access_token": "token"})], [lambda: response(200, content=content, headers={"x-artifact-bytes": str(len(content))})])
        post, client = self.patch_http(fake)
        with post, client:
            with self.assertRaises(provider_dashboard.ControlPlaneError) as caught:
                self.client(fake).get_artifact(wrong_digest)
        self.assertEqual(caught.exception.code, "artifact_digest_invalid")

        fake = FakeHTTP([response(200, json={"access_token": "token"})], [response(200, content=b"x", headers={"x-artifact-bytes": "2"}), response(200, content=b"y", headers={"x-artifact-bytes": "3"})])
        post, client = self.patch_http(fake)
        with post, client:
            with self.assertRaises(provider_dashboard.ControlPlaneError) as caught:
                self.client(fake).get_artifact(hashlib.sha256(b"xy").hexdigest())
        self.assertEqual(caught.exception.code, "artifact_response_changed")

    def test_credential_is_absent_from_errors_and_logs(self):
        secret = os.environ["BOT_DASHBOARD_API_PASSWORD"]
        fake = FakeHTTP([response(200, json={"access_token": "token"})], [response(500, json={"detail": secret})])
        post, client = self.patch_http(fake)
        with self.assertNoLogs(provider_dashboard.__name__, level=logging.INFO):
            with post, client:
                with self.assertRaises(provider_dashboard.ControlPlaneError) as caught:
                    self.client(fake).manager_context()
        self.assertNotIn(secret, str(caught.exception))

if __name__ == "__main__":
    unittest.main()
