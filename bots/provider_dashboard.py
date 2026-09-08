"""Authenticated task-side client for the bot dashboard control plane."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import httpx

_MAX_BODY = 1_048_576
_MAX_ARTIFACT = 8 * 1024 * 1024


class ControlPlaneError(RuntimeError):
    def __init__(self, code: str, retry_class: str = "transient"):
        super().__init__(f"dashboard control-plane failure: {code}")
        self.code = code
        self.retry_class = retry_class


class DashboardClient:
    """Bounded bearer-only client; credentials never leave this process."""

    @classmethod
    def from_environment(cls, *, deadline_at: datetime | None = None) -> "DashboardClient":
        return cls(deadline_at=deadline_at)

    def __init__(self, *, deadline_at: datetime | None = None):
        self.base_url = os.environ.get("AIRFLOW__API__BASE_URL", "").rstrip("/")
        self.username = os.environ.get("BOT_DASHBOARD_API_USERNAME", "")
        self.password = os.environ.get("BOT_DASHBOARD_API_PASSWORD", "")
        parsed = urlsplit(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
            raise ControlPlaneError("api_base_url_invalid", "terminal")
        if not self.username or not self.password:
            raise ControlPlaneError("service_credentials_missing", "terminal")
        self.deadline_at = deadline_at
        self._token: str | None = None

    def _remaining(self, cap: float) -> float:
        if self.deadline_at is None:
            return cap
        deadline = self.deadline_at
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            raise ControlPlaneError("deadline_exhausted", "terminal")
        return min(cap, max(0.1, remaining))

    def _authenticate(self) -> str:
        timeout = self._remaining(15)
        try:
            response = httpx.post(
                f"{self.base_url}/auth/token",
                json={"username": self.username, "password": self.password},
                timeout=httpx.Timeout(timeout, connect=min(5, timeout)),
                follow_redirects=False,
                trust_env=False,
                headers={"Accept": "application/json"},
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise ControlPlaneError("authentication_unavailable") from exc
        if response.status_code not in {200, 201}:
            raise ControlPlaneError(
                f"authentication_status_{response.status_code}", "terminal"
            )
        if len(response.content) > _MAX_BODY:
            raise ControlPlaneError("authentication_response_too_large", "terminal")
        try:
            token = response.json()["access_token"]
        except (ValueError, KeyError, TypeError) as exc:
            raise ControlPlaneError("authentication_response_invalid", "terminal") from exc
        if not isinstance(token, str) or len(token) > 16_384:
            raise ControlPlaneError("authentication_response_invalid", "terminal")
        self._token = token
        return token

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        params: dict | None = None,
        binary: bool = False,
        with_headers: bool = False,
    ) -> Any:
        encoded = json.dumps(body, separators=(",", ":"), default=str).encode() if body is not None else None
        if encoded is not None and len(encoded) > _MAX_BODY:
            raise ControlPlaneError("request_too_large", "terminal")
        for authentication_attempt in range(2):
            token = self._token or self._authenticate()
            timeout = self._remaining(45)
            try:
                with httpx.Client(
                    timeout=httpx.Timeout(timeout, connect=min(5, timeout)),
                    follow_redirects=False,
                    trust_env=False,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/octet-stream" if binary else "application/json",
                    },
                ) as client:
                    response = client.request(
                        method,
                        f"{self.base_url}/bot-dashboard/api/internal/{path.lstrip('/')}",
                        content=encoded,
                        params=params,
                        headers={"Content-Type": "application/json"} if encoded is not None else None,
                    )
            except httpx.TimeoutException as exc:
                raise ControlPlaneError("request_timed_out") from exc
            except httpx.NetworkError as exc:
                raise ControlPlaneError("api_unavailable") from exc
            if response.status_code == 401 and authentication_attempt == 0:
                self._token = None
                continue
            if response.status_code >= 500:
                raise ControlPlaneError(f"api_status_{response.status_code}")
            if response.status_code >= 400:
                raise ControlPlaneError(
                    f"api_status_{response.status_code}", "terminal"
                )
            content = response.content
            if len(content) > _MAX_BODY:
                raise ControlPlaneError("response_too_large", "terminal")
            if binary:
                return (content, dict(response.headers)) if with_headers else content
            try:
                value = response.json()
            except ValueError as exc:
                raise ControlPlaneError("response_invalid", "terminal") from exc
            if not isinstance(value, (dict, list)):
                raise ControlPlaneError("response_invalid", "terminal")
            return value
        raise ControlPlaneError("authentication_rejected", "terminal")

    def claim_budget(self, identity: dict, configured_total_seconds: int) -> dict:
        return self._request(
            "POST",
            "runs/claim-budget",
            body={
                "identity": identity,
                "configured_total_seconds": configured_total_seconds,
            },
        )

    def submit_run(self, envelope: dict) -> dict:
        return self._request("POST", "runs/report", body={"envelope": envelope})

    def query_runs(
        self, bots: list[str], *, days: int = 7, outcomes: list[str] | None = None, limit: int = 100
    ) -> dict:
        return self._request(
            "POST",
            "runs/query",
            body={
                "bots": bots,
                "days": days,
                "outcomes": outcomes or [],
                "limit": limit,
            },
        )

    def manager_context(self, days: int = 7) -> dict:
        return self._request("GET", "manager/context", params={"days": days})

    def reconcile_manager(self, identity: dict, try_number: int) -> dict:
        return self._request(
            "POST",
            "manager/reconcile",
            body={"identity": identity, "try_number": try_number},
        )

    def airflow_failures(self, hours: int = 24, limit: int = 100) -> dict:
        value = self._request(
            "GET", "airflow/failures", params={"hours": hours, "limit": limit}
        )
        if not isinstance(value, dict) or not isinstance(value.get("items"), list):
            raise ControlPlaneError("failure_response_invalid", "terminal")
        remaining = value.get("remaining_after_batch")
        if not isinstance(remaining, int) or remaining < 0:
            raise ControlPlaneError("failure_response_invalid", "terminal")
        return {"items": value["items"], "remaining_after_batch": remaining}

    def claim_execution(
        self,
        *,
        dag_id: str,
        run_id: str,
        conf: dict,
        kind: str,
        deadline_at: datetime,
    ) -> dict:
        return self._request(
            "POST",
            "executions/claim",
            body={
                "dag_id": dag_id,
                "run_id": run_id,
                "conf": conf,
                "kind": kind,
                "deadline_at": deadline_at.isoformat(),
            },
        )

    def get_artifact(self, digest: str) -> bytes:
        chunks: list[bytes] = []
        offset = 0
        expected_size: int | None = None
        while expected_size is None or offset < expected_size:
            chunk, headers = self._request(
                "GET",
                f"artifacts/{digest}",
                params={"offset": offset, "limit": 900_000},
                binary=True,
                with_headers=True,
            )
            try:
                observed_size = int(headers["x-artifact-bytes"])
            except (KeyError, ValueError) as exc:
                raise ControlPlaneError("artifact_response_invalid", "terminal") from exc
            if observed_size < 1 or observed_size > _MAX_ARTIFACT:
                raise ControlPlaneError("artifact_response_invalid", "terminal")
            if expected_size is not None and observed_size != expected_size:
                raise ControlPlaneError("artifact_response_changed", "terminal")
            expected_size = observed_size
            if not chunk or len(chunk) > 900_000:
                raise ControlPlaneError("artifact_response_invalid", "terminal")
            chunks.append(chunk)
            offset += len(chunk)
        content = b"".join(chunks)
        if len(content) != expected_size or hashlib.sha256(content).hexdigest() != digest:
            raise ControlPlaneError("artifact_digest_invalid", "terminal")
        return content

    def put_artifact(
        self, content: bytes, *, kind: str, owner_execution_id: str | None = None
    ) -> dict:
        digest = hashlib.sha256(content).hexdigest()
        return self._request(
            "PUT",
            f"artifacts/{digest}",
            body={
                "kind": kind,
                "content_base64": base64.b64encode(content).decode(),
                "owner_execution_id": owner_execution_id,
            },
        )

    def publish_execution(self, dag_id: str, run_id: str, result: dict) -> dict:
        return self._request(
            "POST",
            "executions/publish",
            body={"dag_id": dag_id, "run_id": run_id, "executor_result": result},
        )

    def finalize_execution(
        self, dag_id: str, run_id: str, kind: str, result: dict
    ) -> dict:
        return self._request(
            "POST",
            "executions/finalize",
            body={"dag_id": dag_id, "run_id": run_id, "kind": kind, "result": result},
        )

    def claim_dispatch(self, limit: int) -> list[dict]:
        return self._request("POST", "dispatch/claim", body={"limit": limit})["items"]

    def maintenance(self, limit: int = 100) -> dict:
        return self._request("POST", "maintenance/run", body={"limit": limit})


def enabled() -> bool:
    try:
        from airflow.configuration import conf
    except ImportError:
        return False
    configured = conf.getboolean("bot_dashboard", "enabled", fallback=False)
    username = os.environ.get("BOT_DASHBOARD_API_USERNAME", "")
    password = os.environ.get("BOT_DASHBOARD_API_PASSWORD", "")
    expected = conf.get("bot_dashboard", "internal_user", fallback="bot-worker")
    base = urlsplit(os.environ.get("AIRFLOW__API__BASE_URL", ""))
    return bool(
        configured
        and username
        and password
        and username == expected
        and base.scheme in {"http", "https"}
        and base.netloc
    )


def manager_backlog(days: int = 7) -> dict:
    return DashboardClient.from_environment().manager_context(days)


def airflow_failures(hours: int, limit: int = 100) -> dict:
    return DashboardClient.from_environment().airflow_failures(hours, limit)


def claim_pending(limit: int = 20) -> list[dict]:
    return DashboardClient.from_environment().claim_dispatch(limit)


def run_maintenance() -> dict:
    return DashboardClient.from_environment().maintenance()


def reconcile_current_run(context: dict) -> dict:
    ti = context["ti"]
    return DashboardClient.from_environment().reconcile_manager(
        {
            "dag_id": ti.dag_id,
            "run_id": context["dag_run"].run_id,
            "task_id": ti.task_id,
            "map_index": getattr(ti, "map_index", -1),
        },
        getattr(ti, "try_number", 1),
    )


def run_admitted(context: dict, cfg: dict, runner):
    import admitted_runner

    return admitted_runner.run(context, cfg, runner, sys.modules[__name__])
