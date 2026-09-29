"""Read failed task instances and their logs through Airflow's public REST API (v2).

Works with any Airflow 3 deployment and any log backend, because the API
server resolves logs. Authenticate with a username/password (``POST
/auth/token``) or a pre-issued bearer token.
"""
from __future__ import annotations

import json
from datetime import datetime
from urllib.parse import quote

from . import http
from .config import Config

LOG_TAIL_CHARS = 20_000


class Airflow:
    def __init__(self, url: str, ui_url: str, *, username: str | None = None,
                 password: str | None = None, token: str | None = None):
        self.url, self.ui_url = url, ui_url
        self._username, self._password, self._token = username, password, token

    @classmethod
    def from_config(cls, cfg: Config) -> "Airflow":
        return cls(cfg.airflow_url, cfg.airflow_ui_url,
                   username=cfg.secret(cfg.airflow_username_env),
                   password=cfg.secret(cfg.airflow_password_env),
                   token=cfg.secret(cfg.airflow_token_env))

    def _call(self, method: str, path: str, body: object = None, *, retry: bool = True):
        if not self._token:
            if not (self._username and self._password):
                raise RuntimeError("Airflow credentials missing: set the username/password or token env vars")
            self._token = http.request("POST", f"{self.url}/auth/token",
                                       body={"username": self._username, "password": self._password})["access_token"]
        try:
            return http.request(method, f"{self.url}{path}", body=body,
                                headers={"Authorization": f"Bearer {self._token}"})
        except http.HTTPError as exc:
            if exc.status == 401 and retry and self._password:
                self._token = None
                return self._call(method, path, body, retry=False)
            raise

    def _list(self, body: dict, limit: int | None = None) -> list[dict]:
        rows, offset = [], 0
        while True:
            page = self._call("POST", "/api/v2/dags/~/dagRuns/~/taskInstances/list",
                              {**body, "page_limit": 100, "page_offset": offset})
            rows += page["task_instances"]
            offset += 100
            if offset >= page["total_entries"] or (limit and len(rows) >= limit):
                return rows[:limit] if limit else rows

    def failures(self, since: datetime) -> list[dict]:
        """Task instances that ended in `failed` since the given time, newest first."""
        return self._list({"state": ["failed"], "end_date_gte": since.isoformat(), "order_by": "-end_date"})

    def history(self, dag_id: str, task_id: str, limit: int = 20, since: datetime | None = None) -> list[dict]:
        """Most recent finished (success/failed) tries of one task, newest first."""
        body = {"dag_ids": [dag_id], "task_ids": [task_id], "state": ["success", "failed"], "order_by": "-end_date"}
        if since:
            body["end_date_gte"] = since.isoformat()
        return self._list(body, limit=limit)

    def log(self, ti: dict, max_chars: int = LOG_TAIL_CHARS) -> str:
        path = (f"/api/v2/dags/{ti['dag_id']}/dagRuns/{quote(ti['dag_run_id'], safe='')}"
                f"/taskInstances/{ti['task_id']}/logs/{ti['try_number']}?full_content=true")
        if ti.get("map_index", -1) >= 0:
            path += f"&map_index={ti['map_index']}"
        content = self._call("GET", path)
        return format_log(content.get("content", []) if isinstance(content, dict) else [], max_chars)

    def dag_file(self, dag_id: str) -> str:
        """Where the DAG is defined, as the Airflow host sees it."""
        try:
            return self._call("GET", f"/api/v2/dags/{dag_id}").get("fileloc") or "unknown"
        except http.HTTPError:
            return "unknown"

    def link(self, ti: dict) -> str:
        link = f"{self.ui_url}/dags/{ti['dag_id']}/runs/{quote(ti['dag_run_id'], safe='')}/tasks/{ti['task_id']}"
        return link + (f"/mapped/{ti['map_index']}" if ti.get("map_index", -1) >= 0 else "")


def format_log(events: list, max_chars: int = LOG_TAIL_CHARS) -> str:
    """Flatten Airflow's structured log events into readable text, keeping the tail."""
    lines = []
    for event in events:
        if not isinstance(event, dict):
            lines.append(str(event))
            continue
        level = str(event.get("level") or "").upper()
        lines.append(f"{event.get('timestamp', '')} {level} {event.get('event', '')}".strip())
        for error in event.get("error_detail") or []:
            lines.append(f"  {error.get('exc_type')}: {_exc_value(error.get('exc_value'))}")
            for frame in error.get("frames") or []:
                lines.append(f"    at {frame.get('filename')}:{frame.get('lineno')} in {frame.get('name')}")
    text = "\n".join(line.rstrip() for line in "\n".join(lines).split("\n"))
    return text if len(text) <= max_chars else "...(truncated)...\n" + text[-max_chars:]


def _exc_value(value: object) -> str:
    # Some operators raise with a JSON payload; pretty-print it so stderr/tracebacks stay readable.
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return value
        if isinstance(parsed, dict):
            return "\n" + "\n".join(f"    {key}: {val}" for key, val in parsed.items())
    return str(value)
