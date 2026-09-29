"""Minimal JSON-over-HTTP helper (stdlib only)."""
from __future__ import annotations

import json
import urllib.error
import urllib.request


class HTTPError(RuntimeError):
    def __init__(self, status: int, method: str, url: str, body: str):
        super().__init__(f"{method} {url} -> HTTP {status}: {body[:300]}")
        self.status = status
        self.body = body


def request(method: str, url: str, *, headers: dict | None = None, body: object = None, timeout: float = 30):
    data = None if body is None else json.dumps(body).encode()
    all_headers = {"Accept": "application/json", **(headers or {})}
    if data is not None:
        all_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=all_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode()
    except urllib.error.HTTPError as exc:
        raise HTTPError(exc.code, method, url, exc.read().decode(errors="replace")) from None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return raw
