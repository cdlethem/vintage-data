"""Worker-only, provider-neutral GitHub/GitLab HTTP adapters."""
from __future__ import annotations

import fnmatch
import ipaddress
import json
import socket
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
from airflow.configuration import conf
from airflow.models.connection import Connection
from airflow._shared.secrets_masker import mask_secret

MAX_RESPONSE = 1_048_576

class GitProviderError(RuntimeError): pass
class GitProviderTransientError(GitProviderError): pass

@dataclass(frozen=True)
class RepositoryConfig:
    provider: str
    project: str
    api_base_url: str
    clone_url: str
    base_branch: str
    allowed_path_globs: tuple[str, ...]
    denied_path_globs: tuple[str, ...]
    max_changed_files: int
    max_diff_bytes: int
    service_account_id: str
    token: str


def _canonical_https(url: str) -> tuple[str, str]:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise GitProviderError("repository endpoint must be canonical HTTPS without credentials, query, or fragment")
    if parsed.port not in {None, 443}: raise GitProviderError("non-default HTTPS ports are not allowed")
    return parsed.hostname.lower(), parsed.path.rstrip("/")


def _allowed_host(host: str) -> str | None:
    entries = [item.strip() for item in conf.get("bot_dashboard", "allowed_git_hosts", fallback="").split(",") if item.strip()]
    for entry in entries:
        name, separator, address = entry.partition("=")
        if name.lower() == host.lower(): return address or None
    raise GitProviderError("Git host is not allowlisted")


def _resolve_host(host: str) -> None:
    pinned = _allowed_host(host)
    addresses = {item[4][0] for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)}
    if pinned:
        if addresses != {pinned}: raise GitProviderError("Git host resolution does not match its pinned address")
        return
    for value in addresses:
        address = ipaddress.ip_address(value)
        if not address.is_global: raise GitProviderError("Git host resolves to a non-public address without an exact pin")


def load_repository_config() -> RepositoryConfig:
    conn_id = conf.get("bot_dashboard", "git_conn_id", fallback="bot_dashboard_git")
    connection = Connection.get_connection_from_secrets(conn_id)
    extra = connection.extra_dejson
    required = {"provider", "project", "api_base_url", "clone_url", "base_branch", "allowed_path_globs", "denied_path_globs", "max_changed_files", "max_diff_bytes", "service_account_id"}
    missing = sorted(required - extra.keys())
    if missing: raise GitProviderError(f"Git connection extras are missing: {missing}")
    provider = extra["provider"]
    if provider not in {"github", "gitlab"}: raise GitProviderError("unsupported Git provider")
    api_host, _ = _canonical_https(str(extra["api_base_url"]))
    clone_host, clone_path = _canonical_https(str(extra["clone_url"]))
    if api_host != clone_host: raise GitProviderError("API and clone hosts must match")
    _resolve_host(api_host)
    project = str(extra["project"]).strip("/")
    expected_suffix = f"/{project}.git"
    if not clone_path.endswith(expected_suffix): raise GitProviderError("clone URL does not match configured project")
    allowed = tuple(extra["allowed_path_globs"])
    denied = tuple(extra["denied_path_globs"])
    if not allowed or not all(isinstance(item, str) and item for item in allowed): raise GitProviderError("allowed_path_globs must be nonempty")
    denied = tuple(dict.fromkeys((*denied, ".git/**", ".github/workflows/**", ".gitlab-ci.yml")))
    max_changed_files = int(extra["max_changed_files"])
    max_diff_bytes = int(extra["max_diff_bytes"])
    if max_changed_files < 1 or max_changed_files > 200 or max_diff_bytes < 1 or max_diff_bytes > 700_000:
        raise GitProviderError("repository change limits exceed the authenticated API boundary")
    service_account_id = str(extra["service_account_id"])
    if not service_account_id:
        raise GitProviderError("service_account_id must be configured")
    token = connection.password or ""
    if not token: raise GitProviderError("Git token is not configured")
    mask_secret(token)
    return RepositoryConfig(provider, project, str(extra["api_base_url"]).rstrip("/"), str(extra["clone_url"]), str(extra["base_branch"]), allowed, denied, max_changed_files, max_diff_bytes, service_account_id, token)


def validate_changed_paths(config: RepositoryConfig, paths: list[str], diff_bytes: int) -> None:
    if len(paths) > config.max_changed_files or diff_bytes > config.max_diff_bytes: raise GitProviderError("change set exceeds configured limits")
    folded: set[str] = set()
    for path in paths:
        if not path or path.startswith(("/", "../")) or "/../" in f"/{path}" or "\\" in path: raise GitProviderError("changed path escapes repository")
        lower = path.casefold()
        if lower in folded: raise GitProviderError("changed paths have a case collision")
        folded.add(lower)
        if not any(fnmatch.fnmatchcase(path, rule) for rule in config.allowed_path_globs): raise GitProviderError("changed path is outside allowed globs")
        if any(fnmatch.fnmatchcase(path, rule) for rule in config.denied_path_globs): raise GitProviderError("changed path is denied")


class GitProvider:
    def __init__(self, config: RepositoryConfig): self.config = config
    def _headers(self) -> dict[str, str]: raise NotImplementedError
    def _request(self, method: str, path: str, *, payload: dict | None = None) -> Any:
        url = f"{self.config.api_base_url}/{path.lstrip('/')}"
        host, _ = _canonical_https(url); _resolve_host(host)
        timeout = httpx.Timeout(connect=5, read=15, write=15, pool=5)
        with httpx.Client(timeout=timeout, follow_redirects=False, trust_env=False, headers=self._headers()) as client:
            request = client.build_request(method, url, json=payload)
            response = client.send(request, stream=True)
            try:
                if response.status_code in {301, 302, 303, 307, 308}: raise GitProviderError("provider redirects are forbidden")
                body = bytearray()
                for chunk in response.iter_bytes():
                    body.extend(chunk)
                    if len(body) > MAX_RESPONSE: raise GitProviderError("provider response exceeds 1 MiB")
                if response.status_code == 429 or response.status_code >= 500: raise GitProviderTransientError(f"provider transient status {response.status_code}")
                if response.status_code >= 400: raise GitProviderError(f"provider rejected request with status {response.status_code}")
                return json.loads(body or b"null")
            finally: response.close()
    def create_change(self, branch: str, title: str, body: str) -> dict: raise NotImplementedError
    def read_change(self, number: int) -> dict: raise NotImplementedError
    def post_comment(self, number: int, body: str) -> None: raise NotImplementedError
    def upsert_comment(self, number: int, marker: str, body: str) -> None: raise NotImplementedError
    def find_change(self, branch: str) -> dict | None: raise NotImplementedError


class GitHubProvider(GitProvider):
    def _headers(self): return {"Authorization": f"Bearer {self.config.token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    def create_change(self, branch, title, body): return self._request("POST", f"repos/{self.config.project}/pulls", payload={"head": branch, "base": self.config.base_branch, "title": title[:200], "body": body[:20_000], "draft": True})
    def read_change(self, number): return normalize_github(self._request("GET", f"repos/{self.config.project}/pulls/{number}"))
    def post_comment(self, number, body): self._request("POST", f"repos/{self.config.project}/issues/{number}/comments", payload={"body": body[:10_000]})
    def upsert_comment(self, number, marker, body):
        comments = self._request("GET", f"repos/{self.config.project}/issues/{number}/comments?per_page=100")
        existing = next((item for item in comments if marker in str(item.get("body", ""))), None)
        payload = {"body": f"{marker}\n{body}"[:10_000]}
        if existing:
            self._request("PATCH", f"repos/{self.config.project}/issues/comments/{existing['id']}", payload=payload)
        else:
            self._request("POST", f"repos/{self.config.project}/issues/{number}/comments", payload=payload)

    def find_change(self, branch):
        owner = self.config.project.split("/", 1)[0]
        values = self._request("GET", f"repos/{self.config.project}/pulls?state=all&head={quote(f'{owner}:{branch}', safe=':')}&per_page=10")
        return normalize_github(values[0]) if values else None

class GitLabProvider(GitProvider):
    def _headers(self): return {"PRIVATE-TOKEN": self.config.token}
    @property
    def project_path(self): return quote(self.config.project, safe="")
    def create_change(self, branch, title, body): return self._request("POST", f"projects/{self.project_path}/merge_requests", payload={"source_branch": branch, "target_branch": self.config.base_branch, "title": title[:200], "description": body[:20_000], "draft": True})
    def read_change(self, number): return normalize_gitlab(self._request("GET", f"projects/{self.project_path}/merge_requests/{number}"))
    def post_comment(self, number, body): self._request("POST", f"projects/{self.project_path}/merge_requests/{number}/notes", payload={"body": body[:10_000]})
    def upsert_comment(self, number, marker, body):
        notes = self._request("GET", f"projects/{self.project_path}/merge_requests/{number}/notes?per_page=100")
        existing = next((item for item in notes if marker in str(item.get("body", ""))), None)
        payload = {"body": f"{marker}\n{body}"[:10_000]}
        if existing:
            self._request("PUT", f"projects/{self.project_path}/merge_requests/{number}/notes/{existing['id']}", payload=payload)
        else:
            self._request("POST", f"projects/{self.project_path}/merge_requests/{number}/notes", payload=payload)

    def find_change(self, branch):
        values = self._request("GET", f"projects/{self.project_path}/merge_requests?state=all&source_branch={quote(branch, safe='')}&per_page=10")
        return normalize_gitlab(values[0]) if values else None

def normalize_github(value: dict) -> dict:
    return {"provider": "github", "number": value["number"], "url": value["html_url"], "state": "merged" if value.get("merged") else value.get("state"), "draft": bool(value.get("draft")), "head_sha": value["head"]["sha"], "head_ref": value["head"]["ref"], "base_ref": value["base"]["ref"], "author_id": str(value["user"]["id"])}


def normalize_gitlab(value: dict) -> dict:
    # GitLab says opened/closed/locked/merged; the lifecycle speaks open/closed/merged.
    state = {"opened": "open", "locked": "open"}.get(value.get("state"), value.get("state"))
    return {"provider": "gitlab", "number": value["iid"], "url": value["web_url"], "state": state, "draft": bool(value.get("draft") or str(value.get("title", "")).lower().startswith("draft:")), "head_sha": value["sha"], "head_ref": value["source_branch"], "base_ref": value["target_branch"], "author_id": str(value["author"]["id"])}


def get_provider(config: RepositoryConfig | None = None) -> GitProvider:
    config = config or load_repository_config()
    return GitHubProvider(config) if config.provider == "github" else GitLabProvider(config)
