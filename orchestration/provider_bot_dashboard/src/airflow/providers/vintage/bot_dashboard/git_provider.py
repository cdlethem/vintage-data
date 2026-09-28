"""Worker-only, provider-neutral GitHub/GitLab HTTP adapters."""
from __future__ import annotations

import fnmatch
import ipaddress
import json
import re
import socket
import time
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
    return repository_config_from_connection(connection)


def repository_config_from_connection(connection: Connection) -> RepositoryConfig:
    """Validate operator configuration before either persistence or use."""
    extra = connection.extra_dejson
    required = {"provider", "project", "api_base_url", "clone_url", "base_branch", "allowed_path_globs", "denied_path_globs", "max_changed_files", "max_diff_bytes", "service_account_id"}
    missing = sorted(required - extra.keys())
    if missing: raise GitProviderError(f"Git connection extras are missing: {missing}")
    provider = extra["provider"]
    if provider not in {"github", "gitlab"}: raise GitProviderError("unsupported Git provider")
    api_host, _ = _canonical_https(str(extra["api_base_url"]))
    clone_host, clone_path = _canonical_https(str(extra["clone_url"]))
    if api_host != clone_host and not (
        provider == "github" and api_host == "api.github.com" and clone_host == "github.com"
    ): raise GitProviderError("API and clone hosts must match or use GitHub's public endpoints")
    _resolve_host(api_host)
    if clone_host != api_host: _resolve_host(clone_host)
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
        # Adapter paths include bounded query parameters; credentials and endpoint
        # identity are validated against the configured canonical base instead.
        host, _ = _canonical_https(self.config.api_base_url); _resolve_host(host)
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
    def read_validation_workflow(
        self, pr_number: int, head_sha: str, workflow_path: str, job_name: str
    ) -> dict | None:
        raise GitProviderError("workflow attestation capability unavailable for this Git provider")
    def read_base_identity(self) -> str: raise NotImplementedError
    def post_comment(self, number: int, body: str) -> None: raise NotImplementedError
    def upsert_comment(self, number: int, marker: str, body: str) -> None: raise NotImplementedError
    def find_change(self, branch: str) -> dict | None: raise NotImplementedError
    def mark_ready(self, number: int, head_sha: str) -> None: raise NotImplementedError
    def merge_change(self, number: int, head_sha: str) -> None: raise NotImplementedError
    def close_change(self, number: int, head_sha: str) -> None: raise NotImplementedError
    def refresh_change(self, number: int, head_sha: str) -> dict:
        raise GitProviderError("Provider cannot update a behind bot PR")
    def refreshed_change_preserves_paths(self, old_sha: str, new_sha: str, changed_paths: list[str]) -> bool:
        return False


class GitHubProvider(GitProvider):
    def _headers(self): return {"Authorization": f"Bearer {self.config.token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    def create_change(self, branch, title, body): return self._request("POST", f"repos/{self.config.project}/pulls", payload={"head": branch, "base": self.config.base_branch, "title": title[:200], "body": body[:20_000], "draft": True})
    def read_change(self, number): return normalize_github(self._request("GET", f"repos/{self.config.project}/pulls/{number}"))
    def read_validation_workflow(
        self, pr_number: int, head_sha: str, workflow_path: str, job_name: str
    ) -> dict | None:
        """Attest an Actions job from a trusted base workflow for one current PR head.

        None means that the expected PR run has not finished yet. A completed but
        untrusted or unsuccessful run is a bounded, typed failure, never a pass.
        """
        if type(pr_number) is not int or pr_number < 1:
            raise GitProviderError("invalid pull request number")
        if not isinstance(head_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", head_sha):
            raise GitProviderError("invalid reviewed head SHA")
        if not isinstance(workflow_path, str) or not re.fullmatch(
            r"\.github/workflows/[A-Za-z0-9_.-]+\.ya?ml", workflow_path
        ) or ".." in workflow_path:
            raise GitProviderError("invalid trusted workflow path")
        if not isinstance(job_name, str) or not job_name or len(job_name) > 200:
            raise GitProviderError("invalid trusted job name")

        root = f"repos/{self.config.project}"
        def request(path: str) -> Any:
            try:
                return self._request("GET", path)
            except httpx.RequestError as exc:
                raise GitProviderTransientError("provider request failed during workflow attestation") from exc

        def get(path: str) -> dict:
            value = request(path)
            if not isinstance(value, dict):
                raise GitProviderError("provider returned malformed workflow attestation data")
            return value

        def repo_matches(value: Any, expected: dict) -> bool:
            return (
                isinstance(value, dict)
                and type(value.get("id")) is int
                and value["id"] == expected["id"]
                and value.get("full_name") == self.config.project
            )

        def failure(code: str, run: dict | None = None, check_id: int | None = None) -> dict:
            run_id = run.get("id") if isinstance(run, dict) and type(run.get("id")) is int else None
            return {
                "status": "failed", "diagnostic": code, "head_sha": head_sha,
                "workflow_run_id": run_id, "run_id": run_id,
                "run_url": (
                    f"{self.config.clone_url.removesuffix('.git')}/actions/runs/{run_id}"
                    if run_id and run_id > 0 else None
                ),
                "check_run_id": check_id, "conclusion": None, "event": "pull_request",
                "workflow_path": workflow_path, "job_name": job_name,
            }

        pr = get(f"{root}/pulls/{pr_number}")
        pr_head, pr_base = pr.get("head"), pr.get("base")
        if not isinstance(pr_head, dict) or not isinstance(pr_base, dict):
            raise GitProviderError("provider returned malformed pull request identity")
        base_repo = pr_base.get("repo")
        if not isinstance(base_repo, dict) or not repo_matches(base_repo, base_repo):
            return failure("wrong_base_repository")
        if pr.get("number") != pr_number or pr.get("state") != "open":
            return failure("pull_request_not_open")
        if pr_base.get("ref") != self.config.base_branch or not repo_matches(pr_head.get("repo"), base_repo):
            return failure("wrong_pr_repository_or_base")
        if pr_head.get("sha") != head_sha:
            return failure("pr_head_changed")
        branch = pr_head.get("ref")
        if not isinstance(branch, str) or not branch or len(branch) > 250:
            raise GitProviderError("provider returned invalid pull request branch")

        workflow = get(f"{root}/actions/workflows/{quote(workflow_path.rsplit('/', 1)[-1], safe='')}")
        workflow_id = workflow.get("id")
        if (
            type(workflow_id) is not int or workflow_id < 1
            or workflow.get("path") != workflow_path or workflow.get("state") != "active"
        ):
            return failure("untrusted_workflow_identity")
        base_file = get(
            f"{root}/contents/{workflow_path}?ref={quote(self.config.base_branch, safe='')}"
        )
        base_blob = base_file.get("sha")
        if not isinstance(base_blob, str) or not re.fullmatch(r"[0-9a-f]{40}", base_blob):
            return failure("missing_base_workflow_blob")

        runs = get(
            f"{root}/actions/workflows/{workflow_id}/runs"
            f"?event=pull_request&branch={quote(branch, safe='')}&per_page=100"
        )
        items = runs.get("workflow_runs")
        count = runs.get("total_count")
        if not isinstance(items, list) or type(count) is not int or count < len(items) or count > 100:
            raise GitProviderError("workflow run listing is incomplete or ambiguous")
        merge_sha = pr.get("merge_commit_sha")
        if not isinstance(merge_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", merge_sha):
            merge_sha = None
        candidates = []
        for run in items:
            if not isinstance(run, dict):
                raise GitProviderError("malformed workflow run")
            linked = run.get("pull_requests")
            if not isinstance(linked, list):
                raise GitProviderError("workflow run has malformed pull request association")
            associated = [item for item in linked if isinstance(item, dict) and item.get("number") == pr_number]
            if associated and any(
                isinstance(item.get("head"), dict)
                and item["head"].get("sha") != head_sha for item in associated
            ):
                continue  # Prior run of this PR, not evidence for its current head.
            if associated or run.get("head_sha") in {head_sha, merge_sha}:
                candidates.append(run)


        if len(candidates) > 1:
            return failure("ambiguous_workflow_runs")
        if not candidates:
            return None
        run = candidates[0]
        run_id = run.get("id")
        # GitHub may supply either the candidate or the synthetic PR merge SHA.
        if (
            type(run_id) is not int or run_id < 1
            or run.get("event") != "pull_request"
            or run.get("workflow_id") != workflow_id
            or run.get("path") not in {workflow_path, f"{workflow_path}@{self.config.base_branch}"}
            or not repo_matches(run.get("repository"), base_repo)
            or not repo_matches(run.get("head_repository"), base_repo)
            or run.get("head_branch") != branch
            or not isinstance(run.get("head_sha"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", run["head_sha"])
            or run["head_sha"] not in {head_sha, merge_sha}
            or (
                bool(run["pull_requests"])
                and not any(
                    isinstance(item, dict) and item.get("number") == pr_number
                    and (not isinstance(item.get("head"), dict)
                         or item["head"].get("sha") == head_sha)
                    and (not isinstance(item.get("base"), dict)
                         or item["base"].get("ref") == self.config.base_branch)
                    for item in run["pull_requests"]
                )
            )
        ):
            return failure("untrusted_workflow_run", run)
        if not run["pull_requests"]:
            # GitHub can omit pull_requests even for same-repository PR runs.
            # An exact current SHA and branch only identify this PR if no other
            # open PR shares the head branch (possibly targeting another base).
            owner = self.config.project.split("/", 1)[0]
            open_heads = request(
                f"{root}/pulls?state=open&head={quote(f'{owner}:{branch}', safe=':')}&per_page=2"
            )
            if (
                not isinstance(open_heads, list) or len(open_heads) != 1
                or not isinstance(open_heads[0], dict)
                or open_heads[0].get("number") != pr_number
                or not isinstance(open_heads[0].get("head"), dict)
                or open_heads[0]["head"].get("sha") != head_sha
                or not isinstance(open_heads[0].get("base"), dict)
                or open_heads[0]["base"].get("ref") != self.config.base_branch
            ):
                return failure("ambiguous_pull_request_head", run)
        if run.get("status") != "completed":
            if run.get("status") not in {"queued", "in_progress", "waiting", "requested", "pending"}:
                return failure("unknown_workflow_status", run)
            return None
        # The event executes the trusted PR merge definition even when Actions
        # reports the source SHA, which may not itself contain this workflow.
        executed_ref = merge_sha or run["head_sha"]
        executed_file = get(f"{root}/contents/{workflow_path}?ref={executed_ref}")
        if executed_file.get("sha") != base_blob:
            return failure("workflow_differs_from_base", run)

        jobs = get(f"{root}/actions/runs/{run_id}/jobs?per_page=100")
        job_items, job_count = jobs.get("jobs"), jobs.get("total_count")
        if not isinstance(job_items, list) or type(job_count) is not int or job_count < len(job_items) or job_count > 100:
            return failure("ambiguous_workflow_jobs", run)
        matches = [job for job in job_items if isinstance(job, dict) and job.get("name") == job_name]
        if len(matches) != 1:
            return failure("missing_or_ambiguous_job", run)
        job = matches[0]
        check_id = job.get("id")
        if (
            type(check_id) is not int or check_id < 1
            or job.get("run_id") != run_id or job.get("head_sha") != run["head_sha"]
            or job.get("check_run_url") != f"{self.config.api_base_url}/{root}/check-runs/{check_id}"
        ):
            return failure("untrusted_workflow_job", run)
        if job.get("status") != "completed":
            return failure("job_not_completed", run, check_id)
        check = get(f"{root}/check-runs/{check_id}")
        if (
            check.get("id") != check_id or check.get("name") != job_name
            or check.get("head_sha") != run["head_sha"]
            or type(run.get("check_suite_id")) is not int or run["check_suite_id"] < 1
            or not isinstance(check.get("check_suite"), dict)
            or check["check_suite"].get("id") != run["check_suite_id"]
            or not isinstance(check.get("app"), dict)
            or check["app"].get("slug") != "github-actions"
            or check.get("status") != "completed"
            or check.get("conclusion") != job.get("conclusion")
        ):
            return failure("untrusted_check_run", run, check_id)
        result = failure("job_not_successful", run, check_id)
        result["conclusion"] = job.get("conclusion") if isinstance(job.get("conclusion"), str) else None
        if run.get("conclusion") != "success":
            result["diagnostic"] = "workflow_not_successful"
        elif job.get("conclusion") == "success":
            result.update(status="passed", diagnostic=None)
        return result
    def read_base_identity(self):
        value = self._request(
            "GET", f"repos/{self.config.project}/commits/{quote(self.config.base_branch, safe='')}"
        )
        sha = value.get("sha")
        if not isinstance(sha, str) or len(sha) not in {40, 64}:
            raise GitProviderError("provider returned an invalid current base identity")
        return sha
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

    def mark_ready(self, number, head_sha):
        value = self._request("GET", f"repos/{self.config.project}/pulls/{number}")
        if value["head"]["sha"] != head_sha:
            raise GitProviderError("PR head changed")
        if not value.get("draft"):
            return
        # Enterprise GraphQL lives alongside /api/v3, public GitHub at /graphql.
        from dataclasses import replace
        base = self.config.api_base_url.removesuffix("/api/v3")
        adapter = GitHubProvider(replace(self.config, api_base_url=base))
        response = adapter._request("POST", "api/graphql" if base != self.config.api_base_url else "graphql", payload={
            "query": "mutation($id:ID!){markPullRequestReadyForReview(input:{pullRequestId:$id}){pullRequest{isDraft}}}",
            "variables": {"id": value["node_id"]},
        })
        if response.get("errors") or response.get("data", {}).get("markPullRequestReadyForReview", {}).get("pullRequest", {}).get("isDraft") is not False:
            raise GitProviderError("Provider could not mark PR ready")

    def merge_change(self, number, head_sha):
        result = self._request("PUT", f"repos/{self.config.project}/pulls/{number}/merge", payload={"sha": head_sha, "merge_method": "squash"})
        if result.get("merged") is not True:
            raise GitProviderError("Provider did not merge the reviewed head")
    def close_change(self, number, head_sha):
        current = self.read_change(number)
        if current["state"] == "closed":
            return
        if current["state"] != "open" or current["head_sha"] != head_sha:
            raise GitProviderError("Superseded PR identity changed before retirement")
        self._request("PATCH", f"repos/{self.config.project}/pulls/{number}", payload={"state": "closed"})
    def refresh_change(self, number, head_sha):
        self._request("PUT", f"repos/{self.config.project}/pulls/{number}/update-branch",
                      payload={"expected_head_sha": head_sha})
        for _ in range(5):
            observed = self.read_change(number)
            if observed["head_sha"] != head_sha:
                return observed
            time.sleep(1)
        raise GitProviderTransientError("Provider branch update has not yet appeared")

    def refreshed_change_preserves_paths(self, old_sha, new_sha, changed_paths):
        if not changed_paths or not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (old_sha, new_sha)):
            return False
        comparison = self._request(
            "GET", f"repos/{self.config.project}/compare/{old_sha}...{new_sha}",
        )
        files = comparison.get("files")
        return (
            comparison.get("status") == "ahead"
            and comparison.get("behind_by") == 0
            and isinstance(files, list) and len(files) < 300
            and all(isinstance(item.get("filename"), str) for item in files)
            and not set(changed_paths).intersection(item["filename"] for item in files)
        )

class GitLabProvider(GitProvider):
    def _headers(self): return {"PRIVATE-TOKEN": self.config.token}
    @property
    def project_path(self): return quote(self.config.project, safe="")
    def create_change(self, branch, title, body): return self._request("POST", f"projects/{self.project_path}/merge_requests", payload={"source_branch": branch, "target_branch": self.config.base_branch, "title": title[:200], "description": body[:20_000], "draft": True})
    def read_change(self, number): return normalize_gitlab(self._request("GET", f"projects/{self.project_path}/merge_requests/{number}"))
    def read_base_identity(self):
        value = self._request(
            "GET",
            f"projects/{self.project_path}/repository/branches/{quote(self.config.base_branch, safe='')}",
        )
        sha = value.get("commit", {}).get("id")
        if not isinstance(sha, str) or len(sha) not in {40, 64}:
            raise GitProviderError("provider returned an invalid current base identity")
        return sha
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

    def mark_ready(self, number, head_sha):
        import re
        path = f"projects/{self.project_path}/merge_requests/{number}"
        value = self._request("GET", path)
        if value["sha"] != head_sha:
            raise GitProviderError("MR head changed")
        title = re.sub(r"^(?:\[Draft\]|\(Draft\)|Draft:|WIP:)\s*", "", value["title"], flags=re.IGNORECASE)
        self._request("PUT", path, payload={"title": title})

    def merge_change(self, number, head_sha):
        result = self._request("PUT", f"projects/{self.project_path}/merge_requests/{number}/merge", payload={"sha": head_sha, "squash": True})
        if result.get("state") != "merged":
            raise GitProviderError("Provider did not merge the reviewed head")
    def close_change(self, number, head_sha):
        current = self.read_change(number)
        if current["state"] == "closed":
            return
        if current["state"] != "open" or current["head_sha"] != head_sha:
            raise GitProviderError("Superseded MR identity changed before retirement")
        self._request("PUT", f"projects/{self.project_path}/merge_requests/{number}", payload={"state_event": "close"})

def normalize_github(value: dict) -> dict:
    mergeable = value.get("mergeable")
    mergeability = "mergeable" if mergeable is True else "conflicting" if (
        mergeable is False or value.get("mergeable_state") == "dirty"
    ) else "unknown"
    return {
        "provider": "github", "number": value["number"], "url": value["html_url"],
        "state": "merged" if value.get("merged") else value.get("state"),
        "draft": bool(value.get("draft")), "head_sha": value["head"]["sha"],
        "head_ref": value["head"]["ref"], "base_ref": value["base"]["ref"],
        "base_sha": value["base"].get("sha"), "mergeability": mergeability,
        "author_id": str(value["user"]["id"]),
    }


def normalize_gitlab(value: dict) -> dict:
    # GitLab says opened/closed/locked/merged; the lifecycle speaks open/closed/merged.
    state = {"opened": "open", "locked": "open"}.get(value.get("state"), value.get("state"))
    raw_mergeability = value.get("detailed_merge_status") or value.get("merge_status")
    mergeability = "conflicting" if raw_mergeability in {"cannot_be_merged", "conflict", "conflicts"} else "mergeable" if raw_mergeability in {"mergeable", "can_be_merged"} else "unknown"
    return {
        "provider": "gitlab", "number": value["iid"], "url": value["web_url"],
        "state": state,
        "draft": bool(value.get("draft") or str(value.get("title", "")).lower().startswith("draft:")),
        "head_sha": value["sha"], "head_ref": value["source_branch"],
        "base_ref": value["target_branch"],
        "base_sha": (value.get("diff_refs") or {}).get("base_sha"),
        "mergeability": mergeability, "author_id": str(value["author"]["id"]),
    }


def get_provider(config: RepositoryConfig | None = None) -> GitProvider:
    config = config or load_repository_config()
    return GitHubProvider(config) if config.provider == "github" else GitLabProvider(config)
