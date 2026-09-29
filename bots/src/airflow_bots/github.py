"""GitHub is the ticket tracker: issues for problems, PRs for changes, labels for status.

Bot-authored text carries hidden markers so the bot can recognise its own
threads regardless of which account the token belongs to:

- ``<!-- bots:key=... -->`` in an issue body names the problem it tracks.
- ``<!-- bots:reply-to=<comment id> -->`` in a comment answers a ``/bot`` request.

With ``dry_run`` every write is printed instead of sent.
"""
from __future__ import annotations

import re
from urllib.parse import quote

from . import http
from .config import Config

LABEL = "bots"
STATUS_LABELS = {
    "fixing": "bots:fixing",      # a PR will merge automatically once checks pass
    "review": "bots:review",      # a PR is waiting for a human review
    "waiting": "bots:waiting",    # nothing to change; closes when the task recovers
    "question": "bots:question",  # the bot needs a human decision
}
AUTOMERGE_LABEL = "bots:automerge"
MUTE_LABEL = "bots:mute"

KEY_RE = re.compile(r"<!-- bots:key=(\S+) -->")
REPLY_RE = re.compile(r"<!-- bots:reply-to=(\d+) -->")


def keys(body: str | None) -> list[str]:
    return KEY_RE.findall(body or "")


def key_marker(key: str) -> str:
    return f"<!-- bots:key={key} -->"


class GitHub:
    def __init__(self, repo: str, token: str | None, api: str = "https://api.github.com", dry_run: bool = False):
        self.repo, self.api, self.dry_run = repo, api, dry_run
        self._token = token

    @classmethod
    def from_config(cls, cfg: Config, dry_run: bool = False) -> "GitHub":
        token = cfg.secret(cfg.token_env)
        if not token and not dry_run:
            raise RuntimeError(f"GitHub token missing: set {cfg.token_env}")
        return cls(cfg.github_repo, token, cfg.github_api, dry_run)

    def _call(self, method: str, path: str, body: object = None):
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        url = path if path.startswith("http") else f"{self.api}/repos/{self.repo}{path}"
        return http.request(method, url, headers=headers, body=body)

    def _write(self, method: str, path: str, body: object = None, fake: dict | None = None):
        if self.dry_run:
            print(f"[dry-run] {method} {path}")
            for name, value in (body or {}).items() if isinstance(body, dict) else ():
                print(f"  {name}: {value}" if "\n" not in str(value) else f"  {name}:\n{value}\n")
            return fake or {"number": 0, "html_url": "(dry-run)", "id": 0}
        return self._call(method, path, body)

    def _pages(self, path: str, limit: int = 1000) -> list[dict]:
        rows, page = [], 1
        sep = "&" if "?" in path else "?"
        while len(rows) < limit:
            batch = self._call("GET", f"{path}{sep}per_page=100&page={page}")
            rows += batch
            if len(batch) < 100:
                break
            page += 1
        return rows[:limit]

    # --- reads -------------------------------------------------------------
    def issues(self, labels: str = LABEL, state: str = "open", since: str | None = None) -> list[dict]:
        """Issues only (the API also returns PRs; those are dropped)."""
        path = f"/issues?labels={quote(labels)}&state={state}" + (f"&since={since}" if since else "")
        return [row for row in self._pages(path) if "pull_request" not in row]

    def pulls(self, labels: str) -> list[dict]:
        path = f"/issues?labels={quote(labels)}&state=open"
        return [row for row in self._pages(path) if "pull_request" in row]

    def issue(self, number: int) -> dict:
        return self._call("GET", f"/issues/{number}")

    def comments(self, number: int) -> list[dict]:
        return self._pages(f"/issues/{number}/comments")

    def comments_since(self, since: str) -> list[dict]:
        return self._pages(f"/issues/comments?since={since}&sort=created&direction=asc")

    def pull(self, number: int) -> dict:
        return self._call("GET", f"/pulls/{number}")

    def pull_files(self, number: int) -> list[str]:
        return [row["filename"] for row in self._pages(f"/pulls/{number}/files")]

    def check_runs(self, sha: str) -> list[dict]:
        return self._call("GET", f"/commits/{sha}/check-runs?per_page=100")["check_runs"]

    # --- writes ------------------------------------------------------------
    def create_issue(self, title: str, body: str, labels: list[str]) -> dict:
        return self._write("POST", "/issues", {"title": title, "body": body, "labels": labels})

    def update_issue(self, number: int, **fields: object) -> dict:
        return self._write("PATCH", f"/issues/{number}", fields)

    def comment(self, number: int, body: str) -> dict:
        return self._write("POST", f"/issues/{number}/comments", {"body": body})

    def set_status(self, number: int, status: str | None, current_labels: list[str]) -> None:
        """Replace the bots:<status> label, leaving other labels alone."""
        keep = [name for name in current_labels if name not in STATUS_LABELS.values()]
        labels = keep + ([STATUS_LABELS[status]] if status else [])
        self._write("PUT", f"/issues/{number}/labels", {"labels": labels})

    def add_labels(self, number: int, labels: list[str]) -> None:
        self._write("POST", f"/issues/{number}/labels", {"labels": labels})

    def remove_label(self, number: int, label: str) -> None:
        if self.dry_run:
            print(f"[dry-run] DELETE label {label} from #{number}")
            return
        try:
            self._call("DELETE", f"/issues/{number}/labels/{quote(label)}")
        except http.HTTPError as exc:
            if exc.status != 404:
                raise

    def create_pull(self, branch: str, base: str, title: str, body: str, labels: list[str]) -> dict:
        pr = self._write("POST", "/pulls", {"head": branch, "base": base, "title": title, "body": body})
        if labels:
            self.add_labels(pr["number"], labels)
        return pr

    def merge(self, number: int, sha: str) -> dict:
        return self._write("PUT", f"/pulls/{number}/merge", {"sha": sha, "merge_method": "squash"})

    def update_branch(self, number: int) -> None:
        self._write("PUT", f"/pulls/{number}/update-branch", {})
