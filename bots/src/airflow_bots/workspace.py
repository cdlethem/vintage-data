"""Private git clone + throwaway worktrees for agent runs.

The bot keeps a bare clone in ``<state_dir>/repo.git``. Each agent run gets a
fresh detached worktree; afterwards the bot (not the agent) commits whatever
changed and pushes a branch. The GitHub token is passed to git only for
fetch/push, through environment config, so it never lands on disk.
"""
from __future__ import annotations

import base64
import fcntl
import os
import re
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path

from .config import Config


class Workspace:
    def __init__(self, cfg: Config, dry_run: bool = False):
        self.git_dir = cfg.state_dir / "repo.git"
        self.work_root = cfg.state_dir / "work"
        host = "github.com" if cfg.github_api == "https://api.github.com" else re.sub(
            r"^https?://([^/]+).*$", r"\1", cfg.github_api)
        self.url = f"https://{host}/{cfg.github_repo}.git"
        self.token = cfg.secret(cfg.token_env)
        name, _, email = cfg.author.partition("<")
        self.author = (name.strip(), email.rstrip(">").strip())
        self.dry_run = dry_run

    def _git(self, *args: str, cwd: Path | None = None, auth: bool = False) -> str:
        env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
        if auth and self.token:
            basic = base64.b64encode(f"x-access-token:{self.token}".encode()).decode()
            env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader",
                       GIT_CONFIG_VALUE_0=f"AUTHORIZATION: basic {basic}")
        command = ["git", *args] if cwd else ["git", f"--git-dir={self.git_dir}", *args]
        result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError(f"git {' '.join(args[:3])} failed: {result.stderr.strip()[-500:]}")
        return result.stdout.strip()

    @contextmanager
    def _lock(self):
        """Parallel agent runs share one clone; ref updates and worktree bookkeeping take turns."""
        self.git_dir.parent.mkdir(parents=True, exist_ok=True)
        with open(self.git_dir.parent / "repo.lock", "w") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            yield

    def _ensure_repo(self) -> None:
        if not self.git_dir.exists():
            subprocess.run(["git", "init", "--quiet", "--bare", str(self.git_dir)], check=True)
            self._git("remote", "add", "origin", self.url)

    def fetch(self, branch: str) -> str:
        """Fetch one branch and return its commit sha."""
        with self._lock():
            self._ensure_repo()
            self._git("fetch", "--quiet", "--no-tags", "origin",
                      f"+refs/heads/{branch}:refs/remotes/origin/{branch}", auth=True)
            return self._git("rev-parse", f"refs/remotes/origin/{branch}")

    def ensure_commit(self, sha: str) -> None:
        """Make sure an exact commit (e.g. from an eval case) is available locally."""
        with self._lock():
            self._ensure_repo()
            try:
                self._git("cat-file", "-e", f"{sha}^{{commit}}")
            except RuntimeError:
                self._git("fetch", "--quiet", "--no-tags", "origin", sha, auth=True)

    @contextmanager
    def checkout(self, sha: str, name: str):
        """A detached worktree at ``sha``; removed afterwards."""
        path = self.work_root / name
        with self._lock():
            if path.exists():
                shutil.rmtree(path)
                self._git("worktree", "prune")
            path.parent.mkdir(parents=True, exist_ok=True)
            self._git("worktree", "add", "--quiet", "--detach", str(path), sha)
        try:
            yield path
        finally:
            shutil.rmtree(path, ignore_errors=True)
            with self._lock():
                self._git("worktree", "prune")

    def changes(self, worktree: Path, base_sha: str) -> tuple[list[str], str]:
        """(changed paths, unified diff) of everything in the worktree relative to ``base_sha``."""
        self._git("add", "--all", cwd=worktree)
        files = self._git("diff", "--cached", "--name-only", base_sha, cwd=worktree).splitlines()
        diff = self._git("diff", "--cached", base_sha, cwd=worktree) if files else ""
        return files, diff

    def push(self, worktree: Path, branch: str, message: str) -> str:
        """Commit staged changes (if any) and push HEAD to ``branch``. Returns the pushed sha."""
        if self._git("status", "--porcelain", cwd=worktree):
            name, email = self.author
            self._git("-c", f"user.name={name}", "-c", f"user.email={email}",
                      "commit", "--quiet", "--no-verify", "-m", message, cwd=worktree)
        sha = self._git("rev-parse", "HEAD", cwd=worktree)
        if self.dry_run:
            print(f"[dry-run] git push {sha[:10]} -> {branch}")
        else:
            with self._lock():
                self._git("push", "--quiet", "origin", f"HEAD:refs/heads/{branch}", cwd=worktree, auth=True)
        return sha
