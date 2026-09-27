#!/usr/bin/env python3
"""Pull only merged origin/main into a clean local main; never stage private runtime files.

Exit codes: deploy no-op 0, deploy changed 10, errors 1. stdout is key=value
status; diagnostics go to stderr. Rollback is limited to the last recorded release.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


class DeploymentError(Exception):
    pass


class Sync:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.state_dir = self.root / "orchestration/airflow_home/deploy"
        self.state_file = self.state_dir / "release.json"

    def run(self, args: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
        result = subprocess.run(
            args, cwd=cwd or self.root, env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if result.returncode:
            # stderr can contain a token-bearing remote URL or private test output.
            raise DeploymentError(f"command failed ({args[0]} {args[1] if len(args) > 1 else ''}, exit {result.returncode})")
        return result.stdout.strip()

    def git(self, *args: str) -> str:
        return self.run(["git", *args])

    def revision(self, ref: str) -> str:
        return self.git("rev-parse", "--verify", f"{ref}^{{commit}}")

    def ancestor(self, older: str, newer: str) -> bool:
        result = subprocess.run(
            ["git", "merge-base", "--is-ancestor", older, newer], cwd=self.root,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
        )
        if result.returncode not in (0, 1):
            raise DeploymentError("unable to inspect repository ancestry")
        return result.returncode == 0

    def clean_main(self) -> str:
        if self.git("symbolic-ref", "--quiet", "--short", "HEAD") != "main":
            raise DeploymentError("deployment checkout must be on main")
        current = self.revision("HEAD")
        if current != self.revision("refs/heads/main"):
            raise DeploymentError("checkout HEAD does not match local main")
        if self.git("status", "--porcelain=v1", "--untracked-files=normal"):
            raise DeploymentError("deployment checkout has tracked or untracked changes")
        return current

    def target(self, current: str) -> str:
        try:
            previous_remote = self.revision("refs/remotes/origin/main")
        except DeploymentError:
            previous_remote = None
        # Never fetch a bot branch or arbitrary refspec, including tags.
        self.git("fetch", "--no-tags", "origin", "refs/heads/main:refs/remotes/origin/main")
        target = self.revision("refs/remotes/origin/main")
        if previous_remote and not self.ancestor(previous_remote, target):
            raise DeploymentError("origin/main history was rewritten")
        if not self.ancestor(current, target):
            raise DeploymentError("local main diverges from origin/main")
        return target

    def protect_runtime_files(self, target: str) -> None:
        private = (
            "orchestration/config.env", "orchestration/airflow.env",
            "orchestration/airflow.secrets.env", "orchestration/airflow_home/",
            "orchestration/.venv/", "orchestration/generated/", "bots/models.yml",
        )
        if self.git("ls-tree", "-r", "--name-only", target, "--", *(path.rstrip("/") for path in private)):
            raise DeploymentError("origin/main tracks private runtime paths")
        # Ensure the candidate does not remove ignore rules that protect local
        # files already present (including the release record itself).
        with tempfile.TemporaryDirectory(prefix="vintage-ignore-") as temporary:
            source = Path(temporary) / "source"
            self.git("worktree", "add", "--detach", "--", str(source), target)
            try:
                for path in private:
                    example = path + "deploy/release.json" if path.endswith("airflow_home/") else (
                        path + "runtime-file" if path.endswith("/") else path
                    )
                    result = subprocess.run(
                        ["git", "check-ignore", "--quiet", "--no-index", example],
                        cwd=source, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        check=False,
                    )
                    if result.returncode != 0:
                        raise DeploymentError(f"origin/main no longer ignores {path}")
            finally:
                self.git("worktree", "remove", "--", str(source))

    def record(self) -> dict[str, str] | None:
        if not self.state_file.exists():
            return None
        try:
            data = json.loads(self.state_file.read_text())
            if data["previous"] == data["current"] or any(
                not isinstance(data[key], str) or len(data[key]) != 40
                or any(char not in "0123456789abcdef" for char in data[key])
                for key in ("previous", "current")
            ):
                raise ValueError("invalid revision")
            if data["status"] not in ("pending", "installed", "rejected"):
                raise ValueError("invalid status")
            return data
        except (KeyError, ValueError, TypeError) as exc:
            raise DeploymentError("invalid deployment record; refusing to proceed") from exc

    def save(self, previous: str, current: str, status: str) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, filename = tempfile.mkstemp(prefix="release-", dir=self.state_dir)
        try:
            with os.fdopen(descriptor, "w") as output:
                json.dump({"previous": previous, "current": current, "status": status}, output)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(filename, self.state_file)
        finally:
            if os.path.exists(filename):
                os.unlink(filename)

    def stage(self, sha: str, *, validate: bool) -> Path:
        """Prepare a wheel from an immutable Git commit in a disposable worktree."""
        cached = self.state_dir / sha
        existing = list(cached.glob("*.whl"))
        if not validate and len(existing) == 1:
            return existing[0]
        if len(existing) > 1:
            raise DeploymentError("ambiguous cached provider wheels")
        # Reuse only dependency caches, never the candidate checkout or its
        # credentials. Otherwise each release redownloads Corepack and 300 UI
        # packages before the verified build can begin.
        tool_cache = self.state_dir / "tool-cache"
        tool_cache.mkdir(parents=True, exist_ok=True, mode=0o700)
        with tempfile.TemporaryDirectory(prefix="vintage-stage-") as temporary:
            scratch = Path(temporary)
            source = scratch / "source"
            self.git("worktree", "add", "--detach", "--", str(source), sha)
            try:
                if self.run(["git", "rev-parse", "HEAD"], cwd=source) != sha:
                    raise DeploymentError("staged worktree revision differs from candidate")
                # No original .venv, airflow.env, credentials, or other ignored files are copied.
                isolated = {
                    key: os.environ[key] for key in ("PATH", "LANG", "LC_ALL", "TZ")
                    if key in os.environ
                }
                isolated.update({
                    "HOME": str(scratch / "home"), "XDG_CACHE_HOME": str(tool_cache / "xdg"),
                    "XDG_DATA_HOME": str(tool_cache / "data"),
                    "COREPACK_HOME": str(tool_cache / "corepack"),
                    "UV_CACHE_DIR": str(tool_cache / "uv"),
                    "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
                    "AIRFLOW_HOME": str(scratch / "airflow"), "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
                    "PYTHONNOUSERSITE": "1",
                })
                (scratch / "home").mkdir()
                ui = source / "orchestration/provider_bot_dashboard/ui"
                provider = source / "orchestration/provider_bot_dashboard"
                if validate:
                    self.validate(source, scratch, isolated)
                self.run(["corepack", "pnpm", "install", "--frozen-lockfile",
                          "--store-dir", str(tool_cache / "pnpm-store")], cwd=ui, env=isolated)
                if validate:
                    self.run(["corepack", "pnpm", "test"], cwd=ui, env=isolated)
                    self.run(["corepack", "pnpm", "exec", "tsc", "--noEmit"], cwd=ui, env=isolated)
                self.run(["corepack", "pnpm", "build"], cwd=ui, env=isolated)
                if validate and self.run(
                    ["git", "status", "--porcelain", "--untracked-files=all", "--",
                     "orchestration/provider_bot_dashboard/src/airflow/providers/vintage/bot_dashboard/static"],
                    cwd=source, env=isolated,
                ):
                    raise DeploymentError("staged UI bundles differ from committed assets")
                wheels = scratch / "wheels"
                self.run(["uv", "build", "--wheel", "--out-dir", str(wheels), str(provider)], cwd=source, env=isolated)
                produced = list(wheels.glob("*.whl"))
                if len(produced) != 1:
                    raise DeploymentError("expected exactly one staged provider wheel")
                cached.mkdir(parents=True, exist_ok=True, mode=0o700)
                destination = cached / produced[0].name
                shutil.copy2(produced[0], destination)
                for stale in existing:
                    if stale != destination:
                        stale.unlink()
                return destination
            finally:
                # Only the disposable generated worktree is force-removed (UI builds create files).
                self.git("worktree", "remove", "--force", "--", str(source))

    def validate(self, source: Path, scratch: Path, env: dict[str, str]) -> None:
        # CI's Python test lane, installed only into the disposable staging venv.
        python = scratch / "venv/bin/python"
        self.run(["uv", "venv", "--python", "3.13", str(python.parent.parent)], cwd=source, env=env)
        self.run([
            "uv", "pip", "install", "--python", str(python), "-r", "load/requirements.txt",
            "-r", "transform/requirements.txt", "pytest",
        ], cwd=source, env=env)
        self.run(["uv", "pip", "install", "--python", str(python),
                  "-e", "orchestration/provider_bot_dashboard"], cwd=source, env=env)
        env["PATH"] = f"{python.parent}:{env.get('PATH', os.defpath)}"
        self.run([str(python), "-m", "pytest", "-q", "bots", "orchestration",
                  "extract"], cwd=source, env=env)

    def install(self, wheel: Path) -> None:
        python = self.root / "orchestration/.venv/bin/python"
        if not python.is_file():
            raise DeploymentError("Airflow runtime virtualenv is missing")
        self.run(["uv", "pip", "install", "--python", str(python), "--force-reinstall", "--no-deps", str(wheel)])

    def prepare_runtime(self) -> None:
        # Tracked config templates may have changed with the release. Private
        # config.env and secrets remain local; only derived files are rendered.
        self.run(["bash", "orchestration/setup/render_config.sh"])
        self.run([
            "bash", "-c",
            "set -a; source orchestration/airflow.env; "
            "source orchestration/airflow.secrets.env; set +a; "
            "exec orchestration/.venv/bin/airflow db migrate",
        ])

    def undo(self, previous: str, current: str) -> None:
        if self.clean_main() != current or self.revision("refs/heads/main") != current:
            raise DeploymentError("checkout changed before rollback")
        # Switching while clean updates tracked files without reset, restore, or
        # forced checkout. The main ref is then moved by atomic compare-and-swap.
        self.git("switch", "--detach", previous)
        if self.revision("HEAD") != previous or self.revision("refs/heads/main") != current:
            raise DeploymentError("checkout changed during rollback")
        self.git("update-ref", "refs/heads/main", previous, current)
        self.git("switch", "main")
        if self.clean_main() != previous:
            raise DeploymentError("rollback did not restore clean previous release")
        self.install(self.cached_wheel(previous))
        self.prepare_runtime()
        # A failed release must not be reinstalled on the next timer tick.
        self.save(previous, current, "rejected")

    def cached_wheel(self, sha: str) -> Path:
        wheels = list((self.state_dir / sha).glob("*.whl"))
        if len(wheels) != 1:
            raise DeploymentError("previous release wheel unavailable")
        return wheels[0]

    def check(self) -> tuple[str, str]:
        current = self.clean_main()
        record = self.record()
        if record and record["status"] == "pending":
            if current != record["previous"]:
                raise DeploymentError("pending deployment requires rollback before another deployment")
            self.save(record["previous"], record["current"], "rejected")
        target = self.target(current)
        if record and record["status"] in ("pending", "rejected") and target == record["current"]:
            raise DeploymentError("origin/main points to rejected release; awaiting a new merged revision")
        return current, target

    def deploy(self) -> int:
        previous, target = self.check()
        if previous == target:
            print("STATUS=no-op")
            return 0
        self.protect_runtime_files(target)
        wheel = self.stage(target, validate=True)
        self.stage(previous, validate=False)  # rollback artifact must exist before promotion
        self.save(previous, target, "pending")
        try:
            self.git("merge", "--ff-only", target)
            self.install(wheel)
            self.prepare_runtime()
        except Exception:
            # Only undo once promotion actually moved HEAD; a failed merge must not
            # accidentally erase unrelated changes made concurrently.
            if self.revision("HEAD") == target and self.clean_main() == target:
                try:
                    self.undo(previous, target)
                except Exception as exc:
                    raise DeploymentError("deployment failed and automatic rollback failed; manual intervention required") from exc
            else:
                self.state_file.unlink()
            raise
        self.save(previous, target, "installed")
        print(f"DEPLOYED_FROM={previous}\nDEPLOYED_TO={target}\nSTATUS=changed")
        return 10

    def rollback(self, requested: str | None) -> int:
        record = self.record()
        if not record or (requested is not None and requested != record["previous"]):
            raise DeploymentError("rollback SHA must match the recorded previous release")
        current = self.clean_main()
        if current != record["current"]:
            raise DeploymentError("checkout is not at the recorded deployed revision")
        previous = record["previous"]
        self.cached_wheel(previous)
        self.undo(previous, current)
        print(f"DEPLOYED_FROM={current}\nDEPLOYED_TO={previous}\nSTATUS=changed")
        return 10


def main(argv: list[str] | None = None, *, root: Path | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2],
                        help="clean deployment checkout (defaults to this script's repository)")
    parser.add_argument("command", choices=("check", "deploy", "rollback"))
    parser.add_argument("sha", nargs="?")
    args = parser.parse_args(argv)
    if args.sha and args.command != "rollback":
        parser.error("SHA is only accepted for rollback")
    sync = Sync(root or args.root)
    try:
        sync.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(sync.state_dir / "release.lock", os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, "rb") as release_lock:
            try:
                fcntl.flock(release_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise DeploymentError("another release command is running") from exc
            if args.command == "deploy":
                return sync.deploy()
            if args.command == "rollback":
                return sync.rollback(args.sha)
            previous, target = sync.check()
            print(f"DEPLOYED_FROM={previous}\nDEPLOYED_TO={target}\nSTATUS={'no-op' if previous == target else 'ready'}")
            return 0
    except (DeploymentError, OSError) as exc:
        print(f"deployment: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
