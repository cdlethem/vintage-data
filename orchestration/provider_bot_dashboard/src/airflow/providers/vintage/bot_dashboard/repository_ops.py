"""Trusted Git materialization and publication; credentials stay in child env."""
from __future__ import annotations
import hashlib
import io
import json
import os
import pathlib
import stat
import subprocess
import tarfile
import tempfile
from sqlalchemy.orm import Session

from .artifacts import put_artifact, read_artifact
from .git_provider import GitProviderError, RepositoryConfig, get_provider, load_repository_config, validate_changed_paths
from .models import Execution, Revision, Task, utcnow
from .service import _event


def _git_environment(root: pathlib.Path, config: RepositoryConfig) -> dict[str, str]:
    askpass = root / "askpass.py"
    askpass.write_text(
        "#!/usr/bin/env python3\n"
        "import os,sys\n"
        "print(os.environ['BOT_GIT_USERNAME'] if 'sername' in sys.argv[1] "
        "else os.environ['BOT_GIT_TOKEN'])\n",
        encoding="utf-8",
    )
    askpass.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": "C.UTF-8",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ASKPASS": str(askpass),
        "BOT_GIT_TOKEN": config.token,
        "BOT_GIT_USERNAME": "x-access-token" if config.provider == "github" else "oauth2",
        "GIT_AUTHOR_NAME": "Vintage Bot Dashboard",
        "GIT_AUTHOR_EMAIL": "bot-dashboard@localhost",
        "GIT_COMMITTER_NAME": "Vintage Bot Dashboard",
        "GIT_COMMITTER_EMAIL": "bot-dashboard@localhost",
        "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z",
        "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
    }


def _git(root: pathlib.Path, config: RepositoryConfig, *args: str, timeout: int = 120) -> str:
    process = subprocess.run(
        ["git", *args],
        cwd=root,
        env=_git_environment(root, config),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if process.returncode:
        raise GitProviderError(f"trusted git operation failed with exit code {process.returncode}")
    return process.stdout.strip()


def _materialize(root: pathlib.Path, config: RepositoryConfig, base_sha: str | None = None) -> tuple[pathlib.Path, str]:
    repository = root / "repository"
    repository.mkdir(mode=0o700)
    _git(repository, config, "init")
    _git(repository, config, "remote", "add", "origin", config.clone_url)
    reference = base_sha or config.base_branch
    _git(repository, config, "fetch", "--depth=1", "origin", reference)
    _git(repository, config, "checkout", "--detach", "FETCH_HEAD")
    resolved = _git(repository, config, "rev-parse", "HEAD")
    if base_sha and resolved != base_sha:
        raise GitProviderError("configured base SHA could not be reproduced")
    return repository, resolved


def create_source_artifact(session: Session, execution: Execution) -> dict:
    if execution.source_artifact_sha256:
        row, _ = read_artifact(session, execution.source_artifact_sha256)
        return {"sha256": row.sha256, "byte_count": row.byte_count}
    config = load_repository_config()
    with tempfile.TemporaryDirectory(prefix="bot-dashboard-source-") as temporary:
        root = pathlib.Path(temporary)
        repository, base_sha = _materialize(root, config)
        archive = subprocess.run(
            ["git", "archive", "--format=tar", "HEAD"],
            cwd=repository,
            capture_output=True,
            timeout=120,
            check=False,
        )
        if archive.returncode:
            raise GitProviderError(
                f"trusted git archive failed with exit code {archive.returncode}"
            )
        stored = put_artifact(
            session,
            kind="source",
            content=archive.stdout,
            owner_execution_id=execution.execution_id,
        )
    execution.base_sha = base_sha
    execution.source_artifact_sha256 = stored["sha256"]
    execution.repository = config.project
    execution.provider = config.provider
    execution.target_branch = config.base_branch
    session.flush()
    return stored


def publish_execution_change(
    session: Session,
    execution: Execution,
    task: Task,
    revision: Revision,
    result: dict,
) -> dict:
    patch_sha = str(result.get("patch_sha256") or "")
    manifest = result.get("verification_manifest")
    changed_paths = result.get("changed_paths")
    if not patch_sha or not isinstance(manifest, dict) or not isinstance(changed_paths, list):
        raise GitProviderError("executor publication result is incomplete")
    if execution.pr_number:
        incoming_base = result.get("base_sha") or manifest.get("base_sha")
        incoming_head = (
            result.get("trusted_head_sha")
            or result.get("head_sha")
            or manifest.get("trusted_head_sha")
            or manifest.get("head_sha")
        )
        if incoming_base != execution.base_sha:
            raise GitProviderError("published execution replay changed its base")
        if execution.patch_sha256 != patch_sha:
            raise GitProviderError("published execution replay changed its artifact")
        if incoming_head is not None and incoming_head != execution.trusted_head_sha:
            raise GitProviderError("published execution replay changed its trusted head")
        stored_head = (execution.provider_state or {}).get("head_sha")
        if stored_head != execution.trusted_head_sha:
            raise GitProviderError("published execution trusted head is inconsistent")
        return {
            "status": "published",
            "provider": execution.provider,
            "repository": execution.repository,
            "branch": execution.branch,
            "pr_number": execution.pr_number,
            "pr_url": execution.pr_url,
            "trusted_head_sha": execution.trusted_head_sha,
        }
    config = load_repository_config()
    if config.provider != execution.provider or config.project != execution.repository:
        raise GitProviderError("repository configuration changed after admission")
    _, patch = read_artifact(session, patch_sha)
    validate_changed_paths(config, changed_paths, len(patch))
    task_allowed = tuple(revision.allowed_path_globs)
    narrowed = RepositoryConfig(
        provider=config.provider,
        project=config.project,
        api_base_url=config.api_base_url,
        clone_url=config.clone_url,
        base_branch=config.base_branch,
        allowed_path_globs=task_allowed,
        denied_path_globs=config.denied_path_globs,
        max_changed_files=config.max_changed_files,
        max_diff_bytes=config.max_diff_bytes,
        service_account_id=config.service_account_id,
        token=config.token,
    )
    validate_changed_paths(narrowed, changed_paths, len(patch))
    branch = f"bot-dashboard/{task.id}/{execution.sequence}-r{execution.revision}"
    with tempfile.TemporaryDirectory(prefix="bot-dashboard-publish-") as temporary:
        root = pathlib.Path(temporary)
        repository, base_sha = _materialize(root, config, execution.base_sha)
        process = subprocess.run(
            ["git", "apply", "--index", "--binary", "-"],
            cwd=repository,
            input=patch,
            capture_output=True,
            timeout=120,
            check=False,
        )
        if process.returncode:
            raise GitProviderError(
                f"trusted patch application failed with exit code {process.returncode}"
            )
        actual_paths = _git(repository, config, "diff", "--cached", "--name-only").splitlines()
        if sorted(actual_paths) != sorted(changed_paths):
            raise GitProviderError("trusted changed paths differ from executor manifest")
        _git(repository, config, "checkout", "-b", branch)
        _git(repository, config, "commit", "-m", f"bot-dashboard: {task.title[:160]}")
        head_sha = _git(repository, config, "rev-parse", "HEAD")
        _git(repository, config, "push", "origin", f"HEAD:refs/heads/{branch}")
    provider = get_provider(config)
    observed = provider.find_change(branch)
    if observed is None:
        created = provider.create_change(
            branch,
            task.title,
            f"Task: {task.id}\n\nExecution: {execution.execution_id}\n\nBase: {base_sha}\nPatch: {patch_sha}",
        )
        number = int(created.get("number") or created.get("iid"))
        observed = provider.read_change(number)
    else:
        number = int(observed["number"])
    if (
        observed["provider"] != config.provider
        or observed["head_ref"] != branch
        or observed["base_ref"] != config.base_branch
        or observed["head_sha"] != head_sha
        or observed["state"] != "open"
        or observed["author_id"] != config.service_account_id
    ):
        raise GitProviderError("created change identity does not match publication")
    execution.branch = branch
    execution.patch_sha256 = patch_sha
    execution.patch_byte_count = len(patch)
    execution.executor_report_sha256 = result.get("report_sha256")
    execution.verification_manifest = manifest
    execution.pr_number = number
    execution.pr_url = observed["url"]
    execution.service_account_id = observed["author_id"]
    execution.trusted_head_sha = head_sha
    execution.provider_state = observed
    execution.provider_fingerprint = hashlib.sha256(
        json.dumps(observed, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    execution.published_at = utcnow()
    execution.stage = "published"
    _event(
        session,
        task,
        "change_published",
        "provider",
        execution.execution_id,
        payload={
            "provider": config.provider,
            "repository": config.project,
            "branch": branch,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "patch_sha256": patch_sha,
            "pr_number": number,
        },
    )
    session.flush()
    return {
        "status": "published",
        "provider": config.provider,
        "repository": config.project,
        "branch": branch,
        "pr_number": number,
        "pr_url": observed["url"],
        "trusted_head_sha": head_sha,
    }
