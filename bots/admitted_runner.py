"""Credential-free protocol-v2 parent for admitted executor and reviewer work."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import pathlib
import stat
import subprocess
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from typing import Any

try:
    from . import usage as usage_tools
except ImportError:
    import usage as usage_tools

_MAX_RESULT = 1_048_576


def _extract_source(content: bytes, root: pathlib.Path, error_type) -> pathlib.Path:
    archive_path = root / "source.tar"
    archive_path.write_bytes(content)
    archive_path.chmod(0o600)
    workdir = root / "workdir"
    workdir.mkdir(mode=0o700)
    with tarfile.open(archive_path, mode="r:") as archive:
        members = archive.getmembers()
        total = 0
        for member in members:
            path = pathlib.PurePosixPath(member.name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or member.issym()
                or member.islnk()
                or member.isdev()
            ):
                raise error_type("source_archive_unsafe", "terminal")
            total += max(member.size, 0)
        if len(members) > 20_000 or total > 512 * 1024 * 1024:
            raise error_type("source_archive_outside_bounds", "terminal")
        archive.extractall(workdir, filter="data")
    return workdir


def _git(
    workdir: pathlib.Path,
    metadata: pathlib.Path,
    *args: str,
    binary: bool = False,
    input_data: bytes | None = None,
) -> str | bytes:
    process = subprocess.run(
        ["git", f"--git-dir={metadata}", f"--work-tree={workdir}", *args],
        cwd=workdir,
        input=input_data,
        capture_output=True,
        text=not binary,
        timeout=120,
        check=False,
        env={
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "LANG": "C.UTF-8",
            "GIT_AUTHOR_NAME": "bot-dashboard baseline",
            "GIT_AUTHOR_EMAIL": "baseline@localhost",
            "GIT_COMMITTER_NAME": "bot-dashboard baseline",
            "GIT_COMMITTER_EMAIL": "baseline@localhost",
            "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z",
            "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
        },
    )
    if process.returncode:
        raise RuntimeError("local Git operation failed")
    return process.stdout


def _initialize_baseline(root: pathlib.Path, workdir: pathlib.Path) -> pathlib.Path:
    metadata = root / "baseline.git"
    process = subprocess.run(
        ["git", "init", "--bare", str(metadata)],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8"},
    )
    if process.returncode:
        raise RuntimeError("local Git baseline initialization failed")
    metadata.chmod(0o700)
    _git(workdir, metadata, "add", "-A")
    _git(workdir, metadata, "commit", "-m", "immutable baseline")
    return metadata


def _snapshot_tree(workdir: pathlib.Path, error_type) -> str:
    digest = hashlib.sha256()
    for path in sorted(workdir.rglob("*"), key=lambda value: value.as_posix()):
        relative = path.relative_to(workdir).as_posix()
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not (
            stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)
        ):
            raise error_type("worktree_entry_unsafe", "terminal")
        digest.update(relative.encode())
        digest.update(b"\x00d\x00" if stat.S_ISDIR(metadata.st_mode) else b"\x00f\x00")
        if stat.S_ISREG(metadata.st_mode):
            with path.open("rb") as handle:
                while chunk := handle.read(65_536):
                    digest.update(chunk)
    return digest.hexdigest()


def _read_result(path: pathlib.Path, error_type) -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise error_type("sandbox_result_missing", "terminal") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size < 2
            or metadata.st_size > _MAX_RESULT
        ):
            raise error_type("sandbox_result_unsafe", "terminal")
        body = bytearray()
        while len(body) <= _MAX_RESULT:
            chunk = os.read(descriptor, min(65_536, _MAX_RESULT + 1 - len(body)))
            if not chunk:
                break
            body.extend(chunk)
    finally:
        os.close(descriptor)
    try:
        value = json.loads(body)
    except (TypeError, json.JSONDecodeError) as exc:
        raise error_type("sandbox_protocol_invalid", "terminal") from exc
    if not isinstance(value, dict):
        raise error_type("sandbox_protocol_invalid", "terminal")
    return value


def _validate_paths(admission: dict, paths: list[str], diff_size: int, error_type) -> None:
    policy = admission["repository_policy"]
    task_allowed = admission["task"]["allowed_path_globs"]
    if len(paths) > policy["max_changed_files"] or diff_size > policy["max_diff_bytes"]:
        raise error_type("change_limits_exceeded", "terminal")
    seen: set[str] = set()
    for path in paths:
        folded = path.casefold()
        if (
            not path
            or path.startswith(("/", "../"))
            or "/../" in f"/{path}"
            or "\\" in path
            or folded in seen
        ):
            raise error_type("changed_path_invalid", "terminal")
        seen.add(folded)
        if not any(fnmatch.fnmatchcase(path, rule) for rule in task_allowed):
            raise error_type("task_path_policy_rejected", "terminal")
        if not any(
            fnmatch.fnmatchcase(path, rule)
            for rule in policy["allowed_path_globs"]
        ):
            raise error_type("repository_path_policy_rejected", "terminal")
        if any(
            fnmatch.fnmatchcase(path, rule)
            for rule in policy["denied_path_globs"]
        ):
            raise error_type("repository_path_denied", "terminal")


def _validated_checks(admission: dict, payload: dict, error_type) -> list[dict]:
    checks = payload.get("verification")
    expected = admission["task"]["verification_commands"]
    if not isinstance(checks, list) or [item.get("argv") for item in checks] != expected:
        raise error_type("verification_manifest_mismatch", "terminal")
    for index, item in enumerate(checks, start=1):
        if item.get("name") != f"verification_{index}":
            raise error_type("verification_manifest_mismatch", "terminal")
        observed = str(item.get("observed") or "")
        if hashlib.sha256(observed.encode()).hexdigest() != item.get("observation_sha256"):
            raise error_type("verification_digest_mismatch", "terminal")
    if payload.get("status") in {"ok", "no_change"} and any(
        item.get("exit_code") != 0 for item in checks
    ):
        raise error_type("verification_failed", "terminal")
    return checks


def run(context: dict, cfg: dict, runner, dashboard_module):
    from airflow.providers.vintage.bot_dashboard.report_schemas import (
        ExecutorAdmissionV2,
        ExecutorResultV2,
        ReviewerAdmissionV2,
        ReviewerResultV2,
        VerificationManifestV1,
    )

    ti = context["ti"]
    dag_run = context["dag_run"]
    identity = {
        "dag_id": ti.dag_id,
        "run_id": dag_run.run_id,
        "task_id": ti.task_id,
        "map_index": getattr(ti, "map_index", -1),
        "try_number": getattr(ti, "try_number", 1),
    }
    kind = "pr_reviewer" if cfg["name"] == "pr_reviewer" else "executor"
    started_at = datetime.now(timezone.utc)
    client = dashboard_module.DashboardClient.from_environment()
    claim = client.claim_budget(
        {key: identity[key] for key in ("dag_id", "run_id", "task_id", "map_index")},
        cfg["timeout_minutes"] * 60,
    )
    deadline_at = datetime.fromisoformat(claim["deadline_at"].replace("Z", "+00:00"))
    client.deadline_at = deadline_at
    budget = runner.RunBudget(deadline_at, cfg["cleanup_margin_seconds"])
    raw_admission = client.claim_execution(
        dag_id=ti.dag_id,
        run_id=dag_run.run_id,
        conf=dict(dag_run.conf or {}),
        kind=kind,
        deadline_at=deadline_at,
    )
    admission_type = ExecutorAdmissionV2 if kind == "executor" else ReviewerAdmissionV2
    admission = admission_type.model_validate(raw_admission).model_dump(mode="json")
    canonical_admission = json.dumps(
        admission, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    context_digest = {
        "sha256": hashlib.sha256(canonical_admission).hexdigest(),
        "byte_count": len(canonical_admission),
        "build_ms": int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000),
    }
    attempts: list[dict] = []
    payload = None
    failure = None
    publication: dict = {}
    result_artifact = None
    outcome, retry_class, reason_code = "failed", "terminal", "sandbox_failed"
    try:
        with tempfile.TemporaryDirectory(prefix=f"bot-{kind}-") as temporary:
            root = pathlib.Path(temporary)
            root.chmod(0o700)
            source = client.get_artifact(admission["source_artifact"]["sha256"])
            workdir = _extract_source(source, root, dashboard_module.ControlPlaneError)
            metadata = _initialize_baseline(root, workdir)
            if kind == "pr_reviewer":
                patch = client.get_artifact(admission["patch_sha256"])
                if hashlib.sha256(patch).hexdigest() != admission["patch_sha256"]:
                    raise dashboard_module.ControlPlaneError("patch_digest_invalid", "terminal")
                _git(workdir, metadata, "apply", "--binary", "-", binary=True, input_data=patch)
            before = _snapshot_tree(workdir, dashboard_module.ControlPlaneError)
            admission_path = root / "admission.json"
            admission_path.write_bytes(canonical_admission)
            admission_path.chmod(0o400)
            result_path = root / "result.json"
            launcher = pathlib.Path(os.environ["BOT_DASHBOARD_SANDBOX_LAUNCHER"])
            attempt_started_at = datetime.now(timezone.utc)
            attempt_started = time.monotonic()
            process = subprocess.run(
                [
                    str(launcher),
                    "--protocol",
                    "v2",
                    "--workdir",
                    str(workdir),
                    "--admission",
                    str(admission_path),
                    "--result",
                    str(result_path),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=budget.timeout(cfg["model_budget_minutes"] * 60),
                check=False,
                env={
                    key: value
                    for key, value in os.environ.items()
                    if key in {"PATH", "LANG", "LC_ALL", "TZ"}
                },
            )
            duration_ms = int((time.monotonic() - attempt_started) * 1000)
            if process.returncode:
                raise dashboard_module.ControlPlaneError(
                    f"sandbox_exit_{process.returncode}", "terminal"
                )
            raw_result = _read_result(result_path, dashboard_module.ControlPlaneError)
            result_type = ExecutorResultV2 if kind == "executor" else ReviewerResultV2
            result = result_type.model_validate(raw_result).model_dump(mode="json")
            payload = result["report"]
            attempts.append(
                {
                    "ordinal": 1,
                    "alias": admission["model_role"],
                    "provider": "confined_launcher",
                    "started_at": attempt_started_at.isoformat(),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "duration_ms": duration_ms,
                    "usage": usage_tools.normalize({}, format="openai"),
                    "reason_code": "sandbox_succeeded",
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                }
            )
            after = _snapshot_tree(workdir, dashboard_module.ControlPlaneError)
            if kind == "pr_reviewer":
                if before != after:
                    raise dashboard_module.ControlPlaneError(
                        "reviewer_modified_worktree", "terminal"
                    )
                result_artifact = client.put_artifact(
                    json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(),
                    kind="review_report",
                    owner_execution_id=admission["execution_id"],
                )
            else:
                _git(workdir, metadata, "add", "-A")
                current_diff = _git(
                    workdir,
                    metadata,
                    "diff",
                    "--cached",
                    "--binary",
                    "HEAD",
                    "--",
                    binary=True,
                )
                paths = [
                    line
                    for line in str(
                        _git(workdir, metadata, "diff", "--cached", "--name-only", "HEAD", "--")
                    ).splitlines()
                    if line
                ]
                _validate_paths(
                    admission,
                    paths,
                    len(current_diff),
                    dashboard_module.ControlPlaneError,
                )
                if sorted(payload["changed_paths"]) != sorted(paths):
                    raise dashboard_module.ControlPlaneError(
                        "executor_changed_paths_mismatch", "terminal"
                    )
                if payload["status"] == "no_change" and current_diff:
                    raise dashboard_module.ControlPlaneError(
                        "no_change_contains_diff", "terminal"
                    )
                if payload["status"] == "ok" and not current_diff:
                    raise dashboard_module.ControlPlaneError(
                        "successful_change_is_empty", "terminal"
                    )
                checks = _validated_checks(
                    admission, payload, dashboard_module.ControlPlaneError
                )
                patch_record = (
                    client.put_artifact(
                        current_diff,
                        kind="patch",
                        owner_execution_id=admission["execution_id"],
                    )
                    if current_diff
                    else None
                )
                manifest = VerificationManifestV1.model_validate(
                    {
                        "manifest_version": 1,
                        "task_id": admission["task_id"],
                        "execution_id": admission["execution_id"],
                        "revision": admission["revision"],
                        "base_sha": admission["base_sha"],
                        "changed_paths": sorted(paths),
                        "patch_sha256": patch_record["sha256"] if patch_record else hashlib.sha256(b"").hexdigest(),
                        "patch_bytes": len(current_diff),
                        "source_report_reference": admission["source_report_reference"],
                        "checks": checks,
                    }
                ).model_dump(mode="json")
                client.put_artifact(
                    json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode(),
                    kind="manifest",
                    owner_execution_id=admission["execution_id"],
                )
                result_artifact = client.put_artifact(
                    json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(),
                    kind="executor_report",
                    owner_execution_id=admission["execution_id"],
                )
                if payload["status"] in {"ok", "no_change"}:
                    publication = client.publish_execution(
                        ti.dag_id,
                        dag_run.run_id,
                        {
                            "status": payload["status"],
                            "patch_sha256": manifest["patch_sha256"],
                            "changed_paths": paths,
                            "verification_manifest": manifest,
                            "report_sha256": result_artifact["sha256"],
                        },
                    )
                else:
                    publication = {"status": "blocked"}
            outcome, retry_class, reason_code = "succeeded", "none", "sandbox_succeeded"
    except subprocess.TimeoutExpired as exc:
        failure = exc
        outcome, retry_class, reason_code = "timed_out", "terminal", "sandbox_timed_out"
    except Exception as exc:  # noqa: BLE001 - converted to a bounded envelope
        failure = exc
        outcome = "failed"
        retry_class = getattr(exc, "retry_class", "terminal")
        reason_code = getattr(exc, "code", "sandbox_failed")
    if retry_class in {"capacity", "transient"} and retry_class not in cfg["retry_on"]:
        retry_class = "terminal"
    envelope = runner._envelope(
        cfg=cfg,
        identity=identity,
        started_at=started_at,
        deadline_at=deadline_at,
        outcome=outcome,
        retry_class=retry_class,
        reason_code=reason_code,
        failure=failure,
        selected_model=admission["model_role"],
        attempts=attempts,
        context_digest=context_digest,
        payload=payload if outcome == "succeeded" else None,
    )
    projection = client.submit_run(envelope)
    projection["reason_code"] = reason_code
    if retry_class not in {"capacity", "transient"}:
        client.finalize_execution(
            ti.dag_id,
            dag_run.run_id,
            kind,
            {
                "projection": projection,
                "publication": publication,
                "result_artifact_sha256": result_artifact["sha256"] if result_artifact else None,
            },
        )
    return runner.RunResult(projection, outcome, retry_class, reason_code)
