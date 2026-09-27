"""Durable execution admission/dispatch primitives. No unconfined fallback exists."""
from __future__ import annotations
import json
import os
import pathlib
from datetime import timedelta
from typing import Any

from airflow.configuration import conf
from sqlalchemy.orm import Session
from sqlalchemy import func, select

from .models import Event, Execution, Revision, RunReport, Task, utcnow
from .service import PreconditionFailed, _event

PROFILE_ALIASES = {"junior": "@task", "senior": "@default", "staff": "@plan"}

REVISION_REPAIR_DIRECTIVE = (
    "Autopilot owns this revision repair. A prior candidate has been applied to the current base. "
    "Inspect the worktree for .rej files, reconcile every rejected hunk into its adjacent target "
    "file, and delete each .rej file. Preserve the candidate intent while incorporating current-base "
    "changes and fix related failures within the admitted paths. There is no external or human "
    "blocker to delegate: complete the repair in this run."
)


def _planned_resolution(action: str, seed: dict | None) -> str:
    if not seed:
        return action
    prefix = REVISION_REPAIR_DIRECTIVE + "\n\nOriginal planned resolution:\n"
    return prefix + action[:20_000 - len(prefix)]


def executor_preconditions() -> list[str]:
    """Return stable fail-closed codes; a command existing is not confinement proof."""
    codes: list[str] = []
    if not conf.getboolean("bot_dashboard", "executor_enabled", fallback=False): codes.append("executor_disabled")
    assertion = os.environ.get("BOT_DASHBOARD_CONFINEMENT_ASSERTION", "")
    policy = os.environ.get("BOT_DASHBOARD_NETWORK_POLICY_ASSERTION", "")
    launcher = os.environ.get("BOT_DASHBOARD_SANDBOX_LAUNCHER", "")
    if assertion != "rootless-userns-cgroup-v1": codes.append("confinement_unverified")
    if policy != "deny-all-except-model-gateway-v1": codes.append("egress_policy_unverified")
    if not launcher: codes.append("sandbox_launcher_missing")
    else:
        path = pathlib.Path(launcher)
        try:
            stat = path.stat()
            if not path.is_file() or stat.st_uid != 0 or stat.st_mode & 0o022: codes.append("sandbox_launcher_unsafe")
        except OSError: codes.append("sandbox_launcher_missing")
    return sorted(set(codes))


def require_executor_preconditions() -> None:
    codes = executor_preconditions()
    if codes: raise PreconditionFailed(",".join(codes))


def claim_pending(session: Session, limit: int = 20) -> list[dict[str, Any]]:
    """Lease admissions deterministically and return exact trigger arguments."""
    require_executor_preconditions()
    now = utcnow()
    rows = session.scalars(
        select(Execution)
        .where(Execution.dispatch_state == "pending", Execution.terminal_at.is_(None))
        .order_by(Execution.created_at, Execution.id)
        .limit(min(limit, 100))
        .with_for_update(skip_locked=True)
    ).all()
    results = []
    for row in rows:
        row.dispatch_state = "leased"
        row.lease_expires_at = now + timedelta(minutes=3)
        dag_id = (
            "bot__pr_reviewer"
            if row.admission_kind == "pr_reviewer"
            else "bot__task_executor"
        )
        results.append({
            "trigger_dag_id": dag_id,
            "trigger_run_id": row.target_run_id,
            "conf": {"execution_id": row.execution_id, "revision": row.revision},
            "skip_when_already_exists": True,
            "wait_for_completion": False,
        })
    session.flush()
    return results


def claim_run(
    session: Session,
    *,
    dag_id: str,
    run_id: str,
    conf_value: dict,
    kind: str,
    deadline_at,
) -> dict[str, Any]:
    expected_dag = "bot__pr_reviewer" if kind == "pr_reviewer" else "bot__task_executor"
    if dag_id != expected_dag:
        raise PreconditionFailed("run uses the wrong executor DAG")
    if set(conf_value) != {"execution_id", "revision"}:
        raise PreconditionFailed("run conf must contain exactly execution_id and revision")
    if not isinstance(conf_value["execution_id"], str) or not isinstance(conf_value["revision"], int):
        raise PreconditionFailed("run conf types are invalid")
    require_executor_preconditions()
    row = session.scalar(
        select(Execution)
        .where(Execution.execution_id == conf_value["execution_id"])
        .with_for_update()
    )
    if row is None or row.admission_kind != kind or row.target_run_id != run_id or row.revision != conf_value["revision"]:
        raise PreconditionFailed("run does not match an admission")
    if row.terminal_at is not None:
        raise PreconditionFailed("admission is terminal")
    if row.claimed_run_id and row.claimed_run_id != run_id:
        raise PreconditionFailed("admission was consumed by another run")
    task = session.scalar(select(Task).where(Task.id == row.task_id).with_for_update())
    if task is None or task.state in {"completed", "dismissed"}:
        raise PreconditionFailed("task is unavailable")
    revision = session.scalar(
        select(Revision)
        .where(Revision.task_id == task.id, Revision.revision_number == row.revision)
    )
    if revision is None:
        raise PreconditionFailed("admitted task revision is unavailable")
    if kind == "executor" and row.pr_number is not None:
        from .git_provider import get_provider, load_repository_config

        config = load_repository_config()
        if config.provider != row.provider or config.project != row.repository:
            raise PreconditionFailed("published execution repository identity changed before resume")
        observation = get_provider(config).read_change(row.pr_number)
        expected = {
            "provider": row.provider, "number": row.pr_number,
            "head_ref": row.branch, "base_ref": row.target_branch,
            "author_id": row.service_account_id, "head_sha": row.trusted_head_sha,
            "state": "open",
        }
        if any(value is None or observation.get(key) != value for key, value in expected.items()):
            raise PreconditionFailed("published execution identity, trusted head, or open state changed before resume")
    row.claimed_run_id = run_id
    row.dispatch_state = "running" if kind == "executor" else "reviewing"
    row.stage = "claimed"
    row.lease_expires_at = None
    if kind == "executor":
        row.executor_deadline_at = deadline_at
    else:
        row.review_deadline_at = deadline_at
    if kind == "executor" and task.state in {"accepted", "in_review", "ready"}:
        previous = task.state
        task.state = "in_progress"
        task.version += 1
        _event(session, task, "execution_started", "executor", run_id, from_state=previous, to_state="in_progress", payload={"sequence": row.sequence, "revision": row.revision})
    source_artifact = None
    repository_policy = None
    seed = None
    if kind == "executor":
        from .git_provider import load_repository_config
        from .repository_ops import create_source_artifact
        from .revision_seed import get_seed

        seed = get_seed(session, row)
        repair_base_sha = seed.get("repair_base_sha") if seed else None
        stored = create_source_artifact(
            session, row, expected_base_sha=repair_base_sha,
        )
        # The admission contract carries only the content-addressed reference.
        source_artifact = {"sha256": stored["sha256"], "byte_count": stored["byte_count"]}
        config = load_repository_config()
        repository_policy = {
            "allowed_path_globs": list(config.allowed_path_globs),
            "denied_path_globs": list(config.denied_path_globs),
            "max_changed_files": config.max_changed_files,
            "max_diff_bytes": config.max_diff_bytes,
        }
    elif row.source_artifact_sha256:
        from .artifacts import read_artifact

        artifact, _ = read_artifact(session, row.source_artifact_sha256)
        source_artifact = {
            "sha256": artifact.sha256,
            "byte_count": artifact.byte_count,
        }
    session.flush()
    source_report_reference = (
        f"{revision.source_dag_id}/{revision.source_run_id}/"
        f"{revision.source_task_id}/{revision.source_map_index}"
        if revision.source_dag_id
        else None
    )
    from .model_settings import model_for_role

    selected_model = model_for_role(session, "pr_reviewer" if kind == "pr_reviewer" else f"executor_{row.profile}")
    admission = {
        "protocol_version": 2,
        "kind": kind,
        "task_id": str(task.id),
        "profile": row.profile,
        "model_role": PROFILE_ALIASES[row.profile],
        "model": selected_model,
        "reviewer_required": row.reviewer_required,
        "sequence": row.sequence,
        "revision": row.revision,
        "execution_id": row.execution_id,
        "deadline_at": deadline_at.isoformat(),
        "task": {
            "title": revision.title,
            "category": task.category,
            "planned_resolution": _planned_resolution(revision.action, seed),
            "verification_commands": revision.verification_commands,
            "allowed_path_globs": revision.allowed_path_globs,
            "resource_keys": revision.resource_keys,
        },
        "source_artifact": source_artifact,
        "base_sha": row.base_sha,
        "patch_sha256": row.patch_sha256,
        "trusted_head_sha": row.trusted_head_sha,
        "pr_number": row.pr_number,
        "pr_url": row.pr_url,
        "verification_manifest": row.verification_manifest,
        "executor_report_sha256": row.executor_report_sha256,
        "repository_policy": repository_policy,
        "source_report_reference": source_report_reference,
    }
    from .report_schemas import ExecutorAdmissionV2, ReviewerAdmissionV2

    if kind == "executor":
        if seed:
            if seed.get("repair_base_sha"):
                valid_seed = (
                    row.base_sha == seed["repair_base_sha"]
                    and row.repository == config.project
                    and row.provider == config.provider
                    and row.target_branch == config.base_branch
                )
            else:
                valid_seed = (
                    row.base_sha == seed["base_sha"]
                    and row.source_artifact_sha256 == seed["source_artifact_sha256"]
                    and row.repository == config.project
                    and row.provider == config.provider
                    and row.target_branch == config.base_branch
                )
            if not valid_seed:
                raise PreconditionFailed("Revision seed source or repository identity changed")
        admission["seed_patch_sha256"] = seed["patch_sha256"] if seed else None
    else:
        from .models import ValidationGate
        admission["validation_evidence"] = [{
            "gate_key": gate.gate_key, "stage": gate.stage, "subject": gate.subject,
            "version": gate.version, "status": gate.status, "evidence": gate.evidence,
        } for gate in session.scalars(select(ValidationGate).where(
            ValidationGate.task_id == task.id, ValidationGate.subject == row.trusted_head_sha,
            ValidationGate.status == "passed",
        ).order_by(ValidationGate.gate_key).limit(20)).all()]

    contract = ExecutorAdmissionV2 if kind == "executor" else ReviewerAdmissionV2
    return contract.model_validate(admission).model_dump(mode="json")

def expire_leases(session: Session) -> int:
    now = utcnow()
    rows = session.scalars(
        select(Execution)
        .where(
            Execution.dispatch_state == "leased",
            Execution.lease_expires_at < now,
        )
        .with_for_update(skip_locked=True)
    ).all()
    for row in rows:
        row.dispatch_state = "pending"
        row.lease_expires_at = None
    return len(rows)


def _record_terminal_metadata(
    row: Execution, report: RunReport | None, reason_code: str
) -> None:
    row.terminal_reason_code = reason_code[:100]
    if report is None:
        row.terminal_failure_class = "TerminalOutcome"
        row.terminal_detail = "run report unavailable"
        return
    row.terminal_failure_class = (report.failure_class or "TerminalOutcome")[:100]
    detail = report.failure_detail or ""
    if not detail and reason_code == "execution_blocked" and isinstance(report.body_json, dict):
        # A completed worker can intentionally return a blocked result rather
        # than raise an exception. Preserve its explanation on the execution.
        from airflow._shared.secrets_masker import redact
        summary = report.body_json.get("summary")
        if isinstance(summary, str):
            detail = str(redact(summary, "summary", max_depth=20))
            row.terminal_failure_class = "ExecutionBlocked"
    row.terminal_detail = detail[:8192]




def finalize_run(
    session: Session, *, dag_id: str, run_id: str, kind: str, result: dict
) -> dict:
    row = session.scalar(
        select(Execution)
        .where(
            Execution.claimed_run_id == run_id,
            Execution.admission_kind == kind,
        )
        .with_for_update()
    )
    if row is None:
        return {"status": "unchanged", "code": "admission_missing"}
    task = session.scalar(select(Task).where(Task.id == row.task_id).with_for_update())
    report = session.scalar(
        select(RunReport)
        .where(
            RunReport.dag_id == dag_id,
            RunReport.run_id == run_id,
            RunReport.task_id == "run",
            RunReport.map_index == -1,
        )
        .order_by(RunReport.try_number.desc())
        .limit(1)
        .with_for_update()
    )
    previous = task.state
    terminal = False
    publication = None
    if report is None:
        target, code, terminal = "blocked", "report_missing", True
    elif report.outcome != "succeeded" or report.body_json is None:
        target, code, terminal = "blocked", report.reason_code, True
    elif kind == "executor":
        report_status = report.body_json.get("status")
        row.executor_report_sha256 = (
            result.get("result_artifact_sha256") or row.executor_report_sha256
        )
        if report_status == "no_change":
            target, code, terminal = "ready", "no_change", True
        elif report_status == "ok" and row.pr_number:
            target, code = "in_review", "changes_submitted"
            if row.reviewer_required:
                row.admission_kind = "pr_reviewer"
                row.target_run_id = (
                    f"review__{task.id}__{row.sequence}__r{row.revision}"
                )
                row.claimed_run_id = None
                row.dispatch_state = "pending"
                row.stage = "review_admitted"
            else:
                row.dispatch_state = "running"
                row.stage = "provider_sync"
        else:
            target, code, terminal = "blocked", "execution_blocked", True
    else:
        from .review import publish_review_comments
        row.review_report_sha256 = (
            result.get("result_artifact_sha256") or report.sha256
        )
        publication = publish_review_comments(session, dag_id=dag_id, run_id=run_id)
        verdict = report.body_json.get("verdict")
        failure_kind = report.body_json.get("failure_kind")
        repair = report.body_json.get("repair")
        row.review_report_sha256 = report.sha256
        row.review_verdict = verdict
        row.provider_state = {
            **(row.provider_state or {}),
            "review_verdict": verdict,
            "review_failure_kind": failure_kind,
            "review_repair": repair,
        }
        target, code = previous, verdict or "review_failed"
        row.stage = "reviewed"
        row.dispatch_state = "running"
    if kind == "executor" and previous not in {"completed", "dismissed"}:
        task.state = target
        if target == "blocked":
            task.blocked_from_state = previous
        task.version += 1
        _event(
            session,
            task,
            "execution_finalized",
            "executor",
            run_id,
            from_state=previous,
            to_state=target,
            payload={"code": code},
        )
    elif kind == "pr_reviewer":
        _event(
            session,
            task,
            "review_finalized",
            "reviewer",
            run_id,
            from_state=previous,
            to_state=previous,
            payload={"verdict": code, "publication": publication},
        )
        task.version += 1
    if terminal:
        _record_terminal_metadata(row, report, code)
        row.stage = "terminal"
        row.dispatch_state = "terminal"
        row.terminal_at = utcnow()
    session.flush()
    return {"status": "ok", "code": code}


def publish_execution_change(
    session: Session, *, dag_id: str, run_id: str, executor_result: dict
) -> dict:
    if dag_id != "bot__task_executor":
        raise PreconditionFailed("publication uses the wrong executor DAG")
    row = session.scalar(
        select(Execution)
        .where(
            Execution.claimed_run_id == run_id,
            Execution.admission_kind == "executor",
            Execution.terminal_at.is_(None),
        )
        .with_for_update()
    )
    if row is None:
        raise PreconditionFailed("executor admission is unavailable")
    task = session.scalar(select(Task).where(Task.id == row.task_id).with_for_update())
    revision = session.scalar(
        select(Revision).where(
            Revision.task_id == row.task_id,
            Revision.revision_number == row.revision,
        )
    )
    if task is None or revision is None:
        raise PreconditionFailed("executor task revision is unavailable")
    status = executor_result.get("status")
    if status == "no_change":
        manifest = executor_result.get("verification_manifest")
        if not isinstance(manifest, dict):
            raise PreconditionFailed("no-change result requires verification manifest")
        row.verification_manifest = manifest
        row.executor_report_sha256 = executor_result.get("report_sha256")
        _event(
            session,
            task,
            "execution_no_change",
            "executor",
            run_id,
            payload={"verification_manifest": manifest},
        )
        session.flush()
        return {"status": "no_change"}
    if status != "ok":
        raise PreconditionFailed("only successful executor output can be published")
    from .repository_ops import publish_execution_change as publish

    return publish(session, row, task, revision, executor_result)
