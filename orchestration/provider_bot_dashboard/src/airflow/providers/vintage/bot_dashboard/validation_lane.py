"""Transactional queue and lease primitives for automatic validation gates.

This module deliberately owns only validation-lane state.  API handlers and DAGs
call these primitives; they do not reimplement version, dependency, lease, or
Autopilot checks.
"""
from __future__ import annotations

import hashlib
import json
import secrets
from datetime import timedelta
from typing import Any, Callable

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from .models import Artifact, Execution, Task, ValidationGate, utcnow
from .service import Conflict, PreconditionFailed, _event
from .validation_recipes import ValidationRecipeError, validate_admission

LEASE_SECONDS = 360
MAX_CLAIM = 100
AUTOMATIC_OWNER = "validation-service"


def _gate_dict(row: ValidationGate, *, candidate: dict[str, str] | None = None) -> dict[str, Any]:
    """Return only the worker admission contract, including its exact lease."""
    value = {
        "id": str(row.id), "task_id": str(row.task_id), "gate_key": row.gate_key,
        "version": row.version, "recipe": row.recipe, "recipe_args": row.recipe_args,
        "required_capability": row.required_capability, "subject": row.subject,
        "dependencies": list(row.dependencies), "lease_id": row.lease_id,
        "lease_run_id": row.lease_run_id,
        "lease_expires_at": row.lease_expires_at.isoformat() if row.lease_expires_at else None,
        "attempt": row.attempt, "status": row.status, "evidence": row.evidence,
    }
    if candidate is not None:
        value["candidate"] = candidate
    return value


def _locked_gate(session: Session, gate_id: str) -> ValidationGate:
    import uuid
    try:
        identity = uuid.UUID(gate_id)
    except ValueError as exc:
        raise PreconditionFailed("validation gate is unavailable") from exc
    row = session.scalar(select(ValidationGate).where(ValidationGate.id == identity).with_for_update())
    if row is None:
        raise PreconditionFailed("validation gate is unavailable")
    return row


def _task(session: Session, row: ValidationGate) -> Task:
    task = session.scalar(select(Task).where(Task.id == row.task_id).with_for_update())
    if task is None or task.state == "dismissed" or (
        task.state == "completed" and row.stage not in {"activation", "completion"}
    ):
        raise PreconditionFailed("validation gate task is unavailable")
    return task


def _dependencies_passed(session: Session, row: ValidationGate) -> bool:
    dependencies = list(row.dependencies)
    if len(dependencies) != len(set(dependencies)) or row.gate_key in dependencies:
        raise PreconditionFailed("validation gate dependencies are invalid")
    if not dependencies:
        return True
    rows = session.scalars(select(ValidationGate).where(
        ValidationGate.task_id == row.task_id, ValidationGate.gate_key.in_(dependencies)
    )).all()
    by_key = {item.gate_key: item for item in rows}
    if set(by_key) != set(dependencies):
        raise PreconditionFailed("validation gate dependency is unavailable")
    return all(item.status == "passed" and item.subject == row.subject for item in by_key.values())


def _record(task: Task, row: ValidationGate, event_type: str, payload: dict[str, Any], session: Session) -> None:
    task.version += 1
    task.updated_at = utcnow()
    _event(session, task, event_type, "system", AUTOMATIC_OWNER, payload={
        "gate_key": row.gate_key, "recipe": row.recipe, "subject": row.subject,
        "attempt": row.attempt, **payload,
    })


def _admission(row: ValidationGate) -> None:
    if row.owner != AUTOMATIC_OWNER:
        raise PreconditionFailed("validation gate has no automatic owner")
    try:
        validate_admission({
            "recipe": row.recipe, "recipe_args": row.recipe_args,
            "required_capability": row.required_capability, "subject": row.subject,
        })
    except ValidationRecipeError as exc:
        raise PreconditionFailed(str(exc)) from exc


def _candidate_snapshot(session: Session, row: ValidationGate) -> dict[str, str]:
    """Attest that the downloaded source+patch remains the live trusted head.

    An archive cannot prove its own Git commit ID.  The control plane therefore
    re-observes the provider and checks the checkpoint manifest before it gives
    a worker the two immutable artifacts.
    """
    execution = session.scalar(select(Execution).where(
        Execution.task_id == row.task_id,
    ).order_by(Execution.sequence.desc(), Execution.revision.desc()).limit(1).with_for_update())
    post_merge = row.stage in {"activation", "completion"}
    if (execution is None or execution.trusted_head_sha != row.subject
            or (post_merge and (not execution.merged_at
                                or (execution.provider_state or {}).get("state") != "merged"))
            or (not post_merge and execution.terminal_at is not None)
            or not all((execution.source_artifact_sha256, execution.patch_sha256,
                        execution.provider, execution.repository, execution.branch,
                        execution.target_branch, execution.service_account_id, execution.pr_number,
                        execution.base_sha, execution.verification_manifest))):
        raise PreconditionFailed("validation candidate checkpoint is unavailable")
    source = session.get(Artifact, execution.source_artifact_sha256)
    patch = session.get(Artifact, execution.patch_sha256)
    if source is None or source.kind != "source" or patch is None or patch.kind != "patch":
        raise PreconditionFailed("validation candidate artifacts are unavailable")
    try:
        from .git_provider import get_provider, load_repository_config
        from .report_schemas import VerificationManifestV1
        config = load_repository_config()
        manifest = VerificationManifestV1.model_validate(execution.verification_manifest)
    except (ValueError, TypeError) as exc:
        raise PreconditionFailed("validation candidate manifest is invalid") from exc
    if (config.provider != execution.provider or config.project != execution.repository
            or (manifest.task_id, manifest.execution_id, manifest.revision, manifest.base_sha, manifest.patch_sha256)
            != (str(row.task_id), execution.execution_id, execution.revision, execution.base_sha, execution.patch_sha256)):
        raise PreconditionFailed("validation candidate checkpoint changed")
    observed = get_provider(config).read_change(execution.pr_number)
    expected = {
        "provider": execution.provider, "number": execution.pr_number,
        "head_ref": execution.branch, "base_ref": execution.target_branch,
        "author_id": execution.service_account_id, "head_sha": row.subject,
        "state": "merged" if post_merge else "open",
    }
    if any(observed.get(key) != value for key, value in expected.items()):
        raise PreconditionFailed("validation candidate trusted head changed")
    return {
        "head_sha": row.subject, "base_sha": execution.base_sha,
        "source_artifact_sha256": source.sha256, "patch_artifact_sha256": patch.sha256,
        "patch_sha256": manifest.patch_sha256,
    }

def _fail_admission(session: Session, row: ValidationGate, error: Exception) -> None:
    """Make an expected unexecutable gate visibly failed without stopping its peers."""
    row.status = "failed"
    row.lease_id = None
    row.lease_run_id = None
    row.lease_expires_at = None
    from airflow.exceptions import AirflowNotFoundException
    from .git_provider import GitProviderError
    unavailable = "validation capability" in str(error) or "configured validation capabilities" in str(error)
    workflow_unavailable = row.recipe == "trusted_workflow_check" and isinstance(
        error, (GitProviderError, AirflowNotFoundException))
    workflow_invalid = row.recipe == "trusted_workflow_check" and "trusted workflow" in str(error)
    row.last_error = ("validation_capability_unavailable" if unavailable
                      else "validation_workflow_attestation_unavailable" if workflow_unavailable
                      else "validation_workflow_attestation_invalid" if workflow_invalid
                      else f"validation_admission_{type(error).__name__}"[:200])
    row.evidence = {
        "label": ("Validation capability unavailable" if unavailable
                  else "Validation workflow attestation unavailable" if workflow_unavailable
                  else "Validation workflow attestation invalid" if workflow_invalid
                  else "Validation admission failed"),
        "observation": f"exact subject {row.subject}; gate was not executed",
        "reason_code": row.last_error,
    }
    row.version += 1
    row.updated_at = utcnow()
    task = session.scalar(select(Task).where(Task.id == row.task_id).with_for_update())
    if task is not None and task.state != "dismissed" and (
        task.state != "completed" or row.stage in {"activation", "completion"}
    ):
        _record(task, row, "validation_gate_admission_failed", {"reason_code": row.last_error}, session)

def claim_pending(
    session: Session, *, limit: int, runner_id: str, autopilot_enabled: bool,
):
    """Lease executable gates in FIFO order without bypassing a disabled Autopilot."""
    if not autopilot_enabled:
        return []
    if not isinstance(runner_id, str) or not runner_id or len(runner_id) > 250:
        raise PreconditionFailed("validation runner identity is invalid")
    requested = min(max(limit, 1), MAX_CLAIM)
    now = utcnow()
    rows = session.scalars(select(ValidationGate).where(
        ValidationGate.status == "pending",
        ValidationGate.owner == AUTOMATIC_OWNER,
        ValidationGate.recipe != "trusted_workflow_check",
        ValidationGate.subject.not_in(("candidate_head", "pending_candidate")),
    ).order_by(ValidationGate.created_at, ValidationGate.id).limit(MAX_CLAIM).with_for_update(skip_locked=True)).all()
    claimed = []
    from .git_provider import GitProviderError
    for row in rows:
        if len(claimed) >= requested:
            break
        # A stale pending lease belongs to a recovery path, never a second runner.
        if row.lease_expires_at is not None and row.lease_expires_at > now:
            continue
        try:
            _task(session, row)
            _admission(row)
            if not _dependencies_passed(session, row):
                continue
            candidate = _candidate_snapshot(session, row)
        except (PreconditionFailed, GitProviderError) as exc:
            _fail_admission(session, row, exc)
            continue
        row.status = "leased"
        row.lease_id = secrets.token_hex(32)
        row.attempt += 1
        row.lease_run_id = f"validation__{row.id}__a{row.attempt}"
        row.lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
        row.version += 1
        row.updated_at = now
        task = _task(session, row)
        _record(task, row, "validation_gate_leased", {"runner_id": runner_id, "lease_run_id": row.lease_run_id}, session)
        claimed.append(_gate_dict(row, candidate=candidate))
    session.flush()
    return claimed


def poll_trusted_workflows(session: Session, *, limit: int, autopilot_enabled: bool) -> list[dict[str, Any]]:
    """Finish only completed, provider-attested workflow checks for exact candidate heads."""
    if not autopilot_enabled:
        return []
    from airflow.exceptions import AirflowNotFoundException
    from .git_provider import GitProviderError, get_provider, load_repository_config

    rows = session.scalars(select(ValidationGate).where(
        ValidationGate.status == "pending",
        ValidationGate.owner == AUTOMATIC_OWNER,
        ValidationGate.recipe == "trusted_workflow_check",
        ValidationGate.subject.not_in(("candidate_head", "pending_candidate")),
    ).order_by(ValidationGate.created_at, ValidationGate.id)
        .limit(min(max(limit, 1), MAX_CLAIM)).with_for_update(skip_locked=True)).all()
    finished = []
    for row in rows:
        try:
            task = _task(session, row)
            _admission(row)
            if not _dependencies_passed(session, row):
                continue
            _candidate_snapshot(session, row)
            execution = session.scalar(select(Execution).where(
                Execution.task_id == row.task_id,
            ).order_by(Execution.sequence.desc(), Execution.revision.desc()).limit(1))
            config = load_repository_config()
            attested = get_provider(config).read_validation_workflow(
                execution.pr_number, row.subject, row.recipe_args["workflow_path"],
                row.recipe_args["job_name"],
            )
            if attested is None:
                continue  # No completed trusted run yet; polling never consumes an attempt.
            if (not isinstance(attested, dict) or attested.get("status") not in {"passed", "failed"}
                    or attested.get("head_sha") != row.subject
                    or attested.get("workflow_path") != row.recipe_args["workflow_path"]
                    or attested.get("job_name") != row.recipe_args["job_name"]):
                raise PreconditionFailed("trusted workflow attestation is invalid")
            run_id, check_id, run_url = (
                attested.get("run_id"), attested.get("check_run_id"), attested.get("run_url"))
            if (run_id is not None and (type(run_id) is not int or run_id <= 0)
                    or check_id is not None and (type(check_id) is not int or check_id <= 0)
                    or run_url is not None and (
                        not isinstance(run_url, str) or not run_url.startswith("https://")
                        or len(run_url) > 2048)):
                raise PreconditionFailed("trusted workflow evidence identity is invalid")
            diagnostic = attested.get("diagnostic")
            if attested["status"] == "failed" and (
                    not isinstance(diagnostic, str) or not diagnostic or len(diagnostic) > 200
                    or attested.get("workflow_run_id") != run_id):
                raise PreconditionFailed("trusted workflow failure lacks a bounded diagnostic")
            if attested["status"] == "passed" and (
                    run_id is None or check_id is None
                    or attested.get("event") != "pull_request" or attested.get("conclusion") != "success"
                    or diagnostic is not None or attested.get("workflow_run_id") != run_id):
                raise PreconditionFailed("trusted workflow pass lacks exact pull-request attestation")
        except (PreconditionFailed, GitProviderError, AirflowNotFoundException) as exc:
            _fail_admission(session, row, exc)
            finished.append(_gate_dict(row))
            continue
        status = attested["status"]
        assertions = {
            "workflow_path": attested["workflow_path"], "job_name": attested["job_name"],
            "head_sha": row.subject, "run_id": attested["run_id"],
            "check_run_id": attested["check_run_id"], "event": attested.get("event"),
            "conclusion": attested.get("conclusion"),
        }
        row.status = status
        row.last_error = "validation_workflow_failed" if status == "failed" else None
        row.evidence = {
            "label": "Trusted workflow validation",
            "observation": f"exact subject {row.subject}; attested workflow job {status}",
            "subject": row.subject, "recipe": row.recipe, "command_id": row.recipe_args["command_id"],
            "sha256": hashlib.sha256(json.dumps(assertions, sort_keys=True).encode()).hexdigest(),
            "assertions": assertions,
            **({"url": run_url} if run_url else {}),
            **({"reason_code": "validation_workflow_failed", "diagnostic_tail": diagnostic}
               if status == "failed" else {}),
        }
        row.attempt += 1
        row.version += 1
        row.updated_at = utcnow()
        _record(task, row, "validation_gate_recorded", {
            "status": status, "reason_code": row.last_error, "evidence_sha256": row.evidence["sha256"],
        }, session)
        finished.append(_gate_dict(row))
    session.flush()
    return finished


def start(
    session: Session, *, gate_id: str, version: int, lease_id: str, lease_run_id: str, subject: str,
    autopilot_enabled: bool,
) -> dict[str, Any]:
    """Make a current lease running only after checking all mutable authority again."""
    if not autopilot_enabled:
        raise PreconditionFailed("Autopilot is disabled")
    row = _locked_gate(session, gate_id)
    now = utcnow()
    if row.version != version:
        raise Conflict("validation gate version is stale")
    if row.status != "leased" or row.lease_id != lease_id or row.lease_run_id != lease_run_id:
        raise PreconditionFailed("validation gate lease is unavailable")
    if row.lease_expires_at is None or row.lease_expires_at <= now:
        raise PreconditionFailed("validation gate lease expired")
    if subject != row.subject:
        raise PreconditionFailed("validation gate subject changed")
    _candidate_snapshot(session, row)
    _admission(row)
    if not _dependencies_passed(session, row):
        raise PreconditionFailed("validation gate dependencies are incomplete")
    row.status = "running"
    row.version += 1
    row.updated_at = now
    task = _task(session, row)
    _record(task, row, "validation_gate_started", {"lease_run_id": lease_run_id}, session)
    session.flush()
    return _gate_dict(row)


def finish(
    session: Session, *, gate_id: str, version: int, lease_id: str, lease_run_id: str, subject: str,
    status: str, evidence: dict[str, Any], autopilot_enabled: bool,
) -> dict[str, Any]:
    """Persist a result only for the still-current exact-head running lease."""
    if not autopilot_enabled:
        raise PreconditionFailed("Autopilot is disabled")
    if status not in {"passed", "failed"}:
        raise PreconditionFailed("validation gate result is invalid")
    if not isinstance(evidence, dict) or not evidence.get("observation"):
        raise PreconditionFailed("validation result requires an observation")
    row = _locked_gate(session, gate_id)
    now = utcnow()
    if row.version != version:
        raise Conflict("validation gate version is stale")
    if row.status != "running" or row.lease_id != lease_id or row.lease_run_id != lease_run_id:
        raise PreconditionFailed("validation gate lease is unavailable")
    if row.lease_expires_at is None or row.lease_expires_at <= now:
        raise PreconditionFailed("validation gate lease expired")
    if subject != row.subject:
        raise PreconditionFailed("validation evidence belongs to a different subject")
    task = _task(session, row)
    row.status = status
    row.evidence = evidence
    row.lease_id = None
    row.lease_run_id = None
    row.lease_expires_at = None
    row.last_error = evidence.get("reason_code") if status == "failed" else None
    row.version += 1
    row.updated_at = now
    _record(task, row, "validation_gate_recorded", {
        "status": status, "lease_run_id": lease_run_id, "evidence_sha256": evidence.get("sha256"),
    }, session)
    session.flush()
    return _gate_dict(row)


def recover_expired(session: Session, *, limit: int, autopilot_enabled: bool) -> int:
    """Recover abandoned work and bounded runner failures without erasing evidence."""
    if not autopilot_enabled:
        return 0
    now = utcnow()
    rows = session.scalars(select(ValidationGate).where(
        ValidationGate.owner == AUTOMATIC_OWNER,
        or_(
            and_(ValidationGate.status.in_(("leased", "running")), ValidationGate.lease_expires_at < now),
            and_(ValidationGate.status == "failed", ValidationGate.attempt.between(1, 2),
                 ValidationGate.updated_at < now - timedelta(minutes=15)),
        ),
    ).order_by(ValidationGate.updated_at, ValidationGate.id)
        .limit(min(max(limit, 1), MAX_CLAIM)).with_for_update(skip_locked=True)).all()
    recovered = 0
    for row in rows:
        prior_status, prior_evidence, prior_run = row.status, row.evidence, row.lease_run_id
        if prior_status == "failed" and (prior_evidence or {}).get("label") != "Validation runner failure":
            continue
        task = session.scalar(select(Task).where(Task.id == row.task_id).with_for_update())
        retry = (task is not None and task.state != "dismissed"
                 and (task.state != "completed" or row.stage in {"activation", "completion"})
                 and row.attempt < 3)
        row.status = "pending" if retry else "failed"
        row.lease_id = None
        row.lease_run_id = None
        row.lease_expires_at = None
        row.last_error = "validation_retry_scheduled" if retry else "validation_retry_exhausted"
        row.evidence = None if retry else {
            "label": "Validation lease failed", "reason_code": row.last_error,
            "subject": row.subject, "attempt": row.attempt,
        }
        row.version += 1
        row.updated_at = now
        if task is not None:
            _record(task, row, "validation_gate_retry_scheduled" if retry else "validation_gate_retry_exhausted",
                    {"lease_run_id": prior_run, "previous_status": prior_status, "previous_evidence": prior_evidence}, session)
        recovered += 1
    session.flush()
    return recovered


def rebind_candidate_gates(session: Session, task: Task, new_head: str) -> int:
    """Schedule fresh bot validation when publication attests a different candidate head."""
    from .validation_recipes import _HEAD
    if not _HEAD.fullmatch(new_head):
        raise PreconditionFailed("validation candidate head is invalid")
    rows = session.scalars(select(ValidationGate).where(
        ValidationGate.task_id == task.id,
        ValidationGate.owner == AUTOMATIC_OWNER,
        ValidationGate.status.in_(("pending", "passed", "failed")),
        ValidationGate.subject.not_in(("candidate_head", "pending_candidate", new_head)),
    ).with_for_update()).all()
    for row in rows:
        _schedule_recheck(session, task, row, new_head)
    session.flush()
    return len(rows)


def _schedule_recheck(session: Session, task: Task, row: ValidationGate, new_head: str) -> None:
    prior = {"subject": row.subject, "status": row.status, "evidence": row.evidence,
             "last_error": row.last_error, "attempt": row.attempt}
    row.subject = new_head
    row.status = "pending"
    row.evidence = None
    row.last_error = None
    row.attempt = 0
    row.version += 1
    row.updated_at = utcnow()
    _record(task, row, "validation_gate_recheck_scheduled",
            {"previous_result": prior, "new_head": new_head}, session)


def recheck_new_candidate(session: Session, *, gate_id: str, version: int, new_head: str,
                          autopilot_enabled: bool) -> dict[str, Any]:
    """Explicit bot recheck only after a newly attested, distinct candidate head."""
    from .validation_recipes import _HEAD
    if not autopilot_enabled:
        raise PreconditionFailed("Autopilot is disabled")
    if not _HEAD.fullmatch(new_head):
        raise PreconditionFailed("validation candidate head is invalid")
    row = _locked_gate(session, gate_id)
    if row.version != version:
        raise Conflict("validation gate version is stale")
    if row.owner != AUTOMATIC_OWNER or row.status not in {"failed", "passed"} or row.subject == new_head:
        raise PreconditionFailed("validation gate needs a distinct candidate head and observed result")
    task = _task(session, row)
    old_head = row.subject
    row.subject = new_head
    try:
        _candidate_snapshot(session, row)
        _admission(row)
    finally:
        row.subject = old_head
    _schedule_recheck(session, task, row, new_head)
    session.flush()
    return _gate_dict(row)
