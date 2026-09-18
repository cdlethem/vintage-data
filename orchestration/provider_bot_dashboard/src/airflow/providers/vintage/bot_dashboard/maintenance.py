"""Bounded provider observation, lease recovery, and authoritative retention."""
from __future__ import annotations
import hashlib
import json
import logging
from datetime import timedelta

import httpx

from airflow.configuration import conf
from airflow.sdk.observability import stats
from sqlalchemy import and_, case, delete, or_, select
from sqlalchemy.orm import Session

from .execution import expire_leases
from .git_provider import GitProviderError, GitProviderTransientError, get_provider
from .models import Artifact, Execution, Revision, RunReport, Task, utcnow
from .service import _event

log = logging.getLogger(__name__)
_ALLOWED_FOLLOW_UP = {
    "source_vetting": "bot__source_vetting",
    "source_scheduling": "bot__source_scheduling",
    "analytics_engineer": "bot__analytics_engineer",
    "data_analyst": "bot__data_analyst",
}


def _fingerprint(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _record_terminal(row: Execution, reason_code: str, detail: str) -> None:
    row.terminal_reason_code = reason_code[:100]
    row.terminal_failure_class = "HumanAttention"
    row.terminal_detail = detail[:8192]
    row.terminal_at = row.terminal_at or utcnow()


def sync_provider(session: Session, limit: int = 25) -> dict:
    interval = conf.getint("bot_dashboard", "git_sync_minutes", fallback=5)
    cutoff = utcnow() - timedelta(minutes=interval)
    candidates = session.execute(
        select(Execution.id, Execution.pr_number)
        .where(
            Execution.pr_number.is_not(None),
            Execution.terminal_at.is_(None),
            or_(Execution.synced_at.is_(None), Execution.synced_at < cutoff),
        )
        .order_by(Execution.synced_at.asc().nullsfirst(), Execution.id)
        .limit(min(limit, 100))
    ).all()
    if not candidates:
        return {"checked": 0, "changed": 0, "errors": 0, "follow_up_bots": []}
    try:
        provider = get_provider()
    except (httpx.TransportError, TimeoutError, ConnectionError, OSError) as exc:
        raise GitProviderTransientError("Git provider network failure") from exc
    changed = errors = 0
    follow_ups: list[dict] = []
    emitted_follow_ups: set[tuple[str, str]] = set()
    for execution_id, number in candidates:
        try:
            observation = provider.read_change(number)
        except GitProviderTransientError:
            errors += 1
            continue
        except (httpx.TransportError, TimeoutError, ConnectionError) as exc:
            raise GitProviderTransientError("Git provider network failure") from exc
        except GitProviderError:
            errors += 1
            continue
        fingerprint = _fingerprint(observation)
        row = session.scalar(
            select(Execution).where(Execution.id == execution_id).with_for_update()
        )
        if row is None or row.pr_number != number or row.terminal_at is not None:
            continue
        task = session.scalar(select(Task).where(Task.id == row.task_id).with_for_update())
        if (
            observation["provider"] != row.provider
            or observation["number"] != row.pr_number
            or observation["base_ref"] != row.target_branch
            or observation["head_ref"] != row.branch
            or observation["author_id"] != row.service_account_id
        ):
            observation = {
                "state": "invalid_identity",
                "draft": observation.get("draft"),
                "head_sha": observation.get("head_sha"),
            }
            fingerprint = _fingerprint(observation)
        if observation.get("head_sha") != row.trusted_head_sha:
            observation = {**observation, "state": "head_drift"}
            fingerprint = _fingerprint(observation)
        row.synced_at = utcnow()
        provider_changed = row.provider_fingerprint != fingerprint
        readiness_changed = (task is not None and task.state == "in_review"
                             and observation.get("state") == "open" and not observation.get("draft")
                             and (not row.reviewer_required or row.review_verdict == "approved"))
        if not provider_changed and not readiness_changed:
            continue
        old = row.provider_state or {}
        row.provider_state = observation
        row.provider_fingerprint = fingerprint
        changed += 1
        state = observation.get("state")
        if state == "invalid_identity":
            _record_terminal(
                row,
                "provider_identity_drift",
                "provider change identity no longer matches the immutable admission",
            )
        elif state == "head_drift":
            _record_terminal(
                row,
                "provider_head_drift",
                "provider change head no longer matches the trusted publication head",
            )
        elif state == "closed":
            _record_terminal(
                row,
                "provider_closed_unmerged",
                "provider change was closed without a trusted merge",
            )
        if task and task.state not in {"completed", "dismissed"}:
            previous = task.state
            target = previous
            if state == "merged" and previous in {"in_review", "ready"}:
                target = "ready"
                row.merged_at = utcnow()
            elif state in {"closed", "invalid_identity", "head_drift"} and previous in {
                "in_progress", "in_review", "ready",
            }:
                target = "blocked"
            elif previous == "in_review" and state == "open" and not observation.get("draft"):
                verdict = row.review_verdict or old.get("review_verdict")
                if not row.reviewer_required or verdict == "approved":
                    target = "ready"
            if target != previous:
                task.state = target
                task.version += 1
                if target == "blocked":
                    task.blocked_from_state = previous
            _event(
                session,
                task,
                "provider_observed",
                "provider",
                str(number),
                from_state=previous,
                to_state=target,
                payload={
                    "state": state,
                    "draft": observation.get("draft"),
                    "head_sha": observation.get("head_sha"),
                },
            )
            if state == "merged":
                revision = session.scalar(
                    select(Revision)
                    .where(Revision.task_id == task.id)
                    .order_by(Revision.revision_number.desc())
                    .limit(1)
                )
                for bot in revision.follow_up_bots if revision else []:
                    dag_id = _ALLOWED_FOLLOW_UP.get(bot)
                    if dag_id is None:
                        _event(
                            session,
                            task,
                            "follow_up_route_rejected",
                            "provider",
                            bot,
                            payload={"code": "unknown_follow_up_bot"},
                        )
                        continue
                    trigger_run_id = f"follow_up__{task.id}__{row.sequence}__{bot}"
                    follow_up_key = (dag_id, trigger_run_id)
                    if follow_up_key in emitted_follow_ups:
                        continue
                    emitted_follow_ups.add(follow_up_key)
                    follow_ups.append(
                        {
                            "trigger_dag_id": dag_id,
                            "trigger_run_id": trigger_run_id,
                            "conf": {
                                "task_id": str(task.id),
                                "execution_id": row.execution_id,
                            },
                            "skip_when_already_exists": True,
                            "wait_for_completion": False,
                        }
                    )
        session.flush()
    follow_ups.sort(key=lambda item: (item["trigger_dag_id"], item["trigger_run_id"]))
    return {
        "checked": len(candidates),
        "changed": changed,
        "errors": errors,
        "follow_up_bots": follow_ups,
    }


def prune_reports(session: Session) -> int:
    result = session.execute(delete(RunReport).where(RunReport.expires_at < utcnow()))
    return result.rowcount or 0


def recover_failed_dispatches(session: Session, limit: int = 100) -> int:
    """Surface exhausted Airflow failures, including failures after execution claim."""
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

    expected_dag = case(
        (Execution.admission_kind == "pr_reviewer", "bot__pr_reviewer"),
        else_="bot__task_executor",
    )
    candidates = session.scalars(
        select(Execution.id)
        .join(DagRun, and_(DagRun.dag_id == expected_dag, DagRun.run_id == Execution.target_run_id))
        .where(
            Execution.terminal_at.is_(None),
            Execution.dispatch_state.in_(["pending", "leased", "running", "reviewing"]), DagRun.state == "failed",
        )
        .order_by(Execution.id).limit(max(0, min(limit, 100)))
    ).all()
    recovered = 0
    terminal_task_states = ["success", "failed", "skipped", "upstream_failed", "removed"]
    for identity in candidates:
        row = session.scalar(select(Execution).where(Execution.id == identity).with_for_update())
        if row is None or row.terminal_at is not None or row.dispatch_state not in {"pending", "leased", "running", "reviewing"}:
            continue
        dag_id = "bot__pr_reviewer" if row.admission_kind == "pr_reviewer" else "bot__task_executor"
        # A cleared or retrying run is live again. Lock and reread the DAG run so
        # maintenance cannot classify a stale terminal observation as a failure.
        run_state = session.scalar(select(DagRun.state).where(DagRun.dag_id == dag_id, DagRun.run_id == row.target_run_id).with_for_update())
        if run_state != "failed":
            continue
        live_task = session.scalar(select(TaskInstance.task_id).where(
            TaskInstance.dag_id == dag_id, TaskInstance.run_id == row.target_run_id,
            or_(TaskInstance.state.is_(None), TaskInstance.state.not_in(terminal_task_states)),
        ).limit(1))
        reported = session.scalar(select(RunReport.id).where(RunReport.dag_id == dag_id, RunReport.run_id == row.target_run_id, RunReport.outcome == "succeeded").limit(1))
        if live_task is not None or reported is not None:
            continue
        task = session.scalar(select(Task).where(Task.id == row.task_id).with_for_update())
        failure_report = session.scalar(select(RunReport).where(RunReport.dag_id == dag_id, RunReport.run_id == row.target_run_id)
                                        .order_by(RunReport.try_number.desc()).limit(1))
        code = failure_report.reason_code if failure_report else "report_missing"
        detail = failure_report.failure_detail if failure_report and failure_report.failure_detail else (
            (f"Airflow run {dag_id}/{row.target_run_id} exhausted retries ({code}). " if failure_report else f"Airflow run {dag_id}/{row.target_run_id} failed without a final execution report being saved. ") +
            "Inspect that run's task logs, fix the launch or control-plane error, then unblock the task and start a new execution."
        )
        if code == "model_rate_limited":
            detail = "The selected model remained rate limited after bounded retries. Change the affected bot’s model in Models & connections, then retry this task. Existing PR and review evidence are preserved."
        _record_terminal(row, code, detail)
        row.terminal_failure_class = (failure_report.failure_class if failure_report else None) or ("ModelRateLimited" if code == "model_rate_limited" else "DispatchFailure")
        row.stage = "terminal"
        row.dispatch_state = "terminal"
        row.lease_expires_at = None
        if task is not None and task.state not in {"completed", "dismissed"}:
            previous = task.state
            if previous != "blocked":
                task.blocked_from_state = previous
            task.state = "blocked"
            task.version += 1
            task.updated_at = utcnow()
            _event(session, task, "execution_dispatch_failed", "system", row.target_run_id,
                   from_state=previous, to_state="blocked", payload={"code": code, "dag_id": dag_id, "run_id": row.target_run_id, "sequence": row.sequence, "detail": detail})
        recovered += 1
    session.flush()
    return recovered


def prune_artifacts(session: Session) -> int:
    protected_owners = select(Execution.execution_id).where(
        or_(
            Execution.terminal_at.is_(None),
            and_(Execution.pr_number.is_not(None), Execution.merged_at.is_(None)),
        )
    )
    rows = session.scalars(
        select(Artifact).where(
            Artifact.expires_at < utcnow(),
            or_(
                Artifact.owner_execution_id.is_(None),
                Artifact.owner_execution_id.not_in(protected_owners),
            ),
        )
    ).all()
    removed = 0
    from .artifacts import delete_artifact_file

    for row in rows:
        delete_artifact_file(row.relative_path)
        session.delete(row)
        removed += 1
    return removed


def run_maintenance(session: Session, *, limit: int = 100) -> dict:
    from . import planning
    dispatch_failures = recover_failed_dispatches(session, limit=limit)
    leases = expire_leases(session)
    reports = prune_reports(session)
    artifacts = prune_artifacts(session)
    sync = {"checked": 0, "changed": 0, "errors": 0, "follow_up_bots": []}
    if conf.getboolean("bot_dashboard", "executor_enabled", fallback=False):
        try:
            sync = sync_provider(session, limit=limit)
        except GitProviderError:
            sync["errors"] += 1
    stats.incr("bot_dashboard.maintenance.expired_leases", leases)
    stats.incr("bot_dashboard.maintenance.failed_dispatches", dispatch_failures)
    stats.incr("bot_dashboard.maintenance.pruned_reports", reports)
    stats.incr("bot_dashboard.maintenance.pruned_artifacts", artifacts)
    stats.incr("bot_dashboard.provider.checked", sync["checked"])
    stats.incr("bot_dashboard.provider.changed", sync["changed"])
    stats.incr("bot_dashboard.provider.errors", sync["errors"])
    log.info(
        "bot_dashboard_maintenance expired_leases=%d pruned_reports=%d "
        "pruned_artifacts=%d provider_checked=%d provider_changed=%d provider_errors=%d",
        leases,
        reports,
        artifacts,
        sync["checked"],
        sync["changed"],
        sync["errors"],
    )
    return {
        "failed_dispatches": dispatch_failures,
        "expired_leases": leases,
        "pruned_reports": reports,
        "pruned_artifacts": artifacts,
        "provider": sync,
        "follow_up_bots": sync["follow_up_bots"] + planning.pending(session, limit),
    }
