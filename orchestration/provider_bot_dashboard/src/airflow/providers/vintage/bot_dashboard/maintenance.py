"""Bounded provider observation, lease recovery, and authoritative retention."""
from __future__ import annotations
import hashlib
import json
import logging
from datetime import timedelta

import httpx

from airflow.configuration import conf
from airflow.sdk.observability import stats
from sqlalchemy import and_, delete, or_, select
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
        state = observation.get("state")
        if (
            task is not None
            and task.state == "completed"
            and state == "merged"
            and row.trusted_head_sha
            and observation.get("head_sha") == row.trusted_head_sha
        ):
            # A completed task must not retain a live execution for its
            # trusted merged publication; terminalize it so queue accounting
            # reflects the terminal task state.
            if not row.merged_at:
                row.merged_at = utcnow()
            row.stage = "terminal"
            row.dispatch_state = "terminal"
            row.terminal_reason_code = "merged"
            row.terminal_failure_class = "TerminalOutcome"
            row.terminal_detail = "provider change merged at the trusted publication head"
            row.terminal_at = utcnow()
            row.synced_at = utcnow()
            changed += 1
            _event(
                session,
                task,
                "execution_finalized",
                "system",
                str(number),
                from_state="completed",
                to_state="completed",
                payload={"code": "merged", "execution_id": row.execution_id},
            )
            session.flush()
            continue
        row.synced_at = utcnow()
        if row.provider_fingerprint == fingerprint:
            continue
        old = row.provider_state or {}
        row.provider_state = observation
        row.provider_fingerprint = fingerprint
        changed += 1
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
        "expired_leases": leases,
        "pruned_reports": reports,
        "pruned_artifacts": artifacts,
        "provider": sync,
        "follow_up_bots": sync["follow_up_bots"],
    }
