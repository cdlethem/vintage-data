"""Immutable starting work for a revised admission, retained in its audit event."""
from sqlalchemy import select

from .models import Event


def get_seed(session, execution):
    event = session.scalar(select(Event).where(
        Event.task_id == execution.task_id,
        Event.event_type.in_(["execution_admitted", "revision_seed_recovered"]),
        Event.payload["execution_id"].as_string() == execution.execution_id,
    ).order_by(Event.sequence.desc()).limit(1))
    return (event.payload.get("revision_seed") if event else None)


def capture_seed(session, execution):
    if not execution.patch_sha256:
        return get_seed(session, execution)
    from .artifacts import read_artifact
    from .service import PreconditionFailed
    if not execution.base_sha or not execution.source_artifact_sha256:
        raise PreconditionFailed("Revision requires the previous immutable source and patch")
    read_artifact(session, execution.source_artifact_sha256)
    read_artifact(session, execution.patch_sha256)
    return {key: getattr(execution, key) for key in (
        "base_sha", "source_artifact_sha256", "patch_sha256", "provider",
        "repository", "target_branch", "revision", "execution_id", "pr_number",
        "pr_url", "trusted_head_sha",
    )}


def restore_source(execution, seed):
    if seed:
        for key in ("base_sha", "source_artifact_sha256", "provider", "repository", "target_branch"):
            setattr(execution, key, seed[key])
