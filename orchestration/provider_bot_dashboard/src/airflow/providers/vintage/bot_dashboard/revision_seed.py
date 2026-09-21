"""Immutable starting work for a revised admission, retained in its audit event."""
from sqlalchemy import select

from .models import Event


def get_seed(session, execution):
    event = session.scalar(select(Event).where(
        Event.task_id == execution.task_id,
        Event.event_type.in_([
            "execution_admitted",
            "revision_seed_recovered",
            "merge_conflict_repair_admitted",
        ]),
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
    report_digests = {}
    for key in ("executor_report_sha256", "review_report_sha256"):
        digest = getattr(execution, key)
        if digest:
            try:
                read_artifact(session, digest)
            except FileNotFoundError:
                # Reports are historical context, while source and patch are the
                # executable repair seed. A retention expiry must not turn an
                # otherwise reproducible conflict into a human blocker.
                digest = None
        report_digests[key] = digest
    return {
        **{key: getattr(execution, key) for key in (
            "base_sha", "source_artifact_sha256", "patch_sha256", "provider",
            "repository", "target_branch", "revision", "execution_id", "pr_number",
            "pr_url", "trusted_head_sha", "verification_manifest",
        )},
        **report_digests,
        "review_failure_kind": (execution.provider_state or {}).get("review_failure_kind"),
        "review_repair": (execution.provider_state or {}).get("review_repair"),
    }


def restore_source(execution, seed):
    if not seed:
        return
    if seed.get("repair_base_sha"):
        execution.base_sha = seed["repair_base_sha"]
        execution.provider = seed["provider"]
        execution.repository = seed["repository"]
        execution.target_branch = seed["target_branch"]
        return
    for key in ("base_sha", "source_artifact_sha256", "provider", "repository", "target_branch"):
        setattr(execution, key, seed[key])
