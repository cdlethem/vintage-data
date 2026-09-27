"""Immutable starting work for a revised admission, retained in its audit event."""
from sqlalchemy import select

from .models import Event


_SEED_EVENT_TYPES = (
    "execution_admitted",
    "revision_seed_recovered",
    "merge_conflict_repair_admitted",
)


def get_seed_event(session, execution):
    return session.scalar(select(Event).where(
        Event.task_id == execution.task_id,
        Event.event_type.in_(_SEED_EVENT_TYPES),
        Event.payload["execution_id"].as_string() == execution.execution_id,
    ).order_by(Event.sequence.desc()).limit(1))


def get_seed(session, execution):
    event = get_seed_event(session, execution)
    return event.payload.get("revision_seed") if event else None


def find_seed_lineage(session, execution):
    """Find the latest audited seed leading to an execution, including bare retries."""
    direct = get_seed_event(session, execution)
    if direct and isinstance(direct.payload.get("revision_seed"), dict):
        return direct, direct.payload["revision_seed"]
    cutoff = direct.sequence if direct else None
    events = session.scalars(select(Event).where(
        Event.task_id == execution.task_id,
        Event.event_type.in_(_SEED_EVENT_TYPES),
    ).order_by(Event.sequence.desc())).all()
    for event in events:
        if cutoff is not None and event.sequence >= cutoff:
            continue
        seed = event.payload.get("revision_seed")
        if isinstance(seed, dict) and seed.get("execution_id"):
            return event, seed
    return None, None


def validate_seed_artifacts(session, seed):
    """Return a seed only after its executable immutable artifacts are readable."""
    from .artifacts import read_artifact
    from .service import PreconditionFailed

    required = (
        "base_sha", "source_artifact_sha256", "patch_sha256", "provider",
        "repository", "target_branch", "revision", "execution_id", "pr_number",
        "trusted_head_sha",
    )
    if not isinstance(seed, dict) or any(not seed.get(key) for key in required):
        raise PreconditionFailed("immutable revision seed is incomplete")
    read_artifact(session, seed["source_artifact_sha256"])
    read_artifact(session, seed["patch_sha256"])
    return seed


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
            "repository", "target_branch", "revision", "execution_id", "branch",
            "pr_number", "pr_url", "trusted_head_sha", "verification_manifest",
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
