"""Trusted, idempotent publication boundary for advisory PR review comments."""

from __future__ import annotations
import hashlib

from sqlalchemy import select
from sqlalchemy.orm import Session

from .git_provider import GitProviderError, get_provider
from .models import Execution, RunReport
from .report_schemas import validate_named_report


def publish_review_comments(session: Session, *, dag_id: str, run_id: str) -> dict:
    """Upsert one stable-marker comment after exact immutable identity validation."""
    execution = session.scalar(
        select(Execution)
        .where(
            Execution.claimed_run_id == run_id,
            Execution.admission_kind == "pr_reviewer",
        )
        .with_for_update()
    )
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
    )
    if execution is None or execution.pr_number is None:
        raise GitProviderError("review admission has no immutable change identity")
    if report is None or report.body_json is None or report.outcome != "succeeded":
        raise GitProviderError("successful review projection is unavailable")
    body = validate_named_report("pr_reviewer_v2", report.body_json)
    if body["task_id"] != str(execution.task_id):
        raise GitProviderError("review report task identity does not match admission")
    identity = {
        "provider": execution.provider,
        "number": execution.pr_number,
        "branch": execution.branch,
        "base": execution.target_branch,
        "author": execution.service_account_id,
        "head": execution.trusted_head_sha,
    }
    provider = get_provider()
    observation = provider.read_change(identity["number"])
    if (
        observation["provider"] != identity["provider"]
        or observation["number"] != identity["number"]
        or observation["head_ref"] != identity["branch"]
        or observation["base_ref"] != identity["base"]
        or observation["author_id"] != identity["author"]
        or observation["head_sha"] != identity["head"]
    ):
        raise GitProviderError(
            "provider identity or trusted head changed before review publication"
        )
    lines = [
        f"Automated advisory review: **{body['verdict']}**",
        "",
        body["summary"],
    ]
    for item in body["comments"]:
        location = f"{item.get('path') or ''}:{item.get('line') or ''}".strip(":")
        lines.append(f"- {location + ': ' if location else ''}{item['body']}")
    if body["verification"]:
        lines.extend(["", "Verification observations:"])
        lines.extend(f"- {value}" for value in body["verification"])
    marker = f"<!-- bot-dashboard-review:{execution.execution_id}:r{execution.revision} -->"
    provider.upsert_comment(identity["number"], marker, "\n".join(lines))
    execution.review_comment_fingerprint = hashlib.sha256(marker.encode()).hexdigest()
    execution.review_commented_at = execution.review_commented_at or report.finished_at
    session.flush()
    return {"posted": 1, "verdict": body["verdict"], "marker": marker}
