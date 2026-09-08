"""Bounded, redacted universal run-envelope persistence."""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta

from airflow._shared.secrets_masker import redact, should_hide_value_for_key
from airflow.configuration import conf
from airflow.sdk.observability import stats
from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import RunBudgetClaim, RunReport, utcnow
from .report_schemas import validate_run_envelope

_USAGE_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cached_input_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "total_tokens",
    "requests",
)


def _usage_rollup(envelope: dict, attempts: list[dict]) -> dict | None:
    explicit = envelope.get("usage_total")
    if explicit is not None:
        return explicit
    usages = [attempt.get("usage") for attempt in attempts if attempt.get("usage") is not None]
    if not usages:
        return None
    rollup = {
        "schema": "usage.v1",
        **{field: sum(int(item[field]) for item in usages) for field in _USAGE_FIELDS},
        "cost_micro_usd": (
            sum(int(item["cost_micro_usd"]) for item in usages)
            if all(item.get("cost_micro_usd") is not None for item in usages)
            else None
        ),
        "cost_source": (
            usages[0]["cost_source"]
            if all(item["cost_source"] == usages[0]["cost_source"] for item in usages)
            else "unavailable"
        ),
        "pricing_id": (
            usages[0].get("pricing_id")
            if all(item.get("pricing_id") == usages[0].get("pricing_id") for item in usages)
            else None
        ),
    }
    if rollup["cost_micro_usd"] is None:
        rollup["cost_source"] = "unavailable"
    return rollup

_CREDENTIAL_URL = re.compile(r"(?i)\bhttps?://[^\s/@:]+:[^\s/@]+@")


def _sanitize(
    value: object,
    *,
    depth: int = 0,
    count: list[int] | None = None,
    key: str | None = None,
) -> object:
    if depth > 20:
        raise ValueError("report exceeds maximum nesting depth")
    count = count or [0]
    count[0] += 1
    if count[0] > 10_000:
        raise ValueError("report exceeds maximum item count")
    if isinstance(value, str):
        # Key-name masking applies to strings only: a number carries no secret,
        # and Airflow's masker treats any key containing "token" as sensitive,
        # which would otherwise destroy the envelope's token counters.
        if key and should_hide_value_for_key(key):
            return "***"
        if len(value) > 20_000:
            raise ValueError("report string exceeds maximum length")
        return str(redact(_CREDENTIAL_URL.sub("https://***@", value), key, max_depth=20))
    if isinstance(value, list):
        return [_sanitize(item, depth=depth + 1, count=count) for item in value]
    if isinstance(value, dict):
        return {
            str(name): _sanitize(
                item, depth=depth + 1, count=count, key=str(name)
            )
            for name, item in value.items()
        }
    if value is None or isinstance(value, (bool, int, float)):
        return value
    raise ValueError("report contains an unsupported value")


def persist_run_envelope(session: Session, value: object) -> dict:
    """Persist one Airflow try; identical replay succeeds, changed replay fails."""
    maximum = conf.getint("bot_dashboard", "report_max_bytes", fallback=262_144)
    envelope = validate_run_envelope(value, max_bytes=maximum)
    envelope = _sanitize(envelope)
    canonical = json.dumps(
        envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    if len(canonical) > maximum:
        raise ValueError("run envelope exceeds configured byte limit")
    digest = hashlib.sha256(canonical).hexdigest()
    identity = envelope["identity"]
    budget_claim = session.scalar(
        select(RunBudgetClaim).where(
            RunBudgetClaim.dag_id == identity["dag_id"],
            RunBudgetClaim.run_id == identity["run_id"],
            RunBudgetClaim.task_id == identity["task_id"],
            RunBudgetClaim.map_index == identity["map_index"],
        )
    )
    row = session.scalar(
        select(RunReport)
        .where(
            RunReport.dag_id == identity["dag_id"],
            RunReport.run_id == identity["run_id"],
            RunReport.task_id == identity["task_id"],
            RunReport.map_index == identity["map_index"],
            RunReport.try_number == identity["try_number"],
        )
        .with_for_update()
    )
    if row is not None:
        if row.sha256 != digest:
            raise ValueError("run report identity already contains different output")
        return _projection(row)

    attempts = envelope["attempts"]
    usage = _usage_rollup(envelope, attempts)
    legacy_input = sum(item.get("input_tokens") or 0 for item in attempts) or None
    legacy_output = sum(item.get("output_tokens") or 0 for item in attempts) or None
    legacy_total = sum(item.get("total_tokens") or 0 for item in attempts) or None

    outcome = envelope["outcome"]
    retention_days = 3650 if outcome == "succeeded" else 365 if outcome in {"failed", "timed_out"} else 7
    timing = envelope["timing"]
    started_at = datetime.fromisoformat(timing["started_at"])
    finished_at = datetime.fromisoformat(timing["finished_at"])
    deadline_at = datetime.fromisoformat(timing["deadline_at"])
    context = envelope["context"]
    failure = envelope.get("failure")
    attempts = envelope["attempts"]
    row = RunReport(
        id=uuid.uuid4(),
        dag_id=identity["dag_id"],
        run_id=identity["run_id"],
        task_id=identity["task_id"],
        map_index=identity["map_index"],
        try_number=identity["try_number"],
        bot_name=identity["bot"],
        report_schema=envelope.get("payload_schema"),
        report_format="json",
        model=envelope.get("selected_model"),
        status=outcome,
        outcome=outcome,
        retry_class=envelope["retry_class"],
        reason_code=envelope["reason_code"],
        failure_class=failure.get("class") if failure else None,
        failure_code=failure.get("code") if failure else None,
        failure_fingerprint=failure.get("fingerprint") if failure else None,
        failure_detail=(failure.get("detail") or None) if failure else None,
        started_at=started_at,
        finished_at=finished_at,
        deadline_at=deadline_at,
        budget_claim_id=budget_claim.id if budget_claim else None,
        duration_ms=timing["duration_ms"],
        context_sha256=context["sha256"],
        context_byte_count=context["byte_count"],
        context_build_ms=context["build_ms"],
        attempts_json=attempts,
        input_tokens=usage["input_tokens"] if usage else legacy_input,
        output_tokens=usage["output_tokens"] if usage else legacy_output,
        total_tokens=usage["total_tokens"] if usage else legacy_total,
        cached_input_tokens=usage["cached_input_tokens"] if usage else None,
        cache_write_tokens=usage["cache_write_tokens"] if usage else None,
        reasoning_tokens=usage["reasoning_tokens"] if usage else None,
        model_requests=usage["requests"] if usage else None,
        cost_micro_usd=usage.get("cost_micro_usd") if usage else None,
        cost_source=usage.get("cost_source") if usage else None,
        pricing_id=usage.get("pricing_id") if usage else None,
        provider_duration_ms=sum(item["duration_ms"] for item in attempts) or None,
        deadline_consumed_ms=timing["duration_ms"],
        body_json=envelope.get("payload"),
        body_text=None,
        unavailable_code=None if outcome == "succeeded" else envelope["reason_code"],
        sha256=digest,
        byte_count=len(canonical),
        created_at=utcnow(),
        expires_at=utcnow() + timedelta(days=retention_days),
    )
    session.add(row)
    session.flush()
    if outcome == "succeeded":
        from .service import reconcile_recommendations

        reconcile_recommendations(session, row, envelope["payload"])
    stats.incr(
        "bot_dashboard.run.outcome",
        tags={
            "bot": identity["bot"],
            "outcome": outcome,
            "retry_class": envelope["retry_class"],
        },
    )
    stats.timing(
        "bot_dashboard.run.duration",
        timing["duration_ms"],
        tags={"bot": identity["bot"], "outcome": outcome},
    )
    stats.gauge(
        "bot_dashboard.run.context_bytes",
        context["byte_count"],
        tags={"bot": identity["bot"]},
    )
    metric_tags = {
        "bot": identity["bot"],
        "model": envelope.get("selected_model") or "unknown",
    }
    if usage is not None:
        for field in (
            "input_tokens",
            "output_tokens",
            "cached_input_tokens",
            "cache_write_tokens",
            "reasoning_tokens",
            "total_tokens",
            "requests",
        ):
            stats.incr(f"bot_dashboard.run.usage.{field}", usage[field], tags=metric_tags)
        if usage.get("cost_micro_usd") is not None:
            stats.gauge(
                "bot_dashboard.run.usage.cost_micro_usd",
                usage["cost_micro_usd"],
                tags=metric_tags,
            )
    return _projection(row)


def _projection(row: RunReport) -> dict:
    return {
        "report_projection_id": str(row.id),
        "report_sha256": row.sha256,
        "report_bytes": row.byte_count,
        "outcome": row.outcome,
        "retry_class": row.retry_class,
        "failure_fingerprint": row.failure_fingerprint,
        "execution": {
            "dag_id": row.dag_id,
            "run_id": row.run_id,
            "task_id": row.task_id,
            "map_index": row.map_index,
            "try_number": row.try_number,
        },
    }
