"""Provider-owned SQLAlchemy models; no Airflow core model is modified."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey, Index, Integer,
    JSON, MetaData, String, Text, UniqueConstraint, Uuid, text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s", "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s", "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(naming_convention=NAMING_CONVENTION)


BIGINT_PK = BigInteger().with_variant(Integer, "sqlite")
# A missing document must be SQL NULL, never a JSON `null`: "latest useful
# payload" and manifest presence checks are written as IS NOT NULL.
NULLABLE_JSON = JSON(none_as_null=True)
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    metadata = metadata


TASK_STATES = ("proposed", "accepted", "in_progress", "in_review", "ready", "blocked", "completed", "dismissed")
CATEGORIES = ("reliability", "new_source", "cadence", "load", "storage", "architecture", "credential", "cost", "other")


class Task(Base):
    __tablename__ = "bot_dashboard_task"
    __table_args__ = (
        CheckConstraint("source in ('manager','manual','specialist')", name="source"),
        CheckConstraint("state in ('proposed','accepted','in_progress','in_review','ready','blocked','completed','dismissed')", name="state"),
        CheckConstraint("category in ('reliability','new_source','cadence','load','storage','architecture','credential','cost','other')", name="category"),
        CheckConstraint("assignee_kind is null or assignee_kind in ('human','bot')", name="assignee_kind"),
        CheckConstraint("assignee_profile is null or assignee_profile in ('junior','senior','staff')", name="assignee_profile"),
        CheckConstraint("priority between 1 and 2147483647", name="priority"),
        Index("ix_bot_dashboard_task_queue", "owning_dag_id", "state", "priority", "updated_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    related_task_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, ForeignKey("bot_dashboard_task.id"))
    owning_dag_id: Mapped[str] = mapped_column(String(250), default="bot__manager", nullable=False)
    source: Mapped[str] = mapped_column(String(16), nullable=False)
    source_bot: Mapped[str | None] = mapped_column(String(100))
    recommendation_key: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    category: Mapped[str] = mapped_column(String(32), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    planned_resolution: Mapped[str] = mapped_column(Text, default="", nullable=False)
    assignee_kind: Mapped[str | None] = mapped_column(String(16))
    assignee_id: Mapped[str | None] = mapped_column(String(250))
    assignee_name: Mapped[str | None] = mapped_column(String(250))
    assignee_profile: Mapped[str | None] = mapped_column(String(16))
    reviewer_required: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    blocked_from_state: Mapped[str | None] = mapped_column(String(32))
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dismissed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    revisions: Mapped[list["Revision"]] = relationship(back_populates="task", order_by="Revision.revision_number")
    events: Mapped[list["Event"]] = relationship(back_populates="task", order_by="Event.sequence")


class Revision(Base):
    __tablename__ = "bot_dashboard_revision"
    __table_args__ = (
        UniqueConstraint(
            "task_id", "revision_number", name="uq_bd_revision_task_number"
        ),
        UniqueConstraint(
            "source_dag_id",
            "source_run_id",
            "source_task_id",
            "source_map_index",
            name="uq_bd_revision_source_identity",
        ),
    )
    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid, ForeignKey("bot_dashboard_task.id"), nullable=False, index=True)
    revision_number: Mapped[int] = mapped_column(Integer, nullable=False)
    report_schema: Mapped[str | None] = mapped_column(String(40))
    report_date: Mapped[str | None] = mapped_column(String(10))
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    action: Mapped[str] = mapped_column(Text, default="", nullable=False)
    why_now: Mapped[str] = mapped_column(Text, default="", nullable=False)
    expected_benefit: Mapped[str] = mapped_column(Text, default="", nullable=False)
    resources: Mapped[str] = mapped_column(Text, default="", nullable=False)
    risk: Mapped[str] = mapped_column(Text, default="", nullable=False)
    rollback: Mapped[str] = mapped_column(Text, default="", nullable=False)
    verification: Mapped[str] = mapped_column(Text, default="", nullable=False)
    suggested_executor: Mapped[str | None] = mapped_column(String(16))
    resurface_task_id: Mapped[uuid.UUID | None] = mapped_column(Uuid)
    resurface_reason: Mapped[str | None] = mapped_column(Text)
    source_dag_id: Mapped[str | None] = mapped_column(String(250))
    source_run_id: Mapped[str | None] = mapped_column(String(250))
    source_task_id: Mapped[str | None] = mapped_column(String(250))
    source_map_index: Mapped[int | None] = mapped_column(Integer)
    actor_id: Mapped[str | None] = mapped_column(String(250))
    evidence: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list, nullable=False)
    verification_commands: Mapped[list[list[str]]] = mapped_column(JSON, default=list, nullable=False)
    allowed_path_globs: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    resource_keys: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    follow_up_bots: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    task: Mapped[Task] = relationship(back_populates="revisions")


class Event(Base):
    __tablename__ = "bot_dashboard_event"
    __table_args__ = (
        UniqueConstraint("task_id", "sequence"),
        CheckConstraint("actor_kind in ('user','manager','executor','reviewer','provider','system')", name="actor_kind"),
    )
    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid, ForeignKey("bot_dashboard_task.id"), nullable=False, index=True)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(250), nullable=False)
    from_state: Mapped[str | None] = mapped_column(String(32))
    to_state: Mapped[str | None] = mapped_column(String(32))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    task: Mapped[Task] = relationship(back_populates="events")


class Execution(Base):
    __tablename__ = "bot_dashboard_execution"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "sequence",
            "revision",
            name="uq_bd_execution_task_sequence_revision",
        ),
        UniqueConstraint(
            "task_id",
            "idempotency_key",
            name="uq_bd_execution_task_idempotency",
        ),
        UniqueConstraint(
            "provider",
            "repository",
            "pr_number",
            name="uq_bd_execution_provider_pr",
        ),
        Index("ix_bot_dashboard_execution_dispatch", "dispatch_state", "lease_expires_at"),
        Index(
            "uq_bot_dashboard_execution_active_task",
            "task_id",
            unique=True,
            postgresql_where=text("terminal_at IS NULL"),
            sqlite_where=text("terminal_at IS NULL"),
        ),
    )
    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    execution_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid, ForeignKey("bot_dashboard_task.id"), nullable=False, index=True)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    admission_kind: Mapped[str] = mapped_column(String(24), default="executor", nullable=False)
    target_run_id: Mapped[str] = mapped_column(String(250), nullable=False)
    claimed_run_id: Mapped[str | None] = mapped_column(String(250))
    dispatch_state: Mapped[str] = mapped_column(String(24), default="pending", nullable=False)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stage: Mapped[str] = mapped_column(String(40), default="admitted", nullable=False)
    profile: Mapped[str] = mapped_column(String(16), nullable=False)
    reviewer_required: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    executor_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    review_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    base_sha: Mapped[str | None] = mapped_column(String(64))
    source_artifact_sha256: Mapped[str | None] = mapped_column(String(64))
    patch_sha256: Mapped[str | None] = mapped_column(String(64))
    patch_byte_count: Mapped[int | None] = mapped_column(Integer)
    verification_manifest: Mapped[dict[str, Any] | None] = mapped_column(NULLABLE_JSON)
    executor_report_sha256: Mapped[str | None] = mapped_column(String(64))
    review_report_sha256: Mapped[str | None] = mapped_column(String(64))
    review_verdict: Mapped[str | None] = mapped_column(String(32))
    review_comment_fingerprint: Mapped[str | None] = mapped_column(String(64))
    review_commented_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    merged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    branch: Mapped[str | None] = mapped_column(String(250))
    provider: Mapped[str | None] = mapped_column(String(16))
    repository: Mapped[str | None] = mapped_column(String(512))
    target_branch: Mapped[str | None] = mapped_column(String(250))
    pr_number: Mapped[int | None] = mapped_column(Integer)
    pr_url: Mapped[str | None] = mapped_column(String(2048))
    service_account_id: Mapped[str | None] = mapped_column(String(250))
    trusted_head_sha: Mapped[str | None] = mapped_column(String(64))
    provider_state: Mapped[dict[str, Any] | None] = mapped_column(NULLABLE_JSON)
    provider_fingerprint: Mapped[str | None] = mapped_column(String(64))
    synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    terminal_reason_code: Mapped[str | None] = mapped_column(String(100))
    terminal_failure_class: Mapped[str | None] = mapped_column(String(100))
    terminal_detail: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

class RunReport(Base):
    __tablename__ = "bot_dashboard_run_report"
    __table_args__ = (
        UniqueConstraint(
            "dag_id", "run_id", "task_id", "map_index", "try_number",
            name="uq_bd_report_task_try",
        ),
        Index("ix_bot_dashboard_report_bot_created", "bot_name", "created_at"),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    dag_id: Mapped[str] = mapped_column(String(250), nullable=False)
    run_id: Mapped[str] = mapped_column(String(250), nullable=False)
    task_id: Mapped[str] = mapped_column(String(250), nullable=False)
    map_index: Mapped[int] = mapped_column(Integer, default=-1, nullable=False)
    try_number: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    bot_name: Mapped[str] = mapped_column(String(100), nullable=False)
    airflow_state: Mapped[str | None] = mapped_column(String(32))
    report_schema: Mapped[str | None] = mapped_column(String(64))
    report_format: Mapped[str] = mapped_column(String(16), default="json", nullable=False)
    model: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    retry_class: Mapped[str] = mapped_column(String(16), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(100), nullable=False)
    failure_class: Mapped[str | None] = mapped_column(String(100))
    failure_code: Mapped[str | None] = mapped_column(String(100))
    failure_fingerprint: Mapped[str | None] = mapped_column(String(64))
    failure_detail: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    deadline_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    budget_claim_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid, ForeignKey("bot_dashboard_run_budget_claim.id")
    )
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    context_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    context_byte_count: Mapped[int] = mapped_column(Integer, nullable=False)
    context_build_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    attempts_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list, nullable=False)
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)
    total_tokens: Mapped[int | None] = mapped_column(Integer)
    cached_input_tokens: Mapped[int | None] = mapped_column(Integer)
    cache_write_tokens: Mapped[int | None] = mapped_column(Integer)
    reasoning_tokens: Mapped[int | None] = mapped_column(Integer)
    model_requests: Mapped[int | None] = mapped_column(Integer)
    cost_micro_usd: Mapped[int | None] = mapped_column(Integer)
    cost_source: Mapped[str | None] = mapped_column(String(32))
    pricing_id: Mapped[str | None] = mapped_column(String(200))
    provider_duration_ms: Mapped[int | None] = mapped_column(Integer)
    deadline_consumed_ms: Mapped[int | None] = mapped_column(Integer)
    body_json: Mapped[dict[str, Any] | None] = mapped_column(NULLABLE_JSON)
    body_text: Mapped[str | None] = mapped_column(Text)
    unavailable_code: Mapped[str | None] = mapped_column(String(64))
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    byte_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RunBudgetClaim(Base):
    __tablename__ = "bot_dashboard_run_budget_claim"
    __table_args__ = (
        UniqueConstraint(
            "dag_id", "run_id", "task_id", "map_index",
            name="uq_bd_budget_logical_task",
        ),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    dag_id: Mapped[str] = mapped_column(String(250), nullable=False)
    run_id: Mapped[str] = mapped_column(String(250), nullable=False)
    task_id: Mapped[str] = mapped_column(String(250), nullable=False)
    map_index: Mapped[int] = mapped_column(Integer, default=-1, nullable=False)
    configured_total_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    deadline_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

class Artifact(Base):
    __tablename__ = "bot_dashboard_artifact"
    sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    media_type: Mapped[str] = mapped_column(String(128), nullable=False)
    byte_count: Mapped[int] = mapped_column(Integer, nullable=False)
    relative_path: Mapped[str] = mapped_column(String(512), unique=True, nullable=False)
    owner_execution_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class LegacyImport(Base):
    __tablename__ = "bot_dashboard_legacy_import"
    __table_args__ = (
        UniqueConstraint("kind", "relative_path", name="uq_bd_legacy_kind_path"),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    relative_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    byte_count: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    task_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, ForeignKey("bot_dashboard_task.id", ondelete="SET NULL"))
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    imported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class Policy(Base):
    __tablename__ = "bot_dashboard_policy"
    __table_args__ = (CheckConstraint("mode in ('manual','auto_accept','auto_delegate')", name="mode"),)
    category: Mapped[str] = mapped_column(String(32), primary_key=True)
    mode: Mapped[str] = mapped_column(String(20), default="manual", nullable=False)
    profile: Mapped[str | None] = mapped_column(String(16))
    reviewer_required: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    updated_by: Mapped[str | None] = mapped_column(String(250))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
