"""Bounded public API request models."""
from __future__ import annotations

import re
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from .report_schemas import ExecutorAdmissionV2, ReviewerAdmissionV2, RunEnvelopeV1, UsageV1

CONTROL = re.compile(r"[\x00-\x1f\x7f]")

class StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

class Evidence(StrictBody):
    label: str = Field(min_length=1, max_length=200)
    url: str | None = Field(default=None, max_length=2048)
    observation: str | None = Field(default=None, max_length=20_000)

    @field_validator("url")
    @classmethod
    def safe_url(cls, value: str | None) -> str | None:
        if value is None: return None
        if CONTROL.search(value) or value.startswith("//"): raise ValueError("unsafe URL")
        parsed = urlsplit(value)
        if parsed.scheme:
            if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment: raise ValueError("only canonical HTTPS URLs are allowed")
        elif not value.startswith("/") or parsed.netloc: raise ValueError("relative URLs must be same-origin absolute paths")
        return value

class CreateTask(StrictBody):
    title: str = Field(min_length=1, max_length=200)
    category: Literal["reliability", "new_source", "cadence", "load", "storage", "architecture", "credential", "cost", "other"]
    priority: int = Field(ge=1)
    planned_resolution: str = Field(min_length=1, max_length=20_000)
    evidence: list[Evidence] = Field(default_factory=list, max_length=50)

class PatchTask(StrictBody):
    version: int = Field(ge=1)
    planned_resolution: str | None = Field(default=None, max_length=20_000)
    title: str | None = Field(default=None, min_length=1, max_length=200)
    category: Literal["reliability", "new_source", "cadence", "load", "storage", "architecture", "credential", "cost", "other"] | None = None
    priority: int | None = Field(default=None, ge=1)
    verification_commands: list[list[str]] | None = Field(default=None, max_length=20)
    allowed_path_globs: list[str] | None = Field(default=None, max_length=50)
    resource_keys: list[str] | None = Field(default=None, max_length=50)
    follow_up_bots: list[str] | None = Field(default=None, max_length=10)
    reviewer_required: bool | None = None

    @model_validator(mode="after")
    def validate_execution_policy(self):
        if self.verification_commands is not None:
            for argv in self.verification_commands:
                if (
                    not argv
                    or len(argv) > 32
                    or any(not value or len(value) > 1024 or "\x00" in value for value in argv)
                ):
                    raise ValueError("verification commands require bounded argv")
        for values, label in (
            (self.allowed_path_globs, "allowed_path_globs"),
            (self.resource_keys, "resource_keys"),
            (self.follow_up_bots, "follow_up_bots"),
        ):
            if values is not None and (
                len(values) != len(set(values))
                or any(not value or len(value) > 1024 or "\x00" in value for value in values)
            ):
                raise ValueError(f"{label} entries must be bounded and unique")
        if self.allowed_path_globs is not None and any(
            value.startswith(("/", "../")) or "\\" in value or ".." in value.split("/")
            for value in self.allowed_path_globs
        ):
            raise ValueError("allowed path globs must remain repository-relative")
        allowed_follow_ups = {
            "source_vetting",
            "source_scheduling",
            "analytics_engineer",
            "data_analyst",
        }
        if self.follow_up_bots is not None and not set(self.follow_up_bots) <= allowed_follow_ups:
            raise ValueError("follow_up_bots contains an unknown route")
        return self

class Transition(StrictBody):
    version: int = Field(ge=1)
    state: Literal["proposed", "accepted", "in_progress", "in_review", "ready", "blocked", "completed", "dismissed"]
    reason: str | None = Field(default=None, max_length=20_000)

class Assignment(StrictBody):
    version: int = Field(ge=1)
    kind: Literal["human", "bot"]
    profile: Literal["junior", "senior", "staff"] | None = None
    reviewer_required: bool = True

    @model_validator(mode="after")
    def coherent(self) -> "Assignment":
        if (self.kind == "bot") != (self.profile is not None): raise ValueError("bot assignments require a profile; human assignments cannot name one")
        return self

class Start(StrictBody):
    version: int = Field(ge=1)
    idempotency_key: str = Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")
    revision: bool = False

class CommentBody(StrictBody):
    version: int = Field(ge=1)
    comments: list[str] = Field(min_length=1, max_length=50)
    @field_validator("comments")
    @classmethod
    def bounded(cls, values: list[str]) -> list[str]:
        if any(not value or len(value) > 10_000 or CONTROL.search(value) for value in values): raise ValueError("invalid comment")
        return values

class EvidenceBody(StrictBody):
    version: int = Field(ge=1)
    evidence: list[Evidence] = Field(min_length=1, max_length=50)

class SyncRequest(StrictBody):
    version: int = Field(ge=1)
    idempotency_key: str = Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")

class PromotionPolicy(StrictBody):
    category: Literal["*", "reliability", "new_source", "cadence", "load", "storage", "architecture", "credential", "cost"]
    mode: Literal["manual", "auto_accept", "auto_delegate"]
    profile: Literal["junior", "senior", "staff"] | None = None
    reviewer_required: bool = True
    apply_task_id: str | None = None
    @model_validator(mode="after")
    def policy_profile(self) -> "PromotionPolicy":
        if (self.mode == "auto_delegate") != (self.profile is not None): raise ValueError("only auto_delegate accepts and requires a profile")
        return self


class InternalIdentity(StrictBody):
    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)
    task_id: str = Field(min_length=1, max_length=250)
    map_index: int = Field(ge=-1)


class BudgetClaimRequest(StrictBody):
    identity: InternalIdentity
    configured_total_seconds: int = Field(ge=60, le=86_400)


class BudgetClaimResponse(StrictBody):
    deadline_at: str
    configured_total_seconds: int


class RunReportRequest(StrictBody):
    envelope: RunEnvelopeV1


class RunProjectionResponse(StrictBody):
    report_projection_id: str
    report_sha256: str
    report_bytes: int
    outcome: str
    retry_class: str
    failure_fingerprint: str | None
    execution: dict[str, Any]


class RunQueryRequest(StrictBody):
    bots: list[str] = Field(min_length=1, max_length=20)
    days: int = Field(default=7, ge=1, le=3650)
    outcomes: list[
        Literal["succeeded", "skipped", "capacity_unavailable", "timed_out", "failed"]
    ] = Field(default_factory=list, max_length=5)
    limit: int = Field(default=100, ge=1, le=200)


class ManagerReconcileRequest(StrictBody):
    identity: InternalIdentity
    try_number: int = Field(ge=1)


class ExecutionClaimRequest(StrictBody):
    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)
    conf: dict[str, Any]
    kind: Literal["executor", "pr_reviewer"]
    deadline_at: str


class ArtifactPutRequest(StrictBody):
    kind: Literal["source", "patch", "executor_report", "review_report", "manifest"]
    content_base64: str = Field(min_length=1, max_length=1_398_104)
    owner_execution_id: str | None = Field(default=None, max_length=64)


class ExecutionPublishRequest(StrictBody):
    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)
    executor_result: dict[str, Any]


class ExecutionFinalizeRequest(StrictBody):
    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)
    kind: Literal["executor", "pr_reviewer"]
    result: dict[str, Any]


class DispatchClaimRequest(StrictBody):
    limit: int = Field(default=20, ge=1, le=100)


class MaintenanceRequest(StrictBody):
    limit: int = Field(default=100, ge=1, le=500)



class UsageCounters(StrictBody):
    requests: int = Field(ge=0)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    cached_input_tokens: int = Field(ge=0)
    cache_write_tokens: int = Field(ge=0)
    reasoning_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)
    cost_micro_usd: int = Field(ge=0)
    runs: int = Field(ge=0)
    priced_runs: int = Field(ge=0)
    unpriced_runs: int = Field(ge=0)


class UsageTotals(UsageCounters):
    pass


class UsageByModel(UsageCounters):
    model: str


class UsageByBot(UsageCounters):
    bot: str


class UsageByDay(UsageCounters):
    date: str


class UsageCapResponse(StrictBody):
    daily_spend_cap_micro_usd: int | None = Field(default=None, ge=0)
    spent_today_micro_usd: int = Field(ge=0)
    exceeded: bool


class UsageSummaryResponse(StrictBody):
    days: int = Field(ge=1, le=400)
    currency: Literal["USD"]
    generated_at: str
    totals: UsageTotals
    by_model: list[UsageByModel] = Field(max_length=40)
    by_bot: list[UsageByBot] = Field(max_length=40)
    by_day: list[UsageByDay] = Field(max_length=400)
    cap: UsageCapResponse


class RunReportItem(StrictBody):
    usage_total: UsageV1 | None = None
    report_id: str
    bot: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    try_number: int
    outcome: str
    retry_class: str
    reason_code: str
    selected_model: str | None
    started_at: str
    finished_at: str
    deadline_at: str
    duration_ms: int
    context: dict[str, Any]
    attempts: list[dict[str, Any]]
    requests: int | None = Field(default=None, ge=0)
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    cached_input_tokens: int | None = Field(default=None, ge=0)
    cache_write_tokens: int | None = Field(default=None, ge=0)
    reasoning_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    cost_micro_usd: int | None = Field(default=None, ge=0)
    cost_source: Literal["provider_reported", "price_book", "unavailable"] | None = None
    pricing_id: str | None = None
    failure: dict[str, Any] | None
    payload_schema: str | None
    sha256: str
    byte_count: int
    payload: dict[str, Any] | None = None

class RunQueryResponse(StrictBody):
    items: list[RunReportItem]


class ManagerContextResponse(StrictBody):
    context_schema_version: Literal[1]
    context_kind: Literal["manager"]
    generated_at: str
    as_of: str
    specialists: list[dict[str, Any]]
    stale_agents: list[str]
    missing_agents: list[str]
    failed_agents: list[str]
    freshness_ok: bool
    bot_health: list[dict[str, Any]]
    backlog: list[dict[str, Any]]


class ReconcileResponse(StrictBody):
    status: Literal["ok"]
    created: int
    revised: int
    resurfaced: int


class AirflowFailure(StrictBody):
    dag_id: str
    run_id: str
    task_id: str
    map_index: int = Field(ge=-1)
    try_number: int = Field(ge=1)
    started_at: str | None
    ended_at: str | None
    origin: Literal["airflow"]
    component: str
    error_class: str
    error_code: str
    detail: str = Field(max_length=3500)


class AirflowFailuresResponse(StrictBody):
    items: list[AirflowFailure]
    remaining_after_batch: int = Field(ge=0)


class ArtifactResponse(StrictBody):
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    kind: str
    media_type: str
    byte_count: int
    owner_execution_id: str | None
    created_at: str
    expires_at: str



class PublishResponse(StrictBody):
    status: Literal["published", "no_change"]
    provider: str | None = None
    repository: str | None = None
    branch: str | None = None
    pr_number: int | None = None
    pr_url: str | None = None
    trusted_head_sha: str | None = None


class FinalizeResponse(StrictBody):
    status: Literal["ok", "unchanged"]
    code: str


class TriggerConf(StrictBody):
    execution_id: str
    revision: int


class FollowUpTriggerConf(StrictBody):
    task_id: str
    execution_id: str


class TriggerArguments(StrictBody):
    trigger_dag_id: str
    trigger_run_id: str
    conf: TriggerConf | FollowUpTriggerConf
    skip_when_already_exists: Literal[True]
    wait_for_completion: Literal[False]

class DispatchResponse(StrictBody):
    items: list[TriggerArguments]


class ProviderSyncResponse(StrictBody):
    checked: int
    changed: int
    errors: int
    follow_up_bots: list[TriggerArguments]


class MaintenanceResponse(StrictBody):
    expired_leases: int
    pruned_reports: int
    pruned_artifacts: int
    provider: ProviderSyncResponse
    follow_up_bots: list[TriggerArguments]


ExecutionClaimResponse = ExecutorAdmissionV2 | ReviewerAdmissionV2
