"""Strict run-envelope and model-output contracts shared by workers and provider."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


KEY_PATTERN = r"^[a-z0-9]+(?:-[a-z0-9]+)*$"
OUTCOMES = ("succeeded", "skipped", "capacity_unavailable", "timed_out", "failed")
TaskCategory = Literal[
    "reliability", "new_source", "cadence", "load", "storage",
    "architecture", "credential", "cost", "other",
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_max_length=20_000)
USAGE_COST_SOURCES = Literal["provider_reported", "price_book", "unavailable"]


class UsageV1(StrictModel):
    # The wire key stays "schema"; the attribute is renamed so it does not
    # shadow pydantic's own BaseModel.schema member.
    model_config = ConfigDict(extra="forbid", str_max_length=20_000, populate_by_name=True)

    usage_schema: Literal["usage.v1"] = Field(alias="schema", serialization_alias="schema")
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    cached_input_tokens: int = Field(ge=0)
    cache_write_tokens: int = Field(ge=0)
    reasoning_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)
    requests: int = Field(ge=0)
    cost_micro_usd: int | None = Field(default=None, ge=0)
    cost_source: USAGE_COST_SOURCES
    pricing_id: str | None = Field(default=None, max_length=200)

    @model_validator(mode="after")
    def reasoning_is_output_subset(self) -> "UsageV1":
        if self.reasoning_tokens > self.output_tokens:
            raise ValueError("reasoning tokens cannot exceed output tokens")
        return self


class RunIdentityV1(StrictModel):
    bot: str = Field(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_]*$")
    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)
    task_id: str = Field(min_length=1, max_length=250)
    map_index: int = Field(ge=-1)
    try_number: int = Field(ge=1)


class RunTimingV1(StrictModel):
    started_at: datetime
    finished_at: datetime
    deadline_at: datetime
    duration_ms: int = Field(ge=0)

    @model_validator(mode="after")
    def ordered(self) -> "RunTimingV1":
        if self.finished_at < self.started_at:
            raise ValueError("finished_at precedes started_at")
        if self.duration_ms > 31 * 24 * 60 * 60 * 1000:
            raise ValueError("duration exceeds workflow bound")
        return self


class RunFailureV1(StrictModel):
    class_: str = Field(alias="class", min_length=1, max_length=100)
    code: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9_]+$")
    fingerprint: str = Field(min_length=64, max_length=64, pattern=r"^[a-f0-9]{64}$")
    detail: str = Field(max_length=8192)


class ModelAttemptV1(StrictModel):
    ordinal: int = Field(ge=1, le=20)
    alias: str = Field(min_length=1, max_length=100)
    provider: str = Field(min_length=1, max_length=64)
    started_at: datetime
    finished_at: datetime
    duration_ms: int = Field(ge=0)
    outcome: Literal["succeeded", "capacity_unavailable", "timed_out", "failed"]
    reason_code: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9_]+$")
    fallback_used: bool
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    usage: UsageV1 | None = None

    @model_validator(mode="after")
    def usage_matches_counters(self) -> "ModelAttemptV1":
        if self.usage is not None:
            expected = (
                self.usage.input_tokens,
                self.usage.output_tokens,
                self.usage.total_tokens,
            )
            actual = (self.input_tokens, self.output_tokens, self.total_tokens)
            if any(value is not None and value != wanted for value, wanted in zip(actual, expected)):
                raise ValueError("attempt token counters must equal usage")
            self.input_tokens, self.output_tokens, self.total_tokens = expected
        return self




class ContextDigestV1(StrictModel):
    sha256: str = Field(min_length=64, max_length=64, pattern=r"^[a-f0-9]{64}$")
    byte_count: int = Field(ge=0, le=1_048_576)
    build_ms: int = Field(ge=0)


class RunEnvelopeV1(StrictModel):
    envelope_version: Literal[1]
    identity: RunIdentityV1
    timing: RunTimingV1
    outcome: Literal["succeeded", "skipped", "capacity_unavailable", "timed_out", "failed"]
    retry_class: Literal["none", "capacity", "transient", "terminal"]
    reason_code: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9_]+$")
    failure: RunFailureV1 | None
    selected_model: str | None = Field(default=None, max_length=100)
    attempts: list[ModelAttemptV1] = Field(default_factory=list, max_length=20)
    usage_total: UsageV1 | None = None
    context: ContextDigestV1
    payload_schema: str | None = Field(default=None, max_length=64)
    payload: dict[str, Any] | None

    @model_validator(mode="after")
    def outcome_contract(self) -> "RunEnvelopeV1":
        if self.outcome == "succeeded":
            if self.payload is None or not self.payload_schema:
                raise ValueError("successful envelope requires typed payload")
            validate_named_report(self.payload_schema, self.payload)
        elif self.payload is not None or self.payload_schema is not None:
            raise ValueError("non-successful envelope must not contain bot payload")
        if self.outcome in {"timed_out", "failed"} and self.failure is None:
            raise ValueError("failed or timed-out envelope requires failure metadata")
        if self.outcome in {"succeeded", "skipped", "capacity_unavailable"} and self.failure is not None:
            raise ValueError("green or capacity envelope cannot contain failure metadata")
        if self.retry_class == "none" and self.outcome in {"timed_out", "failed"}:
            raise ValueError("failed outcome requires a classified retry decision")
        return self


class EvidenceReferenceV1(StrictModel):
    kind: str = Field(min_length=1, max_length=40)
    reference: str = Field(min_length=1, max_length=500)
    summary: str = Field(min_length=1, max_length=2000)


class TaskProposalV1(StrictModel):
    recommendation_key: str = Field(min_length=1, max_length=80, pattern=KEY_PATTERN)
    title: str = Field(min_length=1, max_length=200)
    category: TaskCategory
    priority: int = Field(ge=1, le=100)
    planned_resolution: str = Field(min_length=1, max_length=20_000)
    why_now: str = Field(min_length=1, max_length=20_000)
    expected_benefit: str = Field(min_length=1, max_length=20_000)
    risk: str = Field(min_length=1, max_length=20_000)
    rollback: str = Field(min_length=1, max_length=20_000)
    verification_commands: list[list[str]] = Field(min_length=1, max_length=20)
    allowed_path_globs: list[str] = Field(min_length=1, max_length=50)
    resource_keys: list[str] = Field(min_length=1, max_length=50)
    follow_up_bots: list[
        Literal["source_vetting", "source_scheduling", "analytics_engineer", "data_analyst"]
    ] = Field(default_factory=list, max_length=4)
    suggested_executor: Literal["junior", "senior", "staff"]
    reviewer_required: Literal[True]
    evidence: list[EvidenceReferenceV1] = Field(min_length=1, max_length=30)

    @model_validator(mode="after")
    def safe_commands(self) -> "TaskProposalV1":
        for command in self.verification_commands:
            if not command or any(not arg or len(arg) > 1024 for arg in command):
                raise ValueError("verification commands require bounded argv entries")
        if len(set(self.allowed_path_globs)) != len(self.allowed_path_globs):
            raise ValueError("allowed_path_globs must be unique")
        if len(set(self.resource_keys)) != len(self.resource_keys):
            raise ValueError("resource_keys must be unique")
        return self


class SourceProposalV2(StrictModel):
    slug: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9_]+$")
    title: str = Field(min_length=1, max_length=200)
    domain: str = Field(min_length=1, max_length=253)
    url: HttpUrl
    viability: Literal["high", "medium", "low"]
    evidence: list[EvidenceReferenceV1] = Field(min_length=1, max_length=20)
    summary: str = Field(min_length=1, max_length=5000)


class SourceDiscoveryV2(StrictModel):
    schema_version: Literal[2]
    agent: Literal["source_discovery"]
    status: Literal["ok", "degraded_evidence"]
    searches: list[str] = Field(max_length=50)
    proposals: list[SourceProposalV2] = Field(max_length=2)
    dismissed: list[str] = Field(max_length=50)
    user_escalations: list[str] = Field(max_length=20)
    summary: str = Field(min_length=1, max_length=20_000)


class SourceVettingDecisionV2(StrictModel):
    slug: str = Field(min_length=1, max_length=100)
    decision: Literal["recommended", "rejected", "blocked_user_token", "needs_research"]
    reason: str = Field(min_length=1, max_length=20_000)
    task_proposal: TaskProposalV1 | None = None

    @model_validator(mode="after")
    def proposal_matches_decision(self) -> "SourceVettingDecisionV2":
        if (self.decision == "recommended") != (self.task_proposal is not None):
            raise ValueError("recommended decision requires exactly one task proposal")
        return self


class SourceVettingV2(StrictModel):
    schema_version: Literal[2]
    agent: Literal["source_vetting"]
    status: Literal["ok", "degraded_evidence"]
    decisions: list[SourceVettingDecisionV2] = Field(min_length=1, max_length=1)
    summary: str = Field(min_length=1, max_length=20_000)


class ImplementationPlanV2(StrictModel):
    item: str = Field(min_length=1, max_length=200)
    steps: list[str] = Field(min_length=1, max_length=20)
    task_proposal: TaskProposalV1


class SourceSchedulingV2(StrictModel):
    schema_version: Literal[2]
    agent: Literal["source_scheduling"]
    status: Literal["ok", "degraded_evidence"]
    plans: list[ImplementationPlanV2] = Field(min_length=1, max_length=1)
    summary: str = Field(min_length=1, max_length=20_000)


class CadenceSourceReviewV2(StrictModel):
    source: str = Field(min_length=1, max_length=100)
    decision: Literal["keep", "adjust", "observe", "human_review"]
    reason: str = Field(min_length=1, max_length=5000)


class CadenceReviewV2(StrictModel):
    schema_version: Literal[2]
    agent: Literal["cadence_review"]
    status: Literal["ok", "degraded_evidence"]
    source_reviews: list[CadenceSourceReviewV2] = Field(max_length=100)
    issues: list[str] = Field(max_length=50)
    task_proposals: list[TaskProposalV1] = Field(max_length=50)
    summary: str = Field(min_length=1, max_length=20_000)


class FailureActionV2(StrictModel):
    fingerprint: str = Field(min_length=64, max_length=64, pattern=r"^[a-f0-9]{64}$")
    action: Literal["task_proposed", "observe", "human_review"]
    reason: str = Field(min_length=1, max_length=5000)
    task_proposal: TaskProposalV1 | None = None

    @model_validator(mode="after")
    def proposal_matches_action(self) -> "FailureActionV2":
        if (self.action == "task_proposed") != (self.task_proposal is not None):
            raise ValueError("task_proposed action requires exactly one task proposal")
        return self


class FailureTriageV2(StrictModel):
    schema_version: Literal[2]
    agent: Literal["failure_triage"]
    status: Literal["ok", "degraded_evidence"]
    failures: list[FailureActionV2] = Field(max_length=10)
    remaining_unreviewed: int = Field(ge=0)
    summary: str = Field(min_length=1, max_length=20_000)


class AnalyticsPlanV2(StrictModel):
    source: str = Field(min_length=1, max_length=100)
    steps: list[str] = Field(min_length=1, max_length=20)
    task_proposal: TaskProposalV1


class AnalyticsEngineerV2(StrictModel):
    schema_version: Literal[2]
    agent: Literal["analytics_engineer"]
    status: Literal["ok", "degraded_evidence"]
    datasets: list[str] = Field(max_length=20)
    decisions: list[str] = Field(max_length=50)
    plans: list[AnalyticsPlanV2] = Field(min_length=1, max_length=1)
    summary: str = Field(min_length=1, max_length=20_000)


class DimensionProfileV1(StrictModel):
    """A dimension the analyst actually measured, not one it guessed at."""
    column: str = Field(min_length=1, max_length=200)
    distinct_values: int = Field(ge=0)
    null_share: float = Field(ge=0, le=1)
    top_values: list[str] = Field(max_length=10)


class TrendProposalV1(StrictModel):
    slug: str = Field(min_length=1, max_length=60, pattern=r"^[a-z0-9]+(-[a-z0-9]+)*$")
    kind: Literal["total", "breakdown", "composition"]
    metric: str = Field(min_length=1, max_length=100)
    time_column: str = Field(min_length=1, max_length=200)
    grain: Literal["HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]
    breakdown: str | None = Field(default=None, max_length=200)
    top_n: int = Field(ge=0, le=20)
    observed_points: int = Field(ge=0, description="periods with data at this grain, measured not assumed")
    reading: str = Field(min_length=1, max_length=2000, description="what the series shows and how it can be misread")

    @model_validator(mode="after")
    def coherent(self) -> "TrendProposalV1":
        if (self.kind == "total") == bool(self.breakdown):
            raise ValueError("a breakdown or composition trend names a breakdown; a total trend does not")
        if self.kind != "total" and not 2 <= self.top_n <= 20:
            raise ValueError("a pivoted trend needs a readable top_n between 2 and 20")
        return self


class MartAnalysisV1(StrictModel):
    model: str = Field(min_length=1, max_length=200)
    rows: int = Field(ge=0)
    event_time_column: str | None = Field(default=None, max_length=200)
    event_time_span: str | None = Field(default=None, max_length=200)
    collection_time_column: str = Field(min_length=1, max_length=200)
    dimensions: list[DimensionProfileV1] = Field(min_length=1, max_length=20)
    metrics_added: list[str] = Field(max_length=20)
    trends: list[TrendProposalV1] = Field(min_length=1, max_length=8)
    findings: list[str] = Field(min_length=1, max_length=20, description="what the data actually shows, with numbers")

    @model_validator(mode="after")
    def covers_the_standard(self) -> "MartAnalysisV1":
        kinds = {trend.kind for trend in self.trends}
        if "total" not in kinds:
            raise ValueError(f"{self.model}: no overall trend proposed")
        if not kinds & {"breakdown", "composition"}:
            raise ValueError(f"{self.model}: no dimensional trend proposed")
        if self.event_time_column and all(trend.time_column != self.event_time_column for trend in self.trends):
            raise ValueError(f"{self.model}: source event time available but no trend uses it")
        if len({trend.slug for trend in self.trends}) != len(self.trends):
            raise ValueError(f"{self.model}: duplicate trend slug")
        return self


class DataAnalystV1(StrictModel):
    schema_version: Literal[1]
    agent: Literal["data_analyst"]
    status: Literal["ok", "degraded_evidence"]
    family: str = Field(min_length=1, max_length=100)
    queries: list[str] = Field(min_length=1, max_length=40, description="exploratory SQL actually executed")
    analyses: list[MartAnalysisV1] = Field(min_length=1, max_length=6)
    plans: list[AnalyticsPlanV2] = Field(min_length=1, max_length=1)
    summary: str = Field(min_length=1, max_length=20_000)


class Resurface(StrictModel):
    dismissed_task_id: str = Field(min_length=36, max_length=36)
    material_change: str = Field(min_length=1, max_length=20_000)


class ManagerPlanItemV3(StrictModel):
    recommendation_key: str = Field(min_length=1, max_length=80, pattern=KEY_PATTERN)
    title: str = Field(min_length=1, max_length=200)
    priority: int = Field(ge=1, le=7)
    category: TaskCategory
    resources: str = Field(min_length=1, max_length=20_000)
    risk: str = Field(min_length=1, max_length=20_000)
    rollback: str = Field(min_length=1, max_length=20_000)
    verification: str = Field(min_length=1, max_length=20_000)
    approval: Literal["pending"]
    suggested_executor: Literal["human", "junior", "senior", "staff"]
    resurface: Resurface | None = None


class Deferred(StrictModel):
    item: str = Field(min_length=1, max_length=20_000)
    reason: str = Field(min_length=1, max_length=20_000)
    revisit_when: str = Field(min_length=1, max_length=20_000)


class InputFreshnessV3(StrictModel):
    as_of: datetime
    freshness_ok: bool
    stale_agents: list[str] = Field(max_length=10)
    missing_agents: list[str] = Field(max_length=10)
    failed_agents: list[str] = Field(max_length=10)
    max_age_seconds: int = Field(ge=0)


class ManagerV3(StrictModel):
    schema_version: Literal[3]
    agent: Literal["manager"]
    status: Literal["ok", "degraded_evidence"]
    report_date: date
    input_freshness: InputFreshnessV3
    executive_summary: str = Field(min_length=1, max_length=20_000)
    plan: list[ManagerPlanItemV3] = Field(max_length=7)
    deferred: list[Deferred] = Field(max_length=50)
    approvals_required: list[str] = Field(max_length=7)

    @model_validator(mode="after")
    def consistent(self) -> "ManagerV3":
        keys = [item.recommendation_key for item in self.plan]
        priorities = [item.priority for item in self.plan]
        if len(keys) != len(set(keys)) or len(priorities) != len(set(priorities)):
            raise ValueError("manager plan keys and priorities must be unique")
        if set(self.approvals_required) != set(keys):
            raise ValueError("approvals_required must exactly match recommendation keys")
        if not self.input_freshness.freshness_ok and self.status != "degraded_evidence":
            raise ValueError("unhealthy input freshness requires degraded_evidence")
        return self


class VerificationCheckV1(StrictModel):
    name: str = Field(min_length=1, max_length=100)
    argv: list[str] = Field(min_length=1, max_length=32)
    exit_code: int = Field(ge=-1, le=255)
    observed: str = Field(max_length=20_000)
    observation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def validate_check(self):
        if any(not value or len(value) > 1024 or "\x00" in value for value in self.argv):
            raise ValueError("verification argv entries must be bounded")
        canonical = self.observed.encode()
        if hashlib.sha256(canonical).hexdigest() != self.observation_sha256:
            raise ValueError("verification observation digest mismatch")
        return self


class ArtifactReferenceV1(StrictModel):
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    byte_count: int = Field(ge=1, le=8 * 1024 * 1024)


class RepositoryPolicyV1(StrictModel):
    allowed_path_globs: list[str] = Field(min_length=1, max_length=100)
    denied_path_globs: list[str] = Field(max_length=100)
    max_changed_files: int = Field(ge=1, le=200)
    max_diff_bytes: int = Field(ge=1, le=700_000)


class ExecutorTaskV2(StrictModel):
    title: str = Field(min_length=1, max_length=200)
    category: TaskCategory
    planned_resolution: str = Field(min_length=1, max_length=20_000)
    verification_commands: list[list[str]] = Field(min_length=1, max_length=20)
    allowed_path_globs: list[str] = Field(min_length=1, max_length=50)
    resource_keys: list[str] = Field(max_length=50)


class ExecutorAdmissionV2(StrictModel):
    protocol_version: Literal[2]
    kind: Literal["executor"]
    task_id: str = Field(min_length=36, max_length=36)
    profile: Literal["junior", "senior", "staff"]
    model_role: Literal["@task", "@default", "@plan"]
    reviewer_required: bool
    sequence: int = Field(ge=1)
    revision: int = Field(ge=1)
    execution_id: str = Field(min_length=64, max_length=64)
    deadline_at: datetime
    task: ExecutorTaskV2
    source_artifact: ArtifactReferenceV1
    base_sha: str = Field(pattern=r"^[a-f0-9]{40,64}$")
    patch_sha256: None = None
    trusted_head_sha: None = None
    pr_number: None = None
    pr_url: None = None
    verification_manifest: None = None
    executor_report_sha256: None = None
    repository_policy: RepositoryPolicyV1
    source_report_reference: str | None = Field(default=None, max_length=1000)


class ReviewerAdmissionV2(StrictModel):
    protocol_version: Literal[2]
    kind: Literal["pr_reviewer"]
    task_id: str = Field(min_length=36, max_length=36)
    profile: Literal["junior", "senior", "staff"]
    model_role: Literal["@task", "@default", "@plan"]
    reviewer_required: Literal[True]
    sequence: int = Field(ge=1)
    revision: int = Field(ge=1)
    execution_id: str = Field(min_length=64, max_length=64)
    deadline_at: datetime
    task: ExecutorTaskV2
    source_artifact: ArtifactReferenceV1
    base_sha: str = Field(pattern=r"^[a-f0-9]{40,64}$")
    patch_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    trusted_head_sha: str = Field(pattern=r"^[a-f0-9]{40,64}$")
    pr_number: int = Field(ge=1)
    pr_url: str = Field(min_length=1, max_length=2000)
    verification_manifest: dict[str, Any]
    executor_report_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    repository_policy: None = None
    source_report_reference: str | None = Field(default=None, max_length=1000)


class TaskExecutorV2(StrictModel):
    schema_version: Literal[2]
    agent: Literal["task_executor"]
    status: Literal["ok", "blocked", "no_change"]
    task_id: str = Field(min_length=36, max_length=36)
    summary: str = Field(max_length=20_000)
    changed_paths: list[str] = Field(max_length=200)
    verification: list[VerificationCheckV1] = Field(max_length=20)
    blockers: list[str] = Field(max_length=50)


class ExecutorResultV2(StrictModel):
    protocol_version: Literal[2]
    outcome: Literal["succeeded"]
    report: TaskExecutorV2


class ReviewerResultV2(StrictModel):
    protocol_version: Literal[2]
    outcome: Literal["succeeded"]
    report: "PrReviewerV2"


class VerificationManifestV1(StrictModel):
    manifest_version: Literal[1]
    task_id: str = Field(min_length=36, max_length=36)
    execution_id: str = Field(min_length=64, max_length=64)
    revision: int = Field(ge=1)
    base_sha: str = Field(pattern=r"^[a-f0-9]{40,64}$")
    changed_paths: list[str] = Field(max_length=200)
    patch_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    patch_bytes: int = Field(ge=0, le=700_000)
    source_report_reference: str | None = Field(default=None, max_length=1000)
    checks: list[VerificationCheckV1] = Field(max_length=20)

    @model_validator(mode="after")
    def validate_manifest(self):
        if self.changed_paths != sorted(set(self.changed_paths)):
            raise ValueError("manifest changed_paths must be sorted and unique")
        return self


class ReviewComment(StrictModel):
    body: str = Field(min_length=1, max_length=10_000)
    path: str | None = Field(default=None, max_length=1024)
    line: int | None = Field(default=None, ge=1)


class PrReviewerV2(StrictModel):
    schema_version: Literal[2]
    agent: Literal["pr_reviewer"]
    status: Literal["ok", "blocked"]
    task_id: str = Field(min_length=36, max_length=36)
    verdict: Literal["approved", "changes_requested", "unable_to_review"]
    summary: str = Field(max_length=20_000)
    comments: list[ReviewComment] = Field(max_length=50)
    verification: list[str] = Field(max_length=50)


ReviewerResultV2.model_rebuild()


SCHEMAS: dict[str, type[StrictModel]] = {
    "source_discovery_v2": SourceDiscoveryV2,
    "source_vetting_v2": SourceVettingV2,
    "source_scheduling_v2": SourceSchedulingV2,
    "cadence_review_v2": CadenceReviewV2,
    "failure_triage_v2": FailureTriageV2,
    "analytics_engineer_v2": AnalyticsEngineerV2,
    "data_analyst_v1": DataAnalystV1,
    "manager_v3": ManagerV3,
    "task_executor_v2": TaskExecutorV2,
    "pr_reviewer_v2": PrReviewerV2,
}

for _model in (
    UsageV1,
    *SCHEMAS.values(),
    ExecutorAdmissionV2,
    ReviewerAdmissionV2,
    ExecutorResultV2,
    ReviewerResultV2,
):
    assert _model.__pydantic_complete__, f"{_model.__name__} is incomplete"


def _shape_limits(value: object, *, depth: int = 0, counter: list[int] | None = None) -> None:
    if depth > 20:
        raise ValueError("report exceeds maximum nesting depth")
    counter = counter or [0]
    counter[0] += 1
    if counter[0] > 10_000:
        raise ValueError("report exceeds maximum item count")
    if isinstance(value, str) and len(value) > 20_000:
        raise ValueError("report string exceeds maximum length")
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str) or len(key) > 20_000:
                raise ValueError("report object key is invalid")
            _shape_limits(child, depth=depth + 1, counter=counter)
    elif isinstance(value, list):
        for child in value:
            _shape_limits(child, depth=depth + 1, counter=counter)


def validate_named_report(name: str, value: object, *, max_bytes: int = 262_144) -> dict:
    model = SCHEMAS.get(name)
    if model is None:
        raise ValueError(f"unknown output schema {name!r}")
    _shape_limits(value)
    normalized = model.model_validate(value).model_dump(mode="json", by_alias=True)
    encoded = json.dumps(normalized, separators=(",", ":"), ensure_ascii=False).encode()
    if len(encoded) > max_bytes:
        raise ValueError("report exceeds maximum serialized size")
    return normalized


def validate_run_envelope(value: object, *, max_bytes: int = 262_144) -> dict:
    _shape_limits(value)
    normalized = RunEnvelopeV1.model_validate(value).model_dump(mode="json", by_alias=True)
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > max_bytes:
        raise ValueError("run envelope exceeds maximum serialized size")
    return normalized


_VOLATILE = re.compile(
    r"(?:[0-9a-f]{8}-[0-9a-f-]{27,}|\b\d{4}-\d\d-\d\d[T ][^\s]+|"
    r"\b(?:run|attempt|try|duration)[_ =:-]*[\w.+:-]+|/(?:[^\s/]+/)+[^\s]+)",
    re.IGNORECASE,
)


def failure_fingerprint(*, origin: str, component: str, task: str, error_class: str, code: str, detail: str) -> str:
    normalized = _VOLATILE.sub("<volatile>", detail.lower())
    normalized = " ".join(normalized.split())[:8192]
    body = "\0".join((origin, component, task, error_class, code, normalized))
    return hashlib.sha256(body.encode()).hexdigest()
