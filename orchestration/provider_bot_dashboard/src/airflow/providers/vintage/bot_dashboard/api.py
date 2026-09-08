"""Authenticated, same-origin FastAPI surface for the bot dashboard."""
from __future__ import annotations

import base64
import hashlib
import hmac
import importlib.resources
import json
import logging
import re
import secrets
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import airflow
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import func, select

from airflow._shared.secrets_masker import redact
from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import AccessView, ConnectionDetails, DagAccessEntity, DagDetails
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.security import GetUserDep, get_user, requires_access_dag, requires_access_view
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.configuration import conf
from airflow.models import Connection, Pool
from airflow.models.dag import DagModel, DagTag
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.sdk.observability import stats
from airflow.utils.log.log_reader import TaskLogReader
from .models import Event, Execution, Policy, RunReport, Task, utcnow
from . import __version__
from .api_models import (
    AirflowFailuresResponse,
    ArtifactPutRequest,
    ArtifactResponse,
    Assignment,
    BudgetClaimRequest,
    BudgetClaimResponse,
    CommentBody,
    CreateTask,
    DispatchClaimRequest,
    DispatchResponse,
    EvidenceBody,
    ExecutionClaimRequest,
    ExecutionClaimResponse,
    ExecutionFinalizeRequest,
    ExecutionPublishRequest,
    FinalizeResponse,
    MaintenanceRequest,
    MaintenanceResponse,
    ManagerContextResponse,
    ManagerReconcileRequest,
    PatchTask,
    PromotionPolicy,
    PublishResponse,
    ReconcileResponse,
    RunProjectionResponse,
    RunQueryRequest,
    RunQueryResponse,
    RunReportRequest,
    UsageSummaryResponse,
    Start,
    SyncRequest,
    Transition,
)
from .db_manager import BotDashboardDBManager
from .service import (
    DomainError,
    SPECIALIST_FRESHNESS_MINUTES,
    add_event_items,
    assign_task,
    claim_run_budget,
    create_manual_task,
    get_task,
    list_tasks,
    manager_context,
    overview,
    queue_summary,
    patch_task,
    query_run_reports,
    reconcile_manager,
    run_report_dict,
    start_task,
    task_dict,
    transition_task,
    usage_summary,
)
log = logging.getLogger(__name__)
CSRF_COOKIE = "bot_dashboard_csrf"
MAX_BODY = 1_048_576


def _setting_bool(name: str, fallback: bool = False) -> bool:
    return conf.getboolean("bot_dashboard", name, fallback=fallback)


def _base_url() -> str:
    return conf.get("api", "base_url", fallback="")


def _csrf_secret() -> bytes:
    value = conf.get("api", "secret_key", fallback="") or conf.get("webserver", "secret_key", fallback="")
    if not value: value = "bot-dashboard-csrf-unconfigured"
    return value.encode()


def _new_csrf() -> str:
    payload = f"{int(time.time())}.{secrets.token_hex(32)}"
    signature = hmac.new(_csrf_secret(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{signature}"


def _valid_csrf(token: str) -> bool:
    try:
        timestamp, random, signature = token.split(".")
        payload = f"{timestamp}.{random}"
        expected = hmac.new(_csrf_secret(), payload.encode(), hashlib.sha256).hexdigest()
        return len(random) == 64 and abs(time.time() - int(timestamp)) <= 1800 and hmac.compare_digest(signature, expected)
    except (TypeError, ValueError):
        return False


def _same_origin(request: Request) -> None:
    if request.headers.get("authorization", "").lower().startswith("bearer "): return
    expected_parts = urlsplit(_base_url())
    expected = f"{expected_parts.scheme}://{expected_parts.netloc}"
    if not expected_parts.scheme or request.headers.get("origin") != expected: raise HTTPException(status.HTTP_403_FORBIDDEN, "cross-origin request rejected")
    if request.headers.get("sec-fetch-site") != "same-origin": raise HTTPException(status.HTTP_403_FORBIDDEN, "same-origin fetch metadata required")
    cookie = request.cookies.get(CSRF_COOKIE, "")
    header = request.headers.get("x-bot-dashboard-csrf", "")
    if not cookie or cookie != header or not _valid_csrf(cookie): raise HTTPException(status.HTTP_403_FORBIDDEN, "invalid CSRF token")


def _write_guard() -> None:
    if not _setting_bool("write_enabled"): raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "dashboard writes are disabled")
    manager_name = get_auth_manager().__class__.__name__.lower()
    if "simpleauthmanager" in manager_name and not _setting_bool("allow_simple_auth_writes"):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "SimpleAuthManager writes are disabled")


def _executor_guard() -> None:
    _write_guard()
    from .execution import executor_preconditions

    codes = executor_preconditions()
    if codes:
        raise HTTPException(
            status.HTTP_412_PRECONDITION_FAILED,
            {"code": "executor_unavailable", "reasons": codes},
        )


def _identity(user: GetUserDep) -> tuple[str, str]:
    return str(user.get_id()), str(user.get_name())


def _connection_access(method: str, user: GetUserDep) -> None:
    conn_id = conf.get("bot_dashboard", "git_conn_id", fallback="bot_dashboard_git")
    allowed = get_auth_manager().is_authorized_connection(method=method, details=ConnectionDetails(conn_id=conn_id), user=user)
    if not allowed: raise HTTPException(status.HTTP_403_FORBIDDEN, "not authorized")


def _dag_access(user: GetUserDep, dag_id: str, entity: DagAccessEntity) -> None:
    allowed = get_auth_manager().is_authorized_dag(
        method="GET",
        access_entity=entity,
        details=DagDetails(id=dag_id, team_name=DagModel.get_team_name(dag_id)),
        user=user,
    )
    if not allowed:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "resource not found")



def _schema_guard(session: SessionDep) -> None:
    try:
        ready = BotDashboardDBManager(session).check_migration()
    except Exception:
        ready = False
    if not ready:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            {"code": "migration_required"},
        )


def _internal_identity(request: Request, user: GetUserDep) -> None:
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "bearer authentication required")
    if request.headers.get("cookie"):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "browser cookies are forbidden")
    expected = conf.get("bot_dashboard", "internal_user", fallback="bot-worker")
    if str(user.get_name()) != expected and str(user.get_id()) != expected:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "internal service identity required")


def _failure_log_tail(task: TaskInstance) -> tuple[str, str]:
    """Read a bounded, redacted task log without making logs part of the API model."""
    try:
        reader = TaskLogReader()
        metadata: dict[str, object] = {}
        lines: list[str] = []
        for _ in range(64):
            chunks, metadata = reader.read_log_chunks(task, task.try_number, metadata)
            if not isinstance(chunks, (list, tuple)):
                chunks = [chunks]
            for chunk in chunks:
                event = getattr(chunk, "event", chunk)
                if event is not None:
                    lines.extend(str(event).splitlines())
                    del lines[:-30]
            if metadata.get("end_of_log"):
                break
        if not lines:
            return "", "log_unavailable"
        detail = str(redact("\n".join(lines[-30:]), "task_log", max_depth=20))[-3500:]
        return (detail, "task_failed") if detail else ("", "log_unavailable")
    except Exception:
        return "", "log_unavailable"

_EXCEPTION_LINE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception|Failure|Timeout))\b")


def _failure_classification(detail: str) -> tuple[str, str]:
    """Derive a stable error class/code from the bounded tail; never from free text."""
    for line in reversed(detail.splitlines()):
        match = _EXCEPTION_LINE.match(line)
        if match:
            name = match.group(1).rsplit(".", 1)[-1]
            code = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()[:100]
            return name[:100], code
    return "AirflowTaskFailure", "task_failed"

app = FastAPI(title="Vintage bot dashboard", docs_url=None, redoc_url=None, openapi_url=None)


@app.middleware("http")
async def response_policy(request: Request, call_next):
    started = time.perf_counter()
    correlation_id = request.headers.get("x-request-id") or secrets.token_hex(8)

    def reject(body: dict, status_code: int) -> JSONResponse:
        response = JSONResponse(body, status_code=status_code)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Correlation-ID"] = correlation_id
        stats.incr(
            "bot_dashboard.api.error",
            tags={"route": "unmatched", "status": status_code},
        )
        return response

    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        try:
            _same_origin(request)
        except HTTPException as exc:
            return reject({"detail": exc.detail}, exc.status_code)
        content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            return reject({"code": "json_required"}, 415)
        try:
            declared_size = int(request.headers.get("content-length", "0"))
        except ValueError:
            return reject({"code": "invalid_content_length"}, 400)
        if declared_size > MAX_BODY:
            return reject({"code": "body_too_large"}, 413)
        body = await request.body()
        if len(body) > MAX_BODY:
            return reject({"code": "body_too_large"}, 413)

    response = await call_next(request)
    route = getattr(request.scope.get("route"), "path", "unmatched")
    duration_ms = (time.perf_counter() - started) * 1000
    stats.timing(
        "bot_dashboard.api.latency",
        duration_ms,
        tags={"route": route, "method": request.method},
    )
    if response.status_code >= 400:
        stats.incr(
            "bot_dashboard.api.error",
            tags={"route": route, "status": response.status_code},
        )
    response.headers["X-Correlation-ID"] = correlation_id
    log.info(
        "bot_dashboard_api correlation_id=%s route=%s method=%s status=%d duration_ms=%.2f",
        correlation_id,
        route,
        request.method,
        response.status_code,
        duration_ms,
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    api_prefix = f"{request.scope.get('root_path', '')}/api/"
    if request.url.path.startswith(api_prefix):
        response.headers["Cache-Control"] = "no-store"
    return response


@app.exception_handler(DomainError)
async def domain_error_handler(_request: Request, exc: DomainError):
    body = {"code": exc.code, "detail": str(exc)}
    if getattr(exc, "current", None) is not None: body["current"] = exc.current
    return JSONResponse(body, status_code=exc.status_code, headers={"Cache-Control": "no-store"})


@app.get("/api/health")
def health(session: SessionDep):
    codes: list[str] = []
    if airflow.__version__ != "3.3.1": codes.append("incompatible_airflow")
    try:
        if not BotDashboardDBManager(session).check_migration():
            codes.append("migration_required")
        else:
            session.scalar(select(func.count()).select_from(Task))
            required_dags = {
                "bot__manager",
                "bot__task_executor",
                "bot__pr_reviewer",
                "bot_dashboard__dispatch",
                "bot_dashboard__maintenance",
            }
            present = set(
                session.scalars(
                    select(DagModel.dag_id).where(DagModel.dag_id.in_(required_dags))
                )
            )
            if present != required_dags:
                codes.append("dags_missing")
    except Exception:
        codes.append("database_unavailable")
    static = importlib.resources.files("airflow.providers.vintage.bot_dashboard").joinpath("static")
    try:
        manifest = json.loads(static.joinpath("asset-manifest.json").read_text("utf-8"))
        for name in ("dashboard", "app"):
            if not static.joinpath(manifest[name]).is_file(): codes.append("asset_missing")
    except Exception: codes.append("asset_manifest_invalid")
    base = urlsplit(_base_url())
    if base.scheme not in {"http", "https"} or not base.netloc: codes.append("base_url_invalid")
    if _setting_bool("csrf_cookie_secure", True) is False and not (base.hostname in {"localhost", "127.0.0.1", "::1"} and base.scheme == "http"):
        codes.append("insecure_csrf_cookie")
    if _setting_bool("executor_enabled"):
        from .execution import executor_preconditions

        codes.extend(executor_preconditions())
        try:
            conn_id = conf.get(
                "bot_dashboard", "git_conn_id", fallback="bot_dashboard_git"
            )
            if session.scalar(
                select(Connection.conn_id).where(Connection.conn_id == conn_id)
            ) is None:
                codes.append("git_connection_missing")
            pool_name = conf.get(
                "bot_dashboard",
                "executor_pool",
                fallback="bot_dashboard_executor",
            )
            if session.scalar(
                select(Pool.pool).where(Pool.pool == pool_name)
            ) is None:
                codes.append("executor_pool_missing")
        except Exception:
            codes.append("executor_metadata_unavailable")
    return {"status": "ready" if not codes else "not_ready", "codes": sorted(set(codes)), "provider_version": __version__, "airflow_version": airflow.__version__}


auth = APIRouter(prefix="/api", dependencies=[Depends(get_user), Depends(requires_access_view(AccessView.WEBSITE))])
READ_TASK = Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN, param_dag_id="bot__manager"))
WRITE_TASK = Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.RUN, param_dag_id="bot__task_executor"))
WRITE_DEPS = [WRITE_TASK, Depends(_schema_guard), Depends(_write_guard), Depends(_same_origin), Depends(action_logging())]


@auth.get("/capabilities")
def capabilities(response: Response, user: GetUserDep):
    token = _new_csrf()
    base = urlsplit(_base_url())
    secure = _setting_bool("csrf_cookie_secure", True)
    write = _setting_bool("write_enabled")
    if "simpleauthmanager" in get_auth_manager().__class__.__name__.lower() and not _setting_bool("allow_simple_auth_writes"): write = False
    response.set_cookie(CSRF_COOKIE, token, max_age=1800, secure=secure, httponly=False, samesite="strict", path=base.path.rstrip("/") + "/bot-dashboard")
    reasons = []
    if not write: reasons.append("writes_disabled")
    if not _setting_bool("executor_enabled"): reasons.append("executor_disabled")
    return {"write_enabled": write, "executor_enabled": write and _setting_bool("executor_enabled"), "csrf_token": token, "reasons": reasons}

@auth.get("/summary", dependencies=[READ_TASK, Depends(_schema_guard)])
def get_summary(session: SessionDep): return queue_summary(session)


@auth.get("/overview", dependencies=[READ_TASK, Depends(_schema_guard)])
def get_overview(session: SessionDep): return overview(session)

@auth.get("/tasks", dependencies=[READ_TASK, Depends(_schema_guard)])
def get_tasks(session: SessionDep, state: str = "", category: str | None = None, assignee: str | None = None, source: str | None = None, search: str | None = Query(default=None, max_length=200), limit: int = Query(default=25, ge=1, le=100), cursor: str | None = None, sort: str = "priority"):
    return list_tasks(session, states=state.split(","), category=category, assignee=assignee, source=source, search=search, limit=limit, cursor=cursor, sort=sort)


@auth.post("/tasks", dependencies=WRITE_DEPS, status_code=201)
def post_task(body: CreateTask, session: SessionDep, user: GetUserDep):
    actor_id, actor_name = _identity(user)
    return create_manual_task(session, title=body.title, category=body.category, priority=body.priority, planned_resolution=body.planned_resolution, actor_id=actor_id, actor_name=actor_name, evidence=[item.model_dump(exclude_none=True) for item in body.evidence])

@auth.get("/tasks/{task_id}", dependencies=[READ_TASK, Depends(_schema_guard)])
def task_detail(task_id: str, session: SessionDep):
    try: return get_task(session, task_id)
    except (ValueError, DomainError): raise HTTPException(404, "task not found")


@auth.patch("/tasks/{task_id}", dependencies=WRITE_DEPS)
def update_task(task_id: str, body: PatchTask, session: SessionDep, user: GetUserDep):
    changes = body.model_dump(exclude={"version"}, exclude_none=True)
    return patch_task(session, task_id, version=body.version, actor_id=_identity(user)[0], changes=changes)


@auth.post("/tasks/{task_id}/transition", dependencies=WRITE_DEPS)
def transition(task_id: str, body: Transition, session: SessionDep, user: GetUserDep):
    return transition_task(session, task_id, version=body.version, actor_id=_identity(user)[0], to_state=body.state, reason=body.reason)

@auth.post("/tasks/{task_id}/assign", dependencies=WRITE_DEPS)
def assign(task_id: str, body: Assignment, session: SessionDep, user: GetUserDep):
    actor_id, actor_name = _identity(user)
    return assign_task(session, task_id, version=body.version, actor_id=actor_id, actor_name=actor_name, kind=body.kind, profile=body.profile, reviewer_required=body.reviewer_required)


@auth.post("/tasks/{task_id}/start", dependencies=[WRITE_TASK, Depends(_schema_guard), Depends(_executor_guard), Depends(_same_origin), Depends(action_logging())])
def start(task_id: str, body: Start, session: SessionDep, user: GetUserDep):
    return start_task(
        session,
        task_id,
        version=body.version,
        actor_id=_identity(user)[0],
        idempotency_key=body.idempotency_key,
        revision=body.revision,
        max_queued=conf.getint(
            "bot_dashboard", "max_queued_executions", fallback=20
        ),
    )

@auth.post("/tasks/{task_id}/comments", dependencies=WRITE_DEPS)
def comments(task_id: str, body: CommentBody, session: SessionDep, user: GetUserDep):
    return add_event_items(session, task_id, version=body.version, actor_id=_identity(user)[0], event_type="comment_added", items=body.comments)


@auth.post("/tasks/{task_id}/evidence", dependencies=WRITE_DEPS)
def evidence(task_id: str, body: EvidenceBody, session: SessionDep, user: GetUserDep):
    return add_event_items(session, task_id, version=body.version, actor_id=_identity(user)[0], event_type="evidence_added", items=[item.model_dump(exclude_none=True) for item in body.evidence])


@auth.post("/tasks/{task_id}/sync-request", dependencies=WRITE_DEPS)
def sync_request(task_id: str, body: SyncRequest, session: SessionDep, user: GetUserDep):
    return add_event_items(session, task_id, version=body.version, actor_id=_identity(user)[0], event_type="sync_requested", items=[{"idempotency_key": body.idempotency_key}])


@auth.get("/bots", dependencies=[Depends(_schema_guard)])
def bots(session: SessionDep, user: GetUserDep):
    # Bot definitions only: the dispatcher and maintenance DAGs are infrastructure.
    dag_ids = session.scalars(
        select(DagTag.dag_id)
        .where(DagTag.name == "bot-dashboard", DagTag.dag_id.startswith("bot__", autoescape=True))
        .distinct()
        .order_by(DagTag.dag_id)
    ).all()
    items = []
    for dag_id in dag_ids:
        try:
            _dag_access(user, dag_id, DagAccessEntity.RUN)
            _dag_access(user, dag_id, DagAccessEntity.TASK_INSTANCE)
        except HTTPException:
            continue
        dag = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id))
        latest = session.scalar(select(RunReport).where(RunReport.dag_id == dag_id).order_by(RunReport.created_at.desc()).limit(1))
        projection = _run_projection(session, latest) if latest else None
        items.append(
            {
                "name": dag_id.removeprefix("bot__"),
                "dag_id": dag_id,
                "paused": dag.is_paused if dag else None,
                "latest_status": latest.outcome if latest else "missing",
                "latest_reason_code": latest.reason_code if latest else "missing",
                "latest_at": latest.finished_at.isoformat() if latest else None,
                "latest": projection,
            }
        )
    return {"items": items}

@auth.get("/usage", response_model=UsageSummaryResponse, dependencies=[Depends(_schema_guard)])
def usage(session: SessionDep, days: int = Query(default=30, ge=1, le=400)):
    return usage_summary(session, days=days)


def _safe_report_payload(value: object) -> object:
    """Keep report projections useful while excluding operational secrets and paths."""
    denied = (
        "prompt",
        "context",
        "credential",
        "password",
        "secret",
        "token",
        "local",
        "path",
        "file",
        "raw_log",
    )
    if isinstance(value, dict):
        return {
            str(key): _safe_report_payload(item)
            for key, item in value.items()
            if not any(part in str(key).lower() for part in denied)
        }
    if isinstance(value, list):
        return [_safe_report_payload(item) for item in value[:100]]
    if isinstance(value, str):
        return value[:12_000]
    return value


def _run_execution(session: SessionDep, row: RunReport) -> Execution | None:
    return session.scalar(
        select(Execution)
        .where(
            (Execution.claimed_run_id == row.run_id)
            | (Execution.target_run_id == row.run_id)
        )
        .order_by(Execution.updated_at.desc(), Execution.sequence.desc())
        .limit(1)
    )


def _run_freshness(row: RunReport, now: datetime) -> tuple[str, int | None]:
    sla = SPECIALIST_FRESHNESS_MINUTES.get(row.bot_name)
    if row.outcome != "succeeded":
        return ("failed" if row.outcome in {"failed", "timed_out"} else "missing", sla)
    age = max(0, int((now - row.finished_at).total_seconds()))
    return ("fresh" if sla is None or age <= sla * 60 else "stale", sla)


def _run_projection(session: SessionDep, row: RunReport, *, detail: bool = False) -> dict[str, object]:
    execution = _run_execution(session, row)
    task = session.scalar(select(Task).where(Task.id == execution.task_id)) if execution else None
    freshness, sla = _run_freshness(row, utcnow())
    manifest = getattr(execution, "verification_manifest", None) if execution else None
    verification_digest = (
        next(
            (
                manifest.get(key)
                for key in ("sha256", "digest", "manifest_sha256", "patch_sha256")
                if manifest.get(key)
            ),
            None,
        )
        if isinstance(manifest, dict)
        else None
    )
    report = run_report_dict(row)
    result: dict[str, object] = {
        "run_id": row.run_id,
        "task_id": row.task_id,
        "map_index": row.map_index,
        "try_number": row.try_number,
        "outcome": row.outcome,
        "retry_class": row.retry_class,
        "reason_code": row.reason_code,
        "duration_ms": row.duration_ms,
        "deadline_at": row.deadline_at.isoformat(),
        "started_at": row.started_at.isoformat(),
        "finished_at": row.finished_at.isoformat(),
        "deadline_consumed_ms": row.deadline_consumed_ms,
        "expires_at": row.expires_at.isoformat(),
        "report_sha256": row.sha256,
        "report_bytes": row.byte_count,
        "freshness": freshness,
        "freshness_sla_minutes": sla,
        "requests": report.get("requests", getattr(row, "model_requests", 0) or 0),
        "input_tokens": report.get("input_tokens", getattr(row, "input_tokens", None)),
        "output_tokens": report.get("output_tokens", getattr(row, "output_tokens", None)),
        "cached_input_tokens": report.get("cached_input_tokens", getattr(row, "cached_input_tokens", None)),
        "cache_write_tokens": report.get("cache_write_tokens", getattr(row, "cache_write_tokens", None)),
        "reasoning_tokens": report.get("reasoning_tokens", getattr(row, "reasoning_tokens", None)),
        "total_tokens": report.get("total_tokens", getattr(row, "total_tokens", None)),
        "cost_micro_usd": report.get("cost_micro_usd", getattr(row, "cost_micro_usd", None)),
        "cost_source": report.get("cost_source", getattr(row, "cost_source", None)),
        "pricing_id": report.get("pricing_id", getattr(row, "pricing_id", None)),
        "artifact_digests": [
            digest
            for digest in (
                getattr(execution, "source_artifact_sha256", None) if execution else None,
                getattr(execution, "patch_sha256", None) if execution else None,
                getattr(execution, "executor_report_sha256", None) if execution else None,
                getattr(execution, "review_report_sha256", None) if execution else None,
            )
            if digest
        ],
        "verification_digest": verification_digest,
        "pr": {
            "provider": getattr(execution, "provider", None) if execution else None,
            "repository": getattr(execution, "repository", None) if execution else None,
            "branch": getattr(execution, "branch", None) if execution else None,
            "number": getattr(execution, "pr_number", None) if execution else None,
            "url": getattr(execution, "pr_url", None) if execution else None,
            "base_sha": getattr(execution, "base_sha", None) if execution else None,
            "head_sha": getattr(execution, "trusted_head_sha", None) if execution else None,
        },
        "review": {
            "verdict": getattr(execution, "review_verdict", None) if execution else None,
            "commented_at": (
                execution.review_commented_at.isoformat()
                if execution and execution.review_commented_at
                else None
            ),
        },
        "merge_state": (
            "merged"
            if execution and execution.merged_at
            else "open"
            if execution and execution.pr_number
            else "not_published"
        ),
        "merged_at": execution.merged_at.isoformat() if execution and execution.merged_at else None,
        "completed_at": task.completed_at.isoformat() if task and task.completed_at else None,
        "terminal_reason_code": getattr(execution, "terminal_reason_code", None) if execution else None,
        "terminal_failure_class": getattr(execution, "terminal_failure_class", None) if execution else None,
    }
    if detail:
        result["failure"] = (
            {
                "class": row.failure_class,
                "code": row.failure_code,
                "fingerprint": row.failure_fingerprint,
                "detail": (row.failure_detail or "")[:3500],
            }
            if row.failure_fingerprint
            else None
        )
        result["payload"] = _safe_report_payload(
            row.body_json if row.body_json is not None else row.body_text
        )
        result["terminal_detail"] = (
            str(getattr(execution, "terminal_detail", ""))[:3500]
            if execution and getattr(execution, "terminal_detail", None)
            else None
        )
    return result




@auth.get("/bots/{name}/runs", dependencies=[Depends(_schema_guard)])
def bot_runs(name: str, session: SessionDep, user: GetUserDep, limit: int = Query(default=25, ge=1, le=100), cursor: str | None = None):
    dag_id = f"bot__{name}"
    _dag_access(user, dag_id, DagAccessEntity.RUN)
    _dag_access(user, dag_id, DagAccessEntity.TASK_INSTANCE)
    rows = session.scalars(
        select(RunReport)
        .where(RunReport.dag_id == dag_id)
        .order_by(RunReport.created_at.desc())
        .limit(limit)
    ).all()
    return {
        "items": [_run_projection(session, row) for row in rows],
        "next_cursor": None,
    }


@auth.get("/bots/{name}/runs/{run_id}", dependencies=[Depends(_schema_guard)])
def bot_run(name: str, run_id: str, session: SessionDep, user: GetUserDep):
    dag_id = f"bot__{name}"
    _dag_access(user, dag_id, DagAccessEntity.RUN)
    _dag_access(user, dag_id, DagAccessEntity.TASK_INSTANCE)
    _dag_access(user, dag_id, DagAccessEntity.XCOM)
    row = session.scalar(
        select(RunReport)
        .where(RunReport.dag_id == dag_id, RunReport.run_id == run_id)
        .order_by(RunReport.created_at.desc())
        .limit(1)
    )
    if row is None:
        raise HTTPException(404, "run report not found")
    return _run_projection(session, row, detail=True)


@auth.get("/settings/promotion", dependencies=[Depends(_schema_guard)])
def promotion(session: SessionDep, user: GetUserDep):
    _connection_access("GET", user)
    return {"items": [{"category": row.category, "mode": row.mode, "profile": row.profile, "reviewer_required": row.reviewer_required, "updated_at": row.updated_at.isoformat()} for row in session.scalars(select(Policy).order_by(Policy.category)).all()]}


@auth.patch("/settings/promotion", dependencies=[WRITE_TASK, Depends(_schema_guard), Depends(_write_guard), Depends(_same_origin), Depends(action_logging())])
def set_promotion(body: PromotionPolicy, session: SessionDep, user: GetUserDep):
    _connection_access("PUT", user)
    actor_id, _ = _identity(user)
    row = session.get(Policy, body.category)
    if row is None: row = Policy(category=body.category); session.add(row)
    row.mode, row.profile, row.reviewer_required, row.updated_by, row.updated_at = body.mode, body.profile, body.reviewer_required, actor_id, utcnow()
    session.flush()
    return {"category": row.category, "mode": row.mode, "profile": row.profile, "reviewer_required": row.reviewer_required}


@auth.get("/settings/git-status", dependencies=[Depends(_schema_guard)])
def git_status(user: GetUserDep):
    _connection_access("GET", user)
    return {"configured": bool(conf.get("bot_dashboard", "git_conn_id", fallback="")), "provider": None, "repository": None, "cached_age_seconds": None, "codes": ["not_checked"]}


internal = APIRouter(
    prefix="/api/internal",
    dependencies=[
        Depends(get_user),
        Depends(_internal_identity),
        Depends(_schema_guard),
    ],
)


@internal.post("/runs/report", response_model=RunProjectionResponse)
def internal_report(body: RunReportRequest, session: SessionDep):
    from .projection import persist_run_envelope

    return persist_run_envelope(session, body.envelope.model_dump(mode="json", by_alias=True))


@internal.post("/runs/claim-budget", response_model=BudgetClaimResponse)
def internal_claim_budget(body: BudgetClaimRequest, session: SessionDep):
    return claim_run_budget(
        session,
        **body.identity.model_dump(),
        configured_total_seconds=body.configured_total_seconds,
    )


@internal.post("/runs/query", response_model=RunQueryResponse)
def internal_run_query(body: RunQueryRequest, session: SessionDep):
    return query_run_reports(
        session,
        bots=body.bots,
        days=body.days,
        outcomes=list(body.outcomes),
        limit=body.limit,
    )


@internal.get("/manager/context", response_model=ManagerContextResponse)
def internal_manager_context(
    session: SessionDep, days: int = Query(default=7, ge=1, le=3650)
):
    return manager_context(session, days=days)


@internal.post("/manager/reconcile", response_model=ReconcileResponse)
def internal_manager_reconcile(body: ManagerReconcileRequest, session: SessionDep):
    from .report_schemas import validate_named_report

    identity = body.identity
    report = session.scalar(
        select(RunReport)
        .where(
            RunReport.dag_id == identity.dag_id,
            RunReport.run_id == identity.run_id,
            RunReport.task_id == identity.task_id,
            RunReport.map_index == identity.map_index,
            RunReport.try_number == body.try_number,
            RunReport.report_schema == "manager_v3",
            RunReport.body_json.is_not(None),
        )
        .with_for_update()
    )
    if report is None:
        raise HTTPException(404, "manager report not found")
    payload = validate_named_report("manager_v3", report.body_json)
    return {
        "status": "ok",
        **reconcile_manager(
            session,
            payload,
            dag_id=identity.dag_id,
            run_id=identity.run_id,
            task_id=identity.task_id,
            map_index=identity.map_index,
        ),
    }


@internal.get("/airflow/failures", response_model=AirflowFailuresResponse)
def internal_airflow_failures(
    session: SessionDep,
    hours: int = Query(default=24, ge=1, le=720),
    limit: int = Query(default=100, ge=1, le=100),
):
    cutoff = utcnow() - timedelta(hours=hours)
    filters = (
        DagRun.state == "failed",
        DagRun.start_date >= cutoff,
        ~DagRun.dag_id.like("bot__%"),
        ~DagRun.dag_id.like("bot_dashboard__%"),
        TaskInstance.state == "failed",
        TaskInstance.dag_id == DagRun.dag_id,
        TaskInstance.run_id == DagRun.run_id,
    )
    total = session.scalar(
        select(func.count())
        .select_from(TaskInstance)
        .join(
            DagRun,
            (TaskInstance.dag_id == DagRun.dag_id)
            & (TaskInstance.run_id == DagRun.run_id),
        )
        .where(*filters)
    ) or 0
    rows = session.execute(
        select(TaskInstance, DagRun.start_date)
        .join(
            DagRun,
            (TaskInstance.dag_id == DagRun.dag_id)
            & (TaskInstance.run_id == DagRun.run_id),
        )
        .where(*filters)
        .order_by(
            DagRun.start_date.desc(),
            TaskInstance.dag_id,
            TaskInstance.run_id,
            TaskInstance.task_id,
            TaskInstance.map_index,
        )
        .limit(limit)
    ).all()
    items = []
    for task, run_started_at in rows:
        detail, tail_code = _failure_log_tail(task)
        error_class, derived_code = _failure_classification(detail)
        error_code = tail_code if tail_code == "log_unavailable" else derived_code
        items.append(
            {
                "dag_id": task.dag_id,
                "run_id": task.run_id,
                "task_id": task.task_id,
                "map_index": task.map_index,
                "try_number": task.try_number,
                "started_at": (
                    task.start_date or run_started_at
                ).isoformat()
                if (task.start_date or run_started_at)
                else None,
                "ended_at": task.end_date.isoformat() if task.end_date else None,
                "origin": "airflow",
                "component": task.dag_id,
                "error_class": error_class,
                "error_code": error_code,
                "detail": detail,
            }
        )
    return {"items": items, "remaining_after_batch": max(0, total - len(items))}


@internal.post("/executions/claim", response_model=ExecutionClaimResponse)
def internal_execution_claim(body: ExecutionClaimRequest, session: SessionDep):
    from .execution import claim_run

    deadline = datetime.fromisoformat(body.deadline_at.replace("Z", "+00:00"))
    return claim_run(
        session,
        dag_id=body.dag_id,
        run_id=body.run_id,
        conf_value=body.conf,
        kind=body.kind,
        deadline_at=deadline,
    )


@internal.put("/artifacts/{sha256}", response_model=ArtifactResponse)
def internal_put_artifact(
    sha256: str, body: ArtifactPutRequest, session: SessionDep
):
    from .artifacts import put_artifact

    try:
        content = base64.b64decode(body.content_base64, validate=True)
    except ValueError as exc:
        raise HTTPException(422, "invalid artifact encoding") from exc
    return put_artifact(
        session,
        kind=body.kind,
        content=content,
        expected_sha256=sha256,
        owner_execution_id=body.owner_execution_id,
    )


@internal.get("/artifacts/{sha256}")
def internal_get_artifact(
    sha256: str,
    session: SessionDep,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=900_000, ge=1, le=900_000),
):
    from .artifacts import read_artifact_slice

    try:
        row, content = read_artifact_slice(
            session, sha256, offset=offset, limit=limit
        )
    except FileNotFoundError as exc:
        raise HTTPException(404, "artifact not found") from exc
    return Response(
        content=content,
        media_type="application/octet-stream",
        headers={
            "ETag": f'"{row.sha256}"',
            "Content-Length": str(len(content)),
            "Cache-Control": "no-store",
            "X-Artifact-Bytes": str(row.byte_count),
            "X-Artifact-Offset": str(offset),
        },
    )


@internal.post("/executions/publish", response_model=PublishResponse)
def internal_execution_publish(body: ExecutionPublishRequest, session: SessionDep):
    from .execution import publish_execution_change

    return publish_execution_change(
        session,
        dag_id=body.dag_id,
        run_id=body.run_id,
        executor_result=body.executor_result,
    )


@internal.post("/executions/finalize", response_model=FinalizeResponse)
def internal_execution_finalize(body: ExecutionFinalizeRequest, session: SessionDep):
    from .execution import finalize_run

    return finalize_run(
        session,
        dag_id=body.dag_id,
        run_id=body.run_id,
        kind=body.kind,
        result=body.result,
    )


@internal.post("/dispatch/claim", response_model=DispatchResponse)
def internal_dispatch_claim(body: DispatchClaimRequest, session: SessionDep):
    from .execution import claim_pending

    return {"items": claim_pending(session, body.limit)}


@internal.post("/maintenance/run", response_model=MaintenanceResponse)
def internal_maintenance(body: MaintenanceRequest, session: SessionDep):
    from .maintenance import run_maintenance

    return run_maintenance(session, limit=body.limit)


app.include_router(internal)

app.include_router(auth)


@app.get("/static/{area}/{asset_name}", include_in_schema=False)
def static_asset(area: str, asset_name: str):
    if area not in {"dashboard", "app"} or not asset_name.startswith("main.") or not asset_name.endswith(".umd.cjs") or "/" in asset_name: raise HTTPException(404)
    path = Path(str(importlib.resources.files("airflow.providers.vintage.bot_dashboard").joinpath("static", area, asset_name)))
    if not path.is_file(): raise HTTPException(404)
    return FileResponse(path, media_type="text/javascript; charset=utf-8", headers={"Cache-Control": "public, max-age=31536000, immutable", "X-Content-Type-Options": "nosniff"})
