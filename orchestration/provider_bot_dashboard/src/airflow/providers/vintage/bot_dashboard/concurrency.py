"""Persistent bot concurrency, enforced by DAG limits and separate worker pools."""
import json
from typing import Annotated

from pydantic import Field, field_validator
from sqlalchemy import select, func
from airflow.configuration import conf
from airflow.models.variable import Variable
from airflow.models.pool import Pool
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from .api_models import StrictBody
from .models import Policy, utcnow
from .service import Conflict, DomainError

KEY = "bot_dashboard_concurrency"
BOTS = ("task_executor", "pr_reviewer", "source_discovery", "source_vetting", "source_scheduling",
        "cadence_review", "failure_triage", "analytics_engineer", "data_analyst", "manager", "executive")
WORKER_CAPACITY = 16


class Settings(StrictBody):
    version: int = Field(ge=0)
    limits: dict[str, Annotated[int, Field(strict=True, ge=1, le=16)]]

    @field_validator("limits")
    @classmethod
    def known_bots(cls, value):
        if set(value) != set(BOTS):
            raise ValueError("Supply concurrency for every registered bot")
        return value


def _read(session, lock=False):
    if lock:
        session.scalar(select(Policy).where(Policy.category == "*").with_for_update())
    query = select(Variable).where(Variable.key == KEY)
    row = session.scalar(query.with_for_update() if lock else query)
    return row, json.loads(row.val) if row else {"version": 0, "limits": dict.fromkeys(BOTS, 1)}


def scheduler_limits():
    # DAG parsing runs in the Airflow 3 task SDK process: use its supervised API.
    from airflow.sdk import Variable as RuntimeVariable
    state = RuntimeVariable.get(KEY, default={"limits": dict.fromkeys(BOTS, 1)}, deserialize_json=True)
    return state["limits"]


def worker_pool(bot):
    return conf.get("bot_dashboard", "executor_pool", fallback="bot_dashboard_executor") + "__" + bot


def get_settings(session):
    from . import workload
    _, state = _read(session)
    observed = {row.dag_id: (row.max_active_tasks if row.dag_id == "bot__executive" else row.max_active_runs) for row in session.scalars(select(DagModel).where(DagModel.dag_id.in_(["bot__" + b for b in BOTS])))}
    counts = {(dag, status): count for dag, status, count in session.execute(
        select(DagRun.dag_id, DagRun.state, func.count()).where(DagRun.dag_id.in_(["bot__" + b for b in BOTS]),
        DagRun.state.in_(["running", "queued"])).group_by(DagRun.dag_id, DagRun.state))}
    executive_counts = dict(session.execute(select(TaskInstance.state, func.count()).where(
        TaskInstance.dag_id == "bot__executive", TaskInstance.state.in_(["running", "queued", "scheduled"])
    ).group_by(TaskInstance.state)).all())
    counts[("bot__executive", "running")] = executive_counts.get("running", 0)
    counts[("bot__executive", "queued")] = executive_counts.get("queued", 0) + executive_counts.get("scheduled", 0)
    return {"version": state["version"], "limits": state["limits"], "maximum": WORKER_CAPACITY,
            "workload": workload.status(session),
            "worker_capacity": WORKER_CAPACITY, "updated_at": state.get("updated_at"),
            "bots": [{"name": bot, "limit": state["limits"][bot], "applied_limit": observed.get("bot__" + bot),
                      "running": counts.get(("bot__" + bot, "running"), 0), "queued": counts.get(("bot__" + bot, "queued"), 0)} for bot in BOTS]}


def set_settings(session, body, actor_id):
    row, state = _read(session, lock=True)
    if body.version != state["version"]:
        raise Conflict("Concurrency settings changed; refresh before saving")
    if body.limits["task_executor"] + body.limits["pr_reviewer"] > WORKER_CAPACITY:
        raise DomainError("Execution and review concurrency exceed worker capacity")
    for bot in ("task_executor", "pr_reviewer"):
        name = worker_pool(bot)
        pool = session.scalar(select(Pool).where(Pool.pool == name).with_for_update())
        if pool is None:
            pool = Pool(pool=name, slots=body.limits[bot], description=f"Bot dashboard: {bot}", include_deferred=False)
            session.add(pool)
        else:
            pool.slots = body.limits[bot]
    # Runs admitted before per-bot pools retain their serialized task pool.
    # Keep that compatibility pool large enough for the chosen combined limit.
    legacy = session.scalar(select(Pool).where(Pool.pool == conf.get("bot_dashboard", "executor_pool", fallback="bot_dashboard_executor")).with_for_update())
    if legacy is not None:
        legacy.slots = body.limits["task_executor"] + body.limits["pr_reviewer"]
    now = utcnow().isoformat()
    state.update(version=state["version"] + 1, limits=body.limits, updated_at=now, updated_by=actor_id)
    state["history"] = (state.get("history", []) + [{"at": now, "actor_id": actor_id, "limits": body.limits}])[-100:]
    if row is None:
        row = Variable(key=KEY)
        session.add(row)
    row.val = json.dumps(state)
    session.flush()
    return get_settings(session)
