"""Read-only admission pressure for discretionary specialist work.

This gates model work, never ticket recording or approval. In-flight reports remain
valid; operational incidents and completion follow-ups are never discarded.
"""
from datetime import timedelta
from sqlalchemy import select, func
from .models import Task, utcnow

MAX_OPEN = 8
MAX_NEW_SOURCES_PER_DAY = 2


def evaluate(*, open_count, created_24h, completed_24h, completed_7d, new_sources_24h):
    # Keep 20% of measured throughput available for repairs. One daily source
    # bootstraps a new installation; the open-work ceiling bounds cold-start debt.
    source_budget = min(MAX_NEW_SOURCES_PER_DAY, max(1, completed_7d * 4 // 35))
    arrival_budget = max(1, completed_24h * 4 // 5)
    if open_count >= MAX_OPEN:
        reason = "Finish existing work before adding sources."
        code = "open_work_limit"
    elif created_24h >= arrival_budget:
        reason = "Recent arrivals have used the capacity supported by completed work."
        code = "arrival_budget_used"
    elif new_sources_24h >= source_budget:
        reason = "The daily new-source budget has been used."
        code = "source_budget_used"
    else:
        reason = "Capacity is available for bounded source discovery and vetting."
        code = "capacity_available"
    return {"allowed": code == "capacity_available", "reason_code": code, "reason": reason,
            "open_count": open_count, "max_open": MAX_OPEN,
            "created_24h": created_24h, "completed_24h": completed_24h,
            "completed_7d": completed_7d, "new_sources_24h": new_sources_24h,
            "source_budget_24h": source_budget, "arrival_budget_24h": arrival_budget,
            "repair_headroom_percent": 20}


def status(session):
    now = utcnow()
    day, week = now - timedelta(days=1), now - timedelta(days=7)
    def count(*filters):
        return session.scalar(select(func.count()).select_from(Task).where(*filters)) or 0
    result = evaluate(
        open_count=count(Task.state.not_in(("completed", "dismissed"))),
        created_24h=count(Task.created_at >= day),
        completed_24h=count(Task.state == "completed", Task.completed_at >= day),
        completed_7d=count(Task.state == "completed", Task.completed_at >= week),
        new_sources_24h=count(Task.category == "new_source", Task.created_at >= day),
    )
    return {**result, "observed_at": now.isoformat(), "gated_bots": [
        "source_discovery", "source_vetting", "source_scheduling",
        "analytics_engineer", "data_analyst", "cadence_review",
    ]}
