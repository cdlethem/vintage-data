"""Airflow's side of algorithmic scheduling cadence.

The cadence job (in the loader service) publishes one JSON file: the schedule
it has decided for each managed source. This module is the only place the DAG
factory reads it, and it is deliberately paranoid — a missing, stale, or
nonsensical plan must degrade to the schedule declared in the source yml, never
break DAG parsing.

Airflow still never opens the warehouse: the plan file is the seam, exactly
like the job queue is the seam for loading.
"""
import logging
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
LOAD_ROOT = REPO_ROOT / "load"
if str(LOAD_ROOT) not in sys.path:
    sys.path.insert(0, str(LOAD_ROOT))

from loader.cadence import FLOOR_MINUTES, interval_minutes, read_plan
from loader.config import load_config

log = logging.getLogger(__name__)


def plan_sources() -> dict:
    """``{source: entry}`` from the published plan; empty when there is none."""
    try:
        policy = load_config().cadence
        if not policy.enabled:
            return {}
        sources = read_plan(policy.plan_path).get("sources")
    except Exception:
        log.exception("cadence plan unreadable; using declared schedules")
        return {}
    return sources if isinstance(sources, dict) else {}


def effective_schedule(cfg: dict, plan: dict | None = None) -> tuple[str, str]:
    """``(cron, note)`` for one source config.

    The declared cron wins unless the source opted in (``cadence.auto``) *and*
    the plan holds a cron that is a valid, uniform schedule inside the bounds
    the yml itself declares. Those bounds are re-checked here on purpose: they
    live in the yml, so tightening them takes effect on the next DAG parse
    rather than waiting for the next cadence pass to notice.
    """
    declared = cfg["schedule"]
    block = cfg.get("cadence") or {}
    if not block.get("auto"):
        return declared, "declared (cadence detection off)"

    entry = (plan if plan is not None else plan_sources()).get(cfg["name"]) or {}
    cron = entry.get("cron")
    if not isinstance(cron, str) or not cron:
        return declared, "declared (no cadence plan entry yet)"

    minutes = interval_minutes(cron)
    if minutes is None:
        log.warning("cadence plan for %s has an unusable cron %r; using %r",
                    cfg["name"], cron, declared)
        return declared, "declared (plan cron unusable)"

    low = max(FLOOR_MINUTES, int(block.get("min_minutes", FLOOR_MINUTES)))
    high = int(block.get("max_minutes", 1440))
    if not low <= minutes <= high:
        log.warning("cadence plan for %s is %dm, outside its declared %d-%dm bounds; "
                    "using %r", cfg["name"], minutes, low, high, declared)
        return declared, f"declared (plan {minutes}m out of bounds)"

    if cron == declared:
        return cron, f"cadence-managed, at its declared {minutes}m"
    return cron, (f"cadence-managed: {minutes}m "
                  f"({entry.get('decision', 'set')} — {entry.get('reason', 'no reason recorded')})")
