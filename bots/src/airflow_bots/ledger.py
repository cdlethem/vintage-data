"""Append-only record of agent runs, used for spend limits and `airflow-bots status`.

One JSON line per agent run in ``<state_dir>/ledger.jsonl``. Appends take an
exclusive lock so concurrent Airflow tasks on the same host never interleave.
Multi-host deployments must put ``state_dir`` on shared storage or pin bot
tasks to one worker queue.
"""
from __future__ import annotations

import fcntl
import json
from datetime import datetime, timezone
from pathlib import Path

from .config import Limits


def _path(state_dir: Path) -> Path:
    return state_dir / "ledger.jsonl"


def record(state_dir: Path, **entry: object) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    entry = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **entry}
    with _path(state_dir).open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        handle.write(json.dumps(entry, default=str) + "\n")


def entries(state_dir: Path, since: datetime | None = None) -> list[dict]:
    path = _path(state_dir)
    if not path.exists():
        return []
    rows = []
    for line in path.read_text().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if since is None or datetime.fromisoformat(row["at"]) >= since:
            rows.append(row)
    return rows


def today(state_dir: Path) -> tuple[int, float]:
    """(agent runs, USD spent) since midnight UTC."""
    midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = entries(state_dir, midnight)
    return len(rows), sum(float(row.get("cost_usd") or 0) for row in rows)


def over_limit(state_dir: Path, limits: Limits) -> str | None:
    """A human-readable reason when today's budget is used up, else None."""
    runs, spent = today(state_dir)
    if limits.daily_runs is not None and runs >= limits.daily_runs:
        return f"daily run limit reached ({runs}/{limits.daily_runs})"
    if limits.daily_usd is not None and spent >= limits.daily_usd:
        return f"daily spend limit reached (${spent:.2f}/${limits.daily_usd:.2f})"
    return None
