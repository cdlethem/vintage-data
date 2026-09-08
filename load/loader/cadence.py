"""Algorithmic scheduling cadence: let the data pick each source's schedule.

A declared cron is a guess. This module replaces the guess with a measurement:
after every load pass, each managed source's newest extract run is compared
against the run before it, and the schedule steps one rung **faster** when the
run brought genuinely new records and one rung **slower** when it did not.

Three properties matter more than the control law:

* **The probe is cheap.** Only two batches of one table are ever read, matched
  by ``_batch_id`` — which is insert-ordered, so DuckDB's row-group min/max
  statistics prune everything else. Measured on ``raw.sensor_community``
  (8.3M rows): 0.65s to compare two 32k-row batches. A batch bigger than
  ``max_probe_rows`` isn't compared at all; "rows arrived" is taken as the
  signal instead, and the observation says so.
* **Novelty is not "the payload differs".** Every record carries a per-run
  ``fetched_at``, so ``_content_hash`` changes on every run even when the
  upstream published nothing. The comparison therefore hashes the record with
  the volatile envelope keys removed (``volatile_keys``, default
  ``fetched_at``), which is what makes "did we pick up changed data?" answerable
  at all.
* **Every decision is logged.** ``_load.cadence_decisions`` keeps the evidence
  (rows, novel rows, ratio, both batch ids, the signal used) next to the step
  taken, so a schedule that drifted somewhere odd can always be explained.

The chosen cron is published to a JSON plan file, which is the only thing the
Airflow DAG factory reads — Airflow never opens the warehouse, exactly as with
the rest of the load layer.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import pathlib
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from itertools import pairwise
from typing import Any

import yaml

log = logging.getLogger(__name__)

#: Repo-wide rule (see agents.md): nothing polls faster than every 5 minutes.
FLOOR_MINUTES = 5
DAY_MINUTES = 1440

#: Rungs a synthesized cron can actually express: a divisor of an hour, or a
#: whole number of hours that divides a day. 90 minutes is not on the ladder
#: because "every 90 minutes" is not a cron.
def is_rung(minutes: int) -> bool:
    if minutes < FLOOR_MINUTES:
        return False
    if minutes < 60:
        return 60 % minutes == 0
    return minutes <= DAY_MINUTES and minutes % 60 == 0 and DAY_MINUTES % minutes == 0


def volatile_patch(keys: Iterable[str]) -> dict:
    """A JSON merge patch that deletes each (possibly nested) key path.

    In merge-patch semantics a null value removes the key, so
    ``["fetched_at", "page.updated_at"]`` becomes
    ``{"fetched_at": null, "page": {"updated_at": null}}`` — the record keeps
    ``page``, minus the timestamp that moves on every poll. Nesting matters in
    practice: statuspage embeds the whole status page object in every incident,
    and its ``updated_at`` made 50 unchanged incidents look 100% new.
    """
    patch: dict = {}
    for key in keys:
        parts = [p for p in str(key).split(".") if p]
        if not parts:
            continue
        node = patch
        for part in parts[:-1]:
            child = node.setdefault(part, {})
            if not isinstance(child, dict):
                # The parent is already removed wholesale; nothing deeper to do.
                node = None
                break
            node = child
        if node is not None:
            node[parts[-1]] = None
    return patch


# --------------------------------------------------------------------------
# cron: synthesize one, and read the interval back out of one
# --------------------------------------------------------------------------
def _seed(source: str) -> int:
    """Deterministic per-source stagger, so two sources sharing a cadence do
    not fire in the same second (and a given source keeps its offset)."""
    return int(hashlib.md5(source.encode()).hexdigest()[:8], 16)


def cron_for(minutes: int, source: str) -> str:
    """A 5-field cron firing every ``minutes``, offset by the source name."""
    if not is_rung(minutes):
        raise ValueError(f"{minutes} minutes is not expressible as a cron rung")
    seed = _seed(source)
    if minutes < 60:
        return f"{seed % minutes}-59/{minutes} * * * *"
    minute = seed % 60
    if minutes == 60:
        return f"{minute} * * * *"
    step = minutes // 60
    if step >= 24:
        return f"{minute} {(seed // 60) % 24} * * *"
    return f"{minute} {(seed // 60) % step}-23/{step} * * *"


def _slots(field_text: str, period: int) -> list[int] | None:
    """Values a single cron field matches, or None if the shape is unsupported."""
    values: set[int] = set()
    for part in field_text.split(","):
        step = 1
        if "/" in part:
            part, _, step_text = part.partition("/")
            if not step_text.isdigit() or int(step_text) < 1:
                return None
            step = int(step_text)
        if part == "*":
            lo, hi = 0, period - 1
        elif "-" in part:
            lo_text, _, hi_text = part.partition("-")
            if not (lo_text.isdigit() and hi_text.isdigit()):
                return None
            lo, hi = int(lo_text), int(hi_text)
        elif part.isdigit():
            lo = hi = int(part)
        else:
            return None
        if not (0 <= lo <= hi < period):
            return None
        values.update(range(lo, hi + 1, step))
    return sorted(values) or None


def _uniform_step(values: list[int], period: int) -> int | None:
    """The constant gap between firings, or None when they are not evenly spaced."""
    if len(values) == 1:
        return period
    gaps = {b - a for a, b in pairwise(values)}
    gaps.add(values[0] + period - values[-1])
    return gaps.pop() if len(gaps) == 1 else None


def interval_minutes(cron: str) -> int | None:
    """Minutes between firings of a 5-field cron, or None if it isn't uniform.

    Only evenly-spaced day-independent schedules have a single cadence, which
    is exactly the set this layer is allowed to manage: anything with a
    day-of-month or day-of-week restriction is left to its declared cron.
    """
    fields = cron.split()
    if len(fields) != 5:
        return None
    minute, hour, dom, month, dow = fields
    if (dom, month, dow) != ("*", "*", "*"):
        return None
    minutes, hours = _slots(minute, 60), _slots(hour, 24)
    if minutes is None or hours is None:
        return None
    if len(minutes) > 1:
        # Several firings per hour only have one cadence if every hour is live.
        if len(hours) != 24:
            return None
        return _uniform_step(minutes, 60)
    step_hours = _uniform_step(hours, 24)
    return None if step_hours is None else step_hours * 60


# --------------------------------------------------------------------------
# policy (load.yml) and per-source opt-in (extract/sources/*.yml)
# --------------------------------------------------------------------------
DEFAULT_LADDER = (5, 10, 15, 30, 60, 120, 240, 360, 720, 1440)


@dataclass(frozen=True)
class CadencePolicy:
    """Global cadence settings, from the ``cadence`` block of load.yml."""

    enabled: bool = True
    plan_path: pathlib.Path = pathlib.Path("~/.local/share/vintage-data/extract/state/cadence/plan.json")
    sources_dir: pathlib.Path = pathlib.Path("extract/sources")
    ladder_minutes: tuple[int, ...] = DEFAULT_LADDER
    floor_minutes: int = FLOOR_MINUTES
    min_minutes: int = 15               # default per-source floor
    max_minutes: int = 1440             # default per-source ceiling
    change_ratio: float = 0.0           # novel/rows strictly above this = changed
    speed_up_after: int = 1             # consecutive changed runs before stepping faster
    slow_down_after: int = 1            # consecutive unchanged runs before stepping slower
    max_probe_rows: int = 500_000       # bigger batches are not compared
    volatile_keys: tuple[str, ...] = ("fetched_at",)
    auto_default: bool = False          # sources opt in individually by default


def policy_from_config(raw: dict[str, Any] | None, *, repo_root: pathlib.Path) -> CadencePolicy:
    raw = dict(raw or {})
    plan = raw.pop("plan_path", None)
    sources = raw.pop("sources_dir", None)
    ladder = raw.pop("ladder_minutes", None)
    volatile = raw.pop("volatile_keys", None)
    unknown = set(raw) - {f for f in CadencePolicy.__dataclass_fields__}
    if unknown:
        raise ValueError(f"unknown cadence setting(s) {sorted(unknown)}")

    policy = CadencePolicy(**raw)
    if plan:
        policy = replace(policy, plan_path=pathlib.Path(plan).expanduser())
    policy = replace(policy, sources_dir=(pathlib.Path(sources).expanduser() if sources
                                          else repo_root / "extract" / "sources"))
    if ladder is not None:
        rungs = tuple(sorted({int(m) for m in ladder}))
        bad = [m for m in rungs if not is_rung(m)]
        if bad:
            raise ValueError(f"cadence ladder_minutes {bad} cannot be expressed as a cron")
        if not rungs:
            raise ValueError("cadence ladder_minutes is empty")
        policy = replace(policy, ladder_minutes=rungs)
    if volatile is not None:
        policy = replace(policy, volatile_keys=tuple(volatile))
    if policy.floor_minutes < FLOOR_MINUTES:
        raise ValueError(f"cadence floor_minutes must be >= {FLOOR_MINUTES}")
    return policy


@dataclass(frozen=True)
class SourceCadence:
    """One source's declared schedule plus its cadence bounds."""

    name: str
    declared_cron: str
    declared_minutes: int
    min_minutes: int
    max_minutes: int
    change_ratio: float
    volatile_keys: tuple[str, ...]
    note: str = ""

    def rungs(self, policy: CadencePolicy) -> tuple[int, ...]:
        lo = max(policy.floor_minutes, self.min_minutes)
        hi = max(lo, self.max_minutes)
        steps = tuple(m for m in policy.ladder_minutes if lo <= m <= hi)
        # An empty window (bounds between two rungs) still needs somewhere to
        # sit: the nearest expressible rung inside the ladder.
        return steps or (min(policy.ladder_minutes, key=lambda m: (abs(m - lo), -m)),)

    def plan_cron(self, minutes: int) -> str:
        """Cron for an interval, preferring the hand-written one it matches.

        Returning to the declared interval returns the declared cron verbatim,
        so a source that ends up where it started shows no synthetic churn.
        """
        if minutes == self.declared_minutes:
            return self.declared_cron
        return cron_for(minutes, self.name)


def load_source_cadences(policy: CadencePolicy) -> dict[str, SourceCadence]:
    """Managed sources, read from the extract source configs.

    Opt-in is per source (``cadence.auto``), because changing a schedule has
    upstream consequences the yml is the right place to reason about — the
    rate limit and the cadence note live there too. A config that cannot be
    parsed, is disabled, belongs to another DAG factory, or has a schedule with
    no single cadence is skipped, loudly but harmlessly.
    """
    managed: dict[str, SourceCadence] = {}
    for path in sorted(pathlib.Path(policy.sources_dir).glob("*.yml")):
        try:
            cfg = yaml.safe_load(path.read_text()) or {}
            if cfg.get("dag_factory") not in (None, "extract"):
                continue
            if not cfg.get("enabled", True):
                continue
            block = cfg.get("cadence") or {}
            if not block.get("auto", policy.auto_default):
                continue
            name, cron = cfg.get("name"), cfg.get("schedule")
            if not name or not cron:
                raise ValueError("missing name or schedule")
            declared = interval_minutes(cron)
            if declared is None:
                log.warning("cadence: %s has no single cadence (%r); leaving it alone",
                            name, cron)
                continue
            volatile = block.get("volatile_keys")
            managed[name] = SourceCadence(
                name=name,
                declared_cron=cron,
                declared_minutes=declared,
                min_minutes=int(block.get("min_minutes", policy.min_minutes)),
                max_minutes=int(block.get("max_minutes", policy.max_minutes)),
                change_ratio=float(block.get("change_ratio", policy.change_ratio)),
                volatile_keys=tuple(volatile) if volatile is not None else policy.volatile_keys,
                note=str(block.get("note", "")),
            )
        except Exception:
            log.exception("cadence: skipping source config %s", path.name)
    return managed


# --------------------------------------------------------------------------
# state, evidence, decision
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class CadenceState:
    """What the last evaluation left behind for one source."""

    source: str
    interval_minutes: int
    cron: str
    last_batch_id: str | None = None
    changed_streak: int = 0
    unchanged_streak: int = 0


@dataclass(frozen=True)
class Observation:
    """Evidence from one extract run, compared with the run before it."""

    batch_id: str
    prev_batch_id: str | None
    rows: int
    distinct_rows: int
    novel_rows: int
    novel_ratio: float
    changed: bool
    #: how ``changed`` was established: content (hash anti-join), ids,
    #: empty_batch (no rows at all), rows_only (batch too big to compare),
    #: cold_start (nothing to compare against yet)
    signal: str


@dataclass(frozen=True)
class Decision:
    source: str
    decision: str                # faster | slower | hold
    from_minutes: int
    to_minutes: int
    cron: str
    reason: str
    state: CadenceState
    observation: Observation


def snap(minutes: int, steps: tuple[int, ...]) -> int:
    """The rung a (possibly hand-written) interval sits on; ties go slower."""
    return min(steps, key=lambda m: (abs(m - minutes), -m))


def decide(policy: CadencePolicy, source: SourceCadence, state: CadenceState,
           obs: Observation) -> Decision:
    """One step of the control law, from one observation.

    Changed data steps faster, unchanged data steps slower, and the streak
    counters make the required evidence configurable (``speed_up_after`` /
    ``slow_down_after``) without changing anything else. A step always resets
    both counters: the next decision is made on evidence gathered *at the new
    cadence*, which is the only evidence that says anything about it.
    """
    steps = source.rungs(policy)
    current = snap(state.interval_minutes, steps)
    index = steps.index(current)

    if obs.signal == "cold_start":
        return Decision(source.name, "hold", current, current, source.plan_cron(current),
                        "no previous run to compare against",
                        replace(state, interval_minutes=current, cron=source.plan_cron(current),
                                last_batch_id=obs.batch_id),
                        obs)

    changed_streak = state.changed_streak + 1 if obs.changed else 0
    unchanged_streak = 0 if obs.changed else state.unchanged_streak + 1
    decision, target = "hold", current

    if obs.changed:
        if changed_streak < policy.speed_up_after:
            reason = (f"{obs.novel_rows} new row(s), "
                      f"{changed_streak}/{policy.speed_up_after} runs before speeding up")
        elif index == 0:
            reason = f"{obs.novel_rows} new row(s) but already at the {current}m floor"
        else:
            decision, target = "faster", steps[index - 1]
            reason = (f"{obs.novel_rows} of {obs.rows} row(s) new "
                      f"({obs.novel_ratio:.0%}) via {obs.signal}")
    else:
        if unchanged_streak < policy.slow_down_after:
            reason = (f"no new rows, "
                      f"{unchanged_streak}/{policy.slow_down_after} runs before slowing down")
        elif index == len(steps) - 1:
            reason = f"no new rows and already at the {current}m ceiling"
        else:
            decision, target = "slower", steps[index + 1]
            reason = (f"no new rows in {obs.rows} row(s) via {obs.signal}"
                      if obs.rows else "the run produced no rows at all")

    if decision != "hold":
        changed_streak = unchanged_streak = 0
    cron = source.plan_cron(target)
    return Decision(source.name, decision, current, target, cron, reason,
                    CadenceState(source.name, target, cron, obs.batch_id,
                                 changed_streak, unchanged_streak),
                    obs)


# --------------------------------------------------------------------------
# the plan file: the only thing Airflow reads
# --------------------------------------------------------------------------
def write_plan(policy: CadencePolicy, entries: dict[str, dict], *,
               generated_at: datetime | None = None) -> pathlib.Path:
    """Publish the effective schedule per source, atomically."""
    path = pathlib.Path(policy.plan_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": (generated_at or datetime.now(timezone.utc)).isoformat(),
        "floor_minutes": policy.floor_minutes,
        "ladder_minutes": list(policy.ladder_minutes),
        "sources": entries,
    }
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def read_plan(path: str | os.PathLike) -> dict:
    """Load a published plan; an unreadable plan is simply "no plan"."""
    try:
        data = json.loads(pathlib.Path(path).expanduser().read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


# --------------------------------------------------------------------------
# the job: probe, decide, record, publish
# --------------------------------------------------------------------------
class CadenceEvaluator:
    """Runs one cadence pass inside the writer service.

    It is a *reader* of RAW and a writer of the two cadence tables only, so it
    can never touch loaded data. One probe per source, and only when that
    source produced a run the last pass had not yet seen.
    """

    def __init__(self, config, destination, policy: CadencePolicy):
        self.config = config
        self.destination = destination
        self.policy = policy

    def run(self, load_id: str, result: dict, sources: Iterable[str] | None = None) -> None:
        managed = load_source_cadences(self.policy)
        if sources:
            wanted = set(sources)
            managed = {k: v for k, v in managed.items() if k in wanted}
        states = self.destination.cadence_states()
        # The plan is rewritten whole every pass, so a source with no new run
        # keeps the decision it is currently living under rather than reverting
        # to "not yet evaluated".
        previous = read_plan(self.policy.plan_path).get("sources") or {}
        decisions: list[Decision] = []
        plan: dict[str, dict] = {}
        skipped: dict[str, str] = {}

        for name, source in sorted(managed.items()):
            state = self._state_for(source, states.get(name))
            try:
                obs = self._observe(source, state)
            except Exception as exc:
                log.exception("cadence: probe failed for %s", name)
                result["errors"].append(f"{name}: cadence probe failed: {exc}")
                skipped[name] = f"probe failed: {type(exc).__name__}"
                plan[name] = self._plan_entry(source, state, None, previous.get(name))
                continue
            if obs is None:
                skipped[name] = "no new run since the last evaluation"
                plan[name] = self._plan_entry(source, state, None, previous.get(name))
                continue
            decision = decide(self.policy, source, state, obs)
            self.destination.record_cadence_decision(load_id, decision)
            self.destination.save_cadence_state(decision.state)
            decisions.append(decision)
            plan[name] = self._plan_entry(source, decision.state, decision)
            log.info("cadence: %s %s %dm -> %dm (%s) [%s]", name, decision.decision,
                     decision.from_minutes, decision.to_minutes, decision.reason,
                     decision.cron)

        path = write_plan(self.policy, plan)
        result["cadence"] = {
            "managed": len(managed),
            "evaluated": len(decisions),
            "faster": sum(1 for d in decisions if d.decision == "faster"),
            "slower": sum(1 for d in decisions if d.decision == "slower"),
            "held": sum(1 for d in decisions if d.decision == "hold"),
            "skipped": skipped,
            "plan_path": str(path),
            "decisions": [
                {"source": d.source, "decision": d.decision, "from_minutes": d.from_minutes,
                 "to_minutes": d.to_minutes, "cron": d.cron, "signal": d.observation.signal,
                 "rows": d.observation.rows, "novel_rows": d.observation.novel_rows,
                 "reason": d.reason}
                for d in decisions if d.decision != "hold"
            ],
        }

    # -- internals -------------------------------------------------------
    def _state_for(self, source: SourceCadence, row: dict | None) -> CadenceState:
        if row is None:
            return CadenceState(source.name, source.declared_minutes, source.declared_cron)
        return CadenceState(
            source=source.name,
            interval_minutes=int(row.get("interval_minutes") or source.declared_minutes),
            cron=row.get("cron") or source.declared_cron,
            last_batch_id=row.get("last_batch_id"),
            changed_streak=int(row.get("changed_streak") or 0),
            unchanged_streak=int(row.get("unchanged_streak") or 0),
        )

    def _plan_entry(self, source: SourceCadence, state: CadenceState,
                    decision: Decision | None, previous: dict | None = None) -> dict:
        entry = {
            "cron": state.cron,
            "interval_minutes": state.interval_minutes,
            "declared_cron": source.declared_cron,
            "declared_minutes": source.declared_minutes,
            "min_minutes": source.min_minutes,
            "max_minutes": source.max_minutes,
        }
        if decision is not None:
            entry.update(decision=decision.decision, reason=decision.reason,
                         batch_id=decision.observation.batch_id,
                         decided_at=datetime.now(timezone.utc).isoformat())
        elif previous:
            entry.update({k: v for k, v in previous.items()
                          if k in ("decision", "reason", "batch_id", "decided_at")})
        return entry

    def _observe(self, source: SourceCadence, state: CadenceState) -> Observation | None:
        """Compare the newest run of a source with the one before it.

        Returns None when there is nothing new to judge — a source whose extract
        DAG has not run (or has failed) since the last evaluation keeps its
        cadence rather than being slowed down for the pipeline's own silence.
        """
        settings = self.config.for_source(source.name)
        table = settings.table or _sanitized(source.name)
        batches = self.destination.recent_batches(source.name, limit=2)
        if not batches:
            return None
        newest = batches[0]
        if newest["batch_id"] == state.last_batch_id:
            return None
        previous = batches[1] if len(batches) > 1 else None
        rows = int(newest.get("rows_loaded") or 0)

        # Cheapest possible answer first: a run that loaded no rows cannot have
        # brought new data, and needs no query against RAW at all.
        if rows == 0:
            return Observation(newest["batch_id"], previous and previous["batch_id"],
                               0, 0, 0, 0.0, False, "empty_batch")
        if previous is None:
            return Observation(newest["batch_id"], None, rows, rows, rows, 1.0, True,
                               "cold_start")
        if rows > self.policy.max_probe_rows:
            log.info("cadence: %s batch has %d rows (> max_probe_rows %d); "
                     "treating the arrival of rows as the signal",
                     source.name, rows, self.policy.max_probe_rows)
            return Observation(newest["batch_id"], previous["batch_id"], rows, rows, rows,
                               1.0, True, "rows_only")

        columns = self.destination.list_columns(self.config.raw_schema, table)
        if not columns:
            return None
        key = self._novelty_key(columns)
        if key is None:
            return Observation(newest["batch_id"], previous["batch_id"], rows, rows, rows,
                               1.0, True, "rows_only")
        probe = self.destination.probe_novelty(
            table, newest["batch_id"], previous["batch_id"],
            key=key, volatile_keys=source.volatile_keys)
        distinct = int(probe["distinct_rows"])
        novel = int(probe["novel_rows"])
        ratio = novel / distinct if distinct else 0.0
        return Observation(newest["batch_id"], previous["batch_id"], rows, distinct, novel,
                           ratio, ratio > source.change_ratio, key)

    @staticmethod
    def _novelty_key(columns: dict[str, str]) -> str | None:
        """Which per-record identity to compare on.

        ``_payload`` minus the volatile envelope keys is the honest answer: it
        sees a changed *value* on a stable id. ``id`` alone is the fallback for
        a source loaded with ``keep_payload: false``, where only arrival of new
        ids is observable.
        """
        if "_payload" in columns:
            return "content"
        if "id" in columns:
            return "ids"
        return None


def _sanitized(source: str) -> str:
    from .schema import sanitize
    return sanitize(source, fallback="source")
