"""Evals: replay recorded bot inputs against the current prompts, grade them, track the scores.

The loop for improving prompts:

1. A production run goes wrong (or right). ``airflow-bots eval capture <run-id>``
   turns its recorded input into a case file; you add a short rubric.
2. Edit a prompt (or point ``--prompt`` at a candidate, or ``--agent`` at another model).
3. ``airflow-bots eval run`` replays every case at its original commit, applies the
   deterministic expectations, and asks the judge agent to grade the rubric.
4. ``airflow-bots eval report`` / ``compare`` show pass rates per prompt version.

Results live in SQLite (``evals.db`` in the config, default ``<state_dir>/evals.sqlite``).
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import sqlite3
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml

from . import agent
from .config import PACKAGE_PROMPTS, Config
from .workflows import build_prompt, slug
from .workspace import Workspace

SCHEMA = """
create table if not exists runs (
    id text primary key, started_at text, label text, prompt_hash text, agent text, judge text,
    cases integer, passed integer, cost_usd real
);
create table if not exists results (
    run_id text, case_id text, attempt integer, passed integer, action text, checks text,
    criteria text, score real, cost_usd real, seconds real, error text, run_dir text
);
"""


@dataclass
class Case:
    id: str
    kind: str
    base_sha: str
    variables: dict
    expect: dict = field(default_factory=dict)
    rubric: str = ""
    notes: str = ""

    @classmethod
    def load(cls, path: Path) -> "Case":
        raw = yaml.safe_load(path.read_text())
        return cls(id=raw.get("id") or path.stem, kind=raw["kind"], base_sha=raw["base_sha"],
                   variables=raw["variables"], expect=raw.get("expect") or {},
                   rubric=(raw.get("rubric") or "").strip(), notes=raw.get("notes") or "")


def db_path(cfg: Config) -> Path:
    return cfg.eval_db or cfg.state_dir / "evals.sqlite"


def connect(cfg: Config) -> sqlite3.Connection:
    path = db_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, check_same_thread=False)
    db.executescript(SCHEMA)
    return db


def template_for(cfg: Config, kind: str, override: Path | None) -> Path:
    if override:
        return override
    if kind == "heal":
        return cfg.heal.prompt
    if kind == "request":
        return cfg.respond_prompt
    if kind.startswith("job:"):
        return PACKAGE_PROMPTS / "job.md"
    raise ValueError(f"unknown case kind {kind!r}")


def agent_for(cfg: Config, kind: str) -> str:
    if kind.startswith("job:"):
        return cfg.jobs[kind.split(":", 1)[1]].agent
    return cfg.heal.agent


def case_variables(cfg: Config, case: Case) -> dict:
    variables = dict(case.variables)
    if case.kind.startswith("job:"):  # the job prompt is the thing under test, so use the current one
        variables["job_prompt"] = cfg.jobs[case.kind.split(":", 1)[1]].prompt.read_text()
    return variables


def prompt_hash(cfg: Config, cases: list[Case], override: Path | None) -> str:
    """Fingerprint of every prompt text a run used, to group results by prompt version."""
    digest = hashlib.sha256()
    parts = {template_for(cfg, case.kind, override) for case in cases}
    parts |= {cfg.jobs[c.kind.split(":", 1)[1]].prompt for c in cases if c.kind.startswith("job:")}
    parts |= {PACKAGE_PROMPTS / "result.md", *([cfg.instructions] if cfg.instructions else [])}
    for path in sorted(parts):
        digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


def check(expect: dict, result: agent.Result | None, files: list[str]) -> dict:
    """Deterministic expectations. Returns {name: (ok, detail)}."""
    checks = {}
    if "action" in expect:
        wanted = expect["action"] if isinstance(expect["action"], list) else [expect["action"]]
        got = result.action if result else None
        checks["action"] = (got in wanted, f"expected {'/'.join(wanted)}, got {got}")
    if "paths" in expect:
        outside = [f for f in files if not any(fnmatch.fnmatchcase(f, p) for p in expect["paths"])]
        checks["paths"] = (not outside, f"changed outside expected paths: {outside}" if outside else "ok")
    if "forbid" in expect:
        hit = [f for f in files if any(fnmatch.fnmatchcase(f, p) for p in expect["forbid"])]
        checks["forbid"] = (not hit, f"changed forbidden paths: {hit}" if hit else "ok")
    return checks


def parse_verdict(text: str) -> dict:
    data = agent.last_json_object(text)
    if not data or not isinstance(data.get("criteria"), list) or not data["criteria"]:
        raise ValueError("judge returned no criteria")
    return data


def judge(cfg: Config, case: Case, prompt: str, result: agent.Result, diff: str, run_dir: Path) -> agent.Run:
    task = prompt if len(prompt) <= 12000 else prompt[:12000] + "\n...(truncated)"
    judge_prompt = agent.render(
        (PACKAGE_PROMPTS / "judge.md").read_text(), task=task, rubric=case.rubric,
        output=json.dumps(asdict(result), indent=2), diff=(diff[:30000] or "(no changes)"))
    with tempfile.TemporaryDirectory(prefix="bots-judge-") as scratch:
        return agent.run(cfg.agent(cfg.eval_judge), judge_prompt, Path(scratch), run_dir / "judge", parse=parse_verdict)


def run_case(cfg: Config, workspace: Workspace, case: Case, attempt: int, run_id: str,
             override: Path | None, agent_name: str | None) -> dict:
    run_dir = cfg.state_dir / "evals" / run_id / f"{case.id}-{attempt}"
    prompt = build_prompt(cfg, template_for(cfg, case.kind, override), case_variables(cfg, case))
    with workspace.checkout(case.base_sha, f"eval-{run_id}-{slug(case.id)}-{attempt}") as worktree:
        bot = agent.run(cfg.agent(agent_name or agent_for(cfg, case.kind)), prompt, worktree, run_dir)
        files, diff = workspace.changes(worktree, case.base_sha)
    (run_dir / "diff.patch").write_text(diff)
    checks = check(case.expect, bot.result, files)
    criteria, error, cost = [], bot.error, bot.cost_usd or 0.0
    if bot.ok and case.rubric:
        verdict = judge(cfg, case, prompt, bot.result, diff, run_dir)
        cost += verdict.cost_usd or 0.0
        if verdict.ok:
            criteria = verdict.result["criteria"]
        else:
            error = f"judge failed: {verdict.error}"
    met = [bool(c.get("met")) for c in criteria]
    passed = bot.ok and not error and all(ok for ok, _ in checks.values()) and all(met)
    return {"case_id": case.id, "attempt": attempt, "passed": passed,
            "action": bot.result.action if bot.result else None, "checks": checks, "criteria": criteria,
            "score": (sum(met) / len(met)) if met else None, "cost_usd": cost, "seconds": bot.seconds,
            "error": error, "run_dir": str(run_dir)}


def run(cfg: Config, case_files: list[Path], *, label: str = "", repeat: int = 1, parallel: int = 1,
        prompt: Path | None = None, agent_name: str | None = None, echo=print) -> str:
    cases = [Case.load(path) for path in case_files]
    if not cases:
        raise ValueError("no eval cases found")
    workspace = Workspace(cfg)
    for case in cases:
        workspace.ensure_commit(case.base_sha)
    started = datetime.now(timezone.utc)
    run_id = f"{started:%Y%m%dT%H%M%S}"
    phash = prompt_hash(cfg, cases, prompt)
    db = connect(cfg)
    db.execute("insert into runs values (?,?,?,?,?,?,?,?,?)",
               (run_id, started.isoformat(timespec="seconds"), label, phash,
                agent_name or "(configured)", cfg.eval_judge, len(cases) * repeat, 0, 0.0))
    db.commit()
    jobs = [(case, attempt) for case in cases for attempt in range(1, repeat + 1)]
    passed, cost = 0, 0.0
    with ThreadPoolExecutor(max_workers=max(1, parallel)) as pool:
        futures = [pool.submit(run_case, cfg, workspace, case, attempt, run_id, prompt, agent_name)
                   for case, attempt in jobs]
        for future in futures:
            row = future.result()
            passed += row["passed"]
            cost += row["cost_usd"]
            db.execute("insert into results values (?,?,?,?,?,?,?,?,?,?,?,?)", (
                run_id, row["case_id"], row["attempt"], int(row["passed"]), row["action"],
                json.dumps(row["checks"]), json.dumps(row["criteria"]), row["score"], row["cost_usd"],
                row["seconds"], row["error"], row["run_dir"]))
            db.execute("update runs set passed=?, cost_usd=? where id=?", (passed, cost, run_id))
            db.commit()
            echo(f"{'PASS' if row['passed'] else 'FAIL'} {row['case_id']}#{row['attempt']} "
                 f"action={row['action']} score={row['score']} {row['error'] or ''}".rstrip())
    echo(f"run {run_id}: {passed}/{len(jobs)} passed, prompt {phash}, ${cost:.4f}")
    return run_id


def capture(cfg: Config, run_id: str, case_id: str | None = None) -> Path:
    """Turn a recorded production run into an eval case file (add a rubric by hand)."""
    run_dir = cfg.state_dir / "runs" / run_id
    meta = json.loads((run_dir / "meta.json").read_text())
    if not cfg.eval_cases:
        raise ValueError("set evals.cases in the config to capture cases")
    result_file = run_dir / "result.json"
    got = json.loads(result_file.read_text()) if result_file.exists() else None
    case_id = case_id or slug(run_id.split("-", 1)[-1], 60)
    path = cfg.eval_cases / f"{case_id}.yml"
    if path.exists():
        raise FileExistsError(path)
    cfg.eval_cases.mkdir(parents=True, exist_ok=True)
    header = (f"# Captured from run {run_id}. The bot answered: "
              f"{got['action'] + ' - ' + got['title'] if got else 'no usable result'}\n"
              "# Fill in `expect` and `rubric`: what a good answer must contain or avoid.\n")
    variables = {k: v for k, v in meta["variables"].items() if k != "job_prompt"}
    body = {"id": case_id, "kind": meta["kind"], "base_sha": meta["base_sha"],
            "expect": {}, "rubric": "", "notes": "", "variables": variables}
    path.write_text(header + yaml.dump(body, Dumper=_BlockDumper, sort_keys=False, allow_unicode=True, width=120))
    return path


class _BlockDumper(yaml.SafeDumper):
    """Writes multi-line strings (logs, prompts) as readable `|` blocks."""

def _block(dumper: yaml.SafeDumper, value: str):
    if "\n" not in value:
        return dumper.represent_scalar("tag:yaml.org,2002:str", value)
    # YAML block scalars cannot carry trailing spaces; they mean nothing in logs or prompts.
    value = "\n".join(line.rstrip() for line in value.split("\n"))
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style="|")


_BlockDumper.add_representer(str, _block)


def report(cfg: Config, limit: int = 20) -> list[tuple]:
    return connect(cfg).execute(
        "select id, started_at, label, prompt_hash, agent, passed, cases, cost_usd from runs "
        "order by started_at desc limit ?", (limit,)).fetchall()


def results(cfg: Config, run_id: str) -> list[tuple]:
    return connect(cfg).execute(
        "select case_id, attempt, passed, action, score, checks, criteria, error from results "
        "where run_id=? order by case_id, attempt", (run_id,)).fetchall()


def compare(cfg: Config, first: str, second: str) -> list[tuple]:
    """Per case: pass rate in each run."""
    return connect(cfg).execute(
        "select case_id, avg(case when run_id=? then passed end), avg(case when run_id=? then passed end) "
        "from results where run_id in (?, ?) group by case_id order by case_id",
        (first, second, first, second)).fetchall()
