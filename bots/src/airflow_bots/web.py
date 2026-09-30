"""A page inside the Airflow UI: what the bots are doing, what they cost, and their settings.

Server-rendered HTML (FastAPI + Jinja), mounted in Airflow's API server at ``/bots`` and
linked from Airflow's navigation. It reads the same config file, spend ledger and run
records the bots write, and edits the config file in place, so the page, the CLI and a
text editor stay interchangeable.

Permissions follow Airflow's auth manager:
- viewing needs read access to the ``bots_heal`` DAG,
- changing limits, healing and jobs needs edit access to it,
- changing agent commands needs edit access to Airflow's configuration, because an agent
  command runs as the worker's user.
"""
from __future__ import annotations

import difflib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from urllib.parse import quote
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagDetails
from airflow.api_fastapi.core_api.security import GetUserDep

from . import config, evals, ledger
from .agent import parse_decision
from .github import LABEL, STATUS_LABELS, GitHub

HEAL_DAG = "bots_heal"
FILE_HEADER = ("# airflow-bots configuration. Also edited from the Bots page in the Airflow UI, which rewrites\n"
               "# this file (comments are not kept). Reference: bots/README.md in the airflow-bots package.\n")
RUN_ID = re.compile(r"^[0-9]{8}T[0-9]{6}-[a-z0-9-]+$")

templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
NAME = re.compile(r"^[A-Za-z0-9_.-]+$")
app = FastAPI(title="airflow-bots", docs_url=None, redoc_url=None, openapi_url=None)


# --------------------------------------------------------------------------- access
def _can(user, method: str) -> bool:
    return get_auth_manager().is_authorized_dag(method=method, details=DagDetails(id=HEAL_DAG), user=user)


def _can_edit_agents(user) -> bool:
    return get_auth_manager().is_authorized_configuration(method="PUT", user=user)


def viewer(user: GetUserDep):
    if not _can(user, "GET"):
        raise HTTPException(403, "You need read access to the bots_heal DAG to see this page.")
    return user


def _same_origin(request: Request) -> None:
    """The auth cookie is SameSite=Lax; this also rejects any cross-site form post that gets through."""
    origin = request.headers.get("origin") or request.headers.get("referer")
    if origin and not origin.startswith(f"{request.url.scheme}://{request.url.netloc}"):
        raise HTTPException(403, "Cross-site request refused.")


def _page(request: Request, name: str, user, **context) -> HTMLResponse:
    return templates.TemplateResponse(request, name, {"user": user, "base": request.scope.get("root_path", ""),
                                                      **context})


# --------------------------------------------------------------------------- data
def _config_path() -> Path:
    return Path(os.environ.get("BOTS_CONFIG") or "bots.yml").resolve()


def _load() -> tuple[config.Config | None, str | None]:
    try:
        return config.load(_config_path()), None
    except (config.ConfigError, ValueError, TypeError, yaml.YAMLError, OSError) as exc:
        return None, str(exc)


def _dag_status(cfg: config.Config) -> list[dict]:
    """Paused state and latest run of each bot DAG, straight from Airflow's database."""
    from sqlalchemy import select

    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.utils.session import create_session

    dag_ids = ([HEAL_DAG] if cfg.heal.enabled else []) + [f"bots_job__{name}" for name in cfg.jobs]
    rows = []
    with create_session() as session:
        models = {m.dag_id: m for m in session.scalars(select(DagModel).where(DagModel.dag_id.in_(dag_ids)))}
        for dag_id in dag_ids:
            last = session.scalars(select(DagRun).where(DagRun.dag_id == dag_id)
                                   .order_by(DagRun.run_after.desc()).limit(1)).first()
            model = models.get(dag_id)
            rows.append({"dag_id": dag_id, "found": model is not None,
                         "paused": model.is_paused if model else None,
                         "last_state": str(last.state) if last else None,
                         "last_run": last.run_after if last else None})
    return rows


_github_cache: dict = {}


def _open_work(cfg: config.Config) -> dict:
    """Open bot issues and PRs from GitHub, cached for a minute to keep the page quick."""
    if _github_cache.get("at", 0) > time.monotonic() - 60:
        return _github_cache["data"]
    token = cfg.secret(cfg.token_env)
    if not token:
        return {"error": f"Set {cfg.token_env} for the Airflow API server to see issues and pull requests here."}
    try:
        gh = GitHub(cfg.github_repo, token, cfg.github_api)
        rows = gh._pages(f"/issues?labels={LABEL}&state=open")
    except Exception as exc:  # the page still works without GitHub
        return {"error": f"Could not reach GitHub: {exc}"}
    status_by_label = {label: status for status, label in STATUS_LABELS.items()}
    items = []
    for row in rows:
        labels = {label["name"] for label in row.get("labels", [])}
        body = row.get("body") or ""
        decision = re.search(r"^\*\*(?:The decision|Decision needed):\*\* (.+)$", body, flags=re.M)
        items.append({"number": row["number"], "title": row["title"], "url": row["html_url"],
                      "kind": "PR" if "pull_request" in row else "issue",
                      "status": next((status_by_label[l] for l in labels if l in status_by_label), None),
                      "automerge": "bots:automerge" in labels,
                      "decision": decision.group(1) if decision else None})
    order = {"question": 0, "review": 1, "fixing": 2, "waiting": 3, None: 4}
    items.sort(key=lambda item: (order.get(item["status"], 4), item["kind"] != "PR", -item["number"]))
    data = {"items": items, "repo_url": f"https://github.com/{cfg.github_repo}"}
    _github_cache.update(at=time.monotonic(), data=data)
    return data


def _runs(cfg: config.Config, days: int) -> list[dict]:
    rows = ledger.entries(cfg.state_dir, datetime.now(timezone.utc) - timedelta(days=days))
    return list(reversed(rows))


# --------------------------------------------------------------------------- pages
@app.get("/", response_class=HTMLResponse)
def overview(request: Request, user=Depends(viewer)):
    cfg, error = _load()
    if cfg is None:
        return _page(request, "overview.html", user, config_error=error, config_path=_config_path())
    runs_today, spent_today = ledger.today(cfg.state_dir)
    return _page(request, "overview.html", user, cfg=cfg, config_error=None,
                 runs_today=runs_today, spent_today=spent_today,
                 paused_reason=ledger.over_limit(cfg.state_dir, cfg.limits),
                 dags=_dag_status(cfg), work=_open_work(cfg), recent=_runs(cfg, 7)[:10])


@app.get("/runs", response_class=HTMLResponse)
def runs(request: Request, days: int = 7, user=Depends(viewer)):
    cfg, error = _load()
    if cfg is None:
        return _page(request, "overview.html", user, config_error=error, config_path=_config_path())
    rows = _runs(cfg, min(max(days, 1), 90))
    return _page(request, "runs.html", user, cfg=cfg, rows=rows, days=days,
                 spent=sum(float(r.get("cost_usd") or 0) for r in rows))


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail(request: Request, run_id: str, user=Depends(viewer)):
    cfg, error = _load()
    if cfg is None or not RUN_ID.match(run_id):
        raise HTTPException(404, error or "No such run.")
    run_dir = cfg.state_dir / "runs" / run_id
    if not run_dir.is_dir():
        raise HTTPException(404, "No such run.")

    def read(name: str, limit: int = 200_000) -> str | None:
        path = run_dir / name
        return path.read_text(errors="replace")[-limit:] if path.is_file() else None

    result = json.loads(read("result.json") or "null")
    decision = None
    if result and result.get("decision"):
        try:
            decision = parse_decision(result["decision"])
        except ValueError:
            decision = None
    entry = next((row for row in ledger.entries(cfg.state_dir) if row.get("run_id") == run_id), {})
    return _page(request, "run.html", user, cfg=cfg, run_id=run_id, entry=entry, result=result, decision=decision,
                 meta=json.loads(read("meta.json") or "{}"), prompt=read("prompt.md"), diff=read("diff.patch"),
                 output=read("stdout.txt", 60_000), stderr=read("stderr.txt", 20_000))


@app.get("/settings", response_class=HTMLResponse)
def settings(request: Request, saved: str | None = None, user=Depends(viewer)):
    return _settings_page(request, user, saved=saved)


def _settings_page(request: Request, user, *, saved: str | None = None, error: str | None = None,
                   raw: dict | None = None) -> HTMLResponse:
    cfg, config_error = _load()
    path = _config_path()
    raw = raw if raw is not None else (yaml.safe_load(path.read_text()) if path.is_file() else {}) or {}
    prompts = _prompt_suggestions(path, raw)
    history = []
    if cfg:
        history_file = cfg.state_dir / "config_history.jsonl"
        if history_file.exists():
            history = [json.loads(line) for line in history_file.read_text().splitlines()[-10:]][::-1]
    return _page(request, "settings.html", user, raw=raw, cfg=cfg, config_error=config_error, config_path=path,
                 saved=saved, error=error, prompts=prompts, history=history,
                 can_edit=_can(user, "PUT"), can_edit_agents=_can_edit_agents(user))


def _prompt_suggestions(path: Path, raw: dict) -> list[str]:
    """Markdown files beside the config and beside each job's prompt, written the way the config writes paths."""
    folders = {""} | {os.path.dirname(str((job or {}).get("prompt") or "")) for job in (raw.get("jobs") or {}).values()}
    found = set()
    for folder in folders:
        try:
            directory = path.parent / os.path.expandvars(folder)
            found.update(os.path.join(folder, p.name) for p in directory.glob("*.md"))
        except OSError:
            continue
    return sorted(found)


@app.get("/evals", response_class=HTMLResponse)
def eval_runs(request: Request, user=Depends(viewer)):
    cfg, error = _load()
    if cfg is None:
        return _page(request, "overview.html", user, config_error=error, config_path=_config_path())

    keys = ("id", "started_at", "label", "prompt_hash", "agent", "passed", "cases", "cost_usd")
    rows = [dict(zip(keys, row)) for row in evals.report(cfg, 50)] if evals.db_path(cfg).exists() else []
    return _page(request, "evals.html", user, cfg=cfg, rows=rows)


@app.get("/evals/{run_id}", response_class=HTMLResponse)
def eval_detail(request: Request, run_id: str, user=Depends(viewer)):
    cfg, error = _load()
    if cfg is None or not evals.db_path(cfg).exists():
        raise HTTPException(404, error or "No eval results yet.")

    keys = ("case_id", "attempt", "passed", "action", "score", "checks", "criteria", "error")
    rows = [dict(zip(keys, row)) for row in evals.results(cfg, run_id)]
    if not rows:
        raise HTTPException(404, "No such eval run.")
    for row in rows:
        row["checks"] = json.loads(row["checks"] or "{}")
        row["criteria"] = json.loads(row["criteria"] or "[]")
        seen = cfg.state_dir / "evals" / run_id / f"{row['case_id']}-{row['attempt']}" / "seen.md"
        row["seen"] = seen.read_text() if seen.is_file() else None
    return _page(request, "eval.html", user, cfg=cfg, run_id=run_id, rows=rows)


@app.post("/dags/{dag_id}/pause")
async def pause(request: Request, dag_id: str, user=Depends(viewer)):
    """Switch a bot DAG on or off, the same change as the toggle on Airflow's own DAG page."""
    _same_origin(request)
    if dag_id != HEAL_DAG and not dag_id.startswith("bots_job__"):
        raise HTTPException(404, "Not a bot DAG.")
    if not get_auth_manager().is_authorized_dag(method="PUT", details=DagDetails(id=dag_id), user=user):
        raise HTTPException(403, f"You need edit access to {dag_id} to switch it on or off.")
    from airflow.models.dag import DagModel
    from airflow.utils.session import create_session

    paused = (await request.form()).get("paused") == "yes"
    with create_session() as session:
        model = session.get(DagModel, dag_id)
        if model is None:
            raise HTTPException(404, "Airflow has not parsed this DAG yet.")
        model.is_paused = paused
    return RedirectResponse(f"{request.scope.get('root_path', '')}/", status_code=303)


@app.post("/settings", response_class=HTMLResponse)
async def save_settings(request: Request, user=Depends(viewer)):
    _same_origin(request)
    form = await request.form()
    section = str(form.get("section", ""))
    if not _can(user, "PUT") or (section.startswith("agent") and not _can_edit_agents(user)):
        raise HTTPException(403, "You are not allowed to change this setting.")
    path = _config_path()
    raw = yaml.safe_load(path.read_text()) or {}
    try:
        message = apply_form(raw, section, form)
        validate_schedules(raw)
        error = write_config(path, raw, user.get_name())
    except ValueError as exc:
        error = str(exc)
    if error:
        return _settings_page(request, user, error=error, raw=raw)
    return RedirectResponse(f"{request.scope.get('root_path', '')}/settings?saved={quote(message)}", status_code=303)


# --------------------------------------------------------------------------- editing
def _lines(value) -> list[str]:
    return [line.strip() for line in str(value or "").splitlines() if line.strip()]


def _number(value, kind=int):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return kind(text)
    except ValueError:
        raise ValueError(f"{text!r} is not a number") from None


def _set(section: dict, key: str, value) -> None:
    """Blank form fields fall back to the default by removing the key."""
    if value is None or value == [] or value == "":
        section.pop(key, None)
    else:
        section[key] = value


def _section(parent: dict, key: str) -> dict:
    """The mapping at ``parent[key]``, created when missing or empty (``heal:`` with nothing under it)."""
    if not isinstance(parent.get(key), dict):
        parent[key] = {}
    return parent[key]


def _name(form, existing: dict) -> str:
    name = str(form.get("name") or "").strip()
    if not NAME.match(name):
        raise ValueError("names may only use letters, digits, '_', '.' and '-'")
    if form.get("new") and name in existing:
        raise ValueError(f"{name!r} already exists; edit it below instead")
    return name


def _auto_merge(section: dict, form) -> None:
    """Edit auto_merge in place so settings the page does not show (require_checks) survive."""
    auto_merge = _section(section, "auto_merge")
    auto_merge["enabled"] = form.get("auto_merge_enabled") == "on"
    _set(auto_merge, "paths", _lines(form.get("auto_merge_paths")))


def apply_form(raw: dict, section: str, form) -> str:
    """Apply one settings form to the raw config mapping. Returns a short confirmation."""
    if section == "limits":
        limits = _section(raw, "limits")
        _set(limits, "daily_runs", _number(form.get("daily_runs")))
        _set(limits, "daily_usd", _number(form.get("daily_usd"), float))
        _set(limits, "concurrency", _number(form.get("concurrency")))
        _set(limits, "pool", str(form.get("pool") or "").strip())
        return "Limits saved"
    if section == "heal":
        heal = _section(raw, "heal")
        heal["enabled"] = form.get("enabled") == "on"
        for key in ("schedule", "agent"):
            _set(heal, key, str(form.get(key) or "").strip())
        for key in ("lookback_hours", "max_new_per_sweep", "flaky_streaks"):
            _set(heal, key, _number(form.get(key)))
        _set(heal, "ignore", _lines(form.get("ignore")))
        _auto_merge(heal, form)
        return "Healing saved"
    if section == "job":
        jobs = _section(raw, "jobs")
        name = _name(form, jobs)
        if form.get("delete") == "yes":
            jobs.pop(name, None)
            return f"Job {name} removed"
        job = _section(jobs, name)
        for key in ("schedule", "prompt", "agent"):
            _set(job, key, str(form.get(key) or "").strip())
        if form.get("new"):  # afterwards the job is switched on and off by pausing its DAG
            job["enabled"] = form.get("enabled") == "on"
        _auto_merge(job, form)
        return f"Job {name} saved"
    if section == "agent":
        agents = _section(raw, "agents")
        name = _name(form, agents)
        if form.get("delete") == "yes":
            agents.pop(name, None)
            return f"Agent {name} removed"
        agent = _section(agents, name)
        _set(agent, "command", _lines(form.get("command")))
        _set(agent, "timeout_minutes", _number(form.get("timeout_minutes")))
        _set(agent, "env", _lines(form.get("env")))
        _set(agent, "usage", str(form.get("usage") or "").strip())
        return f"Agent {name} saved"
    raise ValueError(f"unknown settings section {section!r}")


def validate_schedules(raw: dict) -> None:
    """Check cron expressions the way Airflow will, so a typo cannot take the bot DAGs down."""
    from airflow.timetables.trigger import CronTriggerTimetable

    schedules = [("healing", (raw.get("heal") or {}).get("schedule"))]
    schedules += [(f"job {name}", (job or {}).get("schedule")) for name, job in (raw.get("jobs") or {}).items()]
    for where, expression in schedules:
        if expression:
            try:
                CronTriggerTimetable(expression, timezone="UTC").validate()
            except Exception as exc:
                raise ValueError(f"The {where} schedule {expression!r} is not a valid cron expression: {exc}") from None


class _PlainDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):  # write repeated values out in full, never as &anchors
        return True


def write_config(path: Path, raw: dict, who: str) -> str | None:
    """Validate and atomically replace the config file; record the change. Returns an error or None."""
    text = FILE_HEADER + yaml.dump(raw, Dumper=_PlainDumper, sort_keys=False, allow_unicode=True, width=120)
    candidate = path.with_name(f".{path.name}.candidate")
    candidate.write_text(text)
    try:
        cfg = config.load(candidate)
    except (config.ConfigError, ValueError, TypeError) as exc:
        candidate.unlink()
        return str(exc)
    before = path.read_text() if path.exists() else ""
    os.replace(candidate, path)
    diff = "".join(difflib.unified_diff(before.splitlines(True), text.splitlines(True), "before", "after", n=1))
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    with (cfg.state_dir / "config_history.jsonl").open("a") as history:
        history.write(json.dumps({"at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                  "who": who, "diff": diff}) + "\n")
    return None
