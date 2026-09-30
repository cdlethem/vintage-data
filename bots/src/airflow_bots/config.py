"""Load and validate the bots YAML configuration.

One file describes everything: which repository and Airflow to talk to, which
agent command(s) to run, spend limits, the failure healer and scheduled jobs.
Relative paths resolve against the config file's directory; ``$VAR`` and
``${VAR}`` are expanded from the environment.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

PACKAGE_PROMPTS = Path(__file__).parent / "prompts"


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Agent:
    """A coding-agent CLI invocation. Placeholders: {prompt}, {prompt_file}, {workdir}, {run_dir}, {config_dir}."""

    command: tuple[str, ...]
    timeout_minutes: int = 30
    env: tuple[str, ...] = ("HOME", "PATH", "LANG", "USER")
    usage: str = "none"  # how to read tokens/cost: omp | claude | none


@dataclass(frozen=True)
class AutoMerge:
    enabled: bool = False
    paths: tuple[str, ...] = ("**",)
    require_checks: bool = True


@dataclass(frozen=True)
class Heal:
    enabled: bool = True
    schedule: str = "*/15 * * * *"
    lookback_hours: int = 24
    max_new_per_sweep: int = 3
    flaky_streaks: int | None = 3  # separate failure streaks in the lookback that mark a recovered task as flaky
    ignore: tuple[str, ...] = ()
    agent: str = "default"
    prompt: Path = PACKAGE_PROMPTS / "heal.md"
    auto_merge: AutoMerge = AutoMerge()


@dataclass(frozen=True)
class Job:
    name: str
    schedule: str
    prompt: Path
    agent: str = "default"
    enabled: bool = True
    auto_merge: AutoMerge = AutoMerge()


@dataclass(frozen=True)
class Limits:
    daily_usd: float | None = None
    daily_runs: int | None = None
    concurrency: int = 1
    pool: str | None = None  # Airflow pool shared by every bot task (create it yourself)


@dataclass(frozen=True)
class Config:
    path: Path
    github_repo: str
    base_branch: str
    token_env: str
    github_api: str
    author: str
    airflow_url: str
    airflow_ui_url: str
    airflow_username_env: str
    airflow_password_env: str
    airflow_token_env: str | None
    state_dir: Path
    instructions: Path | None
    agents: dict[str, Agent]
    limits: Limits
    heal: Heal
    jobs: dict[str, Job] = field(default_factory=dict)
    eval_cases: Path | None = None
    eval_judge: str = "default"
    eval_db: Path | None = None

    def agent(self, name: str) -> Agent:
        try:
            return self.agents[name]
        except KeyError:
            raise ConfigError(f"unknown agent {name!r}; define it under agents:") from None

    @property
    def respond_prompt(self) -> Path:
        return PACKAGE_PROMPTS / "respond.md"

    def secret(self, env_name: str | None) -> str | None:
        return os.environ.get(env_name) if env_name else None


def _expand(value: str) -> str:
    expanded = os.path.expandvars(value)
    missing = re.findall(r"\$\{?(\w+)", expanded)
    if missing:
        raise ConfigError(f"unset environment variable(s) in config: {', '.join(missing)}")
    return expanded


def _take(section: dict | None, where: str, allowed: set[str]) -> dict:
    section = section or {}
    if not isinstance(section, dict):
        raise ConfigError(f"{where} must be a mapping")
    unknown = set(section) - allowed
    if unknown:
        raise ConfigError(f"unknown key(s) in {where}: {', '.join(sorted(unknown))}")
    return section


def _auto_merge(raw: dict | None, where: str) -> AutoMerge:
    raw = _take(raw, where, {"enabled", "paths", "require_checks"})
    return AutoMerge(
        enabled=bool(raw.get("enabled", False)),
        paths=tuple(raw.get("paths", ("**",))),
        require_checks=bool(raw.get("require_checks", True)),
    )


def load(path: str | os.PathLike | None = None) -> Config:
    path = Path(path or os.environ.get("BOTS_CONFIG") or "bots.yml").resolve()
    if not path.is_file():
        raise ConfigError(f"no bots config at {path} (set BOTS_CONFIG)")
    raw = _take(yaml.safe_load(path.read_text()), "config", {
        "github", "airflow", "state_dir", "instructions", "agents", "limits", "heal", "jobs", "evals",
    })
    here = path.parent

    def rel(value: str | None) -> Path | None:
        return None if value is None else (here / _expand(str(value))).resolve()

    gh = _take(raw.get("github"), "github", {"repo", "base", "token_env", "api_url", "author"})
    if not gh.get("repo") or "/" not in gh["repo"]:
        raise ConfigError("github.repo must be 'owner/name'")
    af = _take(raw.get("airflow"), "airflow", {"url", "ui_url", "username_env", "password_env", "token_env"})
    if not af.get("url"):
        raise ConfigError("airflow.url is required")
    if not raw.get("state_dir"):
        raise ConfigError("state_dir is required")

    agents = {}
    for name, spec in (raw.get("agents") or {}).items():
        spec = _take(spec, f"agents.{name}", {"command", "timeout_minutes", "env", "usage"})
        if not spec.get("command") or not isinstance(spec["command"], list):
            raise ConfigError(f"agents.{name}.command must be a non-empty list")
        usage = spec.get("usage", "none")
        if usage not in ("omp", "claude", "none"):
            raise ConfigError(f"agents.{name}.usage must be omp, claude or none")
        agents[name] = Agent(
            command=tuple(_expand(str(part)).replace("{config_dir}", str(here)) for part in spec["command"]),
            timeout_minutes=int(spec.get("timeout_minutes", 30)),
            env=tuple(spec.get("env", Agent.env)),
            usage=usage,
        )
    if not agents:
        raise ConfigError("define at least one agent under agents:")

    lim = _take(raw.get("limits"), "limits", {"daily_usd", "daily_runs", "concurrency", "pool"})
    limits = Limits(
        daily_usd=None if lim.get("daily_usd") is None else float(lim["daily_usd"]),
        daily_runs=None if lim.get("daily_runs") is None else int(lim["daily_runs"]),
        concurrency=int(lim.get("concurrency", 1)),
        pool=lim.get("pool"),
    )

    hl = _take(raw.get("heal"), "heal", {
        "enabled", "schedule", "lookback_hours", "max_new_per_sweep", "flaky_streaks", "ignore", "agent", "prompt",
        "auto_merge",
    })
    heal = Heal(
        enabled=bool(hl.get("enabled", True)),
        schedule=hl.get("schedule", Heal.schedule),
        lookback_hours=int(hl.get("lookback_hours", Heal.lookback_hours)),
        max_new_per_sweep=int(hl.get("max_new_per_sweep", Heal.max_new_per_sweep)),
        flaky_streaks=hl.get("flaky_streaks", Heal.flaky_streaks),
        ignore=tuple(hl.get("ignore", ())),
        agent=hl.get("agent", "default"),
        prompt=rel(hl.get("prompt")) or Heal.prompt,
        auto_merge=_auto_merge(hl.get("auto_merge"), "heal.auto_merge"),
    )

    jobs = {}
    for name, spec in (raw.get("jobs") or {}).items():
        spec = _take(spec, f"jobs.{name}", {"schedule", "prompt", "agent", "enabled", "auto_merge"})
        if not spec.get("schedule") or not spec.get("prompt"):
            raise ConfigError(f"jobs.{name} needs schedule and prompt")
        jobs[name] = Job(
            name=name,
            schedule=spec["schedule"],
            prompt=rel(spec["prompt"]),
            agent=spec.get("agent", "default"),
            enabled=bool(spec.get("enabled", True)),
            auto_merge=_auto_merge(spec.get("auto_merge"), f"jobs.{name}.auto_merge"),
        )

    ev = _take(raw.get("evals"), "evals", {"cases", "judge", "db"})

    cfg = Config(
        path=path,
        github_repo=gh["repo"],
        base_branch=gh.get("base", "main"),
        token_env=gh.get("token_env", "GITHUB_TOKEN"),
        github_api=gh.get("api_url", "https://api.github.com").rstrip("/"),
        author=gh.get("author", "airflow-bots <airflow-bots@users.noreply.github.com>"),
        airflow_url=_expand(af["url"]).rstrip("/"),
        airflow_ui_url=_expand(af.get("ui_url") or af["url"]).rstrip("/"),
        airflow_username_env=af.get("username_env", "BOTS_API_USERNAME"),
        airflow_password_env=af.get("password_env", "BOTS_API_PASSWORD"),
        airflow_token_env=af.get("token_env"),
        state_dir=Path(_expand(str(raw["state_dir"]))),
        instructions=rel(raw.get("instructions")),
        agents=agents,
        limits=limits,
        heal=heal,
        jobs=jobs,
        eval_cases=rel(ev.get("cases")),
        eval_judge=ev.get("judge", "default"),
        eval_db=rel(ev.get("db")),
    )
    for name in [heal.agent, cfg.eval_judge, *(job.agent for job in jobs.values())]:
        cfg.agent(name)
    for prompt in [heal.prompt, *(job.prompt for job in jobs.values())]:
        if not prompt.is_file():
            raise ConfigError(f"prompt file not found: {prompt}")
    return cfg
