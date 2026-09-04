"""Run one bot: gather context, decide whether a model is needed, invoke it.

A *bot* is a recurring agent invocation defined as data: ``bots/<name>/bot.yml``
plus a prompt file. The runner is the only code, so adding a bot is adding a
directory — the same trade the extract layer makes with source ymls.

Shape of a run:

1. **Context** — each entry in ``context:`` runs a command and its stdout is
   substituted into the prompt at ``{{KEY}}``. Deterministic work stays in
   normal code (``monitoring/digest.py``), not in the model.
2. **Gate** — an optional regex over one context value that ends the run
   before any model call. A healthy pipeline therefore costs zero tokens.
3. **Model** — the alias named by ``model:`` is resolved through
   ``bots/models.yml`` and called via its provider (see providers.py).
4. **Report** — written to ``bots/runs/<name>/<utc-ts>.md`` and returned, so
   an Airflow task, a shell run, and a future bot that reads other bots'
   reports all see the same artifact.

Nothing here knows a vendor, a model name, or an endpoint: those live in the
machine-local models.yml.
"""
from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

import yaml

import providers

BOTS_ROOT = pathlib.Path(__file__).resolve().parent
REPO_ROOT = BOTS_ROOT.parent
RUNS_DIR = BOTS_ROOT / "runs"
DEFAULT_MODELS_CONFIG = BOTS_ROOT / "models.yml"

# A bot's context commands inspect the deployment (the sink, the warehouse), so
# a manual run must see what the systemd units see. Loading the rendered env
# file makes `run_bot` from a shell and the scheduled task identical.
sys.path.insert(0, str(REPO_ROOT / "orchestration" / "include"))
import deployment  # noqa: E402

deployment.load_env()

#: verdict-ish first line for a run that ended at the gate
SKIPPED = "skipped"


class BotError(RuntimeError):
    """The bot definition or its wiring is wrong."""


@dataclass
class BotRun:
    """What one invocation did. Returned to Airflow as the task's XCom."""

    bot: str
    status: str                      # ok | skipped | busy
    report_path: str | None
    model_alias: str | None
    report: str
    context: dict[str, str] = field(default_factory=dict)


def discover(root: pathlib.Path | None = None) -> list[pathlib.Path]:
    """Every bot definition, sorted. ``bots/<name>/bot.yml``."""
    root = root or BOTS_ROOT
    return sorted(root.glob("*/bot.yml"))


def load_bot(path: str | os.PathLike) -> dict:
    path = pathlib.Path(path)
    if path.is_dir():
        path = path / "bot.yml"
    if not path.is_file():
        raise BotError(f"no bot definition at {path}")
    cfg = yaml.safe_load(path.read_text()) or {}
    cfg["dir"] = str(path.parent)
    cfg.setdefault("name", path.parent.name)
    if not cfg.get("prompt"):
        raise BotError(f"{path} must name a prompt file")
    if not cfg.get("schedule"):
        raise BotError(f"{path} must name a schedule (cron, or 'manual')")
    return cfg


def load_models(path: str | os.PathLike | None = None) -> dict:
    """Machine-local model wiring: alias -> provider + endpoint + key env var."""
    path = pathlib.Path(
        path or os.environ.get("BOTS_MODELS_CONFIG") or DEFAULT_MODELS_CONFIG
    )
    if not path.is_file():
        raise BotError(
            f"no model configuration at {path}. Copy bots/models.example.yml to "
            f"bots/models.yml and point the aliases at models you can reach."
        )
    cfg = yaml.safe_load(path.read_text()) or {}
    if not cfg.get("models"):
        raise BotError(f"{path} defines no models")
    return cfg


def resolve_model(alias: str | None, models_cfg: dict) -> dict:
    """Pick one alias. ``None`` means "whatever this machine calls default"."""
    name = alias or models_cfg.get("default")
    if not name:
        raise BotError("no model alias given and models.yml sets no default")
    entry = models_cfg["models"].get(name)
    if entry is None:
        raise BotError(
            f"unknown model alias {name!r} (models.yml defines: "
            f"{sorted(models_cfg['models'])})"
        )
    model = dict(entry)
    model["alias"] = name
    if not model.get("provider"):
        raise BotError(f"model alias {name!r} names no provider")
    if not model.get("model") and model["provider"] != "command":
        raise BotError(f"model alias {name!r} names no model id")
    return model


def _run_command(spec: dict | list | str, bot_dir: pathlib.Path) -> str:
    """Run one context/preflight command and return its stdout.

    Two path rules, both repo-root-relative so a bot can reuse existing tooling
    without knowing where the checkout lives:

    * ``cwd`` is resolved against the repo root (default: the bot's directory);
    * the *program* (``command[0]``) is resolved against the repo root when it
      contains a ``/``, so ``orchestration/.venv/bin/python`` works from any
      ``cwd``. A bare name (``python3``) is found on PATH. Later arguments are
      the program's own business and stay relative to ``cwd`` — which is why
      the pipeline_check bot passes ``digest.py`` with ``cwd: monitoring``.
    """
    if isinstance(spec, (list, str)):
        spec = {"command": spec}
    command = spec.get("command")
    if not command:
        raise BotError(f"context entry has no command: {spec}")
    shell = isinstance(command, str)
    cwd = spec.get("cwd")
    workdir = (REPO_ROOT / cwd) if cwd else bot_dir
    if not workdir.is_dir():
        raise BotError(f"context cwd does not exist: {workdir}")

    if not shell:
        command = [str(part) for part in command]
        program = pathlib.Path(command[0])
        if not program.is_absolute() and program.parent != pathlib.Path("."):
            resolved = REPO_ROOT / program
            if not resolved.exists():
                raise BotError(f"command program not found in the repo: {program}")
            command[0] = str(resolved)

    proc = subprocess.run(
        command,
        shell=shell,
        cwd=str(workdir),
        capture_output=True,
        text=True,
        timeout=float(spec.get("timeout_s", 600)),
    )
    if proc.returncode != 0:
        raise BotError(
            f"context command {command!r} exited {proc.returncode}: "
            f"{proc.stderr.strip()[:800]}"
        )
    return proc.stdout.strip()


def build_prompt(cfg: dict) -> tuple[str, dict[str, str]]:
    """Substitute every ``{{KEY}}`` in the prompt file with its context value."""
    bot_dir = pathlib.Path(cfg["dir"])
    prompt_path = bot_dir / cfg["prompt"]
    if not prompt_path.is_file():
        raise BotError(f"prompt file not found: {prompt_path}")
    prompt = prompt_path.read_text()

    context: dict[str, str] = {}
    for key, spec in (cfg.get("context") or {}).items():
        context[key] = _run_command(spec, bot_dir)

    for key, value in context.items():
        prompt = prompt.replace(f"{{{{{key}}}}}", value)

    unresolved = sorted(set(re.findall(r"\{\{([A-Z0-9_]+)\}\}", prompt)))
    if unresolved:
        raise BotError(f"{prompt_path} has placeholders with no context: {unresolved}")
    return prompt, context


def _write_report(name: str, text: str) -> pathlib.Path:
    directory = RUNS_DIR / name
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.md"
    path.write_text(text if text.endswith("\n") else text + "\n", encoding="utf-8")
    return path


def prune_reports(name: str, keep_days: int) -> int:
    """Drop reports older than ``keep_days``. Returns how many were removed."""
    if keep_days <= 0:
        return 0
    directory = RUNS_DIR / name
    if not directory.is_dir():
        return 0
    cutoff = datetime.now(timezone.utc).timestamp() - keep_days * 86400
    removed = 0
    for path in directory.glob("*.md"):
        if path.stat().st_mtime < cutoff:
            path.unlink()
            removed += 1
    return removed


def run(cfg: dict | str | os.PathLike, *, dry_run: bool = False,
        models_config: str | os.PathLike | None = None) -> BotRun:
    """Execute one bot. ``dry_run`` prints the prompt and calls no model."""
    if not isinstance(cfg, dict):
        cfg = load_bot(cfg)
    name = cfg["name"]
    prompt, context = build_prompt(cfg)

    gate = cfg.get("gate") or {}
    if gate:
        key = gate.get("context")
        if key not in context:
            raise BotError(f"gate references unknown context key {key!r}")
        if re.search(gate.get("skip_if_matches", r"\A\Z"), context[key]):
            report = gate.get("skip_report", "no model call needed")
            body = f"{report}\n\n{context[key]}"
            path = None if dry_run else _write_report(name, body)
            return BotRun(bot=name, status=SKIPPED, report_path=str(path) if path else None,
                          model_alias=None, report=body, context=context)

    if dry_run:
        return BotRun(bot=name, status="ok", report_path=None,
                      model_alias=cfg.get("model"), report=prompt, context=context)

    model = resolve_model(cfg.get("model"), load_models(models_config))

    preflight = model.get("preflight")
    if preflight:
        try:
            _run_command(preflight, pathlib.Path(cfg["dir"]))
        except BotError as exc:
            body = f"skipped: model {model['alias']} preflight failed\n\n{exc}"
            path = _write_report(name, body)
            return BotRun(bot=name, status="busy", report_path=str(path),
                          model_alias=model["alias"], report=body, context=context)

    provider = providers.get_provider(model["provider"])
    try:
        report = provider(model, prompt)
    except providers.ProviderBusy as exc:
        body = f"skipped: model {model['alias']} is busy\n\n{exc}"
        path = _write_report(name, body)
        return BotRun(bot=name, status="busy", report_path=str(path),
                      model_alias=model["alias"], report=body, context=context)

    header = f"<!-- bot: {name} | model: {model['alias']} ({model['provider']}) -->"
    path = _write_report(name, f"{header}\n{report}")
    prune_reports(name, int(cfg.get("keep_report_days", 30)))
    return BotRun(bot=name, status="ok", report_path=str(path),
                  model_alias=model["alias"], report=report, context=context)
