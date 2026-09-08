"""Typed, deadline-bounded runner for read-only specialist and admitted bots."""
from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import pathlib
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import providers
import usage as usage_tools
import yaml

BOTS_ROOT = pathlib.Path(__file__).resolve().parent
REPO_ROOT = BOTS_ROOT.parent
DEFAULT_MODELS_CONFIG = BOTS_ROOT / "models.yml"
sys.path.insert(0, str(REPO_ROOT / "orchestration" / "include"))
import deployment

deployment.load_env()

log = logging.getLogger(__name__)
LOCKS_DIR = pathlib.Path(
    os.environ.get(
        "BOTS_LOCKS_DIR",
        pathlib.Path(os.environ.get("EXTRACT_DATA_ROOT", "/tmp")) / "state/bots/locks",
    )
)
CONTEXT_LIMITS = {
    "source_discovery": 32 * 1024,
    "source_vetting": 24 * 1024,
    "source_scheduling": 24 * 1024,
    "cadence_review": 32 * 1024,
    "failure_triage": 48 * 1024,
    "analytics_engineer": 32 * 1024,
    "data_analyst": 40 * 1024,
    "manager": 96 * 1024,
    "task_executor": 96 * 1024,
    "pr_reviewer": 96 * 1024,
}
BOT_KEYS = {
    "name", "description", "schedule", "enabled", "model", "prompt",
    "timeout_minutes", "context_budget_minutes", "model_budget_minutes",
    "publication_budget_minutes", "cleanup_margin_seconds", "capacity_policy",
    "retry_on", "requires_capabilities", "context", "triggers", "gate", "output",
    "freshness_sla_minutes", "runtime_estimate_minutes", "retries",
}
MODEL_KEYS = {
    "provider", "model", "endpoint", "api_key_env", "system", "max_tokens",
    "temperature", "params", "headers", "timeout_s", "argv", "inherit_env",
    "pass_env", "env", "cwd", "capabilities", "max_concurrency", "preflight",
    "usage",
}


class BotError(RuntimeError):
    """Terminal bot definition, context, or report error with a stable code."""

    def __init__(self, detail: str = "", code: str = "runner_failed"):
        self.code = re.sub(r"[^a-z0-9_]", "_", str(code).lower())[:100] or "runner_failed"
        super().__init__(detail or self.code)


@dataclass(frozen=True)
class RunResult:
    projection: dict
    outcome: str
    retry_class: str
    reason_code: str
    debug_prompt: str | None = None

    def xcom(self) -> dict:
        return dict(self.projection)


class RunBudget:
    def __init__(self, deadline_at: datetime, cleanup_margin_seconds: int):
        if deadline_at.tzinfo is None:
            deadline_at = deadline_at.replace(tzinfo=timezone.utc)
        self.deadline_at = deadline_at
        wall_remaining = (deadline_at - datetime.now(timezone.utc)).total_seconds()
        self._monotonic_deadline = time.monotonic() + max(0, wall_remaining)
        self.cleanup_margin_seconds = cleanup_margin_seconds

    def remaining(self, *, include_cleanup: bool = False) -> float:
        remaining = self._monotonic_deadline - time.monotonic()
        if not include_cleanup:
            remaining -= self.cleanup_margin_seconds
        return max(0.0, remaining)

    def timeout(self, cap_seconds: float, *, include_cleanup: bool = False) -> float:
        remaining = self.remaining(include_cleanup=include_cleanup)
        if remaining <= 0:
            raise providers.ProviderTimeout("run deadline exhausted")
        return max(0.05, min(float(cap_seconds), remaining))


def discover(root: pathlib.Path | None = None) -> list[pathlib.Path]:
    return sorted((root or BOTS_ROOT).glob("*/bot.yml"))


def _require_int(cfg: dict, key: str, *, minimum: int = 1) -> int:
    value = cfg.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise BotError(f"{key} must be an integer >= {minimum}")
    return value


def load_bot(path: str | os.PathLike) -> dict:
    path = pathlib.Path(path)
    if path.is_dir():
        path = path / "bot.yml"
    if not path.is_file():
        raise BotError(f"no bot definition at {path}")
    cfg = yaml.safe_load(path.read_text()) or {}
    if not isinstance(cfg, dict):
        raise BotError(f"{path} must contain an object")
    unknown = sorted(set(cfg) - BOT_KEYS)
    if unknown:
        raise BotError(f"{path} contains unknown keys: {unknown}")
    cfg = dict(cfg)
    cfg.setdefault("name", path.parent.name)
    cfg["dir"] = str(path.parent)
    if cfg["name"] != path.parent.name or not re.fullmatch(r"[a-z][a-z0-9_]*", cfg["name"]):
        raise BotError(f"{path} name must match its directory")
    if not isinstance(cfg.get("prompt"), str) or not cfg["prompt"]:
        raise BotError(f"{path} must name a prompt file")
    if not isinstance(cfg.get("schedule"), str) or not cfg["schedule"]:
        raise BotError(f"{path} must name a schedule")
    total = _require_int(cfg, "timeout_minutes") * 60
    context_budget = _require_int(cfg, "context_budget_minutes") * 60
    model_budget = _require_int(cfg, "model_budget_minutes") * 60
    cleanup = _require_int(cfg, "cleanup_margin_seconds")
    if context_budget + model_budget + cleanup > total:
        raise BotError(f"{path} configured sub-budgets exceed timeout_minutes")
    if cfg.get("capacity_policy") not in {"skip", "retry"}:
        raise BotError(f"{path} capacity_policy must be skip or retry")
    retry_on = cfg.get("retry_on")
    if not isinstance(retry_on, list) or any(item not in {"capacity", "transient"} for item in retry_on):
        raise BotError(f"{path} retry_on contains an unsupported class")
    contexts = cfg.get("context") or {}
    if not isinstance(contexts, dict):
        raise BotError(f"{path} context must be an object")
    context_caps = 0.0
    for key, spec in contexts.items():
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key) or not isinstance(spec, dict):
            raise BotError(f"{path} has an invalid context entry")
        if set(spec) - {"command", "cwd", "timeout_s"}:
            raise BotError(f"{path} context {key} has unknown keys")
        command = spec.get("command")
        if not isinstance(command, list) or not command or not all(isinstance(item, str) and item for item in command):
            raise BotError(f"{path} context {key} command must be argv")
        timeout_s = spec.get("timeout_s")
        if not isinstance(timeout_s, (int, float)) or isinstance(timeout_s, bool) or timeout_s <= 0:
            raise BotError(f"{path} context {key} timeout_s must be positive")
        context_caps += timeout_s
    if context_caps > context_budget:
        raise BotError(f"{path} context timeout caps exceed context_budget_minutes")
    gate = cfg.get("gate")
    if gate is not None:
        if not isinstance(gate, dict) or set(gate) != {"context", "path", "operator", "value", "reason_code"}:
            raise BotError(f"{path} gate must use the typed gate contract")
        if gate["context"] not in contexts or gate["operator"] != "equals":
            raise BotError(f"{path} gate references invalid context or operator")
        if not isinstance(gate["path"], str) or not isinstance(gate["reason_code"], str):
            raise BotError(f"{path} gate path and reason_code must be strings")
    output = cfg.get("output")
    if not isinstance(output, dict) or set(output) != {"format", "schema"} or output["format"] != "json":
        raise BotError(f"{path} output must name one JSON schema")
    if not isinstance(output["schema"], str) or not output["schema"]:
        raise BotError(f"{path} output schema is required")
    capabilities = cfg.get("requires_capabilities") or []
    if not isinstance(capabilities, list) or not all(isinstance(item, str) and item for item in capabilities):
        raise BotError(f"{path} requires_capabilities must be names")
    triggers = cfg.get("triggers") or []
    if not isinstance(triggers, list) or not all(isinstance(item, str) for item in triggers):
        raise BotError(f"{path} triggers must be bot names")
    for downstream in triggers:
        if not (path.parent.parent / downstream / "bot.yml").is_file():
            raise BotError(f"{path} triggers unknown bot {downstream!r}")
    cfg["triggers"] = triggers
    cfg["retries"] = 1
    return cfg


def load_models(path: str | os.PathLike | None = None) -> dict:
    path = pathlib.Path(path or os.environ.get("BOTS_MODELS_CONFIG") or DEFAULT_MODELS_CONFIG)
    if not path.is_file():
        raise BotError(f"no model configuration at {path}")
    cfg = yaml.safe_load(path.read_text()) or {}
    if not isinstance(cfg, dict) or set(cfg) - {"default", "concurrency", "models", "bots", "pricing"}:
        raise BotError(f"{path} has invalid top-level model keys")
    if not isinstance(cfg.get("models"), dict) or not cfg["models"]:
        raise BotError(f"{path} defines no models")
    concurrency = cfg.get("concurrency") or {}
    if set(concurrency) - {"max_active"}:
        raise BotError(f"{path} concurrency has unknown keys")
    if not isinstance(concurrency.get("max_active", 2), int) or concurrency.get("max_active", 2) < 1:
        raise BotError(f"{path} concurrency.max_active must be positive")
    pricing = cfg.get("pricing")
    if pricing is not None:
        if not isinstance(pricing, dict) or set(pricing) - {"id", "currency", "models"}:
            raise BotError(f"{path} pricing has invalid keys")
        if not isinstance(pricing.get("id"), str) or not pricing["id"]:
            raise BotError(f"{path} pricing.id is required")
        if pricing.get("currency") != "USD":
            raise BotError(f"{path} pricing.currency must be USD")
        prices = pricing.get("models")
        if not isinstance(prices, dict):
            raise BotError(f"{path} pricing.models must be an object")
        for price_name, rates in prices.items():
            if not isinstance(price_name, str) or not isinstance(rates, dict) or set(rates) != {"input", "output", "cached_input", "cache_write"}:
                raise BotError(f"{path} pricing model {price_name!r} is invalid")
            if any(isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0 for value in rates.values()):
                raise BotError(f"{path} pricing model {price_name!r} rates must be positive")
    for alias, model in cfg["models"].items():
        if not isinstance(alias, str) or not isinstance(model, dict) or set(model) - MODEL_KEYS:
            raise BotError(f"{path} model {alias!r} has invalid keys")
        provider = model.get("provider")
        if provider not in providers.REGISTRY:
            raise BotError(f"{path} model {alias!r} has unknown provider")
        usage_spec = model.get("usage")
        if usage_spec is not None:
            if not isinstance(usage_spec, dict) or set(usage_spec) - {"source", "format", "path", "dir", "marker"}:
                raise BotError(f"{path} model {alias!r} usage has invalid keys")
            source, fmt = usage_spec.get("source"), usage_spec.get("format")
            if source not in {"file", "stdout_trailer", "session_dir"} or fmt not in {"openai", "anthropic", "normalized", "omp_session"}:
                raise BotError(f"{path} model {alias!r} usage source or format is invalid")
            if source == "file" and ("path" not in usage_spec or not any("{usage_file}" in item for item in model.get("argv", []))):
                raise BotError(f"{path} model {alias!r} file usage requires {{usage_file}} in argv")
            if source == "session_dir" and not isinstance(usage_spec.get("dir"), str):
                raise BotError(f"{path} model {alias!r} session_dir usage requires dir")
            if source == "stdout_trailer" and not isinstance(usage_spec.get("marker"), str) or source == "stdout_trailer" and not usage_spec.get("marker"):
                raise BotError(f"{path} model {alias!r} stdout_trailer usage requires marker")
        if provider == "command":
            argv = model.get("argv")
            if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
                raise BotError(f"{path} command model {alias!r} requires argv")
        elif not model.get("endpoint") or not model.get("model"):
            raise BotError(f"{path} HTTP model {alias!r} requires endpoint and model")
        capabilities = model.get("capabilities") or []
        if not isinstance(capabilities, list) or not all(isinstance(item, str) for item in capabilities):
            raise BotError(f"{path} model {alias!r} capabilities must be names")
        maximum = model.get("max_concurrency", 1)
        if not isinstance(maximum, int) or isinstance(maximum, bool) or maximum < 1:
            raise BotError(f"{path} model {alias!r} max_concurrency must be positive")
    assignments = cfg.get("bots") or {}
    if not isinstance(assignments, dict):
        raise BotError(f"{path} bots must map names to aliases")
    known = set(cfg["models"])
    for bot, selection in assignments.items():
        aliases = [selection] if isinstance(selection, str) else selection
        if not isinstance(aliases, list) or not aliases or any(alias not in known for alias in aliases):
            raise BotError(f"{path} bot {bot!r} references unknown model aliases")
    if cfg.get("default") not in known:
        raise BotError(f"{path} default model alias is invalid")
    return cfg


def resolve_model(alias: str | None, models_cfg: dict) -> dict:
    name = alias or models_cfg["default"]
    try:
        model = dict(models_cfg["models"][name])
    except KeyError as exc:
        raise BotError(f"unknown model alias {name!r}") from exc
    model["alias"] = name
    model.setdefault("capabilities", [])
    model.setdefault("max_concurrency", 1)
    return model


def _model_aliases(cfg: dict, models_cfg: dict) -> list[str | None]:
    selected = cfg.get("_model_override")
    if selected is None:
        selected = (models_cfg.get("bots") or {}).get(cfg["name"], cfg.get("model"))
    if selected is None:
        return [None]
    aliases = [selected] if isinstance(selected, str) else selected
    if not isinstance(aliases, list) or not aliases or not all(isinstance(item, str) and item for item in aliases):
        raise BotError(f"bot {cfg['name']!r} model assignment is invalid")
    return list(dict.fromkeys(aliases))


def resolve_bot_models(cfg: dict, models_cfg: dict) -> list[dict]:
    aliases = _model_aliases(cfg, models_cfg)
    models: list[dict] = []
    unavailable: list[str] = []
    for ordinal, alias in enumerate(aliases):
        model = resolve_model(alias, models_cfg)
        key_name = model.get("api_key_env")
        if key_name and not os.environ.get(key_name):
            if ordinal == 0:
                raise BotError(
                    f"required model {model['alias']!r} is unavailable: {key_name} is unset"
                )
            unavailable.append(model["alias"])
            continue
        models.append(model)
    if unavailable:
        log.warning(
            "bot=%s optional_fallbacks_unavailable=%s",
            cfg["name"],
            ",".join(unavailable),
        )
    required = set(cfg.get("requires_capabilities") or [])
    for model in models:
        missing = sorted(required - set(model["capabilities"]))
        if missing:
            raise BotError(
                f"model {model['alias']!r} lacks required capabilities {missing}"
            )
        if model["provider"] == "command" and not model.get("cwd"):
            model["cwd"] = str(REPO_ROOT)
    if not models:
        raise BotError(f"bot {cfg['name']!r} has no available model")
    return models


def resolve_bot_model(cfg: dict, models_cfg: dict) -> dict:
    return resolve_bot_models(cfg, models_cfg)[0]


def _run_command(spec: dict, bot_dir: pathlib.Path, budget: RunBudget) -> str:
    command = list(spec["command"])
    program = pathlib.Path(command[0])
    if not program.is_absolute() and program.parent != pathlib.Path("."):
        resolved = REPO_ROOT / program
        if not resolved.exists():
            raise BotError(
                f"context command program is unavailable: {program.name}",
                code="context_program_missing",
            )
        command[0] = str(resolved)
    workdir = REPO_ROOT / spec["cwd"] if spec.get("cwd") else bot_dir
    if not workdir.is_dir():
        raise BotError("context command working directory is unavailable", code="context_cwd_missing")
    environment = dict(os.environ)
    for secret_name in (
        "BOT_DASHBOARD_API_PASSWORD",
        "BOT_DASHBOARD_API_USERNAME",
        "AIRFLOW__API__BASE_URL",
    ):
        environment.pop(secret_name, None)
    try:
        process = subprocess.run(
            command,
            check=False,
            cwd=workdir,
            capture_output=True,
            text=True,
            timeout=budget.timeout(float(spec["timeout_s"])),
            env=environment,
        )
    except subprocess.TimeoutExpired as exc:
        raise providers.ProviderTimeout("context command deadline expired") from exc
    if process.returncode:
        detail = (process.stderr or process.stdout or "").strip().splitlines()
        raise BotError(
            f"context command exited with code {process.returncode}: {detail[-1] if detail else 'no diagnostic output'}",
            code=f"context_command_exit_{process.returncode}",
        )
    return process.stdout.strip()


def _context_object(text: str) -> Any:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise BotError("context command returned invalid JSON", code="context_invalid_json") from exc
    if not isinstance(value, (dict, list)):
        raise BotError("context command must return an object or array", code="context_invalid_shape")
    return value


def build_prompt(cfg: dict, budget: RunBudget) -> tuple[str, dict[str, Any], dict]:
    started = time.monotonic()
    bot_dir = pathlib.Path(cfg["dir"])
    prompt_path = bot_dir / cfg["prompt"]
    if not prompt_path.is_file():
        raise BotError("prompt file is unavailable")
    prompt = prompt_path.read_text()
    supplied = cfg.get("_context_values") or {}
    if not isinstance(supplied, dict):
        raise BotError("supplied context must be an object")
    context: dict[str, Any] = {
        key: _context_object(value) if isinstance(value, str) else value
        for key, value in supplied.items()
    }
    for key, spec in (cfg.get("context") or {}).items():
        if key not in context:
            context[key] = _context_object(_run_command(spec, bot_dir, budget))
    canonical_context = json.dumps(context, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    limit = CONTEXT_LIMITS[cfg["name"]]
    if len(canonical_context) > limit:
        raise BotError(f"context exceeds {limit}-byte bound")
    for key, value in context.items():
        prompt = prompt.replace(
            f"{{{{{key}}}}}",
            json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        )
    unresolved = sorted(set(re.findall(r"\{\{([A-Z0-9_]+)\}\}", prompt)))
    if unresolved:
        raise BotError(f"prompt has unresolved context placeholders: {unresolved}")
    from airflow.providers.vintage.bot_dashboard.report_schemas import SCHEMAS
    report_model = SCHEMAS.get(cfg["output"]["schema"])
    if report_model is None:
        raise BotError("unknown output schema")
    prompt += "\n\nReturn one JSON object conforming to this exact output schema:\n" + json.dumps(
        report_model.model_json_schema(), separators=(",", ":"), ensure_ascii=False
    )
    digest = {
        "sha256": hashlib.sha256(canonical_context).hexdigest(),
        "byte_count": len(canonical_context),
        "build_ms": int((time.monotonic() - started) * 1000),
    }
    return prompt, context, digest


def _lookup(value: Any, path: str) -> Any:
    current = value
    for part in path.split(".") if path else []:
        if not isinstance(current, dict) or part not in current:
            raise BotError(f"typed gate path {path!r} is unavailable")
        current = current[part]
    return current


def _gate_skips(gate: dict, context: dict[str, Any]) -> bool:
    return _lookup(context[gate["context"]], gate["path"]) == gate["value"]


def work_pending(cfg: dict) -> bool:
    gate = cfg.get("gate")
    if not gate:
        return True
    deadline = datetime.now(timezone.utc) + timedelta(minutes=cfg["context_budget_minutes"])
    budget = RunBudget(deadline, 0)
    _, context, _ = build_prompt(cfg, budget)
    return not _gate_skips(gate, context)


def _slot_file(scope: str, index: int) -> pathlib.Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", scope)
    LOCKS_DIR.mkdir(parents=True, exist_ok=True)
    return LOCKS_DIR / f"{safe}.{index}.lock"


def _acquire_one(scope: str, maximum: int):
    for index in range(maximum):
        handle = _slot_file(scope, index).open("a+")
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return handle
        except BlockingIOError:
            handle.close()
    return None


@contextmanager
def _inference_slot(models_cfg: dict, model: dict):
    acquired = []
    try:
        global_handle = _acquire_one("global", int((models_cfg.get("concurrency") or {}).get("max_active", 2)))
        if global_handle is None:
            raise providers.ProviderBusy("global inference capacity is unavailable")
        acquired.append(global_handle)
        model_handle = _acquire_one(f"model-{model['alias']}", int(model.get("max_concurrency", 1)))
        if model_handle is None:
            raise providers.ProviderBusy("model inference capacity is unavailable")
        acquired.append(model_handle)
        yield
    finally:
        for handle in reversed(acquired):
            fcntl.flock(handle, fcntl.LOCK_UN)
            handle.close()


def _json_report(text: str, cfg: dict) -> dict:
    decoder = json.JSONDecoder()
    parsed = None
    for offset, character in enumerate(text):
        if character != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(text[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            parsed = candidate
            break
    if parsed is None:
        raise BotError("model report contains no JSON object")
    from airflow.providers.vintage.bot_dashboard.report_schemas import validate_named_report

    try:
        return validate_named_report(cfg["output"]["schema"], parsed)
    except (ValueError, TypeError) as exc:
        # Report field locations/types, never model-provided values or URLs.
        from pydantic import ValidationError
        details = ""
        if isinstance(exc, ValidationError):
            details = ": " + "; ".join(
                f"{'.'.join(map(str, error['loc']))}: {error['type']}"
                for error in exc.errors(include_input=False, include_context=False)[:8]
            )
        raise BotError("model report failed its named schema" + details) from exc


def _failure_detail(exc: Exception) -> str:
    detail = str(exc) or type(exc).__name__
    detail = re.sub(r"(?i)https?://[^\s/@:]+:[^\s/@]+@", "https://***@", detail)
    detail = re.sub(r"(?<![\w.])/(?:[^\s/]+/)+[^\s]+", "<path>", detail)
    return detail[:8192]


def _usage_total(attempts: list[dict]) -> dict | None:
    records = [attempt["usage"] for attempt in attempts if isinstance(attempt.get("usage"), dict)]
    return usage_tools.merge(records) if records else None

def _envelope(
    *,
    cfg: dict,
    identity: dict,
    started_at: datetime,
    deadline_at: datetime,
    outcome: str,
    retry_class: str,
    reason_code: str,
    failure: Exception | None,
    selected_model: str | None,
    attempts: list[dict],
    context_digest: dict,
    payload: dict | None,
) -> dict:
    finished_at = datetime.now(timezone.utc)
    failure_value = None
    if failure is not None and outcome in {"failed", "timed_out"}:
        from airflow.providers.vintage.bot_dashboard.report_schemas import failure_fingerprint

        code = getattr(failure, "code", reason_code)
        detail = _failure_detail(failure)
        failure_value = {
            "class": type(failure).__name__,
            "code": code,
            "fingerprint": failure_fingerprint(
                origin="bot_runner",
                component=cfg["name"],
                task=identity["task_id"],
                error_class=type(failure).__name__,
                code=code,
                detail=detail,
            ),
            "detail": detail,
        }
    return {
        "envelope_version": 1,
        "identity": {"bot": cfg["name"], **identity},
        "timing": {
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "deadline_at": deadline_at.isoformat(),
            "duration_ms": max(0, int((finished_at - started_at).total_seconds() * 1000)),
        },
        "outcome": outcome,
        "retry_class": retry_class,
        "reason_code": reason_code,
        "failure": failure_value,
        "selected_model": selected_model,
        "attempts": attempts,
        "usage_total": _usage_total(attempts),
        "context": context_digest,
        "payload_schema": cfg["output"]["schema"] if payload is not None else None,
        "payload": payload,
    }


def run(
    cfg: dict | str | os.PathLike,
    *,
    identity: dict | None = None,
    dry_run: bool = False,
    ephemeral: bool = False,
    models_config: str | os.PathLike | None = None,
    context_values: dict[str, Any] | None = None,
    model_override: str | list[str] | None = None,
    client=None,
) -> RunResult:
    if not isinstance(cfg, dict):
        cfg = load_bot(cfg)
    else:
        cfg = dict(cfg)
    if context_values is not None:
        cfg["_context_values"] = context_values
    if model_override is not None:
        cfg["_model_override"] = model_override
    if identity is None:
        now_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        identity = {
            "dag_id": f"cli__{cfg['name']}",
            "run_id": f"cli__{now_id}",
            "task_id": "run",
            "map_index": -1,
            "try_number": 1,
        }
    required_identity = {"dag_id", "run_id", "task_id", "map_index", "try_number"}
    if set(identity) != required_identity:
        raise BotError("run identity is incomplete")
    total_seconds = cfg["timeout_minutes"] * 60
    cleanup = cfg["cleanup_margin_seconds"]
    from provider_dashboard import DashboardClient

    control = client
    if dry_run or ephemeral:
        deadline_at = datetime.now(timezone.utc) + timedelta(seconds=total_seconds)
    else:
        control = control or DashboardClient.from_environment()
        claim = control.claim_budget(
            {key: identity[key] for key in ("dag_id", "run_id", "task_id", "map_index")},
            total_seconds,
        )
        deadline_at = datetime.fromisoformat(claim["deadline_at"].replace("Z", "+00:00"))
        control.deadline_at = deadline_at
    budget = RunBudget(deadline_at, cleanup)
    started_at = datetime.now(timezone.utc)
    context_digest = {"sha256": hashlib.sha256(b"").hexdigest(), "byte_count": 0, "build_ms": 0}
    attempts: list[dict] = []
    payload = None
    selected_model = None
    debug_prompt = None
    failure: Exception | None = None
    outcome = "failed"
    retry_class = "terminal"
    reason_code = "runner_failed"
    if budget.remaining() <= 0:
        failure = providers.ProviderTimeout("run deadline exhausted")
        envelope = _envelope(
            cfg=cfg,
            identity=identity,
            started_at=started_at,
            deadline_at=deadline_at,
            outcome="timed_out",
            retry_class="terminal",
            reason_code="deadline_exhausted",
            failure=failure,
            selected_model=None,
            attempts=[],
            context_digest=context_digest,
            payload=None,
        )
        if dry_run or ephemeral:
            projection = {
                "outcome": "timed_out",
                "retry_class": "terminal",
                "reason_code": "deadline_exhausted",
                "execution": identity,
            }
        else:
            projection = control.submit_run(envelope)
            projection["reason_code"] = "deadline_exhausted"
        return RunResult(projection, "timed_out", "terminal", "deadline_exhausted")
    try:
        prompt, context, context_digest = build_prompt(cfg, budget)
        if dry_run:
            debug_prompt = prompt
            outcome, retry_class, reason_code = "skipped", "none", "dry_run"
        elif cfg.get("gate") and _gate_skips(cfg["gate"], context):
            outcome, retry_class, reason_code = "skipped", "none", cfg["gate"]["reason_code"]
        else:
            models_cfg = load_models(models_config)
            models = resolve_bot_models(cfg, models_cfg)
            model_deadline = min(
                budget._monotonic_deadline - cleanup,
                time.monotonic() + cfg["model_budget_minutes"] * 60,
            )
            last_failure: Exception | None = None
            for ordinal, model in enumerate(models, 1):
                attempt_started_at = datetime.now(timezone.utc)
                attempt_started = time.monotonic()
                attempt_failure = None
                result = None
                usage_for_attempt = None
                try:
                    remaining_model = model_deadline - time.monotonic()
                    if remaining_model <= 0:
                        raise providers.ProviderTimeout("model budget exhausted")
                    with _inference_slot(models_cfg, model):
                        if model.get("preflight"):
                            _run_command(model["preflight"], pathlib.Path(cfg["dir"]), budget)
                        provider = providers.get_provider(model["provider"])
                        result = provider(
                            model,
                            prompt,
                            timeout_s=min(remaining_model, budget.remaining()),
                        )
                    raw_usage = result.usage if result is not None else None
                    usage_for_attempt = usage_tools.price(
                        raw_usage,
                        model=model.get("model"),
                        alias_model=model.get("model"),
                        book=usage_tools.load_price_book(models_cfg),
                    ) if raw_usage is not None else None
                    payload = _json_report(result.text, cfg)
                    selected_model = model["alias"]
                except (providers.ProviderFailure, BotError) as exc:
                    attempt_failure = exc
                    last_failure = exc
                    usage_for_attempt = getattr(exc, "usage", None)
                attempt_finished_at = datetime.now(timezone.utc)
                attempts.append(
                    {
                        "ordinal": ordinal,
                        "alias": model["alias"],
                        "provider": model["provider"],
                        "started_at": attempt_started_at.isoformat(),
                        "finished_at": attempt_finished_at.isoformat(),
                        "duration_ms": int((time.monotonic() - attempt_started) * 1000),
                        "outcome": (
                            "succeeded"
                            if attempt_failure is None
                            else "capacity_unavailable"
                            if isinstance(attempt_failure, providers.ProviderBusy)
                            else "timed_out"
                            if isinstance(attempt_failure, providers.ProviderTimeout)
                            else "failed"
                        ),
                        "input_tokens": usage_for_attempt.get("input_tokens") if usage_for_attempt else None,
                        "output_tokens": usage_for_attempt.get("output_tokens") if usage_for_attempt else None,
                        "total_tokens": usage_for_attempt.get("total_tokens") if usage_for_attempt else None,
                        "usage": usage_for_attempt,
                    }
                )
                if attempt_failure is None:
                    outcome, retry_class, reason_code = "succeeded", "none", "model_succeeded"
                    break
                fallbackable = getattr(attempt_failure, "fallbackable", False)
                if not fallbackable or ordinal == len(models) or model_deadline <= time.monotonic() or budget.remaining() <= 0:
                    failure = attempt_failure
                    break
            else:
                failure = last_failure or BotError("no model candidate was attempted")
            if outcome != "succeeded":
                if isinstance(failure, providers.ProviderBusy):
                    outcome = "capacity_unavailable"
                    retry_class = "capacity" if cfg["capacity_policy"] == "retry" else "none"
                    reason_code = failure.code
                elif isinstance(failure, providers.ProviderTimeout):
                    outcome, retry_class, reason_code = "timed_out", "terminal", failure.code
                elif isinstance(failure, providers.ProviderTransient):
                    outcome, retry_class, reason_code = "failed", "transient", failure.code
                else:
                    outcome, retry_class, reason_code = "failed", "terminal", getattr(failure, "code", "runner_failed")
    except providers.ProviderTimeout as exc:
        failure = exc
        outcome, retry_class, reason_code = "timed_out", "terminal", exc.code
    except (BotError, providers.ProviderFailure) as exc:
        failure = exc
        outcome, retry_class, reason_code = "failed", getattr(exc, "retry_class", "terminal"), getattr(exc, "code", "runner_failed")
    if retry_class in {"capacity", "transient"} and retry_class not in cfg["retry_on"]:
        retry_class = "terminal"
    envelope = _envelope(
        cfg=cfg,
        identity=identity,
        started_at=started_at,
        deadline_at=deadline_at,
        outcome=outcome,
        retry_class=retry_class,
        reason_code=reason_code,
        failure=failure,
        selected_model=selected_model,
        attempts=attempts,
        context_digest=context_digest,
        payload=payload if outcome == "succeeded" else None,
    )
    if dry_run or ephemeral:
        projection = {
            "outcome": outcome,
            "retry_class": retry_class,
            "reason_code": reason_code,
            "execution": identity,
        }
    else:
        projection = control.submit_run(envelope)
        projection["reason_code"] = reason_code
    return RunResult(projection, outcome, retry_class, reason_code, debug_prompt)
