"""Bounded model-provider protocols with typed results and classified failures."""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import shutil
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

try:
    from . import usage as usage_tools
except ImportError:
    import usage as usage_tools


log = logging.getLogger(__name__)

_CREDENTIAL_URL = re.compile(r"(?i)https?://[^\s/@:]+:[^\s/@]+@")
_ABSOLUTE_PATH = re.compile(r"(?<![\w.])/(?:[^\s/]+/)+[^\s]+")


def _safe_detail(value: object, limit: int = 2000) -> str:
    text = _CREDENTIAL_URL.sub("https://***@", str(value))
    text = _ABSOLUTE_PATH.sub("<path>", text)
    return text[:limit]


@dataclass(frozen=True)
class ProviderResult:
    text: str
    duration_ms: int
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    usage: dict | None = None

class ProviderFailure(RuntimeError):
    code = "provider_failure"
    fallbackable = False
    retry_class = "terminal"

    def __init__(self, detail: object = ""):
        self.detail = _safe_detail(detail)
        super().__init__(self.detail or self.code)


class ProviderBusy(ProviderFailure):
    code = "provider_capacity"
    fallbackable = True
    retry_class = "capacity"


class ProviderTimeout(ProviderFailure):
    code = "provider_timeout"
    fallbackable = True
    retry_class = "terminal"


class ProviderTransient(ProviderFailure):
    code = "provider_transient"
    fallbackable = True
    retry_class = "transient"


class ProviderTerminal(ProviderFailure):
    code = "provider_terminal"


# Compatibility name for callers that still catch the generic provider failure.
ProviderError = ProviderFailure


def _post_json(
    url: str, payload: dict, headers: dict, timeout: float
) -> tuple[dict, int]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header("Content-Type", "application/json")
    for key, value in headers.items():
        request.add_header(key, value)
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read(1_048_577)
            if len(raw) > 1_048_576:
                raise ProviderTerminal("provider response exceeds 1 MiB")
            value = json.loads(raw.decode("utf-8", "replace"))
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            raise ProviderBusy(f"HTTP {exc.code}") from exc
        if exc.code == 503 or exc.code >= 500:
            raise ProviderTransient(f"HTTP {exc.code}") from exc
        raise ProviderTerminal(f"HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, TimeoutError) or "timed out" in str(exc.reason).lower():
            raise ProviderTimeout("HTTP request deadline expired") from exc
        raise ProviderTransient("HTTP provider unavailable") from exc
    except (TimeoutError, OSError) as exc:
        if isinstance(exc, TimeoutError) or "timed out" in str(exc).lower():
            raise ProviderTimeout("HTTP request deadline expired") from exc
        raise ProviderTransient("HTTP provider unavailable") from exc
    except json.JSONDecodeError as exc:
        raise ProviderTerminal("provider returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise ProviderTerminal("provider response is not an object")
    return value, int((time.monotonic() - started) * 1000)


def _api_key(model: dict) -> str:
    variable = model.get("api_key_env")
    if not variable:
        return ""
    key = os.environ.get(variable, "").strip()
    if not key:
        raise ProviderTerminal(f"required API key variable {variable!r} is unset")
    return key


def openai_chat(
    model: dict, prompt: str, *, timeout_s: float | None = None
) -> ProviderResult:
    payload: dict = {
        "model": model["model"],
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }
    if model.get("system"):
        payload["messages"].insert(0, {"role": "system", "content": model["system"]})
    if model.get("max_tokens"):
        payload["max_tokens"] = int(model["max_tokens"])
    if model.get("temperature") is not None:
        payload["temperature"] = float(model["temperature"])
    payload.update(model.get("params") or {})
    headers = {}
    key = _api_key(model)
    if key:
        headers["Authorization"] = f"Bearer {key}"
    headers.update(model.get("headers") or {})
    timeout = min(float(model.get("timeout_s", 300)), timeout_s or float("inf"))
    data, duration = _post_json(model["endpoint"], payload, headers, timeout)
    try:
        choice = data["choices"][0]
        text = choice["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ProviderTerminal("unexpected response shape") from exc
    usage = usage_tools.normalize(data.get("usage"), format="openai")
    return ProviderResult(
        text=text,
        duration_ms=duration,
        input_tokens=usage["input_tokens"],
        output_tokens=usage["output_tokens"],
        total_tokens=usage["total_tokens"],
        usage=usage,
    )


def anthropic_messages(
    model: dict, prompt: str, *, timeout_s: float | None = None
) -> ProviderResult:
    payload: dict = {
        "model": model["model"],
        "max_tokens": int(model.get("max_tokens", 2048)),
        "messages": [{"role": "user", "content": prompt}],
    }
    if model.get("system"):
        payload["system"] = model["system"]
    if model.get("temperature") is not None:
        payload["temperature"] = float(model["temperature"])
    payload.update(model.get("params") or {})
    headers = {
        "x-api-key": _api_key(model),
        "anthropic-version": model.get("api_version", "2023-06-01"),
    }
    headers.update(model.get("headers") or {})
    endpoint = model.get("endpoint") or "https://api.anthropic.com/v1/messages"
    timeout = min(float(model.get("timeout_s", 300)), timeout_s or float("inf"))
    data, duration = _post_json(endpoint, payload, headers, timeout)
    try:
        text = "".join(
            block.get("text", "")
            for block in data["content"]
            if block.get("type") == "text"
        )
    except (KeyError, TypeError) as exc:
        raise ProviderTerminal("unexpected response shape") from exc
    usage = usage_tools.normalize(data.get("usage"), format="anthropic")
    return ProviderResult(
        text=text,
        duration_ms=duration,
        input_tokens=usage["input_tokens"],
        output_tokens=usage["output_tokens"],
        total_tokens=usage["total_tokens"],
        usage=usage,
    )


def command(
    model: dict, prompt: str, *, timeout_s: float | None = None
) -> ProviderResult:
    argv = model.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(item, str) for item in argv):
        raise ProviderTerminal("command provider requires nonempty string argv")
    executable = argv[0]
    usage_spec = model.get("usage")
    usage_path = None
    usage_dir = None
    if isinstance(usage_spec, dict) and usage_spec.get("source") == "file":
        fd, usage_path = tempfile.mkstemp(prefix="bot-usage-")
        os.fchmod(fd, 0o600)
        os.close(fd)
    if isinstance(usage_spec, dict) and "{usage_dir}" in str(usage_spec.get("dir", "")):
        # A private directory makes telemetry deterministic and unshared.
        usage_dir = tempfile.mkdtemp(prefix="bot-usage-session-")
        os.chmod(usage_dir, 0o700)
    use_stdin = not any("{prompt}" in part for part in argv)
    rendered = [
        part.replace("{model}", str(model.get("model", "")))
        .replace("{prompt}", prompt)
        .replace("{usage_file}", usage_path or "")
        .replace("{usage_dir}", usage_dir or "")
        for part in argv
    ]
    if model.get("inherit_env", True):
        environment = dict(os.environ)
    else:
        pass_env = model.get("pass_env") or []
        if not isinstance(pass_env, list) or not all(isinstance(item, str) for item in pass_env):
            if usage_path:
                try:
                    os.unlink(usage_path)
                except OSError:
                    pass
            raise ProviderTerminal("command pass_env must be a string list")
        environment = {name: os.environ[name] for name in pass_env if name in os.environ}
    environment.update({str(key): str(value) for key, value in (model.get("env") or {}).items()})
    for secret_name in ("BOT_DASHBOARD_API_PASSWORD", "BOT_DASHBOARD_API_USERNAME", "AIRFLOW__API__BASE_URL"):
        environment.pop(secret_name, None)
    timeout = min(float(model.get("timeout_s", 900)), timeout_s or float("inf"))
    started = time.time()
    log.info("command provider starting executable=%r timeout_s=%.3f stdin=%s", executable, timeout, use_stdin)
    try:
        try:
            process = subprocess.run(
                rendered, check=False,
                **({"input": prompt} if use_stdin else {"stdin": subprocess.DEVNULL}),
                capture_output=True, text=True, timeout=timeout, env=environment, cwd=model.get("cwd"),
            )
        except FileNotFoundError as exc:
            raise ProviderTerminal(f"command executable {executable!r} is unavailable") from exc
        except OSError as exc:
            raise ProviderTerminal("command provider could not start") from exc
        except subprocess.TimeoutExpired as exc:
            captured = "".join(part.decode("utf-8", "replace") if isinstance(part, bytes) else (part or "") for part in (exc.stderr, exc.stdout)).strip()
            tail = captured[-2000:]
            raise ProviderTimeout(f"command provider deadline expired after {timeout:.0f}s" + (f"; last output: {tail}" if tail else "; the child produced no output")) from exc
        duration = int((time.time() - started) * 1000)
        discovered = usage_tools.capture(
            usage_spec,
            started_at=started,
            stdout=process.stdout or "",
            usage_file=usage_path,
            usage_dir=usage_dir,
        )
        if process.returncode == 3:
            error = ProviderBusy("command reported capacity unavailable")
            error.usage = discovered
            raise error
        failure_text = "\n".join(part for part in (process.stderr, process.stdout) if part)
        if process.returncode:
            if re.search(r"\b429\b|too many requests|rate[_ -]?limit|usage_limit_reached", failure_text, re.IGNORECASE):
                error = ProviderBusy("command provider was rate limited")
            elif re.search(r"\b503\b|temporar(?:y|ily) unavailable", failure_text, re.IGNORECASE):
                error = ProviderTransient("command provider is temporarily unavailable")
            else:
                error = ProviderTerminal(f"command exited with code {process.returncode}")
            error.usage = discovered
            raise error
        if not process.stdout.strip():
            raise ProviderTerminal("command produced no output")
        return ProviderResult(text=process.stdout, duration_ms=duration, input_tokens=discovered["input_tokens"], output_tokens=discovered["output_tokens"], total_tokens=discovered["total_tokens"], usage=discovered)
    finally:
        if usage_path:
            try:
                os.unlink(usage_path)
            except OSError:
                pass
        if usage_dir:
            shutil.rmtree(usage_dir, ignore_errors=True)


REGISTRY = {
    "openai_chat": openai_chat,
    "anthropic_messages": anthropic_messages,
    "command": command,
}


def get_provider(name: str):
    try:
        return REGISTRY[name]
    except KeyError as exc:
        raise ProviderTerminal(f"unknown provider {name!r}") from exc
