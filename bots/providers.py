"""How a bot actually reaches a model.

One provider = one wire protocol. A model alias in ``bots/models.yml`` names
the provider, so switching a bot from a local llama-server to OpenRouter, or
to a CLI agent running on the same box, is an edit to that file and nothing
else. Bot definitions never mention a vendor.

Every provider is a callable ``(model, prompt) -> str`` where ``model`` is the
resolved alias dict and the return value is the report text. Failures raise
``ProviderError``; a provider that is merely *busy* raises ``ProviderBusy``, so
a scheduled bot can skip a cycle instead of queueing behind a saturated GPU.

Providers are stdlib-only on purpose: the Airflow worker that runs bots needs
no vendor SDK, and adding one is not a dependency negotiation.
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import urllib.error
import urllib.request


class ProviderError(RuntimeError):
    """The model call failed."""


class ProviderBusy(RuntimeError):
    """The model is temporarily unavailable; skip this cycle."""


def _post_json(url: str, payload: dict, headers: dict, timeout: float) -> dict:
    body = json.dumps(payload).encode()
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header("Content-Type", "application/json")
    for key, value in headers.items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:800]
        # 429/503 mean "come back later", which is a skip, not a failure.
        if exc.code in (429, 503):
            raise ProviderBusy(f"{url} returned {exc.code}: {detail}") from exc
        raise ProviderError(f"{url} returned {exc.code}: {detail}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise ProviderBusy(f"{url} unreachable: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ProviderError(f"{url} returned non-JSON: {exc}") from exc


def _api_key(model: dict) -> str:
    """Read the key from the environment variable the alias names.

    Keys are never stored in models.yml: the alias says *which* env var holds
    it, and the var itself lives in orchestration/airflow.secrets.env (loaded
    by the systemd units) or the operator's shell.
    """
    var = model.get("api_key_env")
    if not var:
        return ""
    key = os.environ.get(var, "").strip()
    if not key:
        raise ProviderError(
            f"model {model['alias']!r} needs the API key in ${var}, which is unset. "
            f"Add it to orchestration/airflow.secrets.env (gitignored) for scheduled "
            f"runs, or export it for a manual run."
        )
    return key


def openai_chat(model: dict, prompt: str) -> str:
    """OpenAI-compatible ``/v1/chat/completions``.

    Covers OpenRouter, OpenAI, Groq, Together, Fireworks, DeepSeek, vLLM,
    llama-server and Ollama — they differ only in ``endpoint`` and key.
    """
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

    data = _post_json(model["endpoint"], payload, headers, float(model.get("timeout_s", 300)))
    try:
        choice = data["choices"][0]
        text = choice["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ProviderError(f"unexpected response shape: {json.dumps(data)[:800]}") from exc
    if not (text or "").strip():
        # Reasoning models return chain-of-thought in a separate field and can
        # spend the whole budget there, leaving content empty. Say so, instead
        # of reporting a bare "empty message" the operator cannot act on.
        reason = choice.get("finish_reason")
        thought = len((choice["message"].get("reasoning_content") or "").strip())
        hint = ""
        if reason == "length":
            hint = (" — the token budget ran out"
                    + (f" after {thought} characters of reasoning" if thought else "")
                    + ". Raise max_tokens for this alias, or disable thinking "
                      "(params.chat_template_kwargs.enable_thinking: false).")
        raise ProviderError(f"model returned no content (finish_reason={reason}){hint}")
    return text


def anthropic_messages(model: dict, prompt: str) -> str:
    """Anthropic's native ``/v1/messages`` (different auth header and shape)."""
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
    data = _post_json(endpoint, payload, headers, float(model.get("timeout_s", 300)))
    try:
        text = "".join(
            block.get("text", "") for block in data["content"] if block.get("type") == "text"
        )
    except (KeyError, TypeError) as exc:
        raise ProviderError(f"unexpected response shape: {json.dumps(data)[:800]}") from exc
    if not text.strip():
        raise ProviderError("model returned an empty message")
    return text


def command(model: dict, prompt: str) -> str:
    """Any local program: a coding-agent CLI, a wrapper script, a mock.

    ``argv`` is a list; ``{prompt}`` and ``{model}`` are substituted per element.
    Without a ``{prompt}`` placeholder the prompt is written to stdin, which is
    what most CLIs prefer. Exit code 3 means "busy, skip this cycle" — the same
    contract the pipeline check used when it pre-flighted a local model server.
    """
    argv = model.get("argv")
    if not argv:
        raise ProviderError(f"model {model['alias']!r} uses provider 'command' but sets no argv")

    use_stdin = not any("{prompt}" in str(part) for part in argv)
    rendered = [
        str(part).replace("{model}", str(model.get("model", ""))).replace("{prompt}", prompt)
        for part in argv
    ]

    env = dict(os.environ)
    env.update({str(k): str(v) for k, v in (model.get("env") or {}).items()})

    try:
        proc = subprocess.run(
            rendered,
            input=prompt if use_stdin else None,
            capture_output=True,
            text=True,
            timeout=float(model.get("timeout_s", 900)),
            env=env,
            cwd=model.get("cwd"),
        )
    except FileNotFoundError as exc:
        raise ProviderError(f"{rendered[0]!r} is not on PATH ({shlex.join(rendered[:1])})") from exc
    except subprocess.TimeoutExpired as exc:
        raise ProviderBusy(f"{rendered[0]} timed out after {exc.timeout}s") from exc

    if proc.returncode == 3:
        raise ProviderBusy(f"{rendered[0]} reported busy: {proc.stderr.strip()[:400]}")
    if proc.returncode != 0:
        raise ProviderError(
            f"{rendered[0]} exited {proc.returncode}: {proc.stderr.strip()[:800]}"
        )
    if not proc.stdout.strip():
        raise ProviderError(f"{rendered[0]} produced no output")
    return proc.stdout


REGISTRY = {
    "openai_chat": openai_chat,
    "anthropic_messages": anthropic_messages,
    "command": command,
}


def get_provider(name: str):
    if name not in REGISTRY:
        raise ProviderError(f"unknown provider {name!r} (available: {sorted(REGISTRY)})")
    return REGISTRY[name]
