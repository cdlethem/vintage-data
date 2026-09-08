"""Provider-agnostic token usage normalization, discovery, and pricing."""
from __future__ import annotations

import datetime as _dt
import decimal
import json
import logging
import os
import pathlib
import stat
from typing import Any

log = logging.getLogger(__name__)
_MAX_BYTES = 1_048_576
_FIELDS = (
    "input_tokens", "output_tokens", "cached_input_tokens", "cache_write_tokens",
    "reasoning_tokens", "total_tokens", "requests", "cost_micro_usd", "cost_source", "pricing_id",
)


def _empty() -> dict:
    return {
        "schema": "usage.v1", "input_tokens": 0, "output_tokens": 0,
        "cached_input_tokens": 0, "cache_write_tokens": 0, "reasoning_tokens": 0,
        "total_tokens": 0, "requests": 1, "cost_micro_usd": None,
        "cost_source": "unavailable", "pricing_id": None,
    }


def _nonneg(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        value = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return max(0, value)


def _reported_cost(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        amount = decimal.Decimal(str(value))
        if not amount.is_finite() or amount < 0:
            return None
        return int((amount * 1_000_000).to_integral_value(rounding=decimal.ROUND_HALF_UP))
    except (decimal.InvalidOperation, ValueError, TypeError, OverflowError):
        return None


def normalize(document: Any, *, format: str) -> dict:
    """Normalize a supported provider usage document; malformed input is unavailable."""
    result = _empty()
    if format not in {"openai", "anthropic", "normalized", "omp_session"}:
        return result
    try:
        if format == "omp_session":
            records = document if isinstance(document, list) else [document]
            records = [item for item in records if isinstance(item, dict)]
            if not records:
                return result
            normalized = []
            for record in records:
                # Session records carry usage either at the top level or nested
                # under the assistant message they belong to.
                for value in (record.get("usage"), (record.get("message") or {}).get("usage") if isinstance(record.get("message"), dict) else None):
                    if not isinstance(value, dict):
                        continue
                    entry = _from_omp(value)
                    if entry["total_tokens"] or entry["cost_micro_usd"]:
                        normalized.append(entry)
            if not normalized:
                return result
            return merge(normalized)
        if not isinstance(document, dict):
            return result
        if format == "normalized":
            if document.get("schema") != "usage.v1":
                return result
            result.update({key: document[key] for key in _FIELDS if key in document})
            for key in ("input_tokens", "output_tokens", "cached_input_tokens", "cache_write_tokens", "reasoning_tokens", "total_tokens", "requests"):
                result[key] = _nonneg(result[key])
            if result["requests"] < 1:
                result["requests"] = 1
            if result["cost_source"] not in {"provider_reported", "price_book", "unavailable"}:
                result["cost_source"] = "unavailable"
            if result["cost_micro_usd"] is not None:
                result["cost_micro_usd"] = _nonneg(result["cost_micro_usd"])
            if result["cost_source"] != "unavailable" and not isinstance(result["cost_micro_usd"], int):
                result["cost_source"] = "unavailable"
                result["cost_micro_usd"] = None
            return result
        if format == "openai":
            prompt = _nonneg(document.get("prompt_tokens"))
            output = _nonneg(document.get("completion_tokens"))
            details = document.get("prompt_tokens_details") or {}
            cached = _nonneg(details.get("cached_tokens")) if isinstance(details, dict) else 0
            result["input_tokens"] = max(0, prompt - cached)
            result["cached_input_tokens"] = cached
            result["output_tokens"] = output
            completion = document.get("completion_tokens_details") or {}
            result["reasoning_tokens"] = _nonneg(completion.get("reasoning_tokens")) if isinstance(completion, dict) else 0
            result["total_tokens"] = _nonneg(document.get("total_tokens")) or result["input_tokens"] + output + cached
            result["cost_source"] = "unavailable"
            return result
        # anthropic
        result["input_tokens"] = _nonneg(document.get("input_tokens"))
        result["output_tokens"] = _nonneg(document.get("output_tokens"))
        result["cached_input_tokens"] = _nonneg(document.get("cache_read_input_tokens"))
        result["cache_write_tokens"] = _nonneg(document.get("cache_creation_input_tokens"))
        result["total_tokens"] = result["input_tokens"] + result["output_tokens"] + result["cached_input_tokens"] + result["cache_write_tokens"]
        return result
    except Exception:
        return _empty()


def _from_omp(value: dict) -> dict:
    result = _empty()
    result["input_tokens"] = _nonneg(value.get("input"))
    result["output_tokens"] = _nonneg(value.get("output"))
    result["cached_input_tokens"] = _nonneg(value.get("cacheRead"))
    result["cache_write_tokens"] = _nonneg(value.get("cacheWrite"))
    result["reasoning_tokens"] = _nonneg(value.get("reasoningTokens"))
    result["total_tokens"] = _nonneg(value.get("totalTokens")) or result["input_tokens"] + result["output_tokens"] + result["cached_input_tokens"] + result["cache_write_tokens"]
    cost = value.get("cost")
    if isinstance(cost, dict):
        cost = cost.get("total")
    result["cost_micro_usd"] = _reported_cost(cost)
    if result["cost_micro_usd"] is not None:
        result["cost_source"] = "provider_reported"
    return result


def merge(records: Any) -> dict:
    result = _empty()
    valid = [record for record in (records or []) if isinstance(record, dict)]
    if not valid:
        return result
    for key in ("input_tokens", "output_tokens", "cached_input_tokens", "cache_write_tokens", "reasoning_tokens", "total_tokens", "requests"):
        result[key] = sum(_nonneg(record.get(key)) for record in valid)
    costs = [record.get("cost_micro_usd") for record in valid]
    if all(isinstance(value, int) and not isinstance(value, bool) and value >= 0 for value in costs):
        result["cost_micro_usd"] = sum(costs)
        result["cost_source"] = "provider_reported" if all(record.get("cost_source") == "provider_reported" for record in valid) else "price_book" if all(record.get("cost_source") == "price_book" for record in valid) else "unavailable"
    else:
        result["cost_micro_usd"] = None
        result["cost_source"] = "unavailable"
    return result


def load_price_book(models_cfg: dict) -> dict:
    pricing = (models_cfg or {}).get("pricing") or {}
    if not isinstance(pricing, dict):
        return {"id": None, "models": {}}
    return {"id": pricing.get("id"), "models": pricing.get("models") if isinstance(pricing.get("models"), dict) else {}}


def price(usage: dict, *, model: str | None, alias_model: str | None, book: dict) -> dict:
    result = dict(usage or _empty())
    if result.get("cost_source") == "provider_reported" and isinstance(result.get("cost_micro_usd"), int):
        return result
    models = (book or {}).get("models") or {}
    selected = model if model in models else alias_model if alias_model in models else None
    rates = models.get(selected) if selected else None
    if not isinstance(rates, dict) or any(key not in rates for key in ("input", "output", "cached_input", "cache_write")):
        result["cost_micro_usd"] = None
        result["cost_source"] = "unavailable"
        result["pricing_id"] = None
        return result
    try:
        total = decimal.Decimal(0)
        total += decimal.Decimal(result.get("input_tokens", 0)) * decimal.Decimal(str(rates["input"]))
        total += decimal.Decimal(result.get("output_tokens", 0)) * decimal.Decimal(str(rates["output"]))
        total += decimal.Decimal(result.get("cached_input_tokens", 0)) * decimal.Decimal(str(rates["cached_input"]))
        total += decimal.Decimal(result.get("cache_write_tokens", 0)) * decimal.Decimal(str(rates["cache_write"]))
        result["cost_micro_usd"] = int(total.to_integral_value(rounding=decimal.ROUND_HALF_UP))
        result["cost_source"] = "price_book"
        result["pricing_id"] = (book or {}).get("id")
    except (decimal.InvalidOperation, ValueError, TypeError, OverflowError):
        result["cost_micro_usd"] = None
        result["cost_source"] = "unavailable"
    return result


def _read(path: pathlib.Path) -> str:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        data = os.read(fd, _MAX_BYTES + 1)
    finally:
        os.close(fd)
    if len(data) > _MAX_BYTES:
        return ""
    return data.decode("utf-8", "replace")


def capture(
    spec: dict | None,
    *,
    started_at: Any,
    stdout: str,
    usage_file: str | os.PathLike | None,
    usage_dir: str | os.PathLike | None = None,
) -> dict:
    if not isinstance(spec, dict):
        return _empty()
    source, fmt = spec.get("source"), spec.get("format")
    try:
        if source == "file":
            if not usage_file:
                return _empty()
            return normalize(json.loads(_read(pathlib.Path(usage_file))), format=fmt)
        if source == "stdout_trailer":
            marker = spec.get("marker")
            lines = [line for line in (stdout or "").splitlines() if isinstance(marker, str) and line.startswith(marker)]
            if not lines:
                return _empty()
            return normalize(json.loads(lines[-1][len(marker):].strip()), format=fmt)
        if source == "session_dir":
            configured = str(spec.get("dir", ""))
            # A private per-run directory is deterministic; a shared one needs a
            # start-time threshold because other sessions live beside ours.
            private = "{usage_dir}" in configured
            if private and not usage_dir:
                return _empty()
            directory = pathlib.Path(usage_dir) if private else pathlib.Path(os.path.expanduser(configured))
            threshold = (
                0.0
                if private
                else (started_at.timestamp() if isinstance(started_at, _dt.datetime) else float(started_at))
            )
            records: list[dict] = []
            for path in sorted(_session_files(directory, threshold)):
                for line in _read(path).splitlines():
                    try:
                        value = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(value, dict):
                        records.append(value)
            if not records:
                return _empty()
            return normalize(records, format=fmt)
    except Exception:
        log.warning("usage discovery failed source=%r", source)
    return _empty()


def _session_files(directory: pathlib.Path, threshold: float) -> list[pathlib.Path]:
    """Regular files at or below one nested level, written at/after the threshold."""
    found: list[pathlib.Path] = []
    try:
        entries = list(directory.iterdir())
    except OSError:
        return found
    for path in entries:
        try:
            info = path.stat(follow_symlinks=False)
        except OSError:
            continue
        if stat.S_ISDIR(info.st_mode):
            found.extend(_session_files(path, threshold))
        elif stat.S_ISREG(info.st_mode) and max(info.st_mtime, info.st_ctime) >= threshold:
            found.append(path)
    if threshold > 0 and found:
        newest = max(found, key=lambda item: item.stat().st_mtime)
        return [newest]
    return found
