"""Configuration for the load layer: one yml, env-interpolated.

Settings resolve in three layers — ``defaults`` in the yml, then a per-source
override block, then anything a job passes at submit time. Nothing about a
source is *required*: an unconfigured source loads on the defaults, which is
what makes 164 sources workable without 164 config blocks.
"""
from __future__ import annotations

import os
import pathlib
import re
from dataclasses import dataclass, field, replace
from typing import Any

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "load" / "config" / "load.yml"

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _env(match: re.Match) -> str:
    """``${VAR:-default}`` with shell semantics: the default also wins when the
    variable is set but empty. An ``EXTRACT_WAREHOUSE=`` line in airflow.env
    should mean "unset", not "the current directory is the database"."""
    return os.environ.get(match.group(1)) or match.group(2) or ""


def _expand(value: Any) -> Any:
    """Interpolate ``${VAR}`` / ``${VAR:-default}`` through a parsed yml tree."""
    if isinstance(value, str):
        return _ENV_RE.sub(_env, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def _path(value: str) -> pathlib.Path:
    return pathlib.Path(value).expanduser()


@dataclass(frozen=True)
class SourceSettings:
    """How one source is loaded. Defaults come from the yml ``defaults`` block."""

    name: str = ""
    enabled: bool = True
    table: str | None = None            # default: the sanitized source name
    schema_detection: str = "auto"      # auto | payload_only
    sample_lines: int = 500             # lines per file fed to inference; 0 = all
    keep_payload: bool = True           # retain the verbatim record as _payload
    detect_temporal: bool = True        # promote ISO-8601 strings to date/timestamp
    min_age_s: int = 60                 # ignore files younger than this
    on_malformed_lines: str = "fail"    # fail | skip (see load.yml for what skip does)
    column_types: dict[str, str] = field(default_factory=dict)  # forced, pre-inference
    exclude_keys: list[str] = field(default_factory=list)       # keep in _payload only


@dataclass(frozen=True)
class DagSettings:
    dag_id: str = "load__raw"
    schedule: str = "*/15 * * * *"
    max_files_per_run: int = 400
    timeout_minutes: int = 30
    wait_timeout_s: int = 1500


@dataclass(frozen=True)
class ServiceSettings:
    poll_interval_s: float = 5.0
    idle_release_s: float = 15.0
    done_retention_days: int = 7
    heartbeat_s: float = 15.0


@dataclass(frozen=True)
class LoadConfig:
    destination: dict[str, Any]
    source_root: pathlib.Path
    queue_dir: pathlib.Path
    raw_schema: str
    meta_schema: str
    service: ServiceSettings
    dag: DagSettings
    defaults: SourceSettings
    overrides: dict[str, dict[str, Any]]
    path: pathlib.Path

    def for_source(self, name: str) -> SourceSettings:
        """Effective settings for one source: defaults + its override block."""
        over = self.overrides.get(name, {}) or {}
        merged = replace(self.defaults, name=name)
        for key, value in over.items():
            if key == "column_types":
                value = {**merged.column_types, **(value or {})}
            if not hasattr(merged, key):
                raise ValueError(f"source {name!r}: unknown setting {key!r}")
            merged = replace(merged, **{key: value})
        return merged


def load_config(path: str | os.PathLike | None = None) -> LoadConfig:
    path = pathlib.Path(path or os.environ.get("LOAD_CONFIG") or DEFAULT_CONFIG)
    raw = _expand(yaml.safe_load(path.read_text()) or {})

    name = raw.get("destination")
    destinations = raw.get("destinations") or {}
    if name not in destinations:
        raise ValueError(f"destination {name!r} not defined in {path}")
    dest = dict(destinations[name])
    dest.setdefault("name", name)

    paths = raw.get("paths") or {}
    schemas = raw.get("schemas") or {}
    return LoadConfig(
        destination=dest,
        source_root=_path(paths.get("source_root", "~/.local/share/vintage-data/extract/raw")),
        queue_dir=_path(paths.get("queue_dir", "~/.local/share/vintage-data/extract/_load_queue")),
        raw_schema=schemas.get("raw", "raw"),
        meta_schema=schemas.get("meta", "_load"),
        service=ServiceSettings(**(raw.get("service") or {})),
        dag=DagSettings(**(raw.get("dag") or {})),
        defaults=SourceSettings(**(raw.get("defaults") or {})),
        overrides=raw.get("sources") or {},
        path=path,
    )
