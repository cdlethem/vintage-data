"""The deployment environment, for processes systemd did not start.

`orchestration/airflow.env` is generated from `orchestration/config.env` and is
what every systemd unit loads (`EnvironmentFile=`). Anything that reads the same
world by hand — the monitoring digest, a bot run from a shell — must see the
same `EXTRACT_DATA_ROOT` and `EXTRACT_WAREHOUSE`, or it will quietly answer
questions about a directory the pipeline never writes to.

So: load that file, never overriding what the caller already set. A missing file
is not an error (a fresh checkout has not rendered one yet); a wrong answer
would be.
"""
from __future__ import annotations

import os
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
ENV_FILE = REPO_ROOT / "orchestration" / "airflow.env"


def parse_env_file(path: pathlib.Path) -> dict[str, str]:
    """Parse the systemd `EnvironmentFile` subset the renderer emits.

    `KEY=value` per line, `#` comments, and optional single or double quotes
    around the value (the renderer quotes so values may contain spaces). No
    variable expansion: the file is already fully rendered.
    """
    values: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def load_env(path: str | os.PathLike | None = None, *, override: bool = False) -> dict[str, str]:
    """Merge the deployment env into ``os.environ``; return what was applied."""
    file = pathlib.Path(path or os.environ.get("VINTAGE_DATA_ENV_FILE") or ENV_FILE)
    if not file.is_file():
        return {}
    applied = {}
    for key, value in parse_env_file(file).items():
        if override or key not in os.environ:
            os.environ[key] = value
            applied[key] = value
    return applied
