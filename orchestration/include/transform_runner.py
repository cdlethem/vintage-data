"""Run one tag-selected dbt build under the shared transform lock."""

from __future__ import annotations

import fcntl
import json
import logging
import os
import pathlib
import subprocess
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from typing import Iterator

import yaml

import deployment

deployment.load_env()

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
TRANSFORM_ROOT = REPO_ROOT / "transform"
JOBS_PATH = TRANSFORM_ROOT / "jobs.yml"
EXPECTED_JOBS = {"twice_hourly", "hourly", "daily"}

log = logging.getLogger(__name__)


def read_job_entries(path: pathlib.Path = JOBS_PATH) -> list[dict]:
    value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    jobs = value.get("jobs") if isinstance(value, dict) else None
    if not isinstance(jobs, list):
        raise ValueError(f"{path} must contain a jobs list")
    return jobs


def duplicate_values(entries: list[dict], key: str) -> set[str]:
    values = [str(entry.get(key)) for entry in entries if isinstance(entry, dict) and entry.get(key)]
    return {value for value, count in Counter(values).items() if count > 1}


def validate_job(
    entry: dict,
    *,
    duplicate_names: set[str] | None = None,
    duplicate_schedules: set[str] | None = None,
) -> dict:
    if not isinstance(entry, dict):
        raise ValueError("each transform job must be a mapping")
    name = entry.get("name")
    schedule = entry.get("schedule")
    timeout = entry.get("timeout_minutes")
    lock_wait = entry.get("lock_wait_seconds")
    if name not in EXPECTED_JOBS:
        raise ValueError(f"unknown transform job name {name!r}")
    if name in (duplicate_names or set()):
        raise ValueError(f"duplicate transform job name {name!r}")
    if not isinstance(schedule, str) or len(schedule.split()) != 5:
        raise ValueError(f"transform job {name!r} requires a five-field cron schedule")
    if schedule in (duplicate_schedules or set()):
        raise ValueError(f"duplicate transform job schedule {schedule!r}")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError(f"transform job {name!r} requires positive timeout_minutes")
    if not isinstance(lock_wait, int) or isinstance(lock_wait, bool) or lock_wait <= 0:
        raise ValueError(f"transform job {name!r} requires positive lock_wait_seconds")
    return {
        "name": name,
        "schedule": schedule,
        "timeout_minutes": timeout,
        "lock_wait_seconds": lock_wait,
    }


def load_jobs(path: pathlib.Path = JOBS_PATH) -> dict[str, dict]:
    entries = read_job_entries(path)
    duplicate_names = duplicate_values(entries, "name")
    duplicate_schedules = duplicate_values(entries, "schedule")
    jobs = {}
    errors = []
    for entry in entries:
        try:
            job = validate_job(
                entry,
                duplicate_names=duplicate_names,
                duplicate_schedules=duplicate_schedules,
            )
            jobs[job["name"]] = job
        except ValueError as exc:
            errors.append(str(exc))
    if errors:
        raise ValueError("; ".join(errors))
    missing = EXPECTED_JOBS - set(jobs)
    if missing:
        raise ValueError(f"missing transform jobs: {sorted(missing)}")
    return jobs


def _default_lock_path() -> pathlib.Path:
    data_root = pathlib.Path(
        os.environ.get(
            "EXTRACT_DATA_ROOT",
            pathlib.Path.home() / ".local" / "share" / "vintage-data" / "extract",
        )
    ).expanduser()
    return data_root / "state" / "transform" / "dbt.lock"


@contextmanager
def exclusive_lock(path: pathlib.Path, wait_seconds: float) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        deadline = time.monotonic() + wait_seconds
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"transform lock {path} remained busy for {wait_seconds:g} seconds"
                    )
                time.sleep(min(1.0, remaining))
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _run_command(command: list[str]) -> None:
    log.info("running: %s", " ".join(command))
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def run(
    job: str,
    *,
    jobs_path: pathlib.Path = JOBS_PATH,
    lock_path: pathlib.Path | None = None,
) -> dict:
    jobs = load_jobs(jobs_path)
    if job not in jobs:
        raise ValueError(f"unknown transform job {job!r}; choose from {sorted(jobs)}")
    cfg = jobs[job]
    dbt = str(TRANSFORM_ROOT / "bin" / "dbt")
    manifest = str(TRANSFORM_ROOT / "target" / "manifest.json")
    enabled = os.environ.get("LIGHTDASH_ENABLED") == "1"
    target_args = []
    if enabled:
        # A developer parse must never overwrite the manifest being published.
        target = pathlib.Path(os.environ["LIGHTDASH_STATE_ROOT"]) / "dbt-runs" / str(uuid.uuid4())
        target.mkdir(parents=True, exist_ok=False)
        manifest = str(target / "manifest.json")
        target_args = ["--target-path", str(target)]
    commands = [
        [dbt, "parse", "--target", "prod", *target_args],
        [str(TRANSFORM_ROOT / "bin" / "validate_project"), manifest],
        [dbt, "build", "--target", "prod", *target_args, "--select", f"+tag:{job}"],
    ]
    with exclusive_lock(lock_path or _default_lock_path(), cfg["lock_wait_seconds"]):
        for command in commands:
            _run_command(command)
        publication = None
        if enabled:
            result = subprocess.run(
                [str(REPO_ROOT / "visualization/.venv/bin/python"), str(REPO_ROOT / "visualization/worker.py"), "capture", manifest],
                cwd=REPO_ROOT, check=True, capture_output=True, text=True,
            )
            publication = json.loads(result.stdout)
    return {"job": job, "status": "ok", "commands": commands, "publication": publication}


def publish_marts(build_result: dict) -> dict:
    """Retry publication without rebuilding models or holding the DuckDB lock."""
    batch = build_result.get("publication")
    if not batch:
        return {"status": "disabled"}
    result = subprocess.run(
        [str(REPO_ROOT / "visualization/.venv/bin/python"), str(REPO_ROOT / "visualization/worker.py"), "publish", batch["batch_id"]],
        cwd=REPO_ROOT, check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout)
