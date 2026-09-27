"""Run one family/cadence dbt build under the shared transform lock."""
from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import pathlib
import signal
import subprocess
import tempfile
import time
from collections import Counter
from contextlib import contextmanager
from typing import Iterator

import yaml

import deployment

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
TRANSFORM_ROOT = REPO_ROOT / "transform"
JOBS_PATH = TRANSFORM_ROOT / "jobs.yml"
EXPECTED_JOBS = {"twice_hourly", "hourly", "daily"}
FAMILY_HELPER = TRANSFORM_ROOT / "scripts" / "family_project.py"
TRANSFORM_PYTHON = TRANSFORM_ROOT / ".venv" / "bin" / "python"

log = logging.getLogger(__name__)


def load_runtime_environment() -> None:
    """Load deployment values when a worker runs, not when Airflow imports us."""
    deployment.load_env()
    deployment.load_env(REPO_ROOT / "orchestration" / "airflow.secrets.env")


def discover_families() -> list[dict]:
    """Statically enumerate each mart family's cadences in the transform venv."""
    result = subprocess.run(
        [str(TRANSFORM_PYTHON), str(FAMILY_HELPER), "discover"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    families = json.loads(result.stdout)
    if not isinstance(families, list):
        raise ValueError("family discovery did not return a list")
    return families


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
def exclusive_lock(path: pathlib.Path, wait_seconds: float) -> Iterator[int]:
    """Hold the shared transform lock, yielding its descriptor.

    The descriptor is handed to the publication worker so an uncatchable parent
    exit cannot release the warehouse while that child is still reading it.
    """
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
            yield handle.fileno()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _run_command(command: list[str]) -> None:
    log.info("running: %s", " ".join(command))
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def _terminate_group(process: subprocess.Popen) -> None:
    """Stop the publication worker and its children, never anything else."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    finally:
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()


def _run_worker(
    command: list[str], *, environment: dict[str, str] | None = None, lock_fd: int | None = None
) -> subprocess.CompletedProcess:
    if lock_fd is None:
        try:
            return subprocess.run(
                command,
                cwd=REPO_ROOT,
                check=True,
                capture_output=True,
                text=True,
                env=environment,
            )
        except subprocess.CalledProcessError as exc:
            if exc.stdout:
                log.error("trusted visualization worker stdout:\n%s", exc.stdout)
            if exc.stderr:
                log.error("trusted visualization worker stderr:\n%s", exc.stderr)
            raise
    os.set_inheritable(lock_fd, True)
    process = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
        pass_fds=(lock_fd,),
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate()
    except BaseException:
        # A cancelled or timed-out task must not leave an orphan holding the
        # inherited lock; the original failure still propagates.
        _terminate_group(process)
        raise
    if process.returncode != 0:
        if stdout:
            log.error("trusted visualization worker stdout:\n%s", stdout)
        if stderr:
            log.error("trusted visualization worker stderr:\n%s", stderr)
        raise subprocess.CalledProcessError(process.returncode, command, stdout, stderr)
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def _create_isolated_project(
    family: str, cadence: str, destination: pathlib.Path
) -> dict:
    result = subprocess.run(
        [
            str(TRANSFORM_PYTHON),
            str(FAMILY_HELPER),
            "create",
            "--family",
            family,
            "--cadence",
            cadence,
            "--destination",
            str(destination),
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    project = json.loads(result.stdout)
    if not isinstance(project, dict) or project.get("project_dir") != str(destination):
        raise ValueError("isolated family project helper returned an invalid project")
    return project


def _write_vintage_scope(
    manifest_path: pathlib.Path, family: str, cadence: str
) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata = manifest.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError(f"dbt manifest has no metadata object: {manifest_path}")
    metadata["vintage_scope"] = {"family": family, "cadence": cadence}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")


def _temporary_run_root(enabled: bool) -> tempfile.TemporaryDirectory[str]:
    parent = None
    if enabled:
        parent = pathlib.Path(os.environ["LIGHTDASH_STATE_ROOT"]) / "dbt-runs"
        parent.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix="family-", dir=parent)


def run(
    job: str,
    *,
    family: str,
    jobs_path: pathlib.Path = JOBS_PATH,
    lock_path: pathlib.Path | None = None,
) -> dict:
    load_runtime_environment()
    jobs = load_jobs(jobs_path)
    if job not in jobs:
        raise ValueError(f"unknown transform job {job!r}; choose from {sorted(jobs)}")
    cfg = jobs[job]
    enabled = os.environ.get("LIGHTDASH_ENABLED") == "1"
    dbt = str(TRANSFORM_ROOT / "bin" / "dbt")

    with _temporary_run_root(enabled) as root:
        run_root = pathlib.Path(root)
        project_dir = run_root / "project"
        project = _create_isolated_project(family, job, project_dir)
        target = run_root / "target"
        manifest = target / "manifest.json"
        target_args = ["--project-dir", str(project_dir), "--target-path", str(target)]
        commands = [
            [dbt, "parse", "--target", "prod", *target_args],
            [str(TRANSFORM_ROOT / "bin" / "validate_project"), str(manifest)],
            [
                dbt,
                "build",
                "--target",
                "prod",
                *target_args,
                "--select",
                f"+tag:{job}",
            ],
        ]
        with exclusive_lock(lock_path or _default_lock_path(), cfg["lock_wait_seconds"]) as lock_fd:
            for command in commands:
                _run_command(command)
            _write_vintage_scope(manifest, family, job)
            publication = None
            if enabled:
                environment = os.environ.copy()
                environment["LIGHTDASH_CONTENT_ROOT"] = str(project_dir)
                # The transfer streams out of the warehouse this build just
                # wrote, so it runs inside the same lock the build held.
                result = _run_worker(
                    [
                        str(REPO_ROOT / "visualization/.venv/bin/python"),
                        str(REPO_ROOT / "visualization/worker.py"),
                        "publish",
                        str(manifest),
                    ],
                    environment=environment,
                    lock_fd=lock_fd,
                )
                publication = json.loads(result.stdout)
    return {
        "family": family,
        "job": job,
        "status": "ok",
        "commands": commands,
        "project": project,
        "publication": publication,
    }



def sync_lightdash(build_result: dict) -> dict:
    """Deploy the published family's serving metadata and reviewed content."""
    publication = build_result.get("publication")
    if not publication:
        return {"status": "disabled"}
    if publication.get("stale"):
        raise ValueError(
            "refusing Lightdash deployment from a stale publication batch: "
            + ", ".join(publication["stale"])
        )
    result = _run_worker([
        str(REPO_ROOT / "visualization/.venv/bin/python"),
        str(REPO_ROOT / "visualization/worker.py"),
        "sync",
        publication["batch_id"],
    ])
    return json.loads(result.stdout)
