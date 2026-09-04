"""A filesystem job queue between Airflow and the single writer.

Airflow must not open the warehouse — only the writer service does. So a DAG
run submits a job file and waits for its result. Directories are the whole
protocol, which keeps the queue dependency-free and inspectable with ``ls``:

    pending/   submitted, not yet claimed   (client writes, service reads)
    running/   claimed by the service       (at most one at a time)
    done/      terminal result              (service writes, client reads)

Every write is tmp-file + atomic rename, so a reader never sees half a job.
"""
from __future__ import annotations

import json
import os
import pathlib
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

STATES = ("pending", "running", "done")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_atomic(path: pathlib.Path, payload: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)


@dataclass
class Job:
    job_id: str
    body: dict
    path: pathlib.Path

    @property
    def kind(self) -> str:
        return self.body.get("kind", "scan")


class JobQueue:
    def __init__(self, root: str | os.PathLike):
        self.root = pathlib.Path(root).expanduser()
        for state in STATES:
            (self.root / state).mkdir(parents=True, exist_ok=True)

    # -- client side -----------------------------------------------------
    def submit(self, body: dict) -> str:
        """Enqueue a job; returns its id. Filenames sort by submit time."""
        job_id = body.get("job_id") or uuid.uuid4().hex
        body = {**body, "job_id": job_id, "submitted_at": _utcnow()}
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        _write_atomic(self.root / "pending" / f"{stamp}_{job_id}.json", body)
        return job_id

    def result(self, job_id: str) -> dict | None:
        path = self.root / "done" / f"{job_id}.json"
        try:
            return json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def wait(self, job_id: str, timeout_s: float, poll_s: float = 2.0,
             on_poll=None) -> dict | None:
        """Block until the service publishes a result, or the timeout runs out."""
        deadline = time.monotonic() + timeout_s
        while True:
            result = self.result(job_id)
            if result is not None:
                return result
            if time.monotonic() >= deadline:
                return None
            if on_poll:
                on_poll()
            time.sleep(poll_s)

    def depth(self) -> int:
        return len(list((self.root / "pending").glob("*.json")))

    # -- service side ----------------------------------------------------
    def claim(self) -> Job | None:
        """Take the oldest pending job. The rename *is* the claim."""
        for path in sorted((self.root / "pending").glob("*.json")):
            target = self.root / "running" / path.name
            try:
                os.replace(path, target)
            except OSError:
                continue  # someone else got it; shouldn't happen with one writer
            try:
                body = json.loads(target.read_text())
            except json.JSONDecodeError:
                self.complete(Job("malformed", {}, target),
                              {"status": "failed", "error": f"malformed job {path.name}"})
                continue
            return Job(body.get("job_id", target.stem), body, target)
        return None

    def recover(self) -> list[str]:
        """Return jobs orphaned by a service restart to pending."""
        recovered = []
        for path in sorted((self.root / "running").glob("*.json")):
            os.replace(path, self.root / "pending" / path.name)
            recovered.append(path.name)
        return recovered

    def complete(self, job: Job, result: dict) -> pathlib.Path:
        result = {**result, "job_id": job.job_id, "finished_at": _utcnow()}
        done = self.root / "done" / f"{job.job_id}.json"
        _write_atomic(done, result)
        job.path.unlink(missing_ok=True)
        return done

    def prune(self, retention_days: int) -> int:
        cutoff = time.time() - retention_days * 86400
        removed = 0
        for path in (self.root / "done").glob("*.json"):
            if path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
                removed += 1
        return removed

    # -- liveness --------------------------------------------------------
    @property
    def status_path(self) -> pathlib.Path:
        return self.root / "service.status.json"

    def heartbeat(self, **fields) -> None:
        _write_atomic(self.status_path, {"at": _utcnow(), "pid": os.getpid(), **fields})

    def service_status(self) -> dict | None:
        try:
            return json.loads(self.status_path.read_text())
        except (OSError, json.JSONDecodeError):
            return None
