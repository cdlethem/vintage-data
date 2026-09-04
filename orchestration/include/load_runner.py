"""Airflow's side of the load layer.

The DAG never opens the warehouse. It submits a job to the loader queue and
waits for the single writer service to publish the result — so Airflow needs
no warehouse driver, no credentials, and can't collide with another writer.
Everything that touches the destination lives in ``load/loader``.
"""
import json
import logging
import pathlib
import sys
import time
import uuid
from datetime import datetime, timezone

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
LOAD_ROOT = REPO_ROOT / "load"
if str(LOAD_ROOT) not in sys.path:
    sys.path.insert(0, str(LOAD_ROOT))

from loader.config import load_config
from loader.queue import JobQueue

#: A heartbeat older than this means the writer service is down or wedged.
STALE_HEARTBEAT_S = 300

log = logging.getLogger(__name__)


def _heartbeat_age(queue: JobQueue) -> float | None:
    status = queue.service_status()
    if not status:
        return None
    try:
        at = datetime.fromisoformat(status["at"])
    except (KeyError, ValueError):
        return None
    return (datetime.now(timezone.utc) - at).total_seconds()


def run(max_files: int | None = None, sources=None, **_) -> dict:
    """Submit one scan-and-load job and wait for the writer to finish it."""
    config = load_config()
    queue = JobQueue(config.queue_dir)

    age = _heartbeat_age(queue)
    if age is None or age > STALE_HEARTBEAT_S:
        raise RuntimeError(
            f"loader service heartbeat is {'missing' if age is None else f'{age:.0f}s old'} "
            f"({queue.status_path}). Check: systemctl status extract-loader")

    body = {"kind": "scan", "load_id": uuid.uuid4().hex,
            "max_files": max_files if max_files else config.dag.max_files_per_run}
    if sources:
        body["sources"] = list(sources)
    job_id = queue.submit(body)
    log.info("submitted load job %s (queue depth %d)", job_id, queue.depth())

    started = time.monotonic()
    result = queue.wait(job_id, timeout_s=config.dag.wait_timeout_s, poll_s=5.0)
    if result is None:
        raise RuntimeError(
            f"load job {job_id} did not finish within {config.dag.wait_timeout_s}s; "
            f"it stays queued — check `journalctl -u extract-loader -f`")

    log.info("load job %s finished in %.1fs:\n%s", job_id, time.monotonic() - started,
             json.dumps(result, indent=2, default=str))
    for source, stats in sorted((result.get("by_source") or {}).items()):
        log.info("  %-40s %6d files  %10d rows  %d failed",
                 source, stats["files"], stats["rows"], stats["failed"])
    if result.get("files_pending_after"):
        log.info("more files remain (max_files reached); the next run continues")

    if result.get("status") == "failed":
        raise RuntimeError(f"load job {job_id} failed: {result.get('errors')}")
    if result.get("files_failed"):
        raise RuntimeError(
            f"load job {job_id} loaded {result['files_loaded']} file(s) but "
            f"{result['files_failed']} failed: {result.get('errors')[:5]}")
    return result
