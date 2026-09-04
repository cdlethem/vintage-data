"""The single writer.

One process, one connection, one job at a time. Airflow (or the CLI) submits
jobs; this loop claims them in order and applies them serially, which is the
whole concurrency model of the load layer: no distributed locking, no
write conflicts, and a warehouse that can be swapped without any of the
callers noticing.

Between jobs the connection is closed once the service has been idle for
``service.idle_release_s``, so a human can open the same DuckDB file to poke
around. The next job reopens it, waiting out any reader that holds the lock.
"""
from __future__ import annotations

import json
import logging
import pathlib
import signal
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from .config import LoadConfig, load_config
from .destinations import LoadRequest, get_destination
from .discovery import scan
from .queue import Job, JobQueue
from .schema import infer_columns, sanitize, table_columns

log = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sample_records(path, limit: int) -> tuple[list[dict], int]:
    """Read up to ``limit`` records (0 = all) for schema inference."""
    records: list[dict] = []
    malformed = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if limit and len(records) >= limit:
                break
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if isinstance(record, dict):
                records.append(record)
    return records, malformed


class LoaderService:
    def __init__(self, config: LoadConfig):
        self.config = config
        self.queue = JobQueue(config.queue_dir)
        self.destination = get_destination(config.destination,
                                           config.raw_schema, config.meta_schema)
        self._stop = False
        self._idle_since: float | None = None

    # -- lifecycle -------------------------------------------------------
    def stop(self, *_):
        log.info("stop requested; finishing the current job first")
        self._stop = True

    def run_forever(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        recovered = self.queue.recover()
        if recovered:
            log.warning("requeued %d job(s) orphaned by a restart: %s",
                        len(recovered), ", ".join(recovered))
        log.info("loader service up: destination=%s db=%s queue=%s",
                 self.destination.name, self.config.destination.get("database"),
                 self.config.queue_dir)
        last_prune = 0.0
        while not self._stop:
            job = self.queue.claim()
            if job is None:
                self._tick_idle()
                if time.time() - last_prune > 3600:
                    self.queue.prune(self.config.service.done_retention_days)
                    last_prune = time.time()
                time.sleep(self.config.service.poll_interval_s)
                continue
            self._idle_since = None
            self.queue.heartbeat(state="working", job_id=job.job_id, kind=job.kind)
            result = self.run_job(job)
            self.queue.complete(job, result)
            self.queue.heartbeat(state="idle", last_job=job.job_id)
        self.destination.close()
        self.queue.heartbeat(state="stopped")
        log.info("loader service stopped")

    def _tick_idle(self) -> None:
        """Release the warehouse after a quiet spell so readers can attach."""
        now = time.time()
        if self._idle_since is None:
            self._idle_since = now
        self.queue.heartbeat(state="idle", connected=self.destination.connected,
                             queue_depth=self.queue.depth())
        if (self.destination.connected
                and now - self._idle_since >= self.config.service.idle_release_s):
            log.info("idle for %.0fs; releasing the warehouse connection",
                     now - self._idle_since)
            self.destination.close()

    # -- jobs ------------------------------------------------------------
    def run_job(self, job: Job) -> dict:
        started = _utcnow()
        load_id = job.body.get("load_id") or uuid.uuid4().hex
        result = {
            "status": "ok", "load_id": load_id, "kind": job.kind,
            "submitted_at": job.body.get("submitted_at"),
            "started_at": started.isoformat(),
            "files_seen": 0, "files_loaded": 0, "files_failed": 0, "files_skipped": 0,
            "rows_loaded": 0, "by_source": {}, "errors": [],
        }
        try:
            self.destination.connect()
            self.destination.ensure_schemas()
            if job.kind == "ping":
                result["message"] = "service alive"
            elif job.kind in ("scan", "files"):
                self._run_load(job, load_id, result)
            else:
                raise ValueError(f"unknown job kind {job.kind!r}")
        except Exception as exc:
            log.exception("job %s failed", job.job_id)
            result["status"] = "failed"
            result["errors"].append(f"{type(exc).__name__}: {exc}")
        finished = _utcnow()
        result["finished_at"] = finished.isoformat()
        result["duration_s"] = round((finished - started).total_seconds(), 3)
        if result["files_failed"] and result["status"] == "ok":
            result["status"] = "partial"
        try:
            if self.destination.connected:
                self.destination.record_job({**result, "error": "; ".join(result["errors"])})
        except Exception:
            log.exception("could not record job %s in the ledger", load_id)
        log.info("job %s (%s): %s — %d files, %d rows in %.1fs", job.job_id, job.kind,
                 result["status"], result["files_loaded"], result["rows_loaded"],
                 result["duration_s"])
        return result

    def _candidates(self, job: Job):
        """Files this job should load: discovered, minus what the ledger has."""
        body = job.body
        if job.kind == "files":
            wanted = {str(pathlib.Path(p).expanduser()) for p in body.get("paths", [])}
            found = [f for f in scan(self.config.source_root, min_age_s=0)
                     if f.key in wanted]
        else:
            found = scan(self.config.source_root,
                         sources=body.get("sources"),
                         min_age_s=body.get("min_age_s", self.config.defaults.min_age_s))
        ledger = self.destination.ledger()
        candidates, skipped = [], 0
        for file in found:
            if not self.config.for_source(file.source).enabled:
                skipped += 1
                continue
            if self.destination.should_skip(file.key, ledger):
                skipped += 1
                continue
            candidates.append(file)
        # Oldest first: RAW then reads as the history it is, and a truncated
        # run always leaves the newest data as the next run's work.
        candidates.sort(key=lambda f: (f.mtime, f.key))
        max_files = body.get("max_files")
        truncated = False
        if max_files and len(candidates) > max_files:
            candidates, truncated = candidates[:max_files], True
        return found, candidates, skipped, truncated

    def _run_load(self, job: Job, load_id: str, result: dict) -> None:
        found, candidates, skipped, truncated = self._candidates(job)
        result["files_seen"] = len(found)
        result["files_skipped"] = skipped
        result["files_pending_after"] = truncated
        log.info("job %s: %d files in sink, %d already loaded/skipped, %d to load%s",
                 job.job_id, len(found), skipped, len(candidates),
                 " (capped by max_files)" if truncated else "")

        by_source: dict[str, list] = defaultdict(list)
        for file in candidates:
            by_source[file.source].append(file)

        for source, files in by_source.items():
            settings = self.config.for_source(source)
            table = settings.table or sanitize(source, fallback="source")
            stats = {"files": 0, "rows": 0, "failed": 0, "table": table}
            try:
                columns = self._columns_for(source, table, files, settings, load_id)
            except Exception as exc:
                log.exception("schema sync failed for %s", source)
                result["errors"].append(f"{source}: schema sync failed: {exc}")
                for file in files:
                    self._record_failure(file, table, load_id, str(exc), result)
                    stats["failed"] += 1
                result["by_source"][source] = stats
                continue

            for file in files:
                if self._stop:
                    log.warning("stopping mid-job; %s left for the next run", file.path.name)
                    result["files_pending_after"] = True
                    break
                try:
                    outcome = self.destination.load_file(LoadRequest(
                        source=source, table=table, file=file, columns=columns,
                        load_id=load_id, loaded_at=_utcnow(),
                        keep_payload=settings.keep_payload))
                    stats["files"] += 1
                    stats["rows"] += outcome.rows
                    result["files_loaded"] += 1
                    result["rows_loaded"] += outcome.rows
                    log.info("loaded %s -> %s.%s (%d rows, %.1fs)", file.path.name,
                             self.config.raw_schema, table, outcome.rows,
                             outcome.duration_s)
                except Exception as exc:
                    log.exception("failed to load %s", file.path)
                    self._record_failure(file, table, load_id, f"{type(exc).__name__}: {exc}",
                                         result)
                    stats["failed"] += 1
                    result["errors"].append(f"{file.path.name}: {exc}")
                self.queue.heartbeat(state="working", job_id=job.job_id,
                                     loaded=result["files_loaded"])
            result["by_source"][source] = stats

    def _columns_for(self, source, table, files, settings, load_id):
        """Infer the source's columns from a sample, then evolve the table."""
        sample: list[dict] = []
        malformed = 0
        for file in files:
            records, bad = sample_records(file.path, settings.sample_lines)
            sample.extend(records)
            malformed += bad
        if malformed:
            log.warning("%s: %d malformed line(s) in the inference sample", source, malformed)
        columns = table_columns(infer_columns(sample, settings), settings)
        return self.destination.sync_table(table, columns, load_id)

    def _record_failure(self, file, table, load_id, error, result) -> None:
        result["files_failed"] += 1
        try:
            self.destination.record_failure(file, table, load_id, error)
        except Exception:
            log.exception("could not record the failure of %s in the ledger", file.path)


def main(argv=None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Run the load layer's single writer.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    LoaderService(load_config(args.config)).run_forever()
    return 0
