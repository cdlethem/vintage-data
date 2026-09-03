"""Run one extract script and land its stdout through a sink.

The extract contract (see the repo README): a script is invoked as
``python fetch_<source>.py [positional args...]`` and prints NDJSON records to
stdout, each carrying the envelope keys ``source``, ``fetched_at``, ``id``.
Non-zero exit means failure; exit 0 with empty stdout is a valid empty result.
"""
import json
import logging
import pathlib
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

from sinks import get_sink

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "pipelines" / "extract" / "scripts"
ENVELOPE = ("source", "fetched_at", "id")

log = logging.getLogger(__name__)


def run(cfg: dict) -> dict:
    """Execute the source described by one yml config; return the run manifest."""
    name = cfg["name"]
    script = SCRIPTS_DIR / cfg["script"]
    args = [str(a) for a in cfg.get("args", [])]
    started = datetime.now(timezone.utc)
    filename = f"{name}_{started.strftime('%Y%m%dT%H%M%SZ')}.ndjson"

    sink = get_sink(cfg.get("sink", "local"))
    records = 0
    bytes_written = 0

    # stderr goes to a spooled file, not a pipe: a chatty script must never
    # deadlock against an unread pipe while we consume stdout.
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace") as err:
        proc = subprocess.Popen(
            [sys.executable, str(script), *args],
            cwd=SCRIPTS_DIR,
            stdout=subprocess.PIPE,
            stderr=err,
            text=True,
        )
        try:
            with sink.writer(name, started.strftime("%Y-%m-%d"), filename) as out:
                for line in proc.stdout:
                    if not line.strip():
                        continue
                    if records == 0:
                        _check_envelope(line)
                    out.write(line if line.endswith("\n") else line + "\n")
                    records += 1
                    bytes_written += len(line.encode("utf-8"))
            returncode = proc.wait()
        except BaseException:
            proc.kill()
            proc.wait()
            sink.discard()
            raise

        err.seek(0)
        stderr = err.read().strip()
    if stderr:
        log.info("stderr from %s:\n%s", cfg["script"], stderr)

    if returncode != 0:
        sink.discard()
        raise RuntimeError(f"{cfg['script']} exited {returncode} after {records} records")

    manifest = {
        "source": name,
        "script": cfg["script"],
        "args": args,
        "started_at": started.isoformat(),
        "duration_s": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
        "exit_code": returncode,
        "records": records,
        "bytes": bytes_written,
    }
    manifest_path = sink.commit(manifest)
    log.info("%s: %d records, %d bytes -> %s", name, records, bytes_written,
             manifest["path"] or f"no data file (manifest: {manifest_path})")
    return manifest


def _check_envelope(line: str):
    """Fail fast if the first record doesn't honor the extract contract."""
    try:
        first = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"first stdout line is not JSON: {line[:200]!r}") from exc
    missing = [k for k in ENVELOPE if k not in first]
    if missing:
        raise ValueError(f"first record is missing envelope keys {missing}: {line[:200]!r}")
