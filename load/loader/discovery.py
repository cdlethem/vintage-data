"""Find sink files that are ready to load.

The sink lays runs down as
``<root>/source=<name>/dt=<YYYY-MM-DD>/<name>_<ts>.ndjson`` plus a sibling
``.meta.json`` manifest written after the atomic rename. Discovery is pure
filesystem work — deciding which of these files are *new* needs the ledger,
which only the writer service can read, so that diff lives in the service.
"""
from __future__ import annotations

import json
import pathlib
import re
import time
from dataclasses import dataclass

DT_RE = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})$")
SOURCE_RE = re.compile(r"^source=(.+)$")


@dataclass(frozen=True)
class SinkFile:
    """One committed extract run: the file plus what its manifest knows."""

    source: str
    path: pathlib.Path
    dt: str
    batch_id: str            # the file stem — the extract run's identity
    size_bytes: int
    mtime: float
    records: int | None = None       # manifest record count, when available
    extract_started_at: str | None = None
    script: str | None = None
    # Schema-v2 manifest evidence (present when the sink wrote v2 metadata).
    schema_version: int | None = None
    extract_status: str | None = None        # success | failed
    extract_health: str | None = None        # healthy | degraded | open_circuit | failed | None
    extract_completeness: str | None = None  # complete | partial | unknown | failed | None
    extract_duration_s: float | None = None
    partition_attempted: int | None = None
    partition_succeeded: int | None = None
    partition_failed: int | None = None
    extract_metrics: dict | None = None
    # The ``.hold.json`` marker, if any sits beside the file. ``None`` means
    # no marker; a present marker means the file is on hold and excluded
    # from load candidates (see ``scan``).
    held: dict | None = None

    @property
    def key(self) -> str:
        return str(self.path)


def _manifest(path: pathlib.Path) -> dict:
    sidecar = path.with_name(path.name + ".meta.json")
    try:
        return json.loads(sidecar.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _hold_marker(path: pathlib.Path) -> dict | None:
    """The live hold marker for this artifact, or ``None``.

    Reuses the extract-side canonical filename (``<file>.hold.json``) and a
    ``.hold.released.json`` file — the audit of a release — so a file can
    never be both unhold-able and re-hold-able at the same time without a
    fresh marker.
    """
    active = path.with_name(path.name + ".hold.json")
    released = path.with_name(path.name + ".hold.released.json")
    if not active.is_file() or released.is_file():
        return None
    try:
        return json.loads(active.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _manifest_fields(meta: dict) -> dict:
    """Project a v1 or v2 manifest to the extra ``SinkFile`` fields."""
    if not meta:
        return {}
    fields = {
        "schema_version": meta.get("schema_version"),
        "extract_status": meta.get("status") or meta.get("extract_status"),
        "extract_health": meta.get("health"),
        "extract_completeness": meta.get("completeness"),
        "extract_duration_s": meta.get("duration_s"),
    }
    partitions = meta.get("partitions") or {}
    if isinstance(partitions, dict):
        fields["partition_attempted"] = partitions.get("attempted")
        fields["partition_succeeded"] = partitions.get("succeeded")
        fields["partition_failed"] = partitions.get("failed")
    else:
        for k in ("partition_attempted", "partition_succeeded", "partition_failed"):
            fields[k] = None
    fields["extract_metrics"] = meta.get("metrics") if isinstance(meta.get("metrics"), dict) else None
    return fields


def scan(root: pathlib.Path, *, sources=None, min_age_s: int = 60, now=None,
         include_held: bool = False) -> list[SinkFile]:
    """List loadable files under the sink root, oldest run first.

    Two files are skipped on purpose: anything younger than ``min_age_s`` with
    no manifest yet (the run may still be mid-commit), and ``.tmp`` staging
    files, which the glob already excludes. Skipping is always safe — the next
    scan picks the file up.

    Hold semantics: an active ``.hold.json`` marker makes a file *not* a load
    candidate. ``scan`` honors ``include_held`` (default ``False``): when the
    flag is set the held file is still returned, so callers that need it (the
    ``candidates`` command, the loader's audit logging) can observe it, while
    the loader's candidate path never loads one.
    """
    now = time.time() if now is None else now
    wanted = set(sources) if sources else None
    found: list[SinkFile] = []

    for source_dir in sorted(root.glob("source=*")):
        match = SOURCE_RE.match(source_dir.name)
        if not match or not source_dir.is_dir():
            continue
        source = match.group(1)
        if wanted is not None and source not in wanted:
            continue
        for dt_dir in sorted(source_dir.glob("dt=*")):
            dt_match = DT_RE.match(dt_dir.name)
            if not dt_match:
                continue
            for path in sorted(dt_dir.glob("*.ndjson")):
                try:
                    stat = path.stat()
                except OSError:
                    continue
                meta = _manifest(path)
                hold = _hold_marker(path)
                if hold is not None and not include_held:
                    continue
                if not meta and (now - stat.st_mtime) < min_age_s:
                    continue  # may still be committing; catch it next scan
                found.append(SinkFile(
                    source=source,
                    path=path,
                    dt=dt_match.group(1),
                    batch_id=path.name[: -len(".ndjson")],
                    size_bytes=stat.st_size,
                    mtime=stat.st_mtime,
                    records=meta.get("records"),
                    extract_started_at=meta.get("started_at"),
                    script=meta.get("script"),
                    held=hold,
                    **_manifest_fields(meta),
                ))
    found.sort(key=lambda f: (f.source, f.dt, f.path.name))
    return found
