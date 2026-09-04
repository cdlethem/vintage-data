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

    @property
    def key(self) -> str:
        return str(self.path)


def _manifest(path: pathlib.Path) -> dict:
    sidecar = path.with_name(path.name + ".meta.json")
    try:
        return json.loads(sidecar.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def scan(root: pathlib.Path, *, sources=None, min_age_s: int = 60, now=None) -> list[SinkFile]:
    """List loadable files under the sink root, oldest run first.

    Two files are skipped on purpose: anything younger than ``min_age_s`` with
    no manifest yet (the run may still be mid-commit), and ``.tmp`` staging
    files, which the glob already excludes. Skipping is always safe — the next
    scan picks the file up.
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
                ))
    found.sort(key=lambda f: (f.source, f.dt, f.path.name))
    return found
