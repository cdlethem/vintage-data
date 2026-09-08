"""Filesystem-only hold/release/candidate operations for the single-writer loader.

These commands never open the warehouse writable. They only read/write files under
the configured raw root:

* ``install`` — write a ``<stem>.ndjson.hold.json`` marker next to an artifact.
  Refuses out-of-root paths and a missing success manifest, so an operator
  cannot hold a phantom file or drop a marker on a run that never published.
* ``release`` — lift a hold. Re-hashes the artifact (and its manifest, when the
  marker recorded one) against the hash captured at hold time and refuses to
  proceed if either changed. The active marker is atomically renamed to
  ``<stem>.ndjson.hold.released.json`` with the release evidence, so the audit
  trail survives and the loader can treat the path as a normal candidate again.
* ``candidates`` — classify every NDJSON under the raw root (or a filtered
  subset) and report, per file, which of ``held / disabled / already_loaded /
  attempts_exhausted / young / unmanifested / pending`` it is, plus the counts.
  This is the operator's "what will the next scan pick up" answer.

The marker body and the re-hash-on-release check live in the extract side
(``orchestration/include/run_holds.py``); this module is a thin load-side view:
it reads the same two marker filenames plus the manifest, never re-implements the
 hash check, and keeps every write ``tmp-file + os.replace``.
"""
import pathlib
import sys
import time
from dataclasses import dataclass
from typing import Any

from .config import REPO_ROOT

#: ``run_holds`` lives on the extract side; it is not required on PYTHONPATH
#: for ``load/bin/loader`` or ``python -m loader``. Add it if that failed.
if (REPO_ROOT / "orchestration" / "include").as_posix() not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "orchestration" / "include"))

from run_holds import (
    fingerprint,
    install_hold as _install_hold,
    read_marker,
    release_hold as _release_hold,
)


def hold_marker_path(artifact: pathlib.Path) -> pathlib.Path:
    artifact = pathlib.Path(artifact)
    return artifact.with_name(artifact.name + ".hold.json")


def released_marker_path(artifact: pathlib.Path) -> pathlib.Path:
    artifact = pathlib.Path(artifact)
    return artifact.with_name(artifact.name + ".hold.released.json")


def active_hold(artifact: pathlib.Path) -> dict | None:
    """The live hold body, or ``None`` when this artifact is not on hold."""
    artifact = pathlib.Path(artifact)
    if not hold_marker_path(artifact).is_file() or released_marker_path(artifact).is_file():
        return None
    return read_marker(hold_marker_path(artifact))


def manifest_path(artifact: pathlib.Path) -> pathlib.Path:
    return pathlib.Path(artifact).with_name(pathlib.Path(artifact).name + ".meta.json")


def _resolve_raw_root(path: pathlib.Path, raw_root: pathlib.Path | None) -> pathlib.Path:
    path = pathlib.Path(path).expanduser()
    if raw_root is None:
        # Walk up to the nearest ``source=<name>`` hive directory; its parent is
        # the raw root the loader scans.
        for head in path.parents:
            if head.name.startswith("source="):
                return head.parent
        raise ValueError(f"cannot infer the raw root from {path}; pass --raw-root explicitly")
    raw = pathlib.Path(raw_root).expanduser()
    try:
        path.relative_to(raw)
    except ValueError as exc:
        raise ValueError(f"{path} is not under the raw root {raw}") from exc
    return raw


def install(path: pathlib.Path, *, reason: str, actor: str,
            raw_root: pathlib.Path | None = None,
            metrics: dict | None = None) -> dict:
    """Hold one artifact; the marker is written atomically next to it."""
    path = pathlib.Path(path).expanduser()
    raw = _resolve_raw_root(path, raw_root)
    manifest = manifest_path(path)
    if not manifest.is_file():
        raise FileNotFoundError(f"no success manifest to hold against: {manifest}")
    return _install_hold(path, raw, reason=reason, actor=actor,
                         manifest=manifest, metrics=metrics)


def release(path: pathlib.Path, *, evidence: str, actor: str,
            raw_root: pathlib.Path | None = None,
            expect_sha256: str | None = None) -> dict:
    """Release one hold, re-proving the bytes first; refuses on any change."""
    path = pathlib.Path(path).expanduser()
    raw = _resolve_raw_root(path, raw_root)
    return _release_hold(path, raw, evidence=evidence, actor=actor,
                         expect_sha256=expect_sha256)


def classify(artifact: pathlib.Path, *, ledger: dict | None = None,
             max_attempts: int = 5, age_seconds: int = 300,
             disabled_sources: set[str] | None = None) -> dict:
    """Classify one NDJSON file.

    ``ledger`` maps an absolute artifact path string to its ``_load.files`` row
    (the loader writes one per discovered file); when absent, only the
    filesystem state is reported.
    """
    artifact = pathlib.Path(artifact)
    out: dict = {
        "path": str(artifact),
        "relative_path": _display_rel(artifact),
        "held": False,
        "hold_reason": None,
        "manifest": manifest_path(artifact).is_file(),
        "ledger_status": None,
        "attempts": 0,
        "class": "unknown",
    }

    source = _source_of(artifact)
    if disabled_sources and source in disabled_sources:
        out["class"] = "disabled"
        return out

    hold = active_hold(artifact)
    if hold is not None:
        out["held"] = True
        out["hold_reason"] = hold.get("reason")
        out["class"] = "held"
        return out

    row = (ledger or {}).get(str(artifact))
    if row:
        out["ledger_status"] = row.get("status")
        out["attempts"] = int(row.get("attempts", 0) or 0)
        if row.get("status") == "loaded":
            out["class"] = "already_loaded"
        elif out["attempts"] >= max_attempts:
            out["class"] = "attempts_exhausted"
        else:
            out["class"] = "pending"
        return out

    if not out["manifest"]:
        out["class"] = "unmanifested"
        return out

    age = max(0, int(time.time() - artifact.stat().st_mtime))
    out["class"] = "young" if age < age_seconds else "pending"
    return out


def _source_of(artifact: pathlib.Path) -> str:
    for head in pathlib.Path(artifact).parents:
        if head.name.startswith("source="):
            return head.name.split("=", 1)[1]
    return ""


def _display_rel(artifact: pathlib.Path) -> str:
    """The hive path the operator sees in listings (``source=<n>/dt=<d>/<file>``)."""
    artifact = pathlib.Path(artifact)
    for head in artifact.parents:
        if head.name.startswith("source="):
            try:
                return str(artifact.relative_to(head.parent))
            except ValueError:
                break
    return str(artifact)


def candidates(raw_root: pathlib.Path | None, *, sources: list[str] | None = None,
               paths: list[str] | None = None, ledger: dict | None = None,
               max_attempts: int = 5, age_seconds: int = 300,
               disabled_sources: set[str] | None = None) -> dict:
    """Classify NDJSON files under the raw root (or a filtered subset)."""
    files: list[pathlib.Path] = []
    if paths:
        files = [pathlib.Path(p).expanduser() for p in paths]
    elif raw_root is not None:
        root = pathlib.Path(raw_root).expanduser()
        if root.is_file():
            files = [root]
        else:
            for source_dir in sorted(root.iterdir()):
                if not source_dir.is_dir() or not source_dir.name.startswith("source="):
                    continue
                name = source_dir.name.split("=", 1)[1]
                if sources and name not in sources:
                    continue
                for dt_dir in sorted(source_dir.iterdir()):
                    if not dt_dir.is_dir() or not dt_dir.name.startswith("dt="):
                        continue
                    for artifact in sorted(dt_dir.iterdir()):
                        if artifact.is_file() and artifact.suffix == ".ndjson":
                            files.append(artifact)
    else:
        raise ValueError("pass --paths or a raw root")

    classified = [classify(f, ledger=ledger, max_attempts=max_attempts,
                           age_seconds=age_seconds, disabled_sources=disabled_sources)
                  for f in files]
    by_class: dict[str, int] = {}
    for item in classified:
        by_class[item["class"]] = by_class.get(item["class"], 0) + 1
    counts = {
        "total": len(classified),
        "held": by_class.get("held", 0),
        "disabled": by_class.get("disabled", 0),
        "pending": by_class.get("pending", 0),
        "already_loaded": by_class.get("already_loaded", 0),
        "attempts_exhausted": by_class.get("attempts_exhausted", 0),
        "young": by_class.get("young", 0),
        "unmanifested": by_class.get("unmanifested", 0),
        "unknown": by_class.get("unknown", 0),
    }
    return {
        "raw_root": str(raw_root) if raw_root is not None else None,
        "counts": counts,
        "files": classified,
    }