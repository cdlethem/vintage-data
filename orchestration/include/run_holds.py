"""The run-hold contract: a sibling ``<artifact>.hold.json`` marker.

A *hold* is a filesystem-only way to mark one committed extract artifact as
"do not load this, and do not touch the evidence." It is the containment step
for an anomalous raw file: the artifact and its sidecars stay byte-for-byte
intact, the marker records *why* and *what we measured* at hold time, and the
one-way loader simply stops selecting the file while the mark is active.

Layout, beside the sink output (all under the raw root)::

    raw/source=<name>/dt=<date>/<stem>.ndjson            # the artifact, unchanged
    raw/source=<name>/dt=<date>/<stem>.ndjson.meta.json  # success manifest, unchanged
    raw/source=<name>/dt=<date>/<stem>.ndjson.hold.json  # active hold (this file)

Releasing a hold is evidence-gated: it re-hashes the artifact *and* its manifest
and refuses if either differs from what the marker recorded, so a hold can never
be lifted quietly over evidence that was modified in the meantime. Release does
not delete the artifact or the marker — it atomically renames the active mark to
``<stem>.ndjson.hold.released.json`` carrying the release decision, so the audit
trail survives. Everything that can be wrong is refused loudly and nothing is
ever renamed, truncated, or copied.

This module is the one place the marker schema, its hashing, and its atomic
write/release live. ``sinks.LocalSink`` and the load layer (``discovery`` and the
CLI) both go through it, so a hold and a candidate-exclusion can never drift into
two notions of the same bytes.
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
from datetime import datetime, timezone

#: Hold marker schema version.
HOLD_SCHEMA_VERSION = 1

HOLD_SUFFIX = ".hold.json"
RELEASED_SUFFIX = ".hold.released.json"
_CHUNK = 4 * 1024 * 1024


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: pathlib.Path) -> str:
    """Stream the file's SHA-256 (the artifacts here can be multi-GB)."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fingerprint(path: pathlib.Path) -> dict:
    """The two facts a hold must be able to re-prove: size and sha256."""
    stat = path.stat()
    return {"size_bytes": stat.st_size, "sha256": sha256_file(path)}


def hold_path(artifact: pathlib.Path) -> pathlib.Path:
    return artifact.with_name(artifact.name + HOLD_SUFFIX)


def released_path(artifact: pathlib.Path) -> pathlib.Path:
    return artifact.with_name(artifact.name + RELEASED_SUFFIX)


def write_atomic_json(path: pathlib.Path, payload: dict) -> None:
    """tmp-file + ``os.replace``: a reader never sees a half-written marker."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def read_marker(path: pathlib.Path) -> dict | None:
    try:
        return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def is_active_hold(artifact: pathlib.Path) -> bool:
    """True when a live hold marker sits next the artifact (and no release yet)."""
    artifact = pathlib.Path(artifact)
    return hold_path(artifact).is_file() and not released_path(artifact).is_file()


def build_hold_marker(artifact: pathlib.Path, manifest: pathlib.Path, *,
                      reason: str, actor: str, metrics: dict | None = None) -> dict:
    """Assemble the hold body, fingerprinting the artifact and manifest now.

    Both fingerprints are recorded so a later release can re-prove it covered
    exactly this evidence. ``metrics`` is the evidence the holder relied on
    (validation totals, a comparison to the prior batch, the termination cause)
    — free-form, retained verbatim.
    """
    for path in (artifact, manifest):
        if not path.is_file():
            raise FileNotFoundError(f"hold evidence is missing: {path}")
    return {
        "schema_version": HOLD_SCHEMA_VERSION,
        "created_at": _utcnow_iso(),
        "reason": reason,
        "actor": actor,
        "artifact": str(artifact),
        "artifact_sha256": fingerprint(artifact)["sha256"],
        "artifact_size_bytes": fingerprint(artifact)["size_bytes"],
        "manifest": str(manifest),
        "manifest_sha256": fingerprint(manifest)["sha256"],
        "manifest_size_bytes": fingerprint(manifest)["size_bytes"],
        "metrics": metrics or {},
    }


def install_hold(artifact: pathlib.Path, raw_root: pathlib.Path, *,
                 reason: str, actor: str, manifest: pathlib.Path | None = None
                 , metrics: dict | None = None) -> dict:
    """Write an active hold marker atomically.

    Refuses when the artifact is not under the raw root (a hold must name a file
    the loader actually discovers) or when its success manifest is absent. Does
    not touch the artifact itself under any circumstance.
    """
    artifact = pathlib.Path(artifact).expanduser()
    raw_root = pathlib.Path(raw_root).expanduser()
    try:
        artifact.relative_to(raw_root)
    except ValueError as exc:
        raise ValueError(f"{artifact} is not under the raw root {raw_root}") from exc
    manifest = pathlib.Path(manifest or artifact.with_name(artifact.name + ".meta.json")).expanduser()

    marker = build_hold_marker(artifact, manifest, reason=reason, actor=actor, metrics=metrics)
    write_atomic_json(hold_path(artifact), marker)
    return marker


def release_hold(artifact: pathlib.Path, raw_root: pathlib.Path, *,
                 evidence: str, actor: str,
                 expect_sha256: str | None = None) -> dict:
    """Lift an active hold: re-prove the bytes, then rename the marker to released.

    ``evidence`` must name what was checked out-of-band (a validator report, a
    reviewer verdict, a comparison) and is retained on the released marker. If an
    artifact sha256 is supplied it must also match, so the caller can assert the
    bytes it inspected are still the ones on disk. Refuses — without renaming —
    when the artifact or manifest is missing, changed, or the marker is absent.
    """
    artifact = pathlib.Path(artifact).expanduser()
    active = hold_path(artifact)
    if not active.is_file():
        raise FileNotFoundError(f"no active hold to release for {artifact}")
    marker = read_marker(active) or {}

    # Re-prove the artifact.
    if not artifact.is_file():
        raise FileNotFoundError(f"hold release: artifact is gone: {artifact}")
    artifact_fp = fingerprint(artifact)
    if marker.get("artifact_sha256") and artifact_fp["sha256"] != marker["artifact_sha256"]:
        raise ValueError(
            "hold release refused: artifact changed since hold "
            f"(recorded {marker['artifact_sha256']}, now {artifact_fp['sha256']})"
        )
    if expect_sha256 and artifact_fp["sha256"] != expect_sha256:
        raise ValueError(
            f"hold release refused: artifact no longer matches expected hash {expect_sha256}"
        )

    # Re-prove the manifest, if the hold recorded one.
    manifest_path = marker.get("manifest")
    if manifest_path:
        manifest_path = pathlib.Path(manifest_path)
        if not manifest_path.is_file():
            raise FileNotFoundError(f"hold release: manifest is gone: {manifest_path}")
        manifest_fp = fingerprint(manifest_path)
        if marker.get("manifest_sha256") and manifest_fp["sha256"] != marker["manifest_sha256"]:
            raise ValueError(
                "hold release refused: manifest changed since hold "
                f"(recorded {marker['manifest_sha256']}, now {manifest_fp['sha256']})"
            )

    release = {
        **marker,
        "released_at": _utcnow_iso(),
        "release_actor": actor,
        "release_evidence": evidence,
        "release_artifact_sha256": artifact_fp["sha256"],
        "release_artifact_size_bytes": artifact_fp["size_bytes"],
        "release_manifest_sha256": (
            fingerprint(pathlib.Path(manifest_path))["sha256"]
            if manifest_path and pathlib.Path(manifest_path).is_file() else None
        ),
    }
    # Move the ACTIVE marker to the released name atomically: the audit body is
    # preserved, the active mark is cleared, and a crash cannot leave two marks.
    staged = active.with_name(active.name + ".released-staged")
    write_atomic_json(staged, release)
    os.replace(staged, released_path(artifact))
    os.replace(active, active.with_name(active.name + ".superseded"))
    os.remove(active.with_name(active.name + ".superseded"))
    return release