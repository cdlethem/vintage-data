"""Content-addressed artifact storage with no-follow, bounded filesystem access."""
from __future__ import annotations

import hashlib
import os
import pathlib
import re
import stat as stat_module
import secrets
from datetime import timedelta

from airflow.configuration import conf
from sqlalchemy.orm import Session

from .models import Artifact, utcnow

_DIGEST = re.compile(r"^[a-f0-9]{64}$")
_MAX_ARTIFACT = 8 * 1024 * 1024
_MEDIA_TYPES = {
    "source": "application/x-tar",
    "patch": "text/x-diff",
    "manifest": "application/json",
    "executor_report": "application/json",
    "review_report": "application/json",
}


def _media_type(kind: str) -> str:
    try:
        return _MEDIA_TYPES[kind]
    except KeyError as exc:
        raise ValueError(f"unsupported artifact kind: {kind}") from exc

def artifact_root() -> pathlib.Path:
    value = conf.get("bot_dashboard", "artifact_root", fallback="")
    if not value:
        raise RuntimeError("bot dashboard artifact root is not configured")
    root = pathlib.Path(value)
    if not root.is_absolute():
        raise RuntimeError("bot dashboard artifact root must be absolute")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = root.stat()
    if (
        root.is_symlink()
        or metadata.st_uid != os.getuid()
        or not stat_module.S_ISDIR(metadata.st_mode)
        or metadata.st_mode & 0o077
    ):
        raise RuntimeError("bot dashboard artifact root ownership or permissions are unsafe")
    return root


def _relative(digest: str) -> pathlib.Path:
    if not _DIGEST.fullmatch(digest):
        raise ValueError("invalid artifact digest")
    return pathlib.Path(digest[:2]) / digest


def put_artifact(
    session: Session,
    *,
    kind: str,
    content: bytes,
    expected_sha256: str | None = None,
    owner_execution_id: str | None = None,
) -> dict:
    if not content or len(content) > _MAX_ARTIFACT:
        raise ValueError("artifact size is outside configured bounds")
    digest = hashlib.sha256(content).hexdigest()
    media_type = _media_type(kind)
    if expected_sha256 is not None and digest != expected_sha256:
        raise ValueError("artifact digest mismatch")
    existing = session.get(Artifact, digest)
    if existing:
        if (
            existing.byte_count != len(content)
            or existing.kind != kind
            or existing.media_type != media_type
        ):
            raise ValueError("artifact identity already contains different content")
        return artifact_dict(existing)
    root = artifact_root()
    relative = _relative(digest)
    directory = root / relative.parent
    directory.mkdir(mode=0o700, exist_ok=True)
    target = root / relative
    temporary = directory / f".{digest}.{secrets.token_hex(8)}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        if target.exists():
            if target.is_symlink() or target.read_bytes() != content:
                raise ValueError("artifact target is unsafe or inconsistent")
            temporary.unlink()
        else:
            os.replace(temporary, target)
            target.chmod(0o600)
    finally:
        if temporary.exists():
            temporary.unlink()
    retention = conf.getint("bot_dashboard", "artifact_retention_days", fallback=365)
    row = Artifact(
        sha256=digest,
        kind=kind,
        media_type=media_type,
        byte_count=len(content),
        relative_path=str(relative),
        owner_execution_id=owner_execution_id,
        expires_at=utcnow() + timedelta(days=retention),
    )
    session.add(row)
    session.flush()
    return artifact_dict(row)


def read_artifact(session: Session, digest: str) -> tuple[Artifact, bytes]:
    row = session.get(Artifact, digest)
    if row is None:
        raise FileNotFoundError("artifact not found")
    root = artifact_root()
    relative = _relative(digest)
    if row.relative_path != str(relative):
        raise ValueError("artifact database path is inconsistent")
    path = root / relative
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat_module.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_mode & 0o077
            or metadata.st_size != row.byte_count
            or metadata.st_size > _MAX_ARTIFACT
        ):
            raise ValueError("artifact file metadata is invalid")
        content = bytearray()
        while len(content) <= _MAX_ARTIFACT:
            chunk = os.read(descriptor, min(65536, _MAX_ARTIFACT + 1 - len(content)))
            if not chunk:
                break
            content.extend(chunk)
    finally:
        os.close(descriptor)
    body = bytes(content)
    if hashlib.sha256(body).hexdigest() != digest:
        raise ValueError("artifact file digest is invalid")
    return row, body


def read_artifact_slice(
    session: Session, digest: str, *, offset: int, limit: int
) -> tuple[Artifact, bytes]:
    if offset < 0 or limit < 1 or limit > 900_000:
        raise ValueError("artifact range is outside configured bounds")
    row = session.get(Artifact, digest)
    if row is None:
        raise FileNotFoundError("artifact not found")
    relative = _relative(digest)
    if row.relative_path != str(relative) or offset > row.byte_count:
        raise ValueError("artifact database range is inconsistent")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(artifact_root() / relative, flags)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat_module.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_mode & 0o077
            or metadata.st_size != row.byte_count
            or metadata.st_size > _MAX_ARTIFACT
        ):
            raise ValueError("artifact file metadata is invalid")
        os.lseek(descriptor, offset, os.SEEK_SET)
        body = os.read(descriptor, min(limit, row.byte_count - offset))
    finally:
        os.close(descriptor)
    return row, body


def delete_artifact_file(relative_path: str) -> None:
    relative = pathlib.Path(relative_path)
    if len(relative.parts) != 2 or relative.is_absolute() or ".." in relative.parts:
        raise ValueError("invalid artifact relative path")
    expected = _relative(relative.name)
    if relative != expected:
        raise ValueError("invalid artifact relative path")
    path = artifact_root() / relative
    try:
        path.unlink()
    except FileNotFoundError:
        pass

def artifact_dict(row: Artifact) -> dict:
    return {
        "sha256": row.sha256,
        "kind": row.kind,
        "byte_count": row.byte_count,
        "media_type": row.media_type,
        "owner_execution_id": row.owner_execution_id,
        "created_at": row.created_at.isoformat(),
        "expires_at": row.expires_at.isoformat(),
    }
