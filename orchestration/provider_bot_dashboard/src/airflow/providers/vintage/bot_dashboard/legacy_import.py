"""CLI-only migration of quarantined filesystem bot state.

This module is deliberately not imported by worker/runtime code.  Files are read
through descriptor-relative handles rooted at the two paths supplied by the
operator; manifest values are never used as filesystem paths without validation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import stat
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from airflow.configuration import conf
from airflow.utils.session import create_session
from sqlalchemy import select

from .artifacts import put_artifact
from .models import Execution, LegacyImport, Revision, RunReport, Task
from .report_schemas import failure_fingerprint, validate_run_envelope
from .service import _event, run_report_dict

_REPORT_CAP = 256 * 1024
_DEFAULT_DIFF_CAP = 700_000
_TIMESTAMP = re.compile(r"(?<!\d)(\d{8}T\d{6}Z|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)(?!\d)")
_BOT = re.compile(r"^[a-z][a-z0-9_]*$")
# Legacy package ids embed a UTC stamp, e.g. source_vetting-20260907T024917Z-3e2eb020.
_PACKAGE = re.compile(r"^[a-z0-9][A-Za-z0-9_-]{0,55}$")
_ROOT_KEYS = {"root", "runs_root", "reviews_root", "repository_root", "source_root", "worktree"}


class LegacyInputError(RuntimeError):
    """An unsafe or otherwise unverifiable legacy input."""


class _Root:
    def __init__(self, value: str, *, label: str):
        path = pathlib.Path(value)
        if not path.is_absolute():
            raise LegacyInputError(f"{label} must be an absolute directory")
        flags = os.O_RDONLY | os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            self.fd = os.open(path, flags)
        except OSError as exc:
            raise LegacyInputError(f"{label} is not a readable directory") from exc
        self.label = label
        self.display_path = str(path)
        metadata = os.fstat(self.fd)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_mode & 0o022
        ):
            os.close(self.fd)
            raise LegacyInputError(f"{label} ownership or permissions are unsafe")
        self.uid = metadata.st_uid
        self.dev = metadata.st_dev

    def close(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def __enter__(self) -> "_Root":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @staticmethod
    def _parts(relative: str | pathlib.PurePosixPath) -> tuple[str, ...]:
        value = str(relative)
        path = pathlib.PurePosixPath(value)
        if (
            not value
            or not path.parts
            or path.is_absolute()
            or "\\" in value
            or value.endswith("/")
            or any(part in {"", ".", ".."} for part in path.parts)
        ):
            raise LegacyInputError("legacy path is not a safe relative reference")
        return path.parts

    def _open_dir(self, parent: int, name: str) -> int:
        flags = os.O_RDONLY | os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(name, flags, dir_fd=parent)
        except OSError as exc:
            raise LegacyInputError("legacy directory is unavailable or unsafe") from exc
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != self.uid
            or metadata.st_dev != self.dev
            or metadata.st_mode & 0o022
        ):
            os.close(descriptor)
            raise LegacyInputError("legacy directory ownership, device, or permissions are unsafe")
        return descriptor

    def read(self, relative: str, *, suffixes: set[str], cap: int) -> tuple[bytes, os.stat_result]:
        parts = self._parts(relative)
        parent = os.dup(self.fd)
        opened: list[int] = [parent]
        try:
            for name in parts[:-1]:
                parent = self._open_dir(parent, name)
                opened.append(parent)
            leaf = parts[-1]
            if pathlib.PurePosixPath(leaf).suffix not in suffixes:
                raise LegacyInputError("legacy file suffix is not allowlisted")
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                descriptor = os.open(leaf, flags, dir_fd=parent)
            except OSError as exc:
                raise LegacyInputError("legacy file is unavailable or symlinked") from exc
            try:
                before = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(before.st_mode)
                    or before.st_uid != self.uid
                    or before.st_dev != self.dev
                    or before.st_mode & 0o022
                    or before.st_size > cap
                ):
                    raise LegacyInputError("legacy file ownership, type, device, or size is unsafe")
                body = bytearray()
                while len(body) <= cap:
                    chunk = os.read(descriptor, min(65_536, cap + 1 - len(body)))
                    if not chunk:
                        break
                    body.extend(chunk)
                after = os.fstat(descriptor)
                if len(body) > cap or after.st_size != before.st_size or after.st_ino != before.st_ino:
                    raise LegacyInputError("legacy file changed or exceeds its bound")
                return bytes(body), before
            finally:
                os.close(descriptor)
        finally:
            for descriptor in reversed(opened):
                os.close(descriptor)

    def files(self, suffixes: set[str], *, cap_for: callable) -> Iterator[dict[str, Any]]:
        """Yield safe files and explicit rejected allowlisted leaves."""
        pending: list[tuple[int, tuple[str, ...]]] = [(os.dup(self.fd), ())]
        while pending:
            directory, prefix = pending.pop()
            try:
                entries = sorted(os.scandir(directory), key=lambda item: item.name)
                for entry in entries:
                    relative = "/".join((*prefix, entry.name))
                    metadata = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(metadata.st_mode):
                        if metadata.st_uid != self.uid or metadata.st_dev != self.dev or metadata.st_mode & 0o022:
                            continue
                        child = self._open_dir(directory, entry.name)
                        pending.append((child, (*prefix, entry.name)))
                        continue
                    if pathlib.PurePosixPath(entry.name).suffix not in suffixes:
                        continue
                    cap = cap_for(entry.name, relative)
                    if not stat.S_ISREG(metadata.st_mode):
                        yield {"relative_path": relative, "body": None, "metadata": metadata, "error": "legacy file is not regular", "cap": cap}
                        continue
                    try:
                        body, metadata = self.read(relative, suffixes=suffixes, cap=cap)
                    except LegacyInputError as exc:
                        yield {"relative_path": relative, "body": None, "metadata": metadata, "error": str(exc), "cap": cap}
                    else:
                        yield {"relative_path": relative, "body": body, "metadata": metadata, "error": None, "cap": cap}
            finally:
                os.close(directory)


def _max_patch_bytes() -> int:
    """Read the policy value without requiring a Git credential for import."""
    try:
        configured = conf.getint("bot_dashboard", "max_diff_bytes", fallback=0)
    except (TypeError, ValueError):
        configured = 0
    if 0 < configured <= _DEFAULT_DIFF_CAP:
        return configured
    # Repository connection extras are the second policy source.  Read only
    # the numeric bound here; do not instantiate the Git provider or resolve a
    # network endpoint merely to inspect legacy files.
    try:
        from airflow.models.connection import Connection

        conn_id = conf.get("bot_dashboard", "git_conn_id", fallback="bot_dashboard_git")
        extra = Connection.get_connection_from_secrets(conn_id).extra_dejson
        configured = int(extra.get("max_diff_bytes", 0))
    except Exception:
        configured = _DEFAULT_DIFF_CAP
    return configured if 0 < configured <= _DEFAULT_DIFF_CAP else _DEFAULT_DIFF_CAP


def _inventory(runs: _Root, reviews: _Root) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for entry in runs.files({".json", ".md"}, cap_for=lambda _name, _path: _REPORT_CAP):
        items.append({**entry, "kind": "run_report", "root": runs, "root_kind": "runs"})
    # The package directory itself is a fixed, operator-supplied descendant.  A
    # missing packages directory is an empty legacy quarantine, not an error.
    try:
        packages_stat = os.stat("packages", dir_fd=reviews.fd, follow_symlinks=False)
        if not stat.S_ISDIR(packages_stat.st_mode) or packages_stat.st_uid != reviews.uid or packages_stat.st_dev != reviews.dev or packages_stat.st_mode & 0o022:
            raise LegacyInputError("reviews packages directory is unsafe")
        package_root = _Root.__new__(_Root)
        package_root.fd = reviews._open_dir(reviews.fd, "packages")
        package_root.uid, package_root.dev, package_root.label, package_root.display_path = reviews.uid, reviews.dev, "packages", "packages"
        try:
            for entry in package_root.files({".json", ".patch"}, cap_for=lambda name, _path: _max_patch_bytes() if name.endswith(".patch") else _REPORT_CAP):
                items.append({**entry, "kind": "review_manifest" if entry["relative_path"].endswith(".json") else "review_patch", "relative_path": f"packages/{entry['relative_path']}", "root": reviews, "root_kind": "reviews"})
        finally:
            package_root.close()
    except FileNotFoundError:
        pass
    return sorted(items, key=lambda item: (item["kind"], item["relative_path"]))


def _summary(items: list[dict[str, Any]], existing: dict[tuple[str, str], LegacyImport]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    pending: list[str] = []
    changed: list[str] = []
    aggregate = hashlib.sha256()
    for item in items:
        kind, relative = item["kind"], item["relative_path"]
        counts[kind] = counts.get(kind, 0) + 1
        digest = item.get("sha256") or hashlib.sha256(("rejected:" + str(item.get("error", ""))).encode()).hexdigest()
        aggregate.update(f"{kind}\0{relative}\0{digest}\n".encode())
        row = existing.get((kind, relative))
        if row is None:
            pending.append(relative)
        elif row.sha256 != digest or row.byte_count != (len(item["body"]) if item.get("body") is not None else 0):
            changed.append(relative)
    return {
        "counts": counts,
        "report_count": counts.get("run_report", 0),
        "package_count": counts.get("review_manifest", 0),
        "patch_count": counts.get("review_patch", 0),
        "aggregate_sha256": aggregate.hexdigest(),
        "pending_count": len(pending),
        "changed_count": len(changed),
        "pending": pending[:100],
        "changed": changed[:100],
    }


def _timestamp(item: dict[str, Any]) -> tuple[datetime, bool]:
    name = pathlib.PurePosixPath(item["relative_path"]).name
    match = _TIMESTAMP.search(name)
    if match:
        value = match.group(1)
        try:
            parsed = datetime.strptime(
                value,
                (
                    "%Y%m%dT%H%M%SZ"
                    if "-" not in value
                    else ("%Y-%m-%dT%H:%M:%S.%fZ" if "." in value else "%Y-%m-%dT%H:%M:%SZ")
                ),
            )
            return parsed.replace(tzinfo=timezone.utc), False
        except ValueError:
            pass
    return datetime.fromtimestamp(item["metadata"].st_mtime, timezone.utc), True


def _safe_json(body: bytes) -> Any:
    return json.loads(body.decode("utf-8"))

def _identity_disagreement(decoded: dict[str, Any], *, bot: str, dag_id: str, run_id: str) -> str | None:
    identity = decoded.get("identity") if isinstance(decoded.get("identity"), dict) else decoded
    if decoded.get("agent") is not None and decoded.get("agent") != bot:
        return "report_bot_identity_mismatch"
    expected = {"bot": bot, "dag_id": dag_id, "run_id": run_id, "task_id": "import", "map_index": -1, "try_number": 1}
    for key, value in expected.items():
        if key in identity and identity[key] != value:
            return "report_identity_mismatch"
    return None


def _failure(*, code: str, detail: str, component: str, task: str = "import") -> dict[str, str]:
    bounded = detail[:8192]
    return {
        "class": "LegacyImport",
        "code": code,
        "fingerprint": failure_fingerprint(origin="legacy", component=component, task=task, error_class="LegacyImport", code=code, detail=bounded),
        "detail": bounded,
    }


def _build_report(item: dict[str, Any]) -> tuple[dict[str, Any], str, dict[str, Any]]:
    relative = pathlib.PurePosixPath(item["relative_path"])
    bot = relative.parts[0] if len(relative.parts) > 1 else "unknown"
    if not _BOT.fullmatch(bot):
        bot = "unknown"
    dag_id = f"legacy__{bot}"
    run_id = f"legacy__{hashlib.sha256(item['relative_path'].encode()).hexdigest()}"
    timestamp, inferred = _timestamp(item)
    details: dict[str, Any] = {"relative_path": item["relative_path"]}
    if inferred:
        details["legacy_timestamp_inferred"] = True
    outcome = "failed"
    reason = "legacy_invalid_input"
    payload = None
    schema = None
    body_json = None
    body_text = None
    failure: dict[str, str] | None = None
    if item.get("error"):
        reason = "legacy_file_rejected"
        failure = _failure(code="unsafe_input", detail=item["error"], component="report")
    elif item["relative_path"].endswith(".json"):
        try:
            decoded = _safe_json(item["body"])
            if not isinstance(decoded, dict):
                raise ValueError("legacy report is not an object")
            body_json = decoded
            disagreement = _identity_disagreement(decoded, bot=bot, dag_id=dag_id, run_id=run_id)
            if disagreement:
                raise ValueError(disagreement)
            for digest_key in ("sha256", "digest", "report_sha256"):
                if digest_key in decoded and decoded[digest_key] != item["sha256"]:
                    raise ValueError("report digest mismatch")
            old_status = str(decoded.get("status") or "ok")
            outcome = {"ok": "succeeded", "skipped": "skipped", "busy": "capacity_unavailable"}.get(old_status, "failed")
            reason = f"legacy_{old_status}"[:100]
            schema_by_bot = {
                "source_discovery": "source_discovery_v2", "source_vetting": "source_vetting_v2",
                "source_scheduling": "source_scheduling_v2", "cadence_review": "cadence_review_v2",
                "failure_triage": "failure_triage_v2", "analytics_engineer": "analytics_engineer_v2",
                "data_analyst": "data_analyst_v1",
                "manager": "manager_v3", "task_executor": "task_executor_v2", "pr_reviewer": "pr_reviewer_v2",
            }
            candidate_schema = schema_by_bot.get(bot)
            if outcome == "succeeded" and candidate_schema:
                try:
                    payload = validate_run_envelope({
                        "envelope_version": 1,
                        "identity": {"bot": bot, "dag_id": dag_id, "run_id": run_id, "task_id": "import", "map_index": -1, "try_number": 1},
                        "timing": {"started_at": timestamp, "finished_at": timestamp, "deadline_at": timestamp, "duration_ms": 0},
                        "outcome": "succeeded", "retry_class": "none", "reason_code": reason,
                        "failure": None, "selected_model": None, "attempts": [],
                        "context": {"sha256": hashlib.sha256(b"{}").hexdigest(), "byte_count": 2, "build_ms": 0},
                        "payload_schema": candidate_schema, "payload": decoded,
                    })["payload"]
                    schema = candidate_schema
                except Exception:
                    outcome = "failed"
                    reason = "legacy_payload_unverifiable"
                    failure = _failure(code="payload_unverifiable", detail="legacy payload does not satisfy its named schema", component="report")
            if outcome == "failed" and failure is None:
                failure = _failure(code="legacy_status", detail=reason, component="report")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            body_text = item["body"].decode("utf-8", errors="replace")
            reason = "legacy_invalid_json"
            failure = _failure(code="invalid_json", detail=str(exc), component="report")
    else:
        body_text = item["body"].decode("utf-8", errors="replace")
        reason = "legacy_text_report"
        failure = _failure(code="text_report", detail="legacy Markdown report has no typed payload", component="report")
    envelope: dict[str, Any] = {
        "envelope_version": 1,
        "identity": {"bot": bot, "dag_id": dag_id, "run_id": run_id, "task_id": "import", "map_index": -1, "try_number": 1},
        "timing": {"started_at": timestamp, "finished_at": timestamp, "deadline_at": timestamp, "duration_ms": 0},
        "outcome": outcome,
        "retry_class": "none" if outcome in {"succeeded", "skipped", "capacity_unavailable"} else "terminal",
        "reason_code": reason,
        "failure": failure,
        "selected_model": None,
        "attempts": [],
        "context": {"sha256": hashlib.sha256(b"{}").hexdigest(), "byte_count": 2, "build_ms": 0},
        "payload_schema": schema,
        "payload": payload,
    }
    try:
        envelope = validate_run_envelope(envelope)
    except Exception:
        # An old success without a recognized/valid named payload is retained as
        # a typed failed historical attempt instead of an invalid envelope.
        envelope["outcome"] = "failed"
        envelope["retry_class"] = "terminal"
        envelope["reason_code"] = "legacy_envelope_unverifiable"
        envelope["payload_schema"] = None
        envelope["payload"] = None
        envelope["failure"] = _failure(code="envelope_unverifiable", detail="legacy report cannot satisfy RunEnvelopeV1", component="report")
        envelope = validate_run_envelope(envelope)
        outcome = "failed"
    details["outcome"] = outcome
    return envelope, outcome, {**details, "body_json": body_json, "body_text": body_text, "reason": reason}


def _import_report(session: Any, item: dict[str, Any]) -> str:
    envelope, outcome, details = _build_report(item)
    timestamp = _timestamp(item)[0]
    artifact = put_artifact(session, kind="source", content=item["body"], expected_sha256=hashlib.sha256(item["body"]).hexdigest())
    item["_artifact_sha256"] = artifact["sha256"]
    identity = envelope["identity"]
    canonical = json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    envelope_sha256 = hashlib.sha256(canonical).hexdigest()
    item["_details"] = {"envelope_sha256": envelope_sha256, "outcome": outcome, "source_sha256": artifact["sha256"]}
    if session.scalar(select(RunReport.id).where(RunReport.dag_id == identity["dag_id"], RunReport.run_id == identity["run_id"], RunReport.task_id == "import", RunReport.map_index == -1, RunReport.try_number == 1)) is not None:
        return "imported"
    failure = envelope.get("failure")
    session.add(RunReport(
        id=uuid.uuid4(), dag_id=identity["dag_id"], run_id=identity["run_id"], task_id="import", map_index=-1, try_number=1,
        bot_name=identity["bot"], airflow_state=None, report_schema=envelope.get("payload_schema"), report_format=pathlib.PurePosixPath(item["relative_path"]).suffix.removeprefix("."), model=None,
        status=outcome, outcome=outcome, retry_class=envelope["retry_class"], reason_code=envelope["reason_code"],
        failure_class=failure.get("class") if failure else None, failure_code=failure.get("code") if failure else None, failure_fingerprint=failure.get("fingerprint") if failure else None, failure_detail=failure.get("detail") if failure else None,
        started_at=timestamp, finished_at=timestamp, deadline_at=timestamp, duration_ms=0, context_sha256=envelope["context"]["sha256"], context_byte_count=2, context_build_ms=0,
        attempts_json=[], input_tokens=None, output_tokens=None, total_tokens=None, provider_duration_ms=None, deadline_consumed_ms=None,
        body_json=details["body_json"], body_text=details["body_text"], unavailable_code=None if outcome == "succeeded" else envelope["reason_code"], sha256=envelope_sha256, byte_count=len(canonical), created_at=timestamp, expires_at=timestamp + timedelta(days=3650),
    ))
    return "imported"


def _safe_manifest_path(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("manifest path must be a string")
    return "/".join(_Root._parts(value))


def _patch_state(patch: bytes) -> str:
    environment = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"}
    for arguments, state_name in ((["git", "apply", "--reverse", "--check", "--binary", "-"], "historical_applied"), (["git", "apply", "--check", "--binary", "-"], "unresolved")):
        try:
            result = subprocess.run(arguments, input=patch, cwd=pathlib.Path(__file__).resolve().parents[7], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=False, env=environment)
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0:
            return state_name
    return "human_attention"


def _package(session: Any, item: dict[str, Any], reviews: _Root, patches: dict[str, dict[str, Any]], reports: dict[str, dict[str, Any]], references: set[str]) -> tuple[str, uuid.UUID]:
    package_id = pathlib.PurePosixPath(item["relative_path"]).stem
    details: dict[str, Any] = {"package_id": package_id}
    valid = True
    status = "human_attention"
    patch_item: dict[str, Any] | None = None
    report_item: dict[str, Any] | None = None
    manifest: dict[str, Any] = {}
    try:
        if item.get("error"):
            raise ValueError(item["error"])
        if not _PACKAGE.fullmatch(package_id) or len(f"legacy-package-{package_id}") > 80:
            raise ValueError("manifest package identity is invalid")
        decoded = _safe_json(item["body"])
        if not isinstance(decoded, dict) or decoded.get("package_id") != package_id:
            raise ValueError("manifest identity mismatch")
        if _ROOT_KEYS.intersection(decoded):
            raise ValueError("manifest-provided roots are forbidden")
        bot = decoded.get("bot")
        if not isinstance(bot, str) or not _BOT.fullmatch(bot):
            raise ValueError("manifest bot is invalid")
        patch_path = _safe_manifest_path(decoded.get("patch_path"))
        if not patch_path.endswith(".patch"):
            raise ValueError("manifest patch reference is not a patch")
        patch_item = patches.get(patch_path)
        if patch_item is None:
            # Resolve through the reviews-root descriptor to make references
            # independent from inventory layout, and retain a clear rejection.
            body, metadata = reviews.read(patch_path, suffixes={".patch"}, cap=_max_patch_bytes())
            patch_item = {"relative_path": patch_path, "body": body, "metadata": metadata, "error": None, "cap": _max_patch_bytes()}
        if patch_item.get("error") or patch_item.get("body") is None:
            raise ValueError(patch_item.get("error") or "patch is unavailable")
        expected_patch = decoded.get("patch_sha256")
        patch_digest = hashlib.sha256(patch_item["body"]).hexdigest()
        if expected_patch is not None and expected_patch != patch_digest:
            raise ValueError("patch digest mismatch")
        changed = decoded.get("files_changed")
        if not isinstance(changed, list) or not changed or any(not isinstance(path, str) for path in changed):
            raise ValueError("changed path manifest is invalid")
        changed = [_safe_manifest_path(path) for path in changed]
        report_path = None
        if decoded.get("report_path") is not None:
            report_path = _safe_manifest_path(decoded["report_path"])
            report_item = reports.get(report_path)
            if report_item is None:
                body, metadata = reviews.read(report_path, suffixes={".json", ".md"}, cap=_REPORT_CAP)
                report_item = {"relative_path": report_path, "body": body, "metadata": metadata, "error": None, "cap": _REPORT_CAP}
            if report_item.get("error") or report_item.get("body") is None:
                raise ValueError("source report is unavailable")
            if decoded.get("report_sha256") and decoded["report_sha256"] != hashlib.sha256(report_item["body"]).hexdigest():
                raise ValueError("source report digest mismatch")
            report_path = report_item["relative_path"]
        manifest_digest = hashlib.sha256(item["body"]).hexdigest()
        manifest_artifact = put_artifact(session, kind="manifest", content=item["body"], expected_sha256=manifest_digest)
        patch_artifact = put_artifact(session, kind="patch", content=patch_item["body"], expected_sha256=patch_digest)
        report_artifact = None
        if report_item is not None:
            report_artifact = put_artifact(session, kind="source", content=report_item["body"], expected_sha256=hashlib.sha256(report_item["body"]).hexdigest())
        details.update({"bot": bot, "files_changed": sorted(changed), "patch_sha256": patch_digest, "manifest_sha256": manifest_artifact["sha256"], "patch_artifact_sha256": patch_artifact["sha256"], "named_by_reviewer": package_id in references})
        if report_artifact is not None:
            details["report_artifact_sha256"] = report_artifact["sha256"]
        patch_status = _patch_state(patch_item["body"])
        if patch_status == "human_attention":
            raise ValueError("patch cannot be reconciled without human attention")
        status = "historical" if package_id in references or patch_status == "historical_applied" else "unresolved"
        details["checkout_reconciliation"] = patch_status
        if report_path:
            details["report_path"] = report_path
        manifest = decoded
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, LegacyInputError, OSError) as exc:
        valid = False
        details["attention_code"] = str(exc)[:200]
    item["_details"] = details
    package_key = (
        package_id.lower()
        if _PACKAGE.fullmatch(package_id) and len(package_id) <= 56
        else hashlib.sha256(item["relative_path"].encode()).hexdigest()[:32]
    )
    recommendation = f"legacy-package-{package_key}"
    task = session.scalar(select(Task).where(Task.recommendation_key == recommendation))
    if task is None:
        task = Task(source="manual", recommendation_key=recommendation, title=f"Review quarantined legacy package {package_id}"[:200], category="architecture", state="proposed" if valid else "blocked", priority=4, planned_resolution="Inspect immutable legacy package evidence; dismiss it or create a new admitted execution. Never apply the quarantined patch directly.", blocked_from_state=None if valid else "proposed", reviewer_required=True)
        session.add(task)
        session.flush()
        session.add(Revision(task_id=task.id, revision_number=1, report_schema="legacy_review_package_v1", title=task.title, action=task.planned_resolution, why_now="Legacy shared-checkout authority was retired.", expected_benefit="Every surviving change receives explicit human disposition.", resources="Legacy quarantine backup", risk="Applying stale patches can overwrite current work.", rollback="Dismiss the task without mutating the checkout.", verification="Compare recorded hashes and current checkout state.", suggested_executor=None, evidence=[{"kind": "legacy_package", "reference": item["relative_path"], "summary": status}], allowed_path_globs=[], resource_keys=[], follow_up_bots=[]))
        _event(session, task, "legacy_package_imported", "system", "import-legacy", to_state=task.state, payload={"status": status, "manifest_sha256": item["sha256"], **details})
    return status, task.id


def _review_references(items: list[dict[str, Any]]) -> set[str]:
    references: set[str] = set()
    for item in items:
        if item["kind"] != "run_report" or not item["relative_path"].startswith("reviewer/") or item.get("body") is None:
            continue
        text = item["body"].decode("utf-8", errors="replace")
        references.update(re.findall(r"[a-z0-9_-]+-\d{8}T\d{6}Z-[a-f0-9]{8}", text))
    return references


def import_legacy(runs_root_value: str, reviews_root_value: str, *, check: bool) -> int:
    with _Root(runs_root_value, label="runs root") as runs, _Root(reviews_root_value, label="reviews root") as reviews:
        items = _inventory(runs, reviews)
        for item in items:
            if item.get("body") == b"":
                item["error"] = "legacy file is empty"
            if item.get("body") is not None and not item.get("error"):
                item["sha256"] = hashlib.sha256(item["body"]).hexdigest()
                item["byte_count"] = len(item["body"])
            elif item.get("body") is not None:
                item["sha256"] = hashlib.sha256(item["body"]).hexdigest()
                item["byte_count"] = 0
            else:
                item["sha256"] = hashlib.sha256(("rejected:" + str(item.get("error", ""))).encode()).hexdigest()
                item["byte_count"] = 0
        with create_session() as session:
            existing = {(row.kind, row.relative_path): row for row in session.scalars(select(LegacyImport)).all()}
            summary = _summary(items, existing)
            if summary["changed_count"]:
                raise RuntimeError("an imported quarantine file changed")
            if not check:
                reports = {item["relative_path"]: item for item in items if item["kind"] == "run_report"}
                patches = {item["relative_path"]: item for item in items if item["kind"] == "review_patch"}
                references = _review_references(items)
                for item in items:
                    if item["kind"] == "run_report":
                        status = _import_report(session, item) if item.get("body") is not None and not item.get("error") else "rejected"
                        details = {"root_kind": item["root_kind"], **item.get("_details", {}), **({"artifact_sha256": item["_artifact_sha256"]} if item.get("_artifact_sha256") else {}), **({"legacy_timestamp_inferred": True} if _timestamp(item)[1] else {})}
                    elif item["kind"] == "review_manifest":
                        status, task_id = _package(session, item, reviews, patches, reports, references)
                        details = {"root_kind": item["root_kind"], **item.get("_details", {})}
                    else:
                        status = "quarantined" if item.get("body") is not None and not item.get("error") else "rejected"
                        details = {"root_kind": item["root_kind"]}
                        if item.get("body") is not None and not item.get("error"):
                            details["artifact_sha256"] = put_artifact(session, kind="patch", content=item["body"], expected_sha256=item["sha256"])["sha256"]
                    session.add(LegacyImport(kind=item["kind"], relative_path=item["relative_path"], sha256=item["sha256"], byte_count=item["byte_count"], status=status, task_id=task_id, details=details))
                session.flush()
                existing = {(row.kind, row.relative_path): row for row in session.scalars(select(LegacyImport)).all()}
                summary = _summary(items, existing)
    print(json.dumps({"status": "ok", "check": check, **summary}, sort_keys=True))
    return 0


def inspect_run(dag_id: str, run_id: str, task_id: str, try_number: int | None) -> int:
    with create_session() as session:
        query = select(RunReport).where(RunReport.dag_id == dag_id, RunReport.run_id == run_id, RunReport.task_id == task_id)
        if try_number is not None:
            query = query.where(RunReport.try_number == try_number)
        row = session.scalar(query.order_by(RunReport.try_number.desc()).limit(1))
        if row is None:
            raise RuntimeError("run report not found")
        value = run_report_dict(row)
    print(json.dumps(value, sort_keys=True))
    return 0


def purge(before: str, *, apply: bool) -> int:
    cutoff = datetime.fromisoformat(before.replace("Z", "+00:00"))
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    with create_session() as session:
        candidates = session.scalars(select(Task).where(Task.state.in_(("completed", "dismissed")), Task.updated_at < cutoff).with_for_update()).all()
        eligible = []
        for task in candidates:
            executions = session.scalars(select(Execution).where(Execution.task_id == task.id)).all()
            if any(row.terminal_at is None or (row.pr_number is not None and (row.provider_state or {}).get("state") != "merged") for row in executions):
                continue
            eligible.append(task)
        ids = [str(task.id) for task in eligible]
        if apply:
            for task in eligible:
                session.delete(task)
    print(json.dumps({"dry_run": not apply, "eligible_task_ids": ids}, sort_keys=True))
    return 0
