"""Run one extract script and land its stdout through a sink.

The extract contract (see the repo README): a script is invoked as
``python fetch_<source>.py [positional args...]`` and prints NDJSON records to
stdout, each carrying the envelope keys ``source``, ``fetched_at``, ``id``.
Non-zero exit means failure; exit 0 with empty stdout is a valid empty result.
"""
import json
import logging
import pathlib
import re
import subprocess
import sys
import tempfile
import traceback
from datetime import datetime, timezone

from run_metadata import (
    RunSummaryError,
    build_manifest,
    extract_summary_line,
    validate_summary,
)
from sinks import get_sink

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "extract" / "scripts"
ENVELOPE = ("source", "fetched_at", "id")

log = logging.getLogger(__name__)


# Failure evidence is stored in manifests and raised into the task runtime. Keep
# it small enough that an unbounded child error cannot exhaust those channels.
FAILURE_MESSAGE_MAX_CHARS = 1_024
FAILURE_TRACEBACK_MAX_CHARS = 4_096
FAILURE_STDERR_MAX_CHARS = 4_096
FAILURE_DIAGNOSTIC_MAX_BYTES = 32 * 1024
_TRUNCATED = "... [truncated]"

_AUTHENTICATED_URL = re.compile(r"(?i)\b((?:https?|ftp)://)[^/\s@]+@")
_SENSITIVE_KEYS = (
    "access[_-]?token|api[_-]?key|authorization|client[_-]?secret|"
    "credential|password|passwd|secret|token"
)
_SENSITIVE_QUERY = re.compile(
    r"(?i)([?&](?:access[_-]?token|api[_-]?key|authorization|client[_-]?secret|"
    r"password|secret|token)=[^&#\s]*)"
)
_SENSITIVE_HEADER = re.compile(
    r"(?im)^(\s*(?:authorization|proxy-authorization|x-api-key|api-key|cookie|"
    r"set-cookie)\s*:\s*)[^\r\n]*"
)
_SENSITIVE_VALUE = re.compile(
    r"(?i)\b(" + _SENSITIVE_KEYS + r")\b(\s*[:=]\s*)"
    r"(?:Bearer\s+)?(?:\"[^\"]*\"|'[^']*'|[^\s,;&]+)"
)
# The same keys quoted as object members, e.g. {"token": "..."}. The closing
# quote between key and separator defeats the unquoted pattern above.
_QUOTED_KEY_VALUE = re.compile(
    r"(?i)([\"'])(" + _SENSITIVE_KEYS + r")\1(\s*[:=]\s*)"
    r"(?![{\[])(?:Bearer\s+)?(?:[\"'][^\"']*[\"']|[^\s,;&}\]]+)"
)
_SENSITIVE_OPTION = re.compile(
    r"(?i)^(?:[a-z0-9]+[-_])?(?:access[_-]?token|api[_-]?key|authorization|auth|"
    r"client[_-]?secret|credential|password|passwd|secret|token)$"
)


def _stringify(value: object) -> str:
    """Return a display-safe string even for exceptions with broken __str__."""
    try:
        text = str(value)
    except Exception:  # noqa: BLE001 - a broken __str__ must not defeat redaction
        text = f"<unprintable {type(value).__name__}>"
    return text.encode("utf-8", "replace").decode("utf-8")


def _redact(text: object) -> str:
    """Remove credential-bearing values before diagnostics leave this process."""
    value = _stringify(text)
    value = _AUTHENTICATED_URL.sub(r"\1[REDACTED]@", value)
    value = _SENSITIVE_QUERY.sub(lambda match: match.group(1).split("=", 1)[0] + "=[REDACTED]", value)
    value = _SENSITIVE_HEADER.sub(r"\1[REDACTED]", value)
    value = _QUOTED_KEY_VALUE.sub(r"\1\2\1\3[REDACTED]", value)
    return _SENSITIVE_VALUE.sub(r"\1\2[REDACTED]", value)


def _is_sensitive_option(arg: str) -> bool:
    """Return True when a bare option names a credential its value follows."""
    if not arg.startswith("-") or "=" in arg or arg in {"-", "--"}:
        return False
    name = arg.lstrip("-")
    for prefix in ("set-", "with-", "x-"):
        if name.lower().startswith(prefix):
            name = name[len(prefix):]
    return bool(_SENSITIVE_OPTION.match(name))


def _redact_args(args: list[str]) -> list[str]:
    """Redact a command line, pairing sensitive options with the value that follows."""
    redacted: list[str] = []
    redacting_value = False
    for arg in args:
        if redacting_value:
            redacting_value = False
            # A following option is not the value of the sensitive option.
            if arg.startswith("-") and arg != "-":
                redacted.append(_redact(arg))
            else:
                redacted.append("[REDACTED]")
            continue
        if _is_sensitive_option(arg):
            redacted.append(arg)
            redacting_value = True
            continue
        redacted.append(_redact(arg))
    return redacted


def _truncate(text: str, limit: int) -> str:
    """Keep a diagnostic field bounded while making lost context explicit."""
    if len(text) <= limit:
        return text
    return text[:limit - len(_TRUNCATED)] + _TRUNCATED


def _truncate_tail(text: str, limit: int) -> str:
    """Keep the final child output, where command-line tools put their error."""
    if len(text) <= limit:
        return text
    return _TRUNCATED + text[-(limit - len(_TRUNCATED)):]


def _failure_diagnostic(exc: BaseException, stderr: str, exit_status: int | None) -> str:
    """Serialize bounded, redacted task-failure evidence for a failed manifest."""
    try:
        formatted_traceback = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    except Exception:  # noqa: BLE001 - broken traceback formatting must not defeat the failure manifest
        formatted_traceback = f"{type(exc).__name__}: {_stringify(exc)}"
    diagnostic = {
        "exception_type": type(exc).__name__,
        "message": _truncate(_redact(exc), FAILURE_MESSAGE_MAX_CHARS),
        "traceback": _truncate(_redact(formatted_traceback), FAILURE_TRACEBACK_MAX_CHARS),
        "stderr": _truncate_tail(_redact(stderr), FAILURE_STDERR_MAX_CHARS),
        "exit_status": exit_status,
    }
    serialized = json.dumps(diagnostic, ensure_ascii=False, sort_keys=True)
    if len(serialized.encode("utf-8")) > FAILURE_DIAGNOSTIC_MAX_BYTES:
        # Field limits above normally make this unreachable; retain valid JSON if
        # a hostile exception expands substantially during serialization.
        diagnostic["traceback"] = _truncate(diagnostic["traceback"], 512)
        diagnostic["stderr"] = _truncate(diagnostic["stderr"], 512)
        diagnostic["message"] = _truncate(diagnostic["message"], 256)
        serialized = json.dumps(diagnostic, ensure_ascii=False, sort_keys=True)
    return serialized


def _failed_manifest(
    cfg: dict,
    args: list[str],
    started: datetime,
    finished: datetime,
    exit_status: int | None,
    records: int,
    bytes_written: int,
    error: str,
) -> dict:
    """Build failure evidence without retaining secrets from configured args."""
    return build_manifest(
        source=cfg["name"], script=_redact(cfg["script"]), args=_redact_args(args),
        started_at=started.isoformat(), finished_at=finished.isoformat(),
        duration_s=(finished - started).total_seconds(), exit_code=exit_status,
        observed_records=records, observed_bytes=bytes_written, summary=None,
        status="failed", error=error,
    )


def run(cfg: dict) -> dict:
    """Execute the source described by one yml config; return the run manifest."""
    name = cfg["name"]
    script = SCRIPTS_DIR / cfg["script"]
    args = [str(a) for a in cfg.get("args", [])]
    safe_args = _redact_args(args)
    started = datetime.now(timezone.utc)
    filename = f"{name}_{started.strftime('%Y%m%dT%H%M%SZ')}.ndjson"

    # A source may pin its sink; otherwise EXTRACT_SINK decides (config.env).
    sink = get_sink(cfg.get("sink"))
    records = 0
    bytes_written = 0

    # stderr goes to a spooled file, not a pipe: a chatty script must never
    # deadlock against an unread pipe while we consume stdout.
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace") as err:
        proc = None
        try:
            # Open the stage before launching so even an executable-start failure
            # can be materialized through the sink's failure marker.
            with sink.writer(name, started.strftime("%Y-%m-%d"), filename) as out:
                proc = subprocess.Popen(
                    [sys.executable, str(script), *args],
                    cwd=SCRIPTS_DIR,
                    stdout=subprocess.PIPE,
                    stderr=err,
                    text=True,
                )
                for line in proc.stdout:
                    if not line.strip():
                        continue
                    if records == 0:
                        _check_envelope(line)
                    out.write(line if line.endswith("\n") else line + "\n")
                    records += 1
                    bytes_written += len(line.encode("utf-8"))
                returncode = proc.wait()
        except Exception as exc:
            exit_status = None
            if proc is not None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                exit_status = proc.wait()
            err.seek(0)
            diagnostic = _failure_diagnostic(exc, err.read().strip(), exit_status)
            failed = _failed_manifest(
                cfg, safe_args, started, datetime.now(timezone.utc), exit_status,
                records, bytes_written, diagnostic,
            )
            sink.fail(failed)
            raise RuntimeError(diagnostic) from exc
        except BaseException:
            if proc is not None:
                proc.kill()
                proc.wait()
            sink.discard()
            raise

        err.seek(0)
        stderr = err.read().strip()

    # Ordinary logs first: the reserved summary line is excluded from them.
    summary_payload, ordinary = None, stderr
    _rs = cfg.get("run_summary")
    required = bool(_rs.get("required", False)) if isinstance(_rs, dict) else bool(_rs)
    try:
        summary_payload, ordinary = extract_summary_line(stderr)
    except RunSummaryError as exc:
        # A malformed, duplicate or oversize summary fails the run; the staged
        # records must not be published.
        failed = _failed_manifest(
            cfg, safe_args, started, datetime.now(timezone.utc), returncode,
            records, bytes_written, _failure_diagnostic(exc, stderr, returncode),
        )
        sink.fail(failed)
        raise RuntimeError(failed["error"]) from exc
    if required and summary_payload is None:
        exc = RuntimeError("run_summary required but the script emitted no summary line")
        failed = _failed_manifest(
            cfg, safe_args, started, datetime.now(timezone.utc), returncode,
            records, bytes_written, _failure_diagnostic(exc, stderr, returncode),
        )
        sink.fail(failed)
        raise RuntimeError(failed["error"]) from exc

    if ordinary:
        log.info("stderr from %s:\n%s", _redact(cfg["script"]), _redact(ordinary))

    finished = datetime.now(timezone.utc)
    if returncode != 0:
        # Hard failure with no safe output: discard the staged records (the
        # runner's existing contract) and record a failed manifest.
        exc = RuntimeError(f"{cfg['script']} exited {returncode} after {records} records")
        diagnostic = _failure_diagnostic(exc, ordinary, returncode)
        failed = _failed_manifest(
            cfg, safe_args, started, finished, returncode, records, bytes_written, diagnostic,
        )
        sink.fail(failed)
        raise RuntimeError(diagnostic) from exc

    if summary_payload:
        try:
            summary = validate_summary(json.loads(summary_payload))
        except ValueError as exc:
            # A summary that breaks the contract (negative count, unknown
            # health, malformed JSON) fails the run through the same
            # failed-manifest path as every other post-extraction failure.
            failed = _failed_manifest(
                cfg, safe_args, started, finished, returncode,
                records, bytes_written, _failure_diagnostic(exc, stderr, returncode),
            )
            sink.fail(failed)
            raise RuntimeError(failed["error"]) from exc
    else:
        summary = {}
    manifest = build_manifest(
        source=name, script=cfg["script"], args=safe_args,
        started_at=started.isoformat(), finished_at=finished.isoformat(),
        duration_s=(finished - started).total_seconds(),
        exit_code=returncode, observed_records=records,
        observed_bytes=bytes_written, summary=summary, status="success",
    )
    manifest_path = sink.commit(manifest)
    log.info("%s: %d records, %d bytes, health=%s completeness=%s -> %s",
             name, records, bytes_written, manifest["health"], manifest["completeness"],
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
