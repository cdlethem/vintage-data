"""The run-evidence contract: one reserved stderr line, one schema-v2 manifest.

Every extract script is a subprocess: it writes NDJSON records to *stdout* and
free-form diagnostic logs to *stderr*. The loader can never tell a log line from
a protocol line, and it can only count what it itself wrote to the sink. So the
two sides agree on a division of labor:

* The **runner** (``extract_runner.run`` / ``tools.backfill.run_unit``) is
  authoritative for the *transport facts* — how many records and bytes it
  actually staged, how long it took, and what the child exited with. It also
  decides the manifest's ``status`` (``success`` for a published manifest,
  ``failed`` for a ``.fail.json`` marker).
* The **script** reports the things only it can know — request counts, partition
  failures, coverage, state change, and its own assessment of source ``health``
  and ``completeness`` — on exactly one reserved stderr line.

Any script that wants its run to carry this evidence emits, on its final stderr
line, a single line of the form::

    VINTAGE_RUN_SUMMARY\t{"health": "degraded", ...}

(UTF-8, one line, the tab, then a JSON object.) Every other stderr line is
ordinary log output and is left untouched. The contract is deliberately small:
at most one reserved line, and duplicate / malformed / oversized / internally
inconsistent summaries fail the run rather than being quietly coerced.

Legacy scripts that never learned the contract keep working: no reserved line
means the manifest still publishes and simply records ``completeness: unknown``.
A source opts into the *required* form by setting ``run_summary: required`` in
its yml; for those, a missing or bad summary is a run failure, not a warning.
"""
from __future__ import annotations

import json

#: The reserved prefix for the single run-summary stderr line.
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

#: A summary line longer than this cannot describe the counters it must carry;
#: treat it as malformed rather than allocate without bound.
SUMMARY_MAX_BYTES = 64 * 1024

#: Manifest schema version written by the runner/backfill path.
MANIFEST_SCHEMA_VERSION = 2

#: The seven fields the runner copies out of the summary into top-level manifest
#: keys (everything else the script reports is retained verbatim under them or
#: under ``metrics``).
STRUCTURED_KEYS = ("health", "completeness", "requests", "partitions")

#: ``health:`` values, transport/source health (never completeness) and run state.
HEALTH_VALUES = ("healthy", "degraded", "open_circuit", "failed")

#: ``completeness:`` values. Never inferred from a zero record count or ID novelty.
COMPLETENESS_VALUES = ("complete", "partial", "unknown", "failed")

#: A partition-failure detail list is bounded so one giant run cannot bloat the
#: manifest; the *total* failed count is always kept.
MAX_PARTITION_FAILURES = 100


class RunSummaryError(ValueError):
    """A reserved summary line was present but unusable. Fails the run."""


def extract_summary_line(stderr_text: str) -> tuple[str | None, str]:
    """Split out the at-most-one reserved summary line.

    Returns ``(line_json_or_None, remaining_stderr)`` where ``remaining_stderr``
    is the ordinary (non-reserved) log output, in original order. Raises
    :class:`RunSummaryError` on the protocol violations that must fail the run:
    a duplicate reserved line, an oversize line, or a line that is not a JSON
    object. A missing line is *not* an error here — the caller decides whether
    the source made the summary required.
    """
    if stderr_text is None:
        return None, ""
    summary: str | None = None
    kept: list[str] = []
    for line in stderr_text.splitlines():
        if line.startswith(SUMMARY_PREFIX):
            payload = line[len(SUMMARY_PREFIX):]
            if summary is not None:
                raise RunSummaryError("run reported two run-summary lines; at most one is allowed")
            if len(payload.encode("utf-8")) > SUMMARY_MAX_BYTES:
                raise RunSummaryError(f"run-summary line is oversize ({len(payload.encode('utf-8'))} bytes > {SUMMARY_MAX_BYTES})")
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise RunSummaryError(f"run-summary is not valid JSON: {exc}") from exc
            if not isinstance(parsed, dict):
                raise RunSummaryError("run-summary must be a JSON object")
            summary = payload
        else:
            kept.append(line)
    return summary, "\n".join(kept)


def validate_summary(summary: dict) -> dict:
    """Normalize and validate a parsed summary; raise on internal inconsistency.

    Counts must be non-negative integers and a block's ``succeeded + failed``
    may not exceed its ``attempted``. ``health``/``completeness`` must be known
    values when present. Unrecognized top-level keys are folded into
    ``metrics`` so a script may carry its own counters without changing the
    contract. The returned dict is safe to embed in a manifest.
    """
    if not isinstance(summary, dict):
        raise RunSummaryError("run-summary must be a JSON object")

    def _int(value, where: str) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RunSummaryError(f"{where} must be a non-negative integer, got {value!r}")
        return value

    def _countblock(value, name: str) -> dict:
        """Validate one attempted/succeeded/failed block; default missing to 0."""
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise RunSummaryError(f"run-summary {name!r} must be an object")
        out = {}
        attempted = _int(value.get("attempted"), f"{name}.attempted")
        succeeded = _int(value.get("succeeded"), f"{name}.succeeded")
        failed = _int(value.get("failed"), f"{name}.failed")
        out["attempted"] = attempted or 0
        out["succeeded"] = succeeded or 0
        out["failed"] = failed or 0
        # Only check the invariant when the script actually reported the numbers.
        if attempted is not None and (succeeded is not None or failed is not None):
            if (succeeded or 0) + (failed or 0) > attempted:
                raise RunSummaryError(
                    f"run-summary {name}: succeeded+failed "
                    f"({(succeeded or 0) + (failed or 0)}) exceeds attempted ({attempted})"
                )
        # Partition blocks carry a bounded detail list; the total stays authoritative.
        if name == "partitions" and "failures" in value:
            failures = value["failures"]
            if not isinstance(failures, list):
                raise RunSummaryError("run-summary partitions.failures must be a list")
            for item in failures[:MAX_PARTITION_FAILURES + 1]:
                if not isinstance(item, dict):
                    raise RunSummaryError("run-summary partitions.failures entries must be objects")
            out["failures"] = failures[:MAX_PARTITION_FAILURES]
        return out

    result: dict = {}
    health = summary.get("health")
    if health is not None:
        if health not in HEALTH_VALUES:
            raise RunSummaryError(f"run-summary health {health!r} is not one of {list(HEALTH_VALUES)}")
        result["health"] = health
    completeness = summary.get("completeness")
    if completeness is not None:
        if completeness not in COMPLETENESS_VALUES:
            raise RunSummaryError(f"run-summary completeness {completeness!r} is not one of {list(COMPLETENESS_VALUES)}")
        result["completeness"] = completeness

    result["requests"] = _countblock(summary.get("requests"), "requests")
    if summary.get("requests"):
        req = summary["requests"]
        retries = _int(req.get("retries"), "requests.retries")
        if retries is not None:
            result["requests"]["retries"] = retries
        payload_bytes = _int(req.get("payload_bytes"), "requests.payload_bytes")
        if payload_bytes is not None:
            result["requests"]["payload_bytes"] = payload_bytes
        backoff_s = req.get("backoff_s")
        if backoff_s is not None:
            if isinstance(backoff_s, bool) or not isinstance(backoff_s, (int, float)) or backoff_s < 0:
                raise RunSummaryError("run-summary requests.backoff_s must be a non-negative number")
            result["requests"]["backoff_s"] = backoff_s

    result["partitions"] = _countblock(summary.get("partitions"), "partitions")

    for key in ("coverage", "state_change", "metrics"):
        value = summary.get(key)
        if value is not None:
            if not isinstance(value, dict):
                raise RunSummaryError(f"run-summary {key!r} must be an object")
            result[key] = value

    # Unknown top-level keys become script-owned metrics, not contract fields.
    metrics = result.get("metrics", {})
    for key, value in summary.items():
        if key not in STRUCTURED_KEYS and key not in ("coverage", "state_change", "metrics"):
            metrics[key] = value
    if metrics:
        result["metrics"] = metrics
    return result


def build_manifest(
    *,
    source: str,
    script: str,
    args: list[str],
    started_at: str,
    finished_at: str,
    duration_s: float,
    exit_code: int,
    observed_records: int,
    observed_bytes: int,
    summary: dict | None,
    status: str,
    backfill_unit: str | None = None,
    error: str | None = None,
) -> dict:
    """Assemble a schema-v2 manifest.

    ``status`` is ``"success"`` (published manifest) or ``"failed"`` (``.fail.json``).
    Transport facts come from the runner; source facts come from the (already
    validated) ``summary``. A failed run is ``health: failed`` / ``completeness:
    failed`` regardless of what the child claimed — the child's own health signal
    is meaningless once the process could not finish.
    """
    if status not in ("success", "failed"):
        raise ValueError(f"manifest status must be success|failed, got {status!r}")

    manifest: dict = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "source": source,
        "script": script,
        "args": list(args),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_s": round(float(duration_s), 3),
        "exit_code": exit_code,
        "status": status,
        # Observed by the runner; never taken from the child's word.
        "records": observed_records,
        "bytes": observed_bytes,
    }
    if backfill_unit is not None:
        manifest["backfill_unit"] = backfill_unit

    summary = summary or {}
    if status == "failed":
        manifest["health"] = "failed"
        manifest["completeness"] = "failed"
        if error:
            manifest["error"] = error
    else:
        manifest["health"] = summary.get("health", "healthy")
        manifest["completeness"] = summary.get("completeness", "unknown")

    # Carry the standard source facts through, defaulting to a zeroed block so a
    # reader never has to special-case a missing section.
    manifest["requests"] = summary.get("requests", {})
    partitions = dict(summary.get("partitions", {}))
    if "failures" not in partitions:
        partitions = {**partitions, "failures": []}
    manifest["partitions"] = partitions
    manifest["coverage"] = summary.get("coverage", {})
    manifest["state_change"] = summary.get("state_change", {})
    if summary.get("metrics"):
        manifest["metrics"] = summary["metrics"]
    return manifest