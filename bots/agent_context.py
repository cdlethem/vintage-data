#!/usr/bin/env python3
"""Deterministic, bounded, API-backed context for recurring bots."""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import yaml

BOTS_ROOT = pathlib.Path(__file__).resolve().parent
REPO_ROOT = BOTS_ROOT.parent
sys.path.insert(0, str(REPO_ROOT / "orchestration" / "include"))
import deployment

deployment.load_env()
deployment.load_env(REPO_ROOT / "orchestration" / "airflow.secrets.env", override=True)
import provider_dashboard

SOURCE_ROOT = REPO_ROOT / "extract" / "sources"
SCRIPT_ROOT = REPO_ROOT / "extract" / "scripts"
TRANSFORM_ROOT = REPO_ROOT / "transform"
MAX_CONTEXT_ITEMS = 100
MAX_TEXT = 1200
CONTEXT_LIMITS = {
    "source_discovery": 32 * 1024,
    "source_vetting": 24 * 1024,
    "source_scheduling": 24 * 1024,
    "cadence_review": 32 * 1024,
    "failure_triage": 48 * 1024,
    "analytics_engineer": 32 * 1024,
    "data_analyst": 40 * 1024,
}


class ContextError(RuntimeError):
    """A control-plane or evidence failure which must fail the bot task."""

    def __init__(self, code: str, detail: str = ""):
        self.code = re.sub(r"[^a-z0-9_]", "_", str(code).lower())[:100]
        # Error text is deliberately code-only: command/API diagnostics can carry secrets.
        super().__init__(self.code)


def _bounded_bytes_text(value: Any, limit: int) -> str:
    text = _bounded_text(value, limit)
    encoded = text.encode("utf-8", "replace")
    if len(encoded) <= limit:
        return text
    return encoded[:limit].decode("utf-8", "ignore")

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bounded_text(value: Any, limit: int = MAX_TEXT) -> str:
    text = str(value or "").replace("\x00", "")
    return text if len(text) <= limit else text[: max(0, limit - 1)] + "…"


def _base(kind: str, *, as_of: str | None = None) -> dict[str, Any]:
    now = _now()
    return {
        "context_schema_version": 1,
        "context_kind": kind,
        "generated_at": now,
        "as_of": as_of or now,
        "errors": [],
    }


def _control_client(*, deadline_at: datetime | None = None):
    if deadline_at is None:
        return provider_dashboard.DashboardClient.from_environment()
    return provider_dashboard.DashboardClient.from_environment(deadline_at=deadline_at)


def _reports(name: str, *, days: int = 3650, limit: int = 100) -> list[dict]:
    try:
        value = _control_client().query_runs(
            bots=[name], days=days, outcomes=[], limit=min(limit, MAX_CONTEXT_ITEMS)
        )
    except Exception as exc:  # control-plane errors are never healthy empty queues
        raise ContextError(getattr(exc, "code", "dashboard_unavailable")) from exc
    if not isinstance(value, dict) or not isinstance(value.get("items"), list):
        raise ContextError("report_response_invalid")
    return [row for row in value["items"] if isinstance(row, dict)]


def _report_time(row: dict) -> str:
    return str(row.get("finished_at") or row.get("created_at") or row.get("started_at") or "")


def _payload_rows(name: str, *, days: int = 3650, limit: int = 100) -> list[dict]:
    rows = [row for row in _reports(name, days=days, limit=limit) if row.get("outcome") == "succeeded" and isinstance(row.get("payload"), dict)]
    return sorted(rows, key=lambda row: (_report_time(row), str(row.get("report_id", ""))), reverse=True)


def _payloads(name: str, *, days: int = 3650) -> list[dict]:
    return [row["payload"] for row in _payload_rows(name, days=days)]


def _source_configs() -> list[dict]:
    rows: list[dict] = []
    if not SOURCE_ROOT.is_dir():
        raise ContextError("source_configs_unavailable")
    for path in sorted(SOURCE_ROOT.glob("*.yml")):
        try:
            value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise ContextError("source_config_unreadable") from exc
        if not isinstance(value, dict):
            raise ContextError("source_config_invalid")
        script = str(value.get("script") or "")
        rows.append(
            {
                "name": str(value.get("name") or path.stem),
                "config": path.name,
                "enabled": bool(value.get("enabled", True)),
                "schedule": value.get("schedule"),
                "url": _bounded_text(value.get("url"), 500),
                "tags": sorted(str(item) for item in (value.get("tags") or []))[:12],
                "script": script,
                "path": str((SCRIPT_ROOT / script).relative_to(REPO_ROOT)) if script else "",
            }
        )
    return sorted(rows, key=lambda row: (row["name"], row["config"]))


def _latest_by_key(payload_rows: list[dict], collection: str, key: str) -> dict[str, dict]:
    values: dict[str, dict] = {}
    for report in reversed(payload_rows):
        payload = report.get("payload", report)
        for item in payload.get(collection, []) if isinstance(payload, dict) else []:
            if isinstance(item, dict) and item.get(key) is not None:
                values[str(item[key])] = item
    return values


def _seed_count() -> int:
    path = BOTS_ROOT / "source_history_seed.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContextError("discovery_seed_unavailable") from exc
    candidates = value.get("candidates") if isinstance(value, dict) else None
    if not isinstance(candidates, list):
        raise ContextError("discovery_seed_invalid")
    return len(candidates)


def _normalized_queries(rows: list[dict]) -> tuple[list[str], dict[str, int]]:
    queries: list[str] = []
    seen: set[str] = set()
    domains: dict[str, int] = {}
    for report in rows:
        payload = report.get("payload") or {}
        for query in payload.get("searches", []) if isinstance(payload, dict) else []:
            normalized = " ".join(str(query).lower().split())
            if normalized and normalized not in seen:
                seen.add(normalized)
                queries.append(normalized)
        for proposal in payload.get("proposals", []) if isinstance(payload, dict) else []:
            if isinstance(proposal, dict):
                domain = str(proposal.get("domain") or "").lower().strip()
                if domain:
                    domains[domain] = domains.get(domain, 0) + 1
        if len(queries) >= 50:
            break
    return queries[:50], dict(sorted(domains.items()))


def discovery_context() -> dict:
    configs = _source_configs()
    discovery_rows = _payload_rows("source_discovery")
    vetting_rows = _payload_rows("source_vetting")
    proposals = _latest_by_key(discovery_rows, "proposals", "slug")
    decisions = _latest_by_key(vetting_rows, "decisions", "slug")
    accepted = {row["name"] for row in configs}
    unresolved = sorted(slug for slug in proposals if slug not in decisions and slug not in accepted)
    queries, domain_counts = _normalized_queries(discovery_rows)
    summaries = []
    for slug, item in sorted(proposals.items()):
        decision = decisions.get(slug, {})
        summaries.append(
            {
                "slug": slug,
                "title": _bounded_text(item.get("title"), 200),
                "domain": _bounded_text(item.get("domain"), 253),
                "viability": item.get("viability"),
                "status": decision.get("decision", "pending"),
                "source_report_id": next((row.get("report_id") for row in discovery_rows if any(p.get("slug") == slug for p in (row.get("payload") or {}).get("proposals", []) if isinstance(p, dict))), None),
            }
        )
    context = _base("source_discovery")
    context.update(
        {
            "existing_source_count": len(configs),
            "existing_sources": [
                {"name": row["name"], "script": row["script"]} for row in configs
            ],
            "existing_scripts": sorted(
                pathlib.PurePosixPath(row["path"]).name
                for row in configs
                if row["path"] and (REPO_ROOT / row["path"]).is_file()
            ),
            "vetting_pending_count": len(unresolved),
            "vetting_inflight_count": 0,
            "backpressure_limit": 5,
            "backpressure": len(unresolved) >= 5,
            "recent_queries": queries,
            "query_domain_counts": domain_counts,
            "proposal_summaries": summaries[:100],
            "seed_count": _seed_count(),
            "max_new_proposals": 2,
            "truncated": len(summaries) > 100,
            "total_count": len(summaries),
            "returned_count": min(len(summaries), 100),
        }
    )
    return _bounded_context(
        context,
        "source_discovery",
        ("proposal_summaries", "recent_queries", "existing_scripts", "existing_sources"),
    )


def vetting_context() -> dict:
    configs = _source_configs()
    discovery_rows = _payload_rows("source_discovery")
    vetting_rows = _payload_rows("source_vetting")
    proposals = _latest_by_key(discovery_rows, "proposals", "slug")
    decisions = _latest_by_key(vetting_rows, "decisions", "slug")
    accepted = {row["name"] for row in configs}
    candidates = []
    for slug, proposal in proposals.items():
        if slug in decisions or slug in accepted:
            continue
        report = next((row for row in discovery_rows if any(isinstance(p, dict) and str(p.get("slug")) == slug for p in (row.get("payload") or {}).get("proposals", []))), {})
        candidates.append((({"high": 0, "medium": 1, "low": 2}.get(proposal.get("viability"), 3), _report_time(report), slug), proposal, report))
    candidates.sort(key=lambda item: item[0])
    selected = candidates[0][1] if candidates else None
    context = _base("source_vetting")
    context.update(
        {
            "pending_count": len(candidates),
            "inflight_count": 0,
            "selected": selected,
            "selection_key": selected.get("slug") if selected else None,
            "already_decided": sorted(decisions),
            "truncated": False,
            "total_count": len(candidates),
            "returned_count": len(candidates),
        }
    )
    return _bounded_context(context, "source_vetting")


def _proposal_path(proposal: dict) -> tuple[str, str]:
    resources = [str(item) for item in proposal.get("resource_keys", []) if isinstance(item, str)]
    script = next((item for item in resources if item.endswith(".py")), "")
    path = next((item for item in resources if "/" in item or item.endswith(".yml")), "")
    return script, path


def scheduling_context() -> dict:
    configs = _source_configs()
    configured = {row["name"] for row in configs}
    rows = _payload_rows("source_vetting")
    candidates_by_slug: dict[str, dict] = {}
    for report in rows:
        payload = report.get("payload") or {}
        for decision in payload.get("decisions", []):
            if not isinstance(decision, dict) or decision.get("decision") != "recommended":
                continue
            slug = str(decision.get("slug") or "")
            if not slug or slug in configured:
                continue
            proposal = decision.get("task_proposal") or {}
            script, path = _proposal_path(proposal)
            candidates_by_slug.setdefault(
                slug,
                {
                    "script": script,
                    "path": path,
                    "slug": slug,
                    "originating_vetting_report_id": report.get("report_id"),
                    "originating_task_reference": proposal.get("recommendation_key"),
                    "source_count": 1,
                    "minute_field_load": 1,
                    "scheduling_rules": {"schedule": "UTC", "minute_field": "deterministic stagger"},
                },
            )
    candidates = [candidates_by_slug[slug] for slug in sorted(candidates_by_slug)]
    selected = candidates[0] if candidates else None
    context = _base("source_scheduling")
    context.update({"backlog_count": len(candidates), "pending_count": len(candidates), "selected": selected, "truncated": False, "total_count": len(candidates), "returned_count": len(candidates)})
    return _bounded_context(context, "source_scheduling")


def _run_capture(command: list[str], timeout: int) -> Any:
    try:
        proc = subprocess.run(
            command,
            check=False,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": "/nonexistent", "LANG": "C.UTF-8"},
        )
    except subprocess.TimeoutExpired as exc:
        raise ContextError("evidence_command_timed_out") from exc
    except OSError as exc:
        raise ContextError("evidence_command_unavailable") from exc
    if proc.returncode:
        raise ContextError(f"evidence_command_exit_{proc.returncode}")
    text = proc.stdout.strip()
    try:
        value, end = json.JSONDecoder().raw_decode(text)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ContextError("evidence_command_invalid_json") from exc
    # Repository tools may append a human summary comment after the document.
    if any(line.strip() and not line.lstrip().startswith("#") for line in text[end:].splitlines()):
        raise ContextError("evidence_command_invalid_json")
    if not isinstance(value, (dict, list)):
        raise ContextError("evidence_response_invalid")
    return value


# The fields monitoring/digest.py publishes and the cadence prompt consumes.
_DIGEST_FIELDS = (
    "status", "enabled", "schedule", "stale", "last_run_age_min", "expected_gap_min",
    "runs_in_window", "zero_record_runs", "failed_runs", "consecutive_failures",
    "records_last", "records_median", "id_novelty", "extract_health",
    "extract_completeness", "held_runs", "last_error",
)


def _digest_fields(row: dict, source: str) -> dict:
    fields: dict[str, Any] = {"source": source}
    for key in _DIGEST_FIELDS:
        if key in row:
            value = row[key]
            fields[key] = _bounded_text(value, 500) if isinstance(value, str) else value
    return fields


def _digest_flagged(row: dict) -> bool:
    return bool(
        str(row.get("status", "")).upper() in {"WATCH", "PROBLEM"}
        or row.get("stale") is True
        or int(row.get("failed_runs") or 0) > 0
        or int(row.get("consecutive_failures") or 0) > 0
        or int(row.get("zero_record_runs") or 0) > 0
        or int(row.get("held_runs") or 0) > 0
        or str(row.get("extract_health", "healthy")) not in {"healthy", "unknown"}
        or row.get("flagged") is True
    )


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except ValueError:
        return None


def cadence_context(window_hours: int = 48, audit_size: int = 12) -> dict:
    # monitoring/digest.py is the repository's health digest and runs in the
    # orchestration environment; it emits one JSON array of source objects.
    digest = _run_capture(
        [sys.executable, str(REPO_ROOT / "monitoring" / "digest.py"), "--window-hours", str(window_hours), "--json"],
        90,
    )
    raw_rows = digest.get("sources") if isinstance(digest, dict) else digest
    if not isinstance(raw_rows, list):
        raise ContextError("digest_response_invalid")
    managed = {row["name"]: row for row in _source_configs()}
    by_source: dict[str, dict] = {}
    for row in raw_rows:
        if not isinstance(row, dict):
            raise ContextError("digest_row_invalid")
        source = str(row.get("source") or row.get("name") or "")
        if source:
            by_source[source] = {**by_source.get(source, {}), **row}
    flagged_sources = []
    rotating_sources = []
    for source in sorted(set(managed) | set(by_source)):
        row = {**managed.get(source, {}), **by_source.get(source, {})}
        row["source"] = source
        flagged = _digest_flagged(row)
        compact = _digest_fields(row, source)
        (flagged_sources if flagged else rotating_sources).append(compact)
    rotating_sources = rotating_sources[: min(12, max(0, audit_size))]
    cadence_rows = _payload_rows("cadence_review", days=30)
    latest_success = _parse_time(_report_time(cadence_rows[0])) if cadence_rows else None
    now = datetime.now(timezone.utc)
    recent_useful = latest_success is not None and now - latest_success <= timedelta(hours=24)
    decision_newer = False
    for row in cadence_rows:
        payload = row.get("payload") or {}
        for decision in payload.get("source_reviews", []) if isinstance(payload, dict) else []:
            decision_at = _parse_time(decision.get("decided_at") if isinstance(decision, dict) else None)
            if decision_at and latest_success and decision_at > latest_success:
                decision_newer = True
    decision_lines: list[str] = []
    for row in cadence_rows:
        payload = row.get("payload") or {}
        if isinstance(payload, dict):
            text = payload.get("summary")
            if text:
                decision_lines.extend(str(text).splitlines())
            for issue in payload.get("issues", []):
                decision_lines.extend(str(issue).splitlines())
        decision_lines.extend(str(row.get("cadence_decision") or "").splitlines())
    decision_text = "\n".join(decision_lines[:40])
    while len(decision_text.encode("utf-8")) > 12_000 and decision_text:
        decision_text = decision_text.rsplit("\n", 1)[0]
    context = _base("cadence_review")
    context.update({"window_hours": window_hours, "flagged_sources": flagged_sources, "rotating_sources": rotating_sources, "cadence_decision": decision_text, "review_required": bool(flagged_sources) or decision_newer or not recent_useful, "audit_size": len(rotating_sources), "truncated": len(set(managed) | set(by_source)) > len(flagged_sources) + len(rotating_sources), "total_count": len(set(managed) | set(by_source)), "returned_count": len(flagged_sources) + len(rotating_sources)})
    return _bounded_context(context, "cadence_review", ("rotating_sources", "flagged_sources"))


def _failure_fingerprint(row: dict, detail: str) -> str:
    from airflow.providers.vintage.bot_dashboard.report_schemas import failure_fingerprint
    return failure_fingerprint(
        origin=str(row.get("origin") or "airflow"),
        component=str(row.get("component") or row.get("dag_id") or "unknown"),
        task=str(row.get("task_id") or "unknown"),
        error_class=str(row.get("error_class") or row.get("exception_type") or "unknown"),
        code=str(row.get("error_code") or row.get("code") or "unknown"),
        detail=detail,
    )


def _normalize_failure(row: dict) -> dict:
    detail = _bounded_bytes_text(row.get("detail") or row.get("error") or row.get("exception") or row.get("reason"), 3500)
    fingerprint = _failure_fingerprint(row, detail)
    return {
        "fingerprint": fingerprint,
        "origin": str(row.get("origin") or "airflow"),
        "component": str(row.get("component") or row.get("dag_id") or "unknown"),
        "task_id": str(row.get("task_id") or "unknown"),
        "error_class": str(row.get("error_class") or row.get("exception_type") or "unknown"),
        "error_code": str(row.get("error_code") or row.get("code") or "unknown"),
        "detail": detail,
        "run_id": row.get("run_id"),
        "occurred_at": row.get("ended_at") or row.get("end_date") or row.get("execution_date"),
        "sink": row.get("sink") or row.get("sink_marker"),
    }


def _query_bot_health(client, days: int) -> list[dict]:
    names = ["source_discovery", "source_vetting", "source_scheduling", "cadence_review", "failure_triage", "analytics_engineer", "data_analyst", "manager", "task_executor", "pr_reviewer"]
    try:
        result = client.query_runs(names, days=days, outcomes=["failed", "timed_out"], limit=100)
    except Exception as exc:
        raise ContextError(getattr(exc, "code", "dashboard_unavailable")) from exc
    rows = result.get("items", []) if isinstance(result, dict) else result
    if not isinstance(rows, list):
        raise ContextError("health_response_invalid")
    compact = []
    for row in rows:
        if not isinstance(row, dict) or row.get("outcome") not in {"failed", "timed_out"}:
            continue
        value = {key: row.get(key) for key in ("report_id", "bot", "dag_id", "run_id", "task_id", "map_index", "try_number", "outcome", "reason_code", "finished_at", "failure") if key in row}
        compact.append(value)
    return sorted(compact, key=lambda row: (str(row.get("finished_at", "")), str(row.get("report_id", ""))), reverse=True)[:50]


def failure_context(hours: int = 24, batch_size: int = 10) -> dict:
    client = _control_client()
    try:
        response = client.airflow_failures(hours=hours, limit=100)
    except Exception as exc:
        raise ContextError(getattr(exc, "code", "dashboard_unavailable")) from exc
    if isinstance(response, list):
        raw_items, remaining = response, 0
    elif isinstance(response, dict) and isinstance(response.get("items"), list):
        raw_items, remaining = response["items"], response.get("remaining_after_batch", 0)
    else:
        raise ContextError("failure_response_invalid")
    if not isinstance(remaining, int) or remaining < 0:
        raise ContextError("failure_response_invalid")
    groups: dict[str, dict] = {}
    for raw in raw_items:
        if not isinstance(raw, dict):
            raise ContextError("failure_row_invalid")
        row = _normalize_failure(raw)
        group = groups.setdefault(
            row["fingerprint"],
            {
                **{key: row[key] for key in ("fingerprint", "origin", "component", "task_id", "error_class", "error_code")},
                "tail": row["detail"],
                "count": 0,
            },
        )
        group["count"] += 1
        group.setdefault("occurrence_run_ids", [])
        group.setdefault("occurrences", [])
        if len(group["occurrences"]) < 10:
            # The bounded tail lives once per group; occurrences stay identity-only.
            group["occurrences"].append({"run_id": row["run_id"], "occurred_at": row["occurred_at"]})
        if row["run_id"] is not None and row["run_id"] not in group["occurrence_run_ids"] and len(group["occurrence_run_ids"]) < 10:
            group["occurrence_run_ids"].append(row["run_id"])
        group.setdefault("sink_markers", [])
        marker = row.get("sink")
        if marker and marker not in group["sink_markers"] and len(group["sink_markers"]) < 20:
            group["sink_markers"].append(marker)
    for group in groups.values():
        group["occurrence_run_ids"] = sorted(group.get("occurrence_run_ids", []), key=str)
        group["sink_markers"] = sorted(group.get("sink_markers", []), key=str)
        group["occurrences"] = sorted(group.get("occurrences", []), key=lambda row: (str(row.get("run_id", "")), str(row.get("occurred_at", ""))))
    selected = [groups[key] for key in sorted(groups)][: max(0, min(batch_size, 10))]
    context = _base("failure_triage")
    context.update(
        {
            "window_hours": hours,
            "failure_count": len(groups),
            "selected_failures": selected,
            "remaining_after_batch": remaining,
            "bot_health": _query_bot_health(client, max(1, (hours + 23) // 24)),
            "bot_dags_excluded": True,
            "truncated": len(groups) > len(selected),
            "total_count": len(groups),
            "returned_count": len(selected),
        }
    )
    _bounded_context(context, "failure_triage", ("bot_health",))
    context["returned_count"] = len(context["selected_failures"])
    while _encoded_size(context) > CONTEXT_LIMITS["failure_triage"] and context["selected_failures"]:
        context["selected_failures"].pop()
        context["truncated"] = True
        context["returned_count"] = len(context["selected_failures"])
    context["remaining_unreviewed"] = max(0, len(groups) - context["returned_count"])
    return _bounded_context(context, "failure_triage")


def _manifest_value(manifest: dict | None = None) -> dict:
    if manifest is not None:
        value = manifest
    else:
        path = TRANSFORM_ROOT / "target" / "manifest.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ContextError("manifest_unavailable") from exc
    if not isinstance(value, dict) or not isinstance(value.get("nodes"), dict):
        raise ContextError("manifest_invalid")
    return value


def analytics_context(*, sync_result: dict | None = None, manifest: dict | None = None,
                      pending_packages: list[dict] | None = None,
                      visualization_report: dict | None = None) -> dict:
    # Shared graph and content checks keep model and visualization ownership in
    # one specialist. Prior recommendations are evidence, not completion state.
    sys.path.insert(0, str(REPO_ROOT))
    from visualization.project import coverage, mart_nodes, source_ancestors
    if sync_result is None:
        # The wrapper re-executes the transform environment, which owns duckdb.
        sync_result = _run_capture([str(TRANSFORM_ROOT / "bin" / "sync_raw_sources"), "--check", "--json"], 180)
    if not isinstance(sync_result, dict) or sync_result.get("error") or not isinstance(sync_result.get("sources"), list):
        raise ContextError("warehouse_sync_failed", str((sync_result or {}).get("error", "warehouse unavailable")))
    manifest_value = _manifest_value(manifest)
    marts = mart_nodes(manifest_value)
    modeled = set().union(*(source_ancestors(manifest_value, uid) for uid in marts)) if marts else set()
    report = visualization_report if visualization_report is not None else coverage(manifest_value)
    if report.get("errors"):
        raise ContextError("visualization_inventory_invalid", str(report["errors"][:3]))
    try:
        manager = _control_client().manager_context(days=7)
        if not isinstance(manager, dict) or not isinstance(manager.get("backlog", []), list):
            raise ContextError("dashboard_response_invalid")
    except Exception as exc:
        raise ContextError(getattr(exc, "code", "dashboard_unavailable")) from exc
    active_tasks = [row for row in manager.get("backlog", []) if isinstance(row, dict)
                    and row.get("state") not in {"completed", "dismissed"}]
    resources = {key for row in active_tasks for key in row.get("resource_keys", [])}
    pending_sources = {str(row.get("source", "")) for row in (pending_packages or [])}
    drift = {}
    for item in sync_result.get("schema_drift", []):
        drift.setdefault(item["source"], []).append("raw column schema drift")
    for kind in ("missing_sources", "missing_bases", "stale_sources"):
        for name in sync_result.get(kind, []):
            drift.setdefault(name, []).append(kind)
    candidates = []
    inventory = {row["name"]: row for row in sync_result["sources"] if isinstance(row, dict) and row.get("name")}
    for name, row in sorted(inventory.items()):
        issues = drift.get(name, []) + list(row.get("issues") or [])
        if name in modeled and not issues:
            continue
        keys = {f"source:{name}", f"analytics:{name}"}
        if name in pending_sources or resources & keys:
            continue
        candidates.append((0 if issues else 1, name, {
            "name": name, "work_kind": "repair" if issues else "model",
            "path": row.get("path") or f"extract/sources/{name}.yml",
            "columns": row.get("columns", []), "rows_loaded": row.get("rows_loaded", 0),
            "last_loaded_at": row.get("last_loaded_at"), "base_model_path": f"transform/models/base/base_{name}.sql",
            "issues": issues, "resource_keys": sorted(keys),
        }))
    families = {}
    for item in report.get("models", []):
        if item.get("issues"):
            families.setdefault(item["family"], []).append(item)
    for family, items in sorted(families.items()):
        keys = {f"analytics:{family}", f"visualization:{family}"}
        keys.update(f"source:{name}" for item in items for name in item.get("sources", []))
        keys.update(f"model:{item['name']}" for item in items)
        if resources & keys or pending_sources & {name for item in items for name in item.get("sources", [])}:
            continue
        broken = any("unknown field" in issue or "wrong explore" in issue for item in items for issue in item["issues"])
        candidates.append((0 if broken else 2, family, {
            "name": family, "work_kind": "visualization", "path": f"transform/models/marts/{family}",
            "models": [{"name": item["name"], "issues": item["issues"], "charts": item["charts"]} for item in items],
            "resource_keys": sorted(keys),
            "allowed_path_globs": [f"transform/models/marts/{family}/**", "transform/lightdash/charts/*.yml", "transform/lightdash/dashboards/*.yml"],
            "instruction": "Propose one bounded family change. Narrow chart path globs to the selected models. Keep metrics and visualizations consistent with model grain.",
        }))
    candidates.sort(key=lambda item: (item[0], item[1], item[2]["work_kind"]))
    selected = candidates[0][2] if candidates else None
    context = _base("analytics_engineer")
    context.update({
        "selected": selected, "backlog_count": len(candidates),
        "unmodeled_count": len(set(inventory) - modeled), "modeled_count": len(modeled),
        "visualization_mart_count": report.get("mart_count", 0),
        "visualization_covered_count": report.get("covered_count", 0),
        "visualization_backlog_count": len(families),
        "active_dashboard_tasks": [
            {
                "task_id": row.get("id") or row.get("task_id"),
                "title": _bounded_text(row.get("title"), 200),
                "state": row.get("state"),
                "resource_keys": sorted(str(key) for key in (row.get("resource_keys") or []))[:50],
            }
            for row in active_tasks[:50]
        ],
        "truncated": len(active_tasks) > 50, "total_count": len(candidates), "returned_count": int(selected is not None),
    })
    return _bounded_context(context, "analytics_engineer", ("active_dashboard_tasks",))


def analyst_context(*, manifest: dict | None = None,
                    visualization_report: dict | None = None) -> dict:
    """Select the next mart family whose dashboard is missing time-series analysis.

    Analysis gaps are distinct from contract issues. A mart with a broken field
    reference belongs to the analytics engineer; a mart whose dashboard only
    ranks a category over the collection window belongs here. Families with a
    live task on the same resource keys are suppressed, and completed work is
    reconsidered whenever gaps remain.
    """
    sys.path.insert(0, str(REPO_ROOT))
    from visualization.project import LINEAGE_TIMES, coverage, mart_nodes, metadata
    manifest_value = _manifest_value(manifest)
    marts = mart_nodes(manifest_value)
    report = visualization_report if visualization_report is not None else coverage(manifest_value)
    if report.get("errors"):
        raise ContextError("visualization_inventory_invalid", str(report["errors"][:3]))
    try:
        manager = _control_client().manager_context(days=7)
        if not isinstance(manager, dict) or not isinstance(manager.get("backlog", []), list):
            raise ContextError("dashboard_response_invalid")
    except Exception as exc:
        raise ContextError(getattr(exc, "code", "dashboard_unavailable")) from exc
    active_tasks = [row for row in manager.get("backlog", []) if isinstance(row, dict)
                    and row.get("state") not in {"completed", "dismissed"}]
    resources = {key for row in active_tasks for key in row.get("resource_keys", [])}
    by_name = {node["name"]: node for node in marts.values()}

    def shape(name: str) -> dict:
        """Declared time and category dimensions, so the analyst knows where to probe."""
        node = by_name.get(name, {})
        spec = (metadata(node).get("vintage", {}).get("visualization") or {})
        times, categories = [], []
        for column, value in (node.get("columns") or {}).items():
            dimension = metadata(value).get("dimension", {})
            if dimension.get("hidden"):
                continue
            if dimension.get("type") in ("date", "timestamp"):
                if column not in LINEAGE_TIMES and column != spec.get("time"):
                    times.append({"column": column,
                                  "intervals": [str(i).upper() for i in dimension.get("time_intervals") or []]})
            elif dimension.get("type") == "string":
                categories.append(column)
        return {"collection_time": spec.get("time"), "event_time_candidates": times[:12],
                "category_dimensions": categories[:30],
                "metrics": sorted(f"{key}" for key in (metadata(node).get("metrics") or {}))
                          + sorted(key for value in (node.get("columns") or {}).values()
                                   for key in (metadata(value).get("metrics") or {}))}

    families, blocked = {}, 0
    for item in report.get("models", []):
        if item.get("gaps") or item.get("issues"):
            families.setdefault(item["family"], []).append(item)
    candidates = []
    for family, items in sorted(families.items()):
        keys = {f"analytics:{family}", f"visualization:{family}", f"analysis:{family}"}
        keys.update(f"source:{name}" for item in items for name in item.get("sources", []))
        keys.update(f"model:{item['name']}" for item in items)
        if resources & keys:
            blocked += 1
            continue
        # Contract breaches belong to the analytics engineer, so a family that
        # is merely missing analysis is worked first.
        broken = any(item["issues"] for item in items)
        candidates.append((1 if broken else 0, family, {
            "name": family, "work_kind": "analysis", "path": f"transform/models/marts/{family}",
            "standard": "visualization/TRENDS.md",
            "exemplar": "transform/models/marts/public_art_arcgis/_public_art_arcgis_models.yml",
            "family_yaml": f"transform/models/marts/{family}/_{family}_models.yml",
            "models": [{"name": item["name"], "gaps": item["gaps"], "issues": item["issues"],
                        "trend_count": item.get("trend_count", 0), **shape(item["name"])} for item in items],
            "resource_keys": sorted(keys),
            "allowed_path_globs": [f"transform/models/marts/{family}/_{family}_models.yml"],
            "instruction": "Profile and query every mart in this family with visualization/bin/eda before"
                           " proposing anything. Every mart here must reach zero gaps, not just the largest."
                           " Bound series counts, name the misreading in each description, and never trend on"
                           " extractor bookkeeping timestamps.",
        }))
    candidates.sort(key=lambda item: (item[0], item[1]))
    selected = candidates[0][2] if candidates else None
    context = _base("data_analyst")
    context.update({
        "selected": selected, "gap_backlog_count": len(candidates),
        "suppressed_family_count": blocked,
        "mart_count": report.get("mart_count", 0),
        "trend_covered_count": report.get("trend_covered_count", 0),
        "trend_chart_count": report.get("trend_chart_count", 0),
        "gap_count": report.get("gap_count", 0),
        "active_dashboard_tasks": [
            {
                "task_id": row.get("id") or row.get("task_id"),
                "title": _bounded_text(row.get("title"), 200),
                "state": row.get("state"),
                "resource_keys": sorted(str(key) for key in (row.get("resource_keys") or []))[:50],
            }
            for row in active_tasks[:50]
        ],
        "truncated": len(active_tasks) > 50, "total_count": len(candidates),
        "returned_count": int(selected is not None),
    })
    return _bounded_context(context, "data_analyst", ("active_dashboard_tasks",))


def _encoded_size(context: dict) -> int:
    return len(json.dumps(context, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode())


def _bounded_context(context: dict, kind: str, shed: tuple[str, ...] = ()) -> dict:
    """Shed optional evidence deterministically; fail only when mandatory evidence overflows."""
    limit = CONTEXT_LIMITS[kind]
    for key in shed:
        if _encoded_size(context) <= limit:
            break
        values = context.get(key)
        if not isinstance(values, list) or not values:
            continue
        while values and _encoded_size(context) > limit:
            values.pop()
            context["truncated"] = True
            if key == shed[0]:
                # Only the primary evidence list defines the returned count.
                context["returned_count"] = len(values)
    if _encoded_size(context) > limit:
        raise ContextError("context_exceeds_limit", f"{kind} exceeds {limit} bytes")
    return context


def manager_context(days: int = 7) -> dict:
    try:
        value = _control_client().manager_context(days=days)
    except Exception as exc:
        raise ContextError(getattr(exc, "code", "dashboard_unavailable")) from exc
    if not isinstance(value, dict):
        raise ContextError("manager_response_invalid")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["discovery", "vetting", "scheduling", "cadence", "failures", "analytics", "analyst", "manager"])
    parser.add_argument("--hours", "--window-hours", dest="hours", type=int)
    parser.add_argument("--audit-size", type=int, default=12)
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    builders = {
        "discovery": discovery_context,
        "vetting": vetting_context,
        "scheduling": scheduling_context,
        "cadence": lambda: cadence_context(args.hours or 48, args.audit_size),
        "failures": lambda: failure_context(args.hours or 24),
        "analytics": analytics_context,
        "analyst": analyst_context,
        "manager": lambda: manager_context(args.days),
    }
    print(json.dumps(builders[args.kind](), indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
