"""Bound manager evidence without hiding freshness or omitted backlog work."""
from copy import deepcopy
import json


def compact_manager_context(context: dict, limit: int = 90 * 1024) -> dict:
    result = deepcopy(context)
    def shorten(value, length=800):
        if isinstance(value, str):
            return value if len(value) <= length else value[:length] + "… [abridged]"
        return value
    def report(value):
        if not isinstance(value, dict):
            return value
        keys = ("id", "bot_name", "dag_id", "run_id", "outcome", "status", "reason_code",
                "failure_class", "failure_detail", "finished_at", "report_schema")
        item = {key: shorten(value[key]) for key in keys if key in value}
        payload = value.get("payload")
        if isinstance(payload, dict):
            item["payload"] = {key: shorten(payload[key], 1600) for key in
                               ("schema_version", "agent", "status", "summary") if key in payload}
            item["payload"]["collection_counts"] = {key: len(val) for key, val in payload.items() if isinstance(val, list)}
        return item
    for specialist in result.get("specialists", []):
        for key in ("latest_run", "latest_useful_payload"):
            if key in specialist:
                specialist[key] = report(specialist[key])
    result["bot_health"] = [report(row) for row in result.get("bot_health", [])]
    keys = ("id", "title", "category", "state", "priority", "source_bot", "recommendation_key",
            "resource_keys", "assignee_kind", "assignee_name", "blocked_from_state", "version", "planned_resolution")
    result["backlog"] = [{key: shorten(row[key], 700) for key in keys if key in row}
                         for row in result.get("backlog", [])]
    result["context_summary"] = {
        "abridged": True, "backlog_input_count": len(result["backlog"]),
        "backlog_omitted_count": 0, "health_omitted_count": 0,
        "note": "Evidence is abridged. Fetch the referenced ticket/report for full details. Omitted work is not absent work; do not propose duplicates when backlog is incomplete.",
    }
    def size():
        return len(json.dumps({"MANAGER_CONTEXT": result}, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode())
    while size() > limit and result["bot_health"]:
        result["bot_health"].pop()
        result["context_summary"]["health_omitted_count"] += 1
    while size() > limit and result["backlog"]:
        result["backlog"].pop()
        result["context_summary"]["backlog_omitted_count"] += 1
    if size() > limit:
        # Keep all freshness metadata even when a specialist emits pathological identifiers.
        for specialist in result.get("specialists", []):
            for key in ("latest_run", "latest_useful_payload"):
                if isinstance(specialist.get(key), dict):
                    specialist[key].pop("payload", None)
    if size() > limit:
        raise ValueError("manager freshness metadata exceeds context budget")
    return result
