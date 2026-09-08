#!/usr/bin/env python3
"""Validate the dbt graph's layering, materialization, cadence, and test policy."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

CADENCE_TAGS = {"twice_hourly", "hourly", "daily"}
CADENCE_ORDER = {"twice_hourly": 0, "hourly": 1, "daily": 2}
PHYSICAL_MATERIALIZATIONS = {"table", "incremental"}


def _models(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        uid: node
        for uid, node in (manifest.get("nodes") or {}).items()
        if node.get("resource_type") == "model" and node.get("package_name") == "vintage_data"
    }


def _raw_sources(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        uid: source
        for uid, source in (manifest.get("sources") or {}).items()
        if source.get("package_name") == "vintage_data"
        and source.get("source_name") == "raw"
    }


def _model_dependencies(node: dict[str, Any], models: dict[str, dict[str, Any]]) -> list[str]:
    return [uid for uid in node.get("depends_on", {}).get("nodes", []) if uid in models]


def _ancestors(uid: str, models: dict[str, dict[str, Any]]) -> set[str]:
    found: set[str] = set()
    stack = list(_model_dependencies(models[uid], models))
    while stack:
        dependency = stack.pop()
        if dependency in found:
            continue
        found.add(dependency)
        stack.extend(_model_dependencies(models[dependency], models))
    return found


def _cadence(node: dict[str, Any]) -> set[str]:
    tags = set(node.get("tags") or node.get("config", {}).get("tags") or [])
    return tags & CADENCE_TAGS


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    models = _models(manifest)
    sources = _raw_sources(manifest)

    for source_uid, source in sorted(sources.items()):
        source_name = str(source["name"])
        expected_name = f"base_{source_name}"
        direct = [
            (uid, model)
            for uid, model in models.items()
            if source_uid in model.get("depends_on", {}).get("nodes", [])
        ]
        expected = [
            (uid, model)
            for uid, model in direct
            if model.get("name") == expected_name
            and str(model.get("original_file_path", "")).startswith("models/base/")
        ]
        if len(expected) != 1 or len(direct) != 1:
            names = sorted(model.get("name", uid) for uid, model in direct)
            errors.append(
                f"raw source {source_name!r} must have exactly one direct "
                f"models/base/{expected_name}.sql consumer; found {names}"
            )

    for uid, model in sorted(models.items()):
        name = str(model.get("name", uid))
        path = str(model.get("original_file_path", ""))
        materialized = model.get("config", {}).get("materialized")
        direct_sources = [
            dependency
            for dependency in model.get("depends_on", {}).get("nodes", [])
            if dependency in sources
        ]
        if direct_sources and not path.startswith("models/base/"):
            errors.append(f"{name}: only models/base may depend directly on raw sources")
        if path.startswith("models/base/") and materialized != "view":
            errors.append(f"{name}: base models must be materialized as views")
        if path.startswith("models/marts/") and not name.startswith("fct_"):
            errors.append(f"{name}: marts models must use the fct_ prefix")
        if name.startswith("fct_") and not path.startswith("models/marts/"):
            errors.append(f"{name}: fct_ models must live under models/marts")

        if not name.startswith("fct_"):
            continue
        if materialized not in PHYSICAL_MATERIALIZATIONS:
            errors.append(f"{name}: final models must be table or incremental")
        ancestor_ids = _ancestors(uid, models)
        if not any(
            str(models[ancestor].get("original_file_path", "")).startswith("models/base/")
            for ancestor in ancestor_ids
        ):
            errors.append(f"{name}: final model has no base-model ancestor")
        cadence = _cadence(model)
        if len(cadence) != 1:
            errors.append(
                f"{name}: final model must have exactly one cadence tag; found {sorted(cadence)}"
            )
        description = str(model.get("description") or "").strip()
        if not description:
            errors.append(f"{name}: final model description is required")
        grain = model.get("meta", {}).get("grain") or model.get("config", {}).get("meta", {}).get("grain")
        if not grain:
            errors.append(f"{name}: meta.grain is required")
        contract = model.get("config", {}).get("contract", {})
        if not contract.get("enforced"):
            errors.append(f"{name}: final model contract must be enforced")
        columns = model.get("columns") or {}
        if not columns:
            errors.append(f"{name}: final model must declare its columns")
        for column_name, column in sorted(columns.items()):
            if not str(column.get("description") or "").strip():
                errors.append(f"{name}.{column_name}: column description is required")
            if not str(column.get("data_type") or "").strip():
                errors.append(f"{name}.{column_name}: column data_type is required")

        if len(cadence) == 1:
            child_order = CADENCE_ORDER[next(iter(cadence))]
            for ancestor in ancestor_ids:
                ancestor_cadence = _cadence(models[ancestor])
                if len(ancestor_cadence) != 1:
                    continue
                ancestor_tag = next(iter(ancestor_cadence))
                if CADENCE_ORDER[ancestor_tag] > child_order:
                    errors.append(
                        f"{name}: cadence is faster than final ancestor "
                        f"{models[ancestor]['name']} ({ancestor_tag})"
                    )

    all_nodes = manifest.get("nodes") or {}
    all_sources = manifest.get("sources") or {}
    for uid, test in sorted(all_nodes.items()):
        if test.get("resource_type") != "test" or test.get("package_name") != "vintage_data":
            continue
        name = str(test.get("name", uid))
        dependencies = test.get("depends_on", {}).get("nodes", [])
        for dependency in dependencies:
            if dependency in all_sources:
                errors.append(f"{name}: data tests may not target sources")
            elif dependency in models:
                materialized = models[dependency].get("config", {}).get("materialized")
                if materialized not in PHYSICAL_MATERIALIZATIONS:
                    errors.append(
                        f"{name}: data tests may target only table/incremental models, "
                        f"not {models[dependency]['name']} ({materialized})"
                    )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", nargs="?", type=pathlib.Path, default=pathlib.Path("target/manifest.json"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    errors = validate_manifest(manifest)
    if args.json:
        print(json.dumps({"ok": not errors, "errors": errors}, indent=2, sort_keys=True))
    elif errors:
        print("dbt project policy failed:")
        for error in errors:
            print(f"- {error}")
    else:
        print("dbt project policy: ok")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
