#!/usr/bin/env python3
"""Verify that reviewed Sumo metadata exactly matches its generated Lightdash content."""
from __future__ import annotations

import pathlib
import sys
from typing import Any

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from visualization import content, project


def model(dbt, session):
    """Keep dbt's Python-model parser from rejecting this colocated operator script."""
    dbt.config(enabled=False)
    return session.sql("select 1 where false")

MODEL_PATH = pathlib.Path(__file__).with_name("_sumo_models.yml")
CONTENT_ROOT = ROOT / "transform" / "lightdash"
SCOPED_PATHS = (
    "charts/fct-sumo-rikishi-observation.yml",
    "charts/fct-sumo-rikishi-observation-details.yml",
    "charts/fct-sumo-rikishi-observation-rikishi-updated.yml",
    "charts/fct-sumo-rikishi-observation-rank-mix.yml",
    "charts/fct-sumo-rikishi-observation-heya-breadth.yml",
    "charts/fct-sumo-rikishi-observation-average-height.yml",
    "charts/fct-sumo-rikishi-observation-average-weight.yml",
    "dashboards/sumo.yml",
)
MAX_DIAGNOSTICS = 16


def _manifest() -> dict[str, Any]:
    document = yaml.safe_load(MODEL_PATH.read_text()) or {}
    models = document.get("models") or []
    if len(models) != 1 or models[0].get("name") != "fct_sumo_rikishi_observation":
        raise ValueError("Sumo metadata must contain exactly the scoped mart model")
    node = project.node_from_yaml(models[0], "models/marts/sumo/fct_sumo_rikishi_observation.sql")
    node.update({"resource_type": "model", "package_name": "vintage_data"})
    return {"nodes": {"model.vintage_data.fct_sumo_rikishi_observation": node}}


def _differences(expected: Any, actual: Any, path: str = "document") -> list[str]:
    if type(expected) is not type(actual):
        return [f"{path}: value type differs"]
    if isinstance(expected, dict):
        messages = []
        for key in sorted(expected.keys() - actual.keys()):
            messages.append(f"{path}.{key}: missing key")
        for key in sorted(actual.keys() - expected.keys()):
            messages.append(f"{path}.{key}: unexpected key")
        for key in sorted(expected.keys() & actual.keys()):
            messages.extend(_differences(expected[key], actual[key], f"{path}.{key}"))
            if len(messages) >= MAX_DIAGNOSTICS:
                break
        return messages
    if isinstance(expected, list):
        if len(expected) != len(actual):
            return [f"{path}: list length differs (expected {len(expected)}, found {len(actual)})"]
        messages = []
        for index, (expected_item, actual_item) in enumerate(zip(expected, actual)):
            messages.extend(_differences(expected_item, actual_item, f"{path}[{index}]"))
            if len(messages) >= MAX_DIAGNOSTICS:
                break
        return messages
    return [] if expected == actual else [f"{path}: value differs"]


def verify() -> list[str]:
    expected_all = content.render(_manifest())
    expected_paths = set(SCOPED_PATHS)
    rendered_paths = set(expected_all)
    if rendered_paths != expected_paths:
        return ["metadata renders a different Sumo file set"]

    diagnostics: list[str] = []
    for relative in SCOPED_PATHS:
        path = CONTENT_ROOT / relative
        if not path.is_file():
            diagnostics.append(f"{relative}: missing")
            continue
        try:
            actual = yaml.safe_load(path.read_text())
        except (OSError, yaml.YAMLError):
            diagnostics.append(f"{relative}: unreadable or invalid YAML")
            continue
        for message in _differences(expected_all[relative], actual):
            diagnostics.append(f"{relative}: {message}")
            if len(diagnostics) >= MAX_DIAGNOSTICS:
                return diagnostics
    return diagnostics


def main() -> int:
    try:
        diagnostics = verify()
    except (OSError, ValueError, yaml.YAMLError) as error:
        print(f"Sumo content verification failed: {type(error).__name__}")
        return 1
    if diagnostics:
        print("Sumo content verification failed:")
        for diagnostic in diagnostics[:MAX_DIAGNOSTICS]:
            print(f"- {diagnostic}")
        if len(diagnostics) > MAX_DIAGNOSTICS:
            print(f"- additional differences omitted ({len(diagnostics) - MAX_DIAGNOSTICS})")
        return 1
    print(f"Sumo content verified: {len(SCOPED_PATHS)} scoped files match reviewed metadata")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
