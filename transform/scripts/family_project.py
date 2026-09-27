#!/usr/bin/env python3
"""Discover and materialize isolated dbt projects for one mart family."""

from __future__ import annotations

import argparse
import json
import pathlib
import shutil
from collections.abc import Iterable
from typing import Any

import yaml
from dbt_common.clients.jinja import get_environment
from jinja2 import nodes
from jinja2.exceptions import TemplateSyntaxError
from dbt_extractor import ExtractionError, py_extract_from_source

CADENCES = ("twice_hourly", "hourly", "daily")
RAW_SOURCES = pathlib.PurePosixPath("models/base/_raw_sources.yml")


class FamilyProjectError(RuntimeError):
    """The selected family cannot be isolated into a valid dbt project."""


def _sql_files(root: pathlib.Path) -> dict[str, list[pathlib.Path]]:
    files: dict[str, list[pathlib.Path]] = {}
    for path in root.joinpath("models").rglob("*.sql"):
        files.setdefault(path.stem, []).append(path)
    return files

def _literal(node: nodes.Node) -> Any:
    if isinstance(node, nodes.Const):
        return node.value
    if isinstance(node, nodes.List):
        return [_literal(item) for item in node.items]
    if isinstance(node, nodes.Tuple):
        return tuple(_literal(item) for item in node.items)
    if isinstance(node, nodes.Dict):
        return {_literal(pair.key): _literal(pair.value) for pair in node.items}
    raise FamilyProjectError(f"non-literal dbt dependency argument: {node!s}")


def _extract_with_jinja(path: pathlib.Path) -> dict[str, Any]:
    """Use dbt's Jinja parser when the Rust extractor rejects valid config syntax."""
    try:
        parsed = get_environment(None, capture_macros=True).parse(
            path.read_text(encoding="utf-8")
        )
    except (OSError, UnicodeError, TemplateSyntaxError) as exc:
        raise FamilyProjectError(f"cannot statically extract {path}: {exc}") from exc
    refs: list[dict[str, str]] = []
    sources: set[tuple[str, str]] = set()
    configs: list[tuple[str, Any]] = []
    for call in parsed.find_all(nodes.Call):
        if not isinstance(call.node, nodes.Name):
            continue
        if call.node.name == "ref":
            if not call.args:
                raise FamilyProjectError(f"{path}: ref requires a literal model name")
            name = _literal(call.args[-1])
            if not isinstance(name, str):
                raise FamilyProjectError(f"{path}: ref requires a literal model name")
            refs.append({"name": name})
        elif call.node.name == "source":
            if len(call.args) < 2:
                raise FamilyProjectError(f"{path}: source requires literal source and table names")
            source, name = (_literal(call.args[0]), _literal(call.args[1]))
            if not isinstance(source, str) or not isinstance(name, str):
                raise FamilyProjectError(f"{path}: source requires literal source and table names")
            sources.add((source, name))
        elif call.node.name == "config":
            for keyword in call.kwargs:
                if keyword.key == "tags":
                    configs.append(("tags", _literal(keyword.value)))
    return {"refs": refs, "sources": sources, "configs": configs}


def _extract(path: pathlib.Path) -> dict[str, Any]:
    try:
        return py_extract_from_source(path.read_text(encoding="utf-8"))
    except ExtractionError:
        return _extract_with_jinja(path)
    except (OSError, UnicodeError) as exc:
        raise FamilyProjectError(f"cannot statically extract {path}: {exc}") from exc


def _ref_names(extracted: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for ref in extracted.get("refs", []):
        if isinstance(ref, dict) and isinstance(ref.get("name"), str):
            names.add(ref["name"])
        elif isinstance(ref, (tuple, list)) and ref and isinstance(ref[-1], str):
            names.add(ref[-1])
    return names


def _raw_source_names(extracted: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for source in extracted.get("sources", []):
        if isinstance(source, dict) and source.get("name") and source.get("source_name") == "raw":
            names.add(str(source["name"]))
        elif isinstance(source, (tuple, list)) and len(source) >= 2 and source[0] == "raw":
            names.add(str(source[1]))
    return names


def _cadences(extracted: dict[str, Any]) -> set[str]:
    found: set[str] = set()
    for config in extracted.get("configs", []):
        if not isinstance(config, (tuple, list)) or len(config) != 2 or config[0] != "tags":
            continue
        tags = config[1]
        if isinstance(tags, str):
            found.add(tags)
        elif isinstance(tags, Iterable):
            found.update(tag for tag in tags if isinstance(tag, str))
    return found & set(CADENCES)


def discover(project_root: pathlib.Path) -> list[dict[str, Any]]:
    """Return family/cadence pairs without invoking dbt or touching a warehouse.

    A static extraction error is attached only to its owning family.  Its three
    cadence DAGs are retained so Airflow reports the failure rather than silently
    removing that family's schedule.
    """
    marts = project_root / "models" / "marts"
    if not marts.is_dir():
        raise FamilyProjectError(f"missing marts directory: {marts}")
    discovered: list[dict[str, Any]] = []
    for family_dir in sorted(path for path in marts.iterdir() if path.is_dir()):
        family = family_dir.name
        try:
            cadences: set[str] = set()
            for path in family_dir.rglob("*.sql"):
                cadences.update(_cadences(_extract(path)))
            discovered.append({"family": family, "cadences": sorted(cadences)})
        except FamilyProjectError as exc:
            discovered.append(
                {"family": family, "cadences": list(CADENCES), "error": str(exc)}
            )
    return discovered


def _copy_sql(project_root: pathlib.Path, destination: pathlib.Path, paths: Iterable[pathlib.Path]) -> None:
    for path in paths:
        relative = path.relative_to(project_root)
        output = destination / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, output)


def _raw_source_document(project_root: pathlib.Path, source_names: set[str]) -> str:
    """Filter the shared generated source declaration without dangling aliases."""
    source_path = project_root / RAW_SOURCES
    try:
        document = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise FamilyProjectError(
            f"cannot read shared raw source declaration {source_path}: {exc}"
        ) from exc
    if not isinstance(document, dict) or not isinstance(document.get("sources"), list):
        raise FamilyProjectError(f"generated raw source declaration is invalid: {source_path}")
    filtered_sources = []
    seen: set[str] = set()
    for source in document["sources"]:
        if not isinstance(source, dict) or source.get("name") != "raw":
            continue
        tables = source.get("tables")
        if not isinstance(tables, list):
            raise FamilyProjectError(f"generated raw source declaration has no tables: {source_path}")
        selected = [
            table
            for table in tables
            if isinstance(table, dict) and table.get("name") in source_names
        ]
        seen.update(str(table["name"]) for table in selected)
        if selected:
            copied = {key: value for key, value in source.items() if key != "tables"}
            copied["tables"] = selected
            filtered_sources.append(copied)
    missing = source_names - seen
    if missing:
        raise FamilyProjectError(f"raw source declarations missing tables: {sorted(missing)}")
    result = {key: value for key, value in document.items() if key != "sources"}
    result["sources"] = filtered_sources
    return yaml.safe_dump(result, sort_keys=False, allow_unicode=True)


def _filtered_schema(path: pathlib.Path, model_names: set[str]) -> str | None:
    """Retain schema entries and their tests only for models in this closure."""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise FamilyProjectError(f"cannot read selected schema {path}: {exc}") from exc
    if not isinstance(document, dict):
        return None
    models = document.get("models")
    if not isinstance(models, list):
        return None
    selected = [model for model in models if isinstance(model, dict) and model.get("name") in model_names]
    if not selected:
        return None
    result = {key: value for key, value in document.items() if key != "models"}
    result["models"] = selected
    return yaml.safe_dump(result, sort_keys=False, allow_unicode=True)


def _copy_scoped_schema(
    project_root: pathlib.Path,
    destination: pathlib.Path,
    model_paths: Iterable[pathlib.Path],
    model_names: set[str],
) -> None:
    candidates: set[pathlib.Path] = set()
    for model_path in model_paths:
        relative = model_path.relative_to(project_root)
        if relative.parts[:2] == ("models", "base"):
            candidates.update(
                path
                for path in (model_path.with_suffix(".yml"), model_path.with_suffix(".yaml"))
                if path.exists()
            )
            continue
        for suffix in ("*.yml", "*.yaml"):
            candidates.update(model_path.parent.glob(suffix))
    candidates.discard(project_root / RAW_SOURCES)
    for path in sorted(candidates):
        contents = _filtered_schema(path, model_names)
        if contents is None:
            continue
        output = destination / path.relative_to(project_root)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(contents, encoding="utf-8")


def create(
    project_root: pathlib.Path, family: str, cadence: str, destination: pathlib.Path
) -> dict[str, Any]:
    if cadence not in CADENCES:
        raise FamilyProjectError(f"unknown cadence {cadence!r}")
    family_dir = project_root / "models" / "marts" / family
    if not family_dir.is_dir():
        raise FamilyProjectError(f"unknown mart family {family!r}")
    model_paths = _sql_files(project_root)
    family_models: dict[str, pathlib.Path] = {}
    for path in family_dir.rglob("*.sql"):
        if path.stem in family_models:
            raise FamilyProjectError(
                f"mart family {family!r} has duplicate model name {path.stem!r}"
            )
        family_models[path.stem] = path
    extracted = {name: _extract(path) for name, path in family_models.items()}
    selected = {
        name for name, data in extracted.items() if cadence in _cadences(data)
    }
    if not selected:
        raise FamilyProjectError(f"mart family {family!r} has no {cadence} models")

    closure = set(selected)
    sources: set[str] = set()
    pending = list(selected)
    while pending:
        name = pending.pop()
        paths = model_paths.get(name)
        if not paths:
            raise FamilyProjectError(f"model {name!r} is not present in the project")
        if len(paths) != 1:
            raise FamilyProjectError(
                f"model {name!r} is ambiguous: {[str(path) for path in paths]}"
            )
        path = paths[0]
        data = extracted.get(name)
        if data is None:
            data = extracted[name] = _extract(path)
        sources.update(_raw_source_names(data))
        for dependency in _ref_names(data):
            dependency_paths = model_paths.get(dependency)
            if not dependency_paths:
                raise FamilyProjectError(f"{path} references missing model {dependency!r}")
            if len(dependency_paths) != 1:
                raise FamilyProjectError(
                    f"{path} references ambiguous model {dependency!r}: "
                    f"{[str(candidate) for candidate in dependency_paths]}"
                )
            if dependency not in closure:
                closure.add(dependency)
                pending.append(dependency)

    selected_paths = [model_paths[name][0] for name in sorted(closure)]
    if destination.exists():
        raise FamilyProjectError(f"isolated project destination already exists: {destination}")
    destination.mkdir(parents=True)
    for name in ("dbt_project.yml", "profiles.yml"):
        shutil.copy2(project_root / name, destination / name)
    macro_root = project_root / "macros"
    if macro_root.exists():
        shutil.copytree(macro_root, destination / "macros")
    _copy_sql(project_root, destination, selected_paths)
    _copy_scoped_schema(project_root, destination, selected_paths, closure)
    raw_yaml = destination / RAW_SOURCES
    raw_yaml.parent.mkdir(parents=True, exist_ok=True)
    raw_yaml.write_text(_raw_source_document(project_root, sources), encoding="utf-8")
    return {
        "project_dir": str(destination),
        "family": family,
        "cadence": cadence,
        "selected_models": sorted(selected),
        "dependency_models": sorted(closure - selected),
        "raw_sources": sorted(sources),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1])
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("discover")
    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("--family", required=True)
    create_parser.add_argument("--cadence", required=True, choices=CADENCES)
    create_parser.add_argument("--destination", required=True, type=pathlib.Path)
    args = parser.parse_args()
    try:
        if args.command == "discover":
            value: Any = discover(args.project_root)
        else:
            value = create(args.project_root, args.family, args.cadence, args.destination)
    except FamilyProjectError as exc:
        parser.error(str(exc))
    print(json.dumps(value, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
