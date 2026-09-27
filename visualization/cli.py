#!/usr/bin/env python3
"""Operator entrypoint. Production changes are explicit subcommands."""
from __future__ import annotations

import argparse
import copy
import json
import os
import pathlib
import re
import shutil
import secrets
import subprocess
import sys
import tempfile

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "orchestration" / "include"))
import deployment
from visualization import content, project, publisher


def load_environment(*, secrets_required=True):
    deployment.load_env()
    if secrets_required:
        deployment.load_env(ROOT / "orchestration" / "airflow.secrets.env")
    identity = publisher.state_root() / "deployment.json"
    if identity.exists():
        os.environ.setdefault("LIGHTDASH_PROJECT", json.loads(identity.read_text())["project_uuid"])


def immutable_batch(batch_id: str) -> pathlib.Path:
    if not re.fullmatch("[a-f0-9]{64}", batch_id):
        raise ValueError("invalid batch identity")
    batch = publisher.state_root() / "batches" / batch_id
    info = json.loads((batch / "batch.json").read_text())
    manifest = json.loads((batch / "manifest.json").read_text())
    if info.get("batch_id") != batch_id or info.get("manifest_sha256") != project.digest(manifest):
        raise ValueError("invalid immutable batch")
    content_root = batch / "content"
    if info.get("content_sha256") != project.content_digest(content_root):
        raise ValueError("immutable batch content failed integrity check")
    return batch


def receipt_path(root: pathlib.Path) -> pathlib.Path:
    return root / "deployments" / "current.json"


def deployment_config(project_uuid: str, bundle: dict) -> str:
    return project.digest({
        "url": os.environ.get("LIGHTDASH_URL"),
        "project_uuid": project_uuid,
        "serving_database": bundle["database"],
        "serving_schema": bundle["schema"],
        "pg_host": os.environ.get("LIGHTDASH_PG_HOST"),
        "pg_port": os.environ.get("LIGHTDASH_PG_PORT"),
        "reader_user": "mart_reader",
    })


def write_receipt(root: pathlib.Path, value: dict) -> None:
    target = receipt_path(root)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    os.replace(temporary, target)



def release_state_path(root: pathlib.Path) -> pathlib.Path:
    return root / "deployments" / "aggregate.json"


def _read_json(path: pathlib.Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid serving release state: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"invalid serving release state: {path}")
    return value


def _batch_scope(batch: pathlib.Path) -> tuple[dict, dict]:
    info = _read_json(batch / "batch.json")
    manifest = _read_json(batch / "manifest.json")
    if info.get("vintage_scope") != project.vintage_scope(manifest):
        raise ValueError("batch vintage scope does not match its manifest")
    return info, manifest


def _nodes_for_tables(manifest: dict, tables: set[str]) -> dict[str, dict]:
    nodes = {}
    for uid, node in project.mart_nodes(manifest).items():
        table = node.get("alias") or node["name"]
        if table in tables:
            nodes[uid] = copy.deepcopy(node)
    if len(nodes) != len(tables):
        raise ValueError("serving release references a mart absent from its immutable manifest")
    return nodes


def _bundle_baseline(root: pathlib.Path, identity: str, serving: dict[str, dict], batch_models: set[str]) -> dict | None:
    """Verify the prior catalog against the ledger outside the current batch."""
    receipt = _read_json(receipt_path(root)) if receipt_path(root).is_file() else None
    candidates = []
    for bundle in (root / "bundles").glob("*"):
        if not bundle.is_dir():
            continue
        try:
            info = _read_json(bundle / "bundle.json")
            manifest = _read_json(bundle / "source-manifest.json")
            deployed = _read_json(bundle / "deployment.json")
            validation = _read_json(bundle / "validation.json")
        except ValueError:
            continue
        release = bundle.name
        if not re.fullmatch("[a-f0-9]{64}", release):
            continue
        if deployed.get("project_uuid") != identity or validation.get("project_uuid") != identity or validation.get("status") != "ok":
            continue
        if info.get("source_manifest_sha256") != project.digest(manifest):
            continue
        if info.get("content_sha256") and project.content_digest(bundle / "content") != info["content_sha256"]:
            continue
        models = {
            node.get("alias") or node["name"]: node
            for node in project.mart_nodes(manifest).values()
        }
        if (
            info.get("database") != os.environ.get("LIGHTDASH_SERVING_DB", "vintage_serving")
            or info.get("schema") != "transform_marts"
            or info.get("mart_count") != len(models)
            or not models
            or set(models) | batch_models != set(serving)
        ):
            continue
        if any(
            not ledger.get("schema_sha256")
            or ledger["schema_sha256"] != project.digest(project.columns(models[name]))
            for name, ledger in serving.items() if name not in batch_models
        ):
            continue
        candidates.append((bundle, release, models))
    if receipt:
        candidates = [item for item in candidates if receipt.get("status") == "ok"
                      and receipt.get("project_uuid") == identity
                      and receipt.get("release_sha256") == item[1]]
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0].joinpath("validation.json").stat().st_mtime_ns, reverse=True)
    if len(candidates) > 1 and candidates[0][0].joinpath("validation.json").stat().st_mtime_ns == candidates[1][0].joinpath("validation.json").stat().st_mtime_ns:
        raise ValueError("multiple equally recent verified local bundles match the serving ledger")
    bundle, release, models = candidates[0]
    return {
        "schema_version": 1,
        "project_uuid": identity,
        "models": {name: {"baseline_release": release} for name in models},
    }


def release_state(root: pathlib.Path, identity: str | None, serving: dict[str, dict], batch: pathlib.Path) -> dict:
    """Load accepted mart snapshots, adopting a provable existing aggregate once."""
    path = release_state_path(root)
    if path.is_file():
        state = _read_json(path)
        if state.get("schema_version") != 1 or not isinstance(state.get("models"), dict):
            raise ValueError("invalid aggregate serving state")
        if state.get("project_uuid") and identity and state["project_uuid"] != identity:
            raise ValueError("aggregate state belongs to a different Lightdash project")
        return state
    info, _ = _batch_scope(batch)
    batch_models = {table["table"] for table in info["tables"]}
    if identity:
        baseline = _bundle_baseline(root, identity, serving, batch_models)
        if baseline:
            return baseline
        raise ValueError(
            "existing Lightdash project has no reliable local aggregate baseline; "
            "restore a matching successful bundle and serving model ledger before partial sync"
        )
    if serving and set(serving) == batch_models:
        return {
            "schema_version": 1, "project_uuid": None,
            "models": {name: {"batch_id": info["batch_id"]} for name in batch_models},
        }
    if serving:
        raise ValueError("serving model ledger exists without an adopted Lightdash project baseline")
    return {"schema_version": 1, "project_uuid": None, "models": {}}


def aggregate_manifest(root: pathlib.Path, state: dict) -> dict:
    """Compose immutable last-good model metadata; checkout siblings never enter."""
    snapshots: dict[str, dict] = {}
    template = None
    for table, entry in sorted(state["models"].items()):
        if "batch_id" in entry:
            batch = immutable_batch(entry["batch_id"])
            info, manifest = _batch_scope(batch)
            available = {row["table"] for row in info["tables"]}
        elif "baseline_release" in entry:
            bundle = root / "bundles" / entry["baseline_release"]
            manifest = _read_json(bundle / "source-manifest.json")
            available = {node.get("alias") or node["name"] for node in project.mart_nodes(manifest).values()}
        else:
            raise ValueError(f"{table}: invalid aggregate model record")
        if table not in available:
            raise ValueError(f"{table}: aggregate record is not present in its snapshot")
        nodes = _nodes_for_tables(manifest, {table})
        snapshots.update(nodes)
        if template is None:
            template = copy.deepcopy(manifest)
    if template is None:
        raise ValueError("cannot deploy an empty serving aggregate")
    template["nodes"] = snapshots
    template.setdefault("metadata", {}).pop("vintage_scope", None)
    return template


def write_scoped_upload(content_root: pathlib.Path, destination: pathlib.Path, tables: set[str]) -> set[str]:
    """Upload only the changed marts and their complete family dashboards."""
    if destination.exists():
        shutil.rmtree(destination)
    selected = {}
    for path in (content_root / "charts").glob("*.yml"):
        value = yaml.safe_load(path.read_text())
        if isinstance(value, dict) and value.get("tableName") in tables:
            selected[value.get("slug")] = path
    if not selected:
        raise ValueError("accepted marts have no captured Lightdash charts")
    for path in selected.values():
        output = destination / "charts" / path.name
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(path.read_bytes())
    for path in (content_root / "dashboards").glob("*.yml"):
        value = yaml.safe_load(path.read_text())
        tiles = value.get("tiles", []) if isinstance(value, dict) else []
        if any(
            tile.get("type") == "saved_chart"
            and tile.get("properties", {}).get("chartSlug") in selected
            for tile in tiles
        ):
            output = destination / "dashboards" / path.name
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(path.read_bytes())
    return set(selected)


def staged_chart_slugs(content_root: pathlib.Path) -> set[str]:
    return {
        value["slug"] for path in (content_root / "charts").glob("*.yml")
        if isinstance((value := yaml.safe_load(path.read_text())), dict) and isinstance(value.get("slug"), str)
    }


def write_release_state(root: pathlib.Path, state: dict) -> None:
    target = release_state_path(root)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2) + "\n")
    os.replace(temporary, target)


def sync_batch(batch_id: str, *, create: bool = False) -> dict:
    """Deploy one accepted family batch into the serialized last-good aggregate."""
    batch = immutable_batch(batch_id)
    batch_info, _ = _batch_scope(batch)
    root = publisher.state_root()
    with publisher.deployment_lock():
        serving = publisher.publication_state(batch)
        if serving["stale"] or serving["missing"]:
            raise ValueError(
                "refusing Lightdash deployment from a non-current batch: "
                + ", ".join(serving["stale"] + serving["missing"])
            )
        if set(serving["current"]) != {table["table"] for table in batch_info["tables"]}:
            raise ValueError("publication state does not cover every captured mart")
        expected_project = os.environ.get("LIGHTDASH_PROJECT")
        previous = release_state(root, expected_project, publisher.serving_model_ledger(), batch)
        candidate = copy.deepcopy(previous)
        candidate["models"].update({
            table["table"]: {"batch_id": batch_id} for table in batch_info["tables"]
        })
        aggregate = aggregate_manifest(root, candidate)
        with tempfile.TemporaryDirectory(prefix="lightdash-aggregate-", dir=root) as staged:
            staged_root = pathlib.Path(staged)
            content_root = staged_root / "content"
            content.write_content(aggregate, destination=content_root)
            release_sha256 = project.digest({
                "manifest": project.serving_manifest_digest(aggregate),
                "content": project.content_digest(content_root),
            })
            destination = root / "bundles" / release_sha256
            info = project.bundle(
                aggregate,
                destination,
                os.environ.get("LIGHTDASH_SERVING_DB", "vintage_serving"),
                content=content_root,
            )
        changed_charts = write_scoped_upload(
            destination / "content",
            destination / "scope-content",
            {table["table"] for table in batch_info["tables"]},
        )
        receipt = receipt_path(root)
        expected_config = deployment_config(expected_project, info) if expected_project else None
        if receipt.is_file():
            value = _read_json(receipt)
            if (
                value.get("status") == "ok"
                and value.get("release_sha256") == release_sha256
                and value.get("project_uuid") == expected_project
                and value.get("deployment_config") == expected_config
            ):
                return {**value, "status": "unchanged"}
        relative = destination.resolve().relative_to(root.resolve())
        deployment = subprocess.run(
            [
                str(ROOT / "visualization/bin/compose"),
                "run", "--rm", "-T",
                "--volume", f"{destination.resolve() / 'scope-content'}:/content:ro",
                "--entrypoint", "python3", "cli", "/tools/deploy.py",
                "deploy", f"/state/{relative}", "1" if create and not expected_project else "0", "1",
            ],
            check=True, capture_output=True, text=True,
        )
        if deployment.stdout:
            sys.stderr.write(deployment.stdout)
        if deployment.stderr:
            sys.stderr.write(deployment.stderr)
        identity = _read_json(destination / "deployment.json")
        from visualization.verify_queries import verify
        query_check = verify(
            os.environ["LIGHTDASH_URL"], os.environ["LIGHTDASH_API_KEY"], identity["project_uuid"],
            chart_slugs=changed_charts,
        )
        if not query_check["ok"]:
            raise ValueError(f"Lightdash query check failed: {query_check['failures']}")
        candidate["project_uuid"] = identity["project_uuid"]
        write_release_state(root, candidate)
        target = root / "deployment.json"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(identity) + "\n")
        os.replace(temporary, target)
        write_receipt(root, {
            "status": "ok",
            "batch_id": batch_id,
            "release_sha256": release_sha256,
            "project_uuid": identity["project_uuid"],
            "deployment_config": deployment_config(identity["project_uuid"], info),
            "query_check": query_check,
            "scope": batch_info["vintage_scope"],
            "models": sorted(table["table"] for table in batch_info["tables"]),
        })
        return identity

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "content", "check-analysis", "bundle", "compile", "deploy", "sync", "preview", "export", "publish", "discard-export-data", "reindex", "status", "query-check", "init-secrets", "bootstrap"])
    parser.add_argument("--manifest", type=pathlib.Path, default=ROOT / "transform/target/manifest.json")
    parser.add_argument("--results", type=pathlib.Path, default=ROOT / "transform/target/run_results.json")
    parser.add_argument("--batch")
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("paths", nargs="*", type=pathlib.Path, help="family YAML files for check-analysis")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--apply", action="store_true", help="perform the retired export removal instead of a dry run")
    parser.add_argument("--create", action="store_true", help="create a Lightdash project on first deploy")
    parser.add_argument("--email", default="admin@vintage-data.local", help="local administrator email for first bootstrap")
    args = parser.parse_args()
    if args.command == "check-analysis":
        targets = args.paths or sorted((ROOT / "transform/models/marts").glob("*/_*_models.yml"))
        results = [project.check_family(path) for path in targets]
        print(json.dumps({"ok": all(item["ok"] for item in results), "families": results}, indent=2))
        raise SystemExit(0 if all(item["ok"] for item in results) else 1)
    load_environment(secrets_required=args.command not in {"validate", "content", "bundle"})
    if args.command == "sync":
        if not args.batch:
            parser.error("sync requires --batch")
        print(json.dumps(sync_batch(args.batch, create=args.create)))
        return
    if args.command == "query-check":
        from visualization.verify_queries import verify
        result = verify(os.environ["LIGHTDASH_URL"], os.environ["LIGHTDASH_API_KEY"], os.environ["LIGHTDASH_PROJECT"])
        print(json.dumps(result, indent=2))
        raise SystemExit(0 if result["ok"] else 1)
    if args.command == "bootstrap":
        from visualization.api import bootstrap
        if os.environ.get("LIGHTDASH_API_KEY"):
            print("Deployment token already configured; keeping existing account and token.")
            return
        password = os.environ.get("LIGHTDASH_ADMIN_PASSWORD")
        if not password:
            raise ValueError("run viz init-secrets before bootstrap")
        token = bootstrap(os.environ["LIGHTDASH_URL"], args.email, password)
        path = ROOT / "orchestration/airflow.secrets.env"
        with path.open("a") as stream:
            path.chmod(0o600)
            stream.write(f"\nLIGHTDASH_API_KEY={token}\nLIGHTDASH_ADMIN_EMAIL={args.email}\n")
        print(f"Administrator {args.email} and deployment token ready. Password is in airflow.secrets.env.")
        return
    if args.command == "init-secrets":
        path = ROOT / "orchestration/airflow.secrets.env"
        current = deployment.parse_env_file(path) if path.exists() else {}
        names = ["LIGHTDASH_DB_ADMIN_PASSWORD", "LIGHTDASH_APP_PASSWORD", "LIGHTDASH_PUBLISHER_PASSWORD", "LIGHTDASH_READER_PASSWORD", "LIGHTDASH_STORAGE_PASSWORD", "LIGHTDASH_SECRET", "LIGHTDASH_ADMIN_PASSWORD"]
        additions = {name: secrets.token_hex(32) for name in names if name not in current}
        if any(not current[name] for name in names if name in current):
            raise ValueError("existing Lightdash secrets may not be empty")
        with path.open("a") as handle:
            path.chmod(0o600)
            for name, value in additions.items():
                handle.write(f"\n{name}={value}\n")
        print(f"Provisioned {len(additions)} Lightdash secrets; existing values preserved.")
        return
    if args.command == "status":
        print(json.dumps(publisher.status(), default=str, indent=2)); return
    if args.command == "reindex":
        print(json.dumps(publisher.reindex(), indent=2)); return
    if args.command == "discard-export-data":
        from transform_runner import _default_lock_path, exclusive_lock
        root = publisher.state_root()
        # Transform, then deployment, then the retired capture lock: an old
        # exporter still holding any of them must drain before data is removed.
        with exclusive_lock(_default_lock_path(), 300), publisher.deployment_lock(root), publisher.legacy_capture_lock(root):
            result = publisher.discard_export_data(root, apply=args.apply)
        print(json.dumps(result, indent=2))
        raise SystemExit(0 if result["ok"] else 1)
    if args.command == "export":
        destination = args.output or publisher.state_root() / "exports" / secrets.token_hex(8)
        relative = destination.resolve().relative_to(publisher.state_root().resolve())
        subprocess.run([str(ROOT / "visualization/bin/compose"), "run", "--rm", "-T", "cli", "download", "--path", f"/state/{relative}"], check=True)
        print(f"Exported to {destination}; review the diff against transform/lightdash before adopting edits.")
        return
    source_content = project.CONTENT
    manifest = json.loads(args.manifest.read_text())
    if args.command == "publish":
        results = json.loads(args.results.read_text())
        _, accepted = publisher.accepted_marts(manifest, results)
        if not accepted:
            raise ValueError("build produced no successful, fully verified marts for its vintage scope")
        expected = content.render_marts(manifest, accepted)
        from transform_runner import _default_lock_path, exclusive_lock
        with tempfile.TemporaryDirectory(prefix="lightdash-content-") as staged:
            staged_root = pathlib.Path(staged)
            content.write_rendered(expected, staged_root)
            scoped = project.manifest_for_marts(manifest, accepted)
            report = project.coverage(scoped, staged_root)
            report["errors"].extend(content.validate_schemas(staged_root, files=set(expected)))
            if not report["ok"] or report["errors"]:
                raise ValueError("content coverage or schema validation failed for published marts")
            # These run results must describe the build currently in the
            # warehouse; a manifest on disk alone proves nothing about it.
            with exclusive_lock(_default_lock_path(), 300), publisher.deployment_lock():
                result = publisher.publish(
                    manifest, results, pathlib.Path(os.environ["EXTRACT_WAREHOUSE"]), content_root=staged_root
                )
        print(json.dumps(result)); return
    if args.command == "content":
        changes = content.write_content(manifest, check=args.check)
        print(json.dumps({"changed": changes}))
        if changes and args.check:
            raise SystemExit(1)
        return
    if args.command == "deploy":
        changes = content.write_content(manifest, check=True, destination=source_content)
        if changes:
            raise ValueError(f"reviewed Lightdash content is stale: {changes}")
    report = project.coverage(manifest, source_content)
    report["errors"].extend(content.validate_schemas(source_content))
    report["ok"] = report["ok"] and not report["errors"]
    if args.command == "validate":
        print(json.dumps(report if args.json else {k: v for k, v in report.items() if k != "models"}, indent=2))
        raise SystemExit(0 if report["ok"] else 1)
    if not report["ok"]:
        raise ValueError("content coverage or schema validation failed; run viz validate --json")
    release_sha256 = project.digest({
        "manifest": project.serving_manifest_digest(manifest),
        "content": project.content_digest(source_content),
    })
    destination = args.output or publisher.state_root() / "bundles" / release_sha256
    info = project.bundle(manifest, destination, os.environ.get("LIGHTDASH_SERVING_DB", "vintage_serving"), content=source_content)
    if args.command == "bundle":
        print(json.dumps({**info, "path": str(destination)}, indent=2)); return
    relative = destination.resolve().relative_to(publisher.state_root().resolve())
    container_path = f"/state/{relative}"
    base = [str(ROOT / "visualization/bin/compose"), "run", "--rm", "-T", "--volume", f"{destination.resolve() / 'content'}:/content:ro", "cli"]
    common = ["--project-dir", container_path, "--profiles-dir", container_path, "--skip-dbt-compile", "--no-partial-compilation"]
    if args.command == "compile":
        subprocess.run(base + ["compile", *common, "--skip-warehouse-catalog"], check=True)
        return
    with publisher.deployment_lock():
        create = args.create and not os.environ.get("LIGHTDASH_PROJECT")
        try:
            deployment = subprocess.run(
                [
                    str(ROOT / "visualization/bin/compose"),
                    "run",
                    "--rm",
                    "--volume",
                    f"{destination.resolve() / 'content'}:/content:ro",
                    "--entrypoint",
                    "python3",
                    "cli",
                    "/tools/deploy.py",
                    args.command,
                    container_path,
                    "1" if create else "0",
                    "0",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            if exc.stdout:
                sys.stderr.write(exc.stdout)
            if exc.stderr:
                sys.stderr.write(exc.stderr)
            raise
        if deployment.stdout:
            sys.stderr.write(deployment.stdout)
        if deployment.stderr:
            sys.stderr.write(deployment.stderr)
        identity = json.loads((destination / "deployment.json").read_text())
        if args.command == "deploy":
            target = publisher.state_root() / "deployment.json"
            temporary = target.with_suffix(".tmp")
            temporary.write_text(json.dumps(identity) + "\n")
            os.replace(temporary, target)
        print(json.dumps(identity))

if __name__ == "__main__":
    main()
