#!/usr/bin/env python3
"""Operator entrypoint. Production changes are explicit subcommands."""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import secrets
import subprocess
import sys
import tempfile

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "content", "check-analysis", "bundle", "compile", "deploy", "preview", "export", "capture", "publish", "reindex", "status", "query-check", "init-secrets", "bootstrap"])
    parser.add_argument("--manifest", type=pathlib.Path, default=ROOT / "transform/target/manifest.json")
    parser.add_argument("--results", type=pathlib.Path, default=ROOT / "transform/target/run_results.json")
    parser.add_argument("--batch", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("paths", nargs="*", type=pathlib.Path, help="family YAML files for check-analysis")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--create", action="store_true", help="create a Lightdash project on first deploy")
    parser.add_argument("--email", default="admin@vintage-data.local", help="local administrator email for first bootstrap")
    args = parser.parse_args()
    if args.command == "check-analysis":
        targets = args.paths or sorted((ROOT / "transform/models/marts").glob("*/_*_models.yml"))
        results = [project.check_family(path) for path in targets]
        print(json.dumps({"ok": all(item["ok"] for item in results), "families": results}, indent=2))
        raise SystemExit(0 if all(item["ok"] for item in results) else 1)
    load_environment(secrets_required=args.command not in {"validate", "content", "bundle"})
    if args.command == "query-check":
        from visualization.verify_queries import verify
        result = verify(os.environ["LIGHTDASH_URL"], os.environ["LIGHTDASH_API_KEY"], os.environ["LIGHTDASH_PROJECT"])
        print(json.dumps(result, indent=2))
        raise SystemExit(0 if result['ok'] else 1)
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
    if args.command == "publish":
        if not args.batch:
            parser.error("publish requires --batch")
        print(json.dumps(publisher.publish(args.batch), indent=2)); return
    if args.command == "export":
        # Download into scratch, never overwrite the reviewed source content.
        destination = args.output or publisher.state_root() / "exports" / secrets.token_hex(8)
        destination.mkdir(parents=True, exist_ok=False)
        relative = destination.resolve().relative_to(publisher.state_root().resolve())
        subprocess.run([str(ROOT / "visualization/bin/compose"), "run", "--rm", "-T", "cli", "download", "--path", f"/state/{relative}"], check=True)
        print(f"Exported to {destination}; review the diff against transform/lightdash before adopting edits.")
        return
    manifest = json.loads(args.manifest.read_text())
    if args.command == "capture":
        from transform_runner import exclusive_lock, _default_lock_path
        with exclusive_lock(_default_lock_path(), 300):
            result = publisher.capture(manifest, json.loads(args.results.read_text()), pathlib.Path(os.environ["EXTRACT_WAREHOUSE"]))
        print(json.dumps(result)); return
    if args.command == "content":
        changes = content.write_content(manifest, check=args.check)
        print(json.dumps({"changed": changes}))
        if changes and args.check:
            raise SystemExit(1)
        return
    report = project.coverage(manifest)
    report["errors"].extend(content.validate_schemas())
    report["ok"] = report["ok"] and not report["errors"]
    if args.command == "validate":
        print(json.dumps(report if args.json else {k:v for k,v in report.items() if k != "models"}, indent=2))
        raise SystemExit(0 if report["ok"] else 1)
    if not report["ok"]:
        raise ValueError("content coverage or schema validation failed; run viz validate --json")
    destination = args.output or publisher.state_root() / "bundles" / project.digest(manifest)
    info = project.bundle(manifest, destination, os.environ.get("LIGHTDASH_SERVING_DB", "vintage_serving"))
    if args.command == "bundle":
        print(json.dumps({**info, "path": str(destination)}, indent=2)); return
    relative = destination.resolve().relative_to(publisher.state_root().resolve())
    container_path = f"/state/{relative}"
    base = [str(ROOT / "visualization/bin/compose"), "run", "--rm", "-T", "cli"]
    common = ["--project-dir", container_path, "--profiles-dir", container_path, "--skip-dbt-compile", "--no-partial-compilation"]
    # Compile offline from declared contract types; production deploy also
    # queries the serving catalog, making missing relations a hard failure.
    if args.command == "compile":
        subprocess.run(base + ["compile", *common, "--skip-warehouse-catalog"], check=True)
    else:
        create = args.create and not os.environ.get("LIGHTDASH_PROJECT")
        subprocess.run([str(ROOT / "visualization/bin/compose"), "run", "--rm", "-T", "--entrypoint", "python3", "cli", "/tools/deploy.py", args.command, container_path, "1" if create else "0"], check=True)
        identity = json.loads((destination / "deployment.json").read_text())
        if args.command == "deploy":
            # The deployment helper runs as root inside its disposable
            # container. Replace its bind-mounted identity atomically so the
            # host operator owns subsequent updates.
            target = publisher.state_root() / "deployment.json"
            temporary = target.with_suffix(".tmp")
            temporary.write_text(json.dumps(identity) + "\n")
            os.replace(temporary, target)
        print(json.dumps(identity))


if __name__ == "__main__":
    main()
