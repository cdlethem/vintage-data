"""Trusted Airflow subprocess interface for streamed publication and deployment."""
import json
import os
import pathlib
import re
import tempfile
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "orchestration/include"))
import deployment
from visualization import content, project
from visualization.publisher import accepted_marts, deployment_lock, publish

deployment.load_env()
command = sys.argv[1]
if command == "publish":
    # Invoked inside the caller's transform lock, which it inherits: the
    # warehouse stays frozen from the build through the whole transfer.
    deployment.load_env(ROOT / "orchestration/airflow.secrets.env")
    manifest = pathlib.Path(sys.argv[2])
    value = json.loads(manifest.read_text())
    results = json.loads(manifest.with_name("run_results.json").read_text())
    scope, accepted = accepted_marts(value, results)
    if not accepted:
        raise ValueError("build produced no successful, fully verified marts for its vintage scope")
    expected = content.render_marts(value, accepted)
    with tempfile.TemporaryDirectory(prefix="lightdash-content-") as staged:
        staged_root = pathlib.Path(staged)
        content.write_rendered(expected, staged_root)
        scoped = project.manifest_for_marts(value, accepted)
        report = project.coverage(scoped, staged_root)
        report["errors"].extend(content.validate_schemas(staged_root, files=set(expected)))
        if not report["ok"] or report["errors"]:
            raise ValueError("content coverage or schema validation failed for published marts")
        with deployment_lock():
            print(json.dumps(publish(
                value,
                results,
                pathlib.Path(os.environ["EXTRACT_WAREHOUSE"]),
                content_root=staged_root,
            )))
elif command == "sync":
    deployment.load_env(ROOT / "orchestration/airflow.secrets.env")
    batch_id = sys.argv[2]
    if not re.fullmatch("[a-f0-9]{64}", batch_id):
        raise ValueError("invalid batch identity")
    try:
        result = subprocess.run(
            [sys.executable, str(ROOT / "visualization" / "cli.py"), "sync", "--batch", batch_id],
            cwd=ROOT,
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
    print(result.stdout, end="")
else:
    raise SystemExit("expected publish or sync")
