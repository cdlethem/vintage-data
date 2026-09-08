"""Trusted Airflow subprocess interface; capture caller must hold dbt.lock."""
import json
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "orchestration/include"))
import deployment
from visualization.publisher import capture, publish, state_root

deployment.load_env()
if sys.argv[1] == "capture":
    manifest = pathlib.Path(sys.argv[2])
    print(json.dumps(capture(json.loads(manifest.read_text()), json.loads(manifest.with_name("run_results.json").read_text()), pathlib.Path(os.environ["EXTRACT_WAREHOUSE"]))))
elif sys.argv[1] == "publish":
    import re
    deployment.load_env(ROOT / "orchestration/airflow.secrets.env")
    batch_id = sys.argv[2]
    if not re.fullmatch("[a-f0-9]{64}", batch_id):
        raise ValueError("invalid batch identity")
    print(json.dumps(publish(state_root() / "batches" / batch_id)))
else:
    raise SystemExit("expected capture or publish")
