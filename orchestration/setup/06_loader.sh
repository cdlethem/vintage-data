#!/usr/bin/env bash
# Load layer: its own venv (the warehouse driver stays out of Airflow's) and
# the single-writer service. Idempotent; rerunning re-copies and restarts.
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON_VERSION=3.13

uv venv --allow-existing --python "$PYTHON_VERSION" ../pipelines/load/.venv
uv pip install --python ../pipelines/load/.venv/bin/python \
    -r ../pipelines/load/requirements.txt

set -a
source airflow.env
set +a
mkdir -p "$(dirname "$EXTRACT_WAREHOUSE")" "$EXTRACT_LOAD_QUEUE"

sudo cp systemd/extract-loader.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now extract-loader
systemctl --no-pager --quiet is-active extract-loader && echo "extract-loader active"
