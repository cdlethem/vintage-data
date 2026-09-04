#!/usr/bin/env bash
# Load layer: its own venv (the warehouse driver stays out of Airflow's) and
# the single-writer service. Idempotent; rerunning re-copies and restarts.
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON_VERSION=3.13
VENV_MODE=--allow-existing
if [[ "${REBUILD_VENV:-0}" == 1 ]]; then
    VENV_MODE=--clear
fi


uv venv "$VENV_MODE" --python "$PYTHON_VERSION" ../load/.venv
uv pip install --python ../load/.venv/bin/python \
    -r ../load/requirements.txt

./setup/render_config.sh

set -a
# shellcheck disable=SC1091
source airflow.env
set +a
mkdir -p "$(dirname "$EXTRACT_WAREHOUSE")" "$EXTRACT_LOAD_QUEUE"

if [[ "${INSTALL_SERVICE:-1}" == 1 ]]; then
    sudo install -m 0644 generated/systemd/extract-loader.service /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable --now extract-loader
    systemctl --no-pager --quiet is-active extract-loader && echo "extract-loader active"
fi
