#!/usr/bin/env bash
# dbt transform layer: isolated venv and local development warehouse directory.
# Idempotent; rerunning reconciles the pinned environment.
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON_VERSION=3.13
VENV_MODE=--allow-existing
if [[ "${REBUILD_VENV:-0}" == 1 ]]; then
    VENV_MODE=--clear
fi

uv venv "$VENV_MODE" --python "$PYTHON_VERSION" ../transform/.venv
uv pip install --python ../transform/.venv/bin/python \
    -r ../transform/requirements.txt

./setup/render_config.sh

set -a
# shellcheck disable=SC1091
source airflow.env
set +a

: "${DBT_DEV_WAREHOUSE:=$(realpath ../transform)/target/dev.duckdb}"
mkdir -p "$(dirname "$DBT_DEV_WAREHOUSE")"

../transform/bin/dbt --version
