#!/usr/bin/env bash
# Airflow venv (pinned install against the official constraints) + db migrate.
# Idempotent: uv venv and pip install are no-ops when already satisfied,
# db migrate is a no-op on an up-to-date schema.
set -euo pipefail
cd "$(dirname "$0")/.."

AIRFLOW_VERSION=3.3.1
PYTHON_VERSION=3.13
VENV_MODE=--allow-existing
if [[ "${REBUILD_VENV:-0}" == 1 ]]; then
    VENV_MODE=--clear
fi


uv venv "$VENV_MODE" --python "$PYTHON_VERSION" .venv
uv pip install --python .venv/bin/python \
    "apache-airflow[celery,postgres]==${AIRFLOW_VERSION}" \
    croniter \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# airflow.env is generated; render it first so a fresh checkout can migrate.
./setup/render_config.sh
set -a
# shellcheck disable=SC1091
source airflow.env
# shellcheck disable=SC1091
source airflow.secrets.env
set +a
.venv/bin/airflow db migrate
