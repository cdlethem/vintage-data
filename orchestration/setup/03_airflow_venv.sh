#!/usr/bin/env bash
# Airflow venv (pinned install against the official constraints) + db migrate.
# Idempotent: uv venv and pip install are no-ops when already satisfied,
# db migrate is a no-op on an up-to-date schema.
set -euo pipefail
cd "$(dirname "$0")/.."

AIRFLOW_VERSION=3.3.1
PYTHON_VERSION=3.13

uv venv --allow-existing --python "$PYTHON_VERSION" .venv
uv pip install --python .venv/bin/python \
    "apache-airflow[celery,postgres]==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

set -a
source airflow.env
source airflow.secrets.env
set +a
.venv/bin/airflow db migrate
