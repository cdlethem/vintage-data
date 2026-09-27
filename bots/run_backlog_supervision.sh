#!/usr/bin/env bash
set -euo pipefail
supervision_repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$supervision_repo"
set -a
source orchestration/airflow.env
source orchestration/airflow.secrets.env
set +a
exec orchestration/.venv/bin/python bots/backlog_supervisor.py "$@"
