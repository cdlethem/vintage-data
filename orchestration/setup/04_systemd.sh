#!/usr/bin/env bash
# Install and start the Airflow systemd units. Source of truth is
# orchestration/systemd/*.template, rendered into orchestration/generated/systemd/
# by setup/render_config.sh; rerunning re-renders, re-copies and restarts cleanly.
set -euo pipefail
cd "$(dirname "$0")/.."

./setup/render_config.sh

# shellcheck disable=SC1091
[[ -f config.env ]] && { set -a; source config.env; set +a; }
: "${AIRFLOW_WORKERS:=2}"

UNITS=(airflow-api-server airflow-scheduler airflow-dag-processor)
for ((i = 1; i <= AIRFLOW_WORKERS; i++)); do
    UNITS+=("airflow-worker@${i}")
done

sudo install -m 0644 \
    generated/systemd/airflow-api-server.service \
    generated/systemd/airflow-scheduler.service \
    generated/systemd/airflow-dag-processor.service \
    generated/systemd/airflow-worker@.service \
    /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "${UNITS[@]}"
systemctl --no-pager --quiet is-active "${UNITS[@]}" && echo "all units active"
