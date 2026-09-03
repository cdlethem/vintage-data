#!/usr/bin/env bash
# Install and start the Airflow systemd units. Source of truth is
# orchestration/systemd/; rerunning re-copies and restarts cleanly.
set -euo pipefail
cd "$(dirname "$0")/../systemd"

UNITS=(airflow-api-server airflow-scheduler airflow-dag-processor "airflow-worker@1" "airflow-worker@2")

sudo cp airflow-api-server.service airflow-scheduler.service \
        airflow-dag-processor.service airflow-worker@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "${UNITS[@]}"
systemctl --no-pager --quiet is-active "${UNITS[@]}" && echo "all units active"
