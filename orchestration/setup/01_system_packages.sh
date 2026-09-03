#!/usr/bin/env bash
# Postgres (Airflow metadata DB) and Redis (Celery broker). Ubuntu's defaults
# already bind both to localhost only; no config edits needed.
set -euo pipefail

sudo apt-get install -y postgresql redis-server
sudo systemctl enable --now postgresql redis-server
