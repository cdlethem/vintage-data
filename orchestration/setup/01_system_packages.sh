#!/usr/bin/env bash
# Postgres (Airflow metadata DB) and Redis (Celery broker). Debian/Ubuntu's
# defaults already bind both to localhost only; no config edits needed.
#
# On any other platform, install the two services yourself and skip this step:
# nothing later in setup assumes apt, only that Postgres and Redis are
# reachable at the POSTGRES_* / CELERY_BROKER_URL values in config.env.
set -euo pipefail

if ! command -v apt-get >/dev/null; then
    echo "no apt-get on this system: install postgresql and redis yourself," >&2
    echo "then rerun setup with SKIP_SYSTEM_PACKAGES=1" >&2
    exit 1
fi

sudo apt-get install -y postgresql redis-server
sudo systemctl enable --now postgresql redis-server
