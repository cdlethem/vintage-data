#!/usr/bin/env bash
# Bring the whole stack up from a clean box. Every step is idempotent.
# Prereqs: sudo, uv (https://docs.astral.sh/uv/), tailscale already logged in.
set -euo pipefail
cd "$(dirname "$0")"

./01_system_packages.sh
./02_database.sh
./03_airflow_venv.sh
./04_systemd.sh
./05_tailscale.sh

echo
echo "Done. Admin password: see orchestration/airflow_home/simple_auth_manager_passwords.json.generated"
