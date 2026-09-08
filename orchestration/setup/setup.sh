#!/usr/bin/env bash
# Bring the whole stack up from a clean box. Every step is idempotent.
#
# Prereqs: sudo, uv (https://docs.astral.sh/uv/), Postgres and Redis reachable
# (step 01 installs them on Debian/Ubuntu), and orchestration/config.env — the
# one machine-local file. It is created from config.example.env on first run;
# review it before the run that installs services.
#
# Env: SKIP_SYSTEM_PACKAGES=1 skips step 01 on non-apt systems.
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f ../config.env ]]; then
    cp ../config.example.env ../config.env
    echo "created orchestration/config.env from config.example.env."
    echo "Review it (service user, data root, ports, model wiring), then rerun setup.sh."
    exit 0
fi

./render_config.sh

[[ "${SKIP_SYSTEM_PACKAGES:-0}" == 1 ]] || ./01_system_packages.sh
./02_database.sh
./03_airflow_venv.sh
./04_systemd.sh
./05_tailscale.sh
./06_loader.sh
./07_transform.sh
./08_lightdash.sh

echo
echo "Done. Admin password: see orchestration/airflow_home/simple_auth_manager_passwords.json.generated"
