#!/usr/bin/env bash
# Run as the Airflow service account after root-installed runtime/probe setup.
set -euo pipefail
cd "$(dirname "$0")/.."
./setup/render_config.sh
set -a
source airflow.env
source airflow.secrets.env
set +a
.venv/bin/airflow pools set bot_dashboard_executor 2 "Confined bot execution and review"
.venv/bin/python - <<'PY'
from airflow.utils.session import create_session
from airflow.providers.vintage.bot_dashboard import concurrency
with create_session() as session:
    current = concurrency.get_settings(session)
    concurrency.set_settings(session, concurrency.Settings(version=current['version'], limits=current['limits']), 'setup')
PY
mkdir -p "$HOME/.config/systemd/user"
install -m 0644 generated/systemd-user/airflow-bot-worker.service "$HOME/.config/systemd/user/airflow-bot-worker.service"
if [[ "${1:-}" == --omp-gateway ]]; then
    install -m 0644 generated/systemd-user/vintage-model-broker.service "$HOME/.config/systemd/user/vintage-model-broker.service"
    install -m 0644 generated/systemd-user/vintage-model-gateway.service "$HOME/.config/systemd/user/vintage-model-gateway.service"
fi
systemctl --user daemon-reload
if [[ "${1:-}" == --omp-gateway ]]; then
    systemctl --user enable --now vintage-model-gateway.service
fi
systemctl --user enable --now airflow-bot-worker.service
