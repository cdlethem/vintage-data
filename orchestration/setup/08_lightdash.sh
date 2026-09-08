#!/usr/bin/env bash
# Pinned optional BI runtime; reruns preserve credentials and persisted data.
set -euo pipefail
cd "$(dirname "$0")/.."
./setup/render_config.sh
set -a
source airflow.env
set +a
if [[ "$LIGHTDASH_ENABLED" != 1 ]]; then
    echo 'Lightdash disabled; set LIGHTDASH_ENABLED=1 in config.env to install.'
    exit 0
fi
command -v docker >/dev/null
docker compose version >/dev/null
uv venv --allow-existing --python 3.13 ../visualization/.venv
uv pip sync --python ../visualization/.venv/bin/python --require-hashes ../visualization/requirements.txt
../visualization/bin/viz init-secrets
mkdir -p "$LIGHTDASH_STATE_ROOT"
../visualization/bin/compose build cli
../visualization/bin/compose up -d --wait db object-store lightdash
if [[ "${SKIP_SYSTEMD:-0}" != 1 ]]; then
    sudo install -m 0644 generated/systemd/vintage-lightdash.service /etc/systemd/system/vintage-lightdash.service
    sudo systemctl daemon-reload
    sudo systemctl enable --now vintage-lightdash.service
fi
echo "Lightdash ready at $LIGHTDASH_URL. See visualization/README.md for first project bootstrap."
