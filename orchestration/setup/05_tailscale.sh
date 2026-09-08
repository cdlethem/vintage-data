#!/usr/bin/env bash
# Optionally publish the Airflow UI to a tailnet (tailnet-only; this is serve,
# not funnel). Off unless config.env sets PUBLISH_TAILSCALE=1, because the UI
# is otherwise reachable on loopback only, which is a safe default anywhere.
# Persists across reboots; rerunning is a no-op.
set -euo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
[[ -f config.env ]] && { set -a; source config.env; set +a; }
: "${PUBLISH_TAILSCALE:=0}"
: "${AIRFLOW_API_PORT:=8082}"
: "${LIGHTDASH_ENABLED:=0}"
: "${LIGHTDASH_PORT:=8083}"
: "${LIGHTDASH_TAILSCALE_HTTPS_PORT:=8083}"

if [[ "$PUBLISH_TAILSCALE" != 1 ]]; then
    echo "PUBLISH_TAILSCALE=0: leaving the UI on 127.0.0.1:${AIRFLOW_API_PORT}"
    exit 0
fi

command -v tailscale >/dev/null || {
    echo "PUBLISH_TAILSCALE=1 but tailscale is not installed" >&2
    exit 1
}

if ! tailscale serve --bg "$AIRFLOW_API_PORT"; then
    echo "Tailscale Serve changes require one-time operator authorization:" >&2
    echo "    sudo tailscale set --operator=$(id -un)" >&2
    echo "Then rerun orchestration/setup/05_tailscale.sh." >&2
    exit 1
fi
if [[ "$LIGHTDASH_ENABLED" == 1 ]]; then
    # Keep Lightdash on loopback and let Tailscale terminate HTTPS on a
    # separate tailnet-only port; Airflow already owns HTTPS 443.
    tailscale serve --bg --https="$LIGHTDASH_TAILSCALE_HTTPS_PORT" \
        "http://127.0.0.1:$LIGHTDASH_PORT"
fi
tailscale serve status
