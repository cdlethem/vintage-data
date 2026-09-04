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

if [[ "$PUBLISH_TAILSCALE" != 1 ]]; then
    echo "PUBLISH_TAILSCALE=0: leaving the UI on 127.0.0.1:${AIRFLOW_API_PORT}"
    exit 0
fi

command -v tailscale >/dev/null || {
    echo "PUBLISH_TAILSCALE=1 but tailscale is not installed" >&2
    exit 1
}

sudo tailscale serve --bg "$AIRFLOW_API_PORT"
tailscale serve status
