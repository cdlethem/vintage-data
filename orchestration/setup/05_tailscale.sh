#!/usr/bin/env bash
# Publish the Airflow UI to the tailnet (tailnet-only; this is serve, not
# funnel). Persists across reboots; rerunning is a no-op.
set -euo pipefail

sudo tailscale serve --bg 8082
tailscale serve status
