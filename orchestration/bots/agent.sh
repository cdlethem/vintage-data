#!/usr/bin/env bash
# Usage: agent.sh <bots config> <agent command...>
# Runs the bot's agent command with this deployment's checkout and the live bots config
# mounted read-only, so an agent can change only its own worktree: not the code Airflow
# runs (an agent once followed a production path from a task log and edited the live
# checkout) and not its own limits.
set -euo pipefail
deployment=$(cd "$(dirname "$0")/../.." && pwd)
config=$1
shift
exec bwrap --dev-bind / / --ro-bind "$deployment" "$deployment" --ro-bind "$config" "$config" -- "$@"
