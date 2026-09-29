#!/usr/bin/env bash
# Runs the bot's agent command with this deployment's checkout mounted read-only,
# so an agent can change only its own worktree. (An agent once followed a
# production path from a task log and edited the live checkout.)
set -euo pipefail
deployment=$(cd "$(dirname "$0")/../.." && pwd)
exec bwrap --dev-bind / / --ro-bind "$deployment" "$deployment" -- "$@"
