#!/usr/bin/env bash
# Run one sanity check: build the digest, render the prompt, ask the local
# model, save the report. Designed to be schedulable (cron/systemd timer).
#
#   run_check.sh              # full run via omp (local model CLI)
#   run_check.sh --dry-run    # print the rendered prompt, call no model
#
# Requires: python3; omp on PATH with a local model configured (same setup
# the source-discovery harness used), unless --dry-run.
set -euo pipefail
cd "$(dirname "$0")"

DIGEST=$(python3 digest.py --window-hours "${WINDOW_HOURS:-24}")
PROMPT=$(python3 - "$DIGEST" <<'EOF'
import sys, pathlib
tpl = pathlib.Path("PROMPT.md").read_text()
print(tpl.replace("{{DIGEST}}", sys.argv[1]))
EOF
)

if [[ "${1:-}" == "--dry-run" ]]; then
    echo "$PROMPT"
    exit 0
fi

# Don't queue blind behind a busy model server — a scheduled check that can't
# get a slot should fail fast and let the next cycle try. The MODEL below is
# omp's `local` provider, which (per ~/.omp/agent/models.yml, verified by
# watching /slots during a call) is the Qwen3.8-27B on :8080 — so the gate
# must poll :8080, the server that actually serves this model.
SLOTS_URL="${SLOTS_URL:-http://127.0.0.1:8080/slots}"
free=$(curl -sf -m 5 "$SLOTS_URL" | python3 -c \
    'import json,sys; print(sum(1 for s in json.load(sys.stdin) if not s.get("is_processing")))' \
    2>/dev/null || echo unknown)
if [[ "$free" == "0" ]]; then
    echo "model server has no free slots; skipping this check" >&2
    exit 3
fi

mkdir -p reports
out="reports/check_$(date -u +%Y%m%dT%H%M%SZ).md"
# MODEL=local is the Qwen3.8-27B on :8080 (NOT the 9B `fast`/:8081 model).
if ! omp -p --mode text --model "${MODEL:-local}" --thinking="${THINK:-low}" \
        --max-time "${MAXT:-10m}" --no-session --no-title "$PROMPT" < /dev/null > "$out" \
   || [[ ! -s "$out" ]]; then
    rm -f "$out"   # don't leave an empty report behind on model failure/timeout
    echo "model call failed or produced no output" >&2
    exit 4
fi
echo "report: $out"
tail -n +1 "$out"
