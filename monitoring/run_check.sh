#!/usr/bin/env bash
# Run one sanity check. Two-stage by design so it's cheap enough for a small
# local model on a schedule:
#   1. Deterministic triage (digest.py --triage) classifies every source.
#   2. The model is invoked ONLY on flagged (WATCH/PROBLEM) sources, to judge
#      them against their known-normal notes. A fully healthy pipeline writes
#      an OK report with no model call at all.
#
#   run_check.sh              # triage, and ask the model about flagged sources
#   run_check.sh --dry-run    # print what the model would receive, call nothing
#
# Env: WINDOW_HOURS (24), MODEL (local = Qwen3.8-27B on :8080), THINK (low),
#      MAXT (6m), SLOTS_URL (:8080/slots).
set -euo pipefail
cd "$(dirname "$0")"

TRIAGE=$(python3 digest.py --triage --window-hours "${WINDOW_HOURS:-24}")
STATUS=$(printf '%s\n' "$TRIAGE" | sed -n 's/^STATUS: \([A-Z]*\).*/\1/p')

mkdir -p reports
out="reports/check_$(date -u +%Y%m%dT%H%M%SZ).md"

# Healthy: no model needed — the triage line is the whole report.
if [[ "$STATUS" == "OK" ]]; then
    if [[ "${1:-}" == "--dry-run" ]]; then echo "$TRIAGE"; echo "(healthy — no model call)"; exit 0; fi
    { echo "VERDICT: OK"; echo "$TRIAGE"; echo "SUMMARY: all sources healthy; no flags this cycle."; } > "$out"
    echo "report: $out"; cat "$out"; exit 0
fi

PROMPT=$(python3 - "$TRIAGE" <<'EOF'
import sys, pathlib
print(pathlib.Path("PROMPT.md").read_text().replace("{{DIGEST}}", sys.argv[1]))
EOF
)

if [[ "${1:-}" == "--dry-run" ]]; then echo "$PROMPT"; exit 0; fi

# The MODEL below is omp's `local` provider = Qwen3.8-27B on :8080 (verified in
# ~/.omp/agent/models.yml). Pre-flight that same server so a saturated model
# makes the check skip cleanly rather than queue blind.
SLOTS_URL="${SLOTS_URL:-http://127.0.0.1:8080/slots}"
free=$(curl -sf -m 5 "$SLOTS_URL" | python3 -c \
    'import json,sys; print(sum(1 for s in json.load(sys.stdin) if not s.get("is_processing")))' \
    2>/dev/null || echo unknown)
if [[ "$free" == "0" ]]; then
    echo "model server (:8080) has no free slots; skipping this check" >&2
    exit 3
fi

if ! omp -p --mode text --model "${MODEL:-local}" --thinking="${THINK:-low}" \
        --max-time "${MAXT:-6m}" --no-session --no-title "$PROMPT" < /dev/null > "$out" \
   || [[ ! -s "$out" ]]; then
    rm -f "$out"
    echo "model call failed or produced no output" >&2
    exit 4
fi
echo "report: $out"
cat "$out"
