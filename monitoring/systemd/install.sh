#!/usr/bin/env bash
# Install the monitoring timer. Idempotent; rerun to pick up unit changes.
set -euo pipefail
cd "$(dirname "$0")"

sudo cp extract-monitor.service extract-monitor.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now extract-monitor.timer
systemctl status extract-monitor.timer --no-pager | head -4
echo
echo "Reports land in monitoring/reports/. Run one now with: sudo systemctl start extract-monitor.service"
echo "Latest report: ls -t monitoring/reports/ | head -1"
