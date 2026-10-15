#!/usr/bin/env bash
# Install, remove or inspect a launchd agent that runs the CTA collector every hour.
#
#   scripts/schedule_collection_macos.sh install    # register / update (starts at the next full hour)
#   scripts/schedule_collection_macos.sh uninstall  # remove
#   scripts/schedule_collection_macos.sh status     # loaded? last exit code?
#
# Each run is a 59-minute session (python -m backend.jobs.scheduled_collection). If the Mac is
# asleep at the scheduled time, launchd runs the job once when it wakes. Runs only while logged in.
set -euo pipefail

LABEL="com.cta-transit-intelligence.collector"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
PYTHON="$REPO/.venv/bin/python"
DOMAIN="gui/$(id -u)"

case "${1:-}" in
  install)
    [[ -x "$PYTHON" ]] || { echo "Virtual environment not found at $PYTHON - complete the setup in docs/setup.md first."; exit 1; }
    [[ -f "$REPO/.env" ]] || { echo ".env not found in $REPO - add your CTA_API_KEY first."; exit 1; }
    mkdir -p "$HOME/Library/LaunchAgents" "$REPO/data/logs"
    cat > "$PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PYTHON</string><string>-m</string><string>backend.jobs.scheduled_collection</string>
    <string>--duration-minutes</string><string>59</string>
  </array>
  <key>WorkingDirectory</key><string>$REPO</string>
  <key>StartCalendarInterval</key><dict><key>Minute</key><integer>0</integer></dict>
  <key>StandardOutPath</key><string>$REPO/data/logs/launchd.out.log</string>
  <key>StandardErrorPath</key><string>$REPO/data/logs/launchd.err.log</string>
  <key>ProcessType</key><string>Background</string>
</dict>
</plist>
PLIST
    launchctl bootout "$DOMAIN" "$PLIST" 2>/dev/null || true
    launchctl bootstrap "$DOMAIN" "$PLIST"
    echo "Installed $LABEL: runs hourly at :00. Logs: $REPO/data/logs/collector.log"
    ;;
  uninstall)
    launchctl bootout "$DOMAIN" "$PLIST" 2>/dev/null || true
    rm -f "$PLIST"
    echo "Removed $LABEL."
    ;;
  status)
    launchctl print "$DOMAIN/$LABEL" 2>/dev/null | grep -E "state|last exit code|runs" || echo "$LABEL is not loaded."
    ;;
  *)
    echo "usage: $0 install|uninstall|status"; exit 64
    ;;
esac
