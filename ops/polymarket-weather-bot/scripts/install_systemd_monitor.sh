#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_SRC="$ROOT_DIR/systemd/polymarket-weather-monitor.service"
UNIT_DST_DIR="$HOME/.config/systemd/user"
UNIT_DST="$UNIT_DST_DIR/polymarket-weather-monitor.service"

if [[ ! -f "$UNIT_SRC" ]]; then
  echo "unit file not found: $UNIT_SRC" >&2
  exit 1
fi

mkdir -p "$UNIT_DST_DIR"
cp "$UNIT_SRC" "$UNIT_DST"

systemctl --user daemon-reload
systemctl --user enable --now polymarket-weather-monitor.service

if systemctl --user is-active --quiet polymarket-weather-monitor.service; then
  echo "installed + enabled: $UNIT_DST"
  systemctl --user --no-pager --full status polymarket-weather-monitor.service | sed -n '1,20p'
else
  echo "service failed to start: polymarket-weather-monitor.service" >&2
  systemctl --user --no-pager --full status polymarket-weather-monitor.service || true
  journalctl --user -u polymarket-weather-monitor.service -n 80 --no-pager || true
  exit 1
fi
