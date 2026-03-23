#!/usr/bin/env bash
set -euo pipefail

# Apply runtime profiles via systemd user drop-in.
# Usage:
#   ./scripts/apply_runtime_profile.sh stable-2h
#   ./scripts/apply_runtime_profile.sh fast-30m-lowapi
#   ./scripts/apply_runtime_profile.sh clear

UNIT="polymarket-weather-monitor.service"
DROPIN_DIR="$HOME/.config/systemd/user/${UNIT}.d"
DROPIN_FILE="$DROPIN_DIR/90-runtime-profile.conf"

profile="${1:-stable-2h}"
mkdir -p "$DROPIN_DIR"

case "$profile" in
  stable-2h)
    cat > "$DROPIN_FILE" <<'EOF'
[Service]
Environment=INTERVAL_SEC=7200
Environment=RUN_TIMEOUT_SEC=1200
Environment=FORECAST_ENSEMBLE_MODELS=gfs_seamless,ecmwf_ifs025,icon_seamless
Environment=MIN_MODEL_FAMILY_COUNT=2
Environment=MODEL_DISCREPANCY_MAX=0.25
Environment=MAX_COORD_DATES_PER_TICK=30
Environment=EXTERNAL_NO_CONSENSUS_GATE_ENABLED=1
Environment=EXTERNAL_NO_CONSENSUS_MARGIN_C=0.0
EOF
    ;;

  fast-30m-lowapi)
    cat > "$DROPIN_FILE" <<'EOF'
[Service]
Environment=INTERVAL_SEC=1800
Environment=RUN_TIMEOUT_SEC=900
Environment=FORECAST_ENSEMBLE_MODELS=gfs_seamless
Environment=MIN_MODEL_FAMILY_COUNT=1
Environment=MODEL_DISCREPANCY_MAX=0.40
Environment=MAX_COORD_DATES_PER_TICK=24
Environment=EXTERNAL_NO_CONSENSUS_GATE_ENABLED=1
Environment=EXTERNAL_NO_CONSENSUS_MARGIN_C=0.0
EOF
    ;;

  clear)
    rm -f "$DROPIN_FILE"
    ;;

  *)
    echo "Unknown profile: $profile"
    echo "Use: stable-2h | fast-30m-lowapi | clear"
    exit 1
    ;;
esac

systemctl --user daemon-reload
systemctl --user restart "$UNIT"

echo "Applied profile: $profile"
systemctl --user status "$UNIT" --no-pager -n 6
