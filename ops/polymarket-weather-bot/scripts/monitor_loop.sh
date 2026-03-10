#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT_DIR/state"
LOG_FILE="$STATE_DIR/monitor.log"

INTERVAL_SEC="${INTERVAL_SEC:-300}"
MIN_HOURS_TO_EXPIRY="${MIN_HOURS_TO_EXPIRY:-0}"
MAX_POSITIONS_PER_CITY="${MAX_POSITIONS_PER_CITY:-2}"
MAX_EVENT_CLUSTER_EXPOSURE_USD="${MAX_EVENT_CLUSTER_EXPOSURE_USD:-10}"
EXIT_EDGE_FLOOR="${EXIT_EDGE_FLOOR:-0.01}"
MIN_HOLDING_MINUTES_FOR_EDGE_EXIT="${MIN_HOLDING_MINUTES_FOR_EDGE_EXIT:-10}"
CONFIRM_TICKS="${CONFIRM_TICKS:-2}"
TRADE_SIZE_USD="${TRADE_SIZE_USD:-10}"
MAX_OPEN_EXPOSURE_USD="${MAX_OPEN_EXPOSURE_USD:-120}"
DAILY_STOP_LOSS_USD="${DAILY_STOP_LOSS_USD:--30}"
PAPER_BANKROLL_USD="${PAPER_BANKROLL_USD:-1000}"
KELLY_FRACTION_CORE="${KELLY_FRACTION_CORE:-0.20}"
KELLY_FRACTION_TAIL="${KELLY_FRACTION_TAIL:-0.08}"
MAX_BET_FRACTION="${MAX_BET_FRACTION:-0.01}"
TAIL_SIZE_CAP_FRACTION="${TAIL_SIZE_CAP_FRACTION:-0.5}"
MIN_EDGE_FOR_ENTRY="${MIN_EDGE_FOR_ENTRY:-0.02}"
ROBUSTNESS_MU_SHIFT_C="${ROBUSTNESS_MU_SHIFT_C:-0.7}"
ROBUSTNESS_SIGMA_SCALE_LOW="${ROBUSTNESS_SIGMA_SCALE_LOW:-0.85}"
ROBUSTNESS_SIGMA_SCALE_HIGH="${ROBUSTNESS_SIGMA_SCALE_HIGH:-1.15}"
ROBUSTNESS_MIN_EDGE="${ROBUSTNESS_MIN_EDGE:-0.0}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$STATE_DIR"

echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] monitor_loop start (interval=${INTERVAL_SEC}s)" >> "$LOG_FILE"

while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] tick" >> "$LOG_FILE"
  "$PYTHON_BIN" "$ROOT_DIR/paper_runner.py" --apply \
    --min-hours-to-expiry "$MIN_HOURS_TO_EXPIRY" \
    --max-positions-per-city "$MAX_POSITIONS_PER_CITY" \
    --max-event-cluster-exposure-usd "$MAX_EVENT_CLUSTER_EXPOSURE_USD" \
    --exit-edge-floor "$EXIT_EDGE_FLOOR" \
    --min-holding-minutes-for-edge-exit "$MIN_HOLDING_MINUTES_FOR_EDGE_EXIT" \
    --confirm-ticks "$CONFIRM_TICKS" \
    --trade-size-usd "$TRADE_SIZE_USD" \
    --max-open-exposure-usd "$MAX_OPEN_EXPOSURE_USD" \
    --daily-stop-loss-usd "$DAILY_STOP_LOSS_USD" \
    --paper-bankroll-usd "$PAPER_BANKROLL_USD" \
    --kelly-fraction-core "$KELLY_FRACTION_CORE" \
    --kelly-fraction-tail "$KELLY_FRACTION_TAIL" \
    --max-bet-fraction "$MAX_BET_FRACTION" \
    --tail-size-cap-fraction "$TAIL_SIZE_CAP_FRACTION" \
    --min-edge-for-entry "$MIN_EDGE_FOR_ENTRY" \
    --robustness-mu-shift-c "$ROBUSTNESS_MU_SHIFT_C" \
    --robustness-sigma-scale-low "$ROBUSTNESS_SIGMA_SCALE_LOW" \
    --robustness-sigma-scale-high "$ROBUSTNESS_SIGMA_SCALE_HIGH" \
    --robustness-min-edge "$ROBUSTNESS_MIN_EDGE" \
    >> "$LOG_FILE" 2>&1 || true

  sleep "$INTERVAL_SEC"
done
