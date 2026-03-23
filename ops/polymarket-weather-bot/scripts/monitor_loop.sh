#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT_DIR/state"
LOG_FILE="$STATE_DIR/monitor.log"

INTERVAL_SEC="${INTERVAL_SEC:-180}"
RUN_TIMEOUT_SEC="${RUN_TIMEOUT_SEC:-840}"
MIN_HOURS_TO_EXPIRY="${MIN_HOURS_TO_EXPIRY:-0}"
MAX_POSITIONS_PER_CITY="${MAX_POSITIONS_PER_CITY:-2}"
MAX_EVENT_CLUSTER_EXPOSURE_USD="${MAX_EVENT_CLUSTER_EXPOSURE_USD:-100}"
EXIT_EDGE_FLOOR="${EXIT_EDGE_FLOOR:-0.01}"
MIN_HOLDING_MINUTES_FOR_EDGE_EXIT="${MIN_HOLDING_MINUTES_FOR_EDGE_EXIT:-10}"
CONFIRM_TICKS="${CONFIRM_TICKS:-2}"
TRADE_SIZE_USD="${TRADE_SIZE_USD:-100}"
MAX_OPEN_EXPOSURE_USD="${MAX_OPEN_EXPOSURE_USD:-500}"
DAILY_STOP_LOSS_USD="${DAILY_STOP_LOSS_USD:--50}"
DAILY_NEW_OPEN_NOTIONAL_CAP_USD="${DAILY_NEW_OPEN_NOTIONAL_CAP_USD:-0}"
PAPER_BANKROLL_USD="${PAPER_BANKROLL_USD:-1000}"
KELLY_FRACTION_CORE="${KELLY_FRACTION_CORE:-0.20}"
KELLY_FRACTION_TAIL="${KELLY_FRACTION_TAIL:-0.08}"
MAX_BET_FRACTION="${MAX_BET_FRACTION:-0.10}"
TAIL_SIZE_CAP_FRACTION="${TAIL_SIZE_CAP_FRACTION:-0.5}"
MIN_EDGE_FOR_ENTRY="${MIN_EDGE_FOR_ENTRY:-0.10}"
ENTRY_SPREAD_PENALTY_MULT="${ENTRY_SPREAD_PENALTY_MULT:-1.0}"
MIN_OPEN_SIZE_USD="${MIN_OPEN_SIZE_USD:-10}"
MAX_ENTRY_PARTICIPATION="${MAX_ENTRY_PARTICIPATION:-0.20}"
FORECAST_ENSEMBLE_MODELS="${FORECAST_ENSEMBLE_MODELS:-gfs_seamless,ecmwf_ifs025,icon_seamless}"
FORECAST_FALLBACK_MODEL="${FORECAST_FALLBACK_MODEL:-gfs_seamless}"
ENSEMBLE_PROB_SHRINK_ALPHA="${ENSEMBLE_PROB_SHRINK_ALPHA:-1.0}"
ENSEMBLE_PROB_SHRINK_BETA="${ENSEMBLE_PROB_SHRINK_BETA:-1.0}"
MIN_MODEL_FAMILY_COUNT="${MIN_MODEL_FAMILY_COUNT:-2}"
MODEL_DISCREPANCY_MAX="${MODEL_DISCREPANCY_MAX:-0.25}"
FAIL_CLOSED_ON_EMPTY_SCAN="${FAIL_CLOSED_ON_EMPTY_SCAN:-1}"
ROBUSTNESS_MU_SHIFT_C="${ROBUSTNESS_MU_SHIFT_C:-0.7}"
ROBUSTNESS_SIGMA_SCALE_LOW="${ROBUSTNESS_SIGMA_SCALE_LOW:-0.85}"
ROBUSTNESS_SIGMA_SCALE_HIGH="${ROBUSTNESS_SIGMA_SCALE_HIGH:-1.15}"
ROBUSTNESS_MIN_EDGE="${ROBUSTNESS_MIN_EDGE:-0.01}"
NO_SYNTHETIC_ASK_PENALTY="${NO_SYNTHETIC_ASK_PENALTY:-0.01}"
NO_SYNTHETIC_ASK_SPREAD_MULT="${NO_SYNTHETIC_ASK_SPREAD_MULT:-0.25}"
MAX_EDGE_SANITY="${MAX_EDGE_SANITY:-0.35}"
MAX_EFFECTIVE_NET_EDGE_SANITY="${MAX_EFFECTIVE_NET_EDGE_SANITY:-0.30}"
ENABLE_EDGE_ROTATION="${ENABLE_EDGE_ROTATION:-1}"
ROTATION_MIN_EDGE_DELTA="${ROTATION_MIN_EDGE_DELTA:-0.05}"
ROTATION_MIN_EV_PER_USD_DELTA="${ROTATION_MIN_EV_PER_USD_DELTA:-0.08}"
ROTATION_MIN_HOLDING_MINUTES="${ROTATION_MIN_HOLDING_MINUTES:-10}"
MAX_ROTATIONS_PER_RUN="${MAX_ROTATIONS_PER_RUN:-1}"
ROTATION_REQUIRE_PROFIT="${ROTATION_REQUIRE_PROFIT:-1}"
COMPOUND_ENABLED="${COMPOUND_ENABLED:-1}"
COMPOUND_TRADE_SIZE_FRACTION="${COMPOUND_TRADE_SIZE_FRACTION:-0.10}"
COMPOUND_MAX_OPEN_EXPOSURE_FRACTION="${COMPOUND_MAX_OPEN_EXPOSURE_FRACTION:-0.25}"
COMPOUND_DAILY_STOP_LOSS_FRACTION="${COMPOUND_DAILY_STOP_LOSS_FRACTION:-0.03}"
COMPOUND_TRADE_SIZE_MIN_USD="${COMPOUND_TRADE_SIZE_MIN_USD:-10}"
COMPOUND_TRADE_SIZE_MAX_USD="${COMPOUND_TRADE_SIZE_MAX_USD:-100}"
COMPOUND_MAX_OPEN_EXPOSURE_MIN_USD="${COMPOUND_MAX_OPEN_EXPOSURE_MIN_USD:-500}"
COMPOUND_MAX_OPEN_EXPOSURE_MAX_USD="${COMPOUND_MAX_OPEN_EXPOSURE_MAX_USD:-500}"
COMPOUND_DAILY_STOP_LOSS_MIN_ABS_USD="${COMPOUND_DAILY_STOP_LOSS_MIN_ABS_USD:-50}"
COMPOUND_DAILY_STOP_LOSS_MAX_ABS_USD="${COMPOUND_DAILY_STOP_LOSS_MAX_ABS_USD:-50}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$STATE_DIR"

echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] monitor_loop start (interval=${INTERVAL_SEC}s, run_timeout=${RUN_TIMEOUT_SEC}s)" >> "$LOG_FILE"

while true; do
  tick_started_epoch="$(date +%s)"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] tick start" >> "$LOG_FILE"

  set +e
  timeout "${RUN_TIMEOUT_SEC}s" "$PYTHON_BIN" "$ROOT_DIR/paper_runner.py" --apply \
    --min-hours-to-expiry "$MIN_HOURS_TO_EXPIRY" \
    --max-positions-per-city "$MAX_POSITIONS_PER_CITY" \
    --max-event-cluster-exposure-usd "$MAX_EVENT_CLUSTER_EXPOSURE_USD" \
    --exit-edge-floor "$EXIT_EDGE_FLOOR" \
    --min-holding-minutes-for-edge-exit "$MIN_HOLDING_MINUTES_FOR_EDGE_EXIT" \
    --confirm-ticks "$CONFIRM_TICKS" \
    --trade-size-usd "$TRADE_SIZE_USD" \
    --max-open-exposure-usd "$MAX_OPEN_EXPOSURE_USD" \
    --daily-stop-loss-usd "$DAILY_STOP_LOSS_USD" \
    --daily-new-open-notional-cap-usd "$DAILY_NEW_OPEN_NOTIONAL_CAP_USD" \
    --paper-bankroll-usd "$PAPER_BANKROLL_USD" \
    --kelly-fraction-core "$KELLY_FRACTION_CORE" \
    --kelly-fraction-tail "$KELLY_FRACTION_TAIL" \
    --max-bet-fraction "$MAX_BET_FRACTION" \
    --tail-size-cap-fraction "$TAIL_SIZE_CAP_FRACTION" \
    --min-edge-for-entry "$MIN_EDGE_FOR_ENTRY" \
    --entry-spread-penalty-mult "$ENTRY_SPREAD_PENALTY_MULT" \
    --min-open-size-usd "$MIN_OPEN_SIZE_USD" \
    --max-entry-participation "$MAX_ENTRY_PARTICIPATION" \
    --forecast-ensemble-models "$FORECAST_ENSEMBLE_MODELS" \
    --forecast-fallback-model "$FORECAST_FALLBACK_MODEL" \
    --ensemble-prob-shrink-alpha "$ENSEMBLE_PROB_SHRINK_ALPHA" \
    --ensemble-prob-shrink-beta "$ENSEMBLE_PROB_SHRINK_BETA" \
    --min-model-family-count "$MIN_MODEL_FAMILY_COUNT" \
    --model-discrepancy-max "$MODEL_DISCREPANCY_MAX" \
    --fail-closed-on-empty-scan "$FAIL_CLOSED_ON_EMPTY_SCAN" \
    --robustness-mu-shift-c "$ROBUSTNESS_MU_SHIFT_C" \
    --robustness-sigma-scale-low "$ROBUSTNESS_SIGMA_SCALE_LOW" \
    --robustness-sigma-scale-high "$ROBUSTNESS_SIGMA_SCALE_HIGH" \
    --robustness-min-edge "$ROBUSTNESS_MIN_EDGE" \
    --no-synthetic-ask-penalty "$NO_SYNTHETIC_ASK_PENALTY" \
    --no-synthetic-ask-spread-mult "$NO_SYNTHETIC_ASK_SPREAD_MULT" \
    --max-edge-sanity "$MAX_EDGE_SANITY" \
    --max-effective-net-edge-sanity "$MAX_EFFECTIVE_NET_EDGE_SANITY" \
    --enable-edge-rotation "$ENABLE_EDGE_ROTATION" \
    --rotation-min-edge-delta "$ROTATION_MIN_EDGE_DELTA" \
    --rotation-min-ev-per-usd-delta "$ROTATION_MIN_EV_PER_USD_DELTA" \
    --rotation-min-holding-minutes "$ROTATION_MIN_HOLDING_MINUTES" \
    --max-rotations-per-run "$MAX_ROTATIONS_PER_RUN" \
    --rotation-require-profit "$ROTATION_REQUIRE_PROFIT" \
    --compound-enabled "$COMPOUND_ENABLED" \
    --compound-trade-size-fraction "$COMPOUND_TRADE_SIZE_FRACTION" \
    --compound-max-open-exposure-fraction "$COMPOUND_MAX_OPEN_EXPOSURE_FRACTION" \
    --compound-daily-stop-loss-fraction "$COMPOUND_DAILY_STOP_LOSS_FRACTION" \
    --compound-trade-size-min-usd "$COMPOUND_TRADE_SIZE_MIN_USD" \
    --compound-trade-size-max-usd "$COMPOUND_TRADE_SIZE_MAX_USD" \
    --compound-max-open-exposure-min-usd "$COMPOUND_MAX_OPEN_EXPOSURE_MIN_USD" \
    --compound-max-open-exposure-max-usd "$COMPOUND_MAX_OPEN_EXPOSURE_MAX_USD" \
    --compound-daily-stop-loss-min-abs-usd "$COMPOUND_DAILY_STOP_LOSS_MIN_ABS_USD" \
    --compound-daily-stop-loss-max-abs-usd "$COMPOUND_DAILY_STOP_LOSS_MAX_ABS_USD" \
    >> "$LOG_FILE" 2>&1
  rc=$?
  set -e

  tick_finished_epoch="$(date +%s)"
  tick_elapsed=$((tick_finished_epoch - tick_started_epoch))

  if [ "$rc" -eq 124 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] tick end rc=124 timeout after ${tick_elapsed}s" >> "$LOG_FILE"
  else
    echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] tick end rc=${rc} elapsed=${tick_elapsed}s" >> "$LOG_FILE"
  fi

  sleep_left=$((INTERVAL_SEC - tick_elapsed))
  if [ "$sleep_left" -gt 0 ]; then
    sleep "$sleep_left"
  fi
done
