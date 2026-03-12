#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT_DIR/state"
PID_FILE="$STATE_DIR/monitor.pid"
LOG_FILE="$STATE_DIR/monitor.log"

INTERVAL_SEC="${INTERVAL_SEC:-180}"
MIN_HOURS_TO_EXPIRY="${MIN_HOURS_TO_EXPIRY:-0}"
MAX_POSITIONS_PER_CITY="${MAX_POSITIONS_PER_CITY:-2}"
MAX_EVENT_CLUSTER_EXPOSURE_USD="${MAX_EVENT_CLUSTER_EXPOSURE_USD:-100}"
EXIT_EDGE_FLOOR="${EXIT_EDGE_FLOOR:-0.01}"
MIN_HOLDING_MINUTES_FOR_EDGE_EXIT="${MIN_HOLDING_MINUTES_FOR_EDGE_EXIT:-10}"
CONFIRM_TICKS="${CONFIRM_TICKS:-2}"
TRADE_SIZE_USD="${TRADE_SIZE_USD:-100}"
MAX_OPEN_EXPOSURE_USD="${MAX_OPEN_EXPOSURE_USD:-250}"
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
ROBUSTNESS_MU_SHIFT_C="${ROBUSTNESS_MU_SHIFT_C:-0.7}"
ROBUSTNESS_SIGMA_SCALE_LOW="${ROBUSTNESS_SIGMA_SCALE_LOW:-0.85}"
ROBUSTNESS_SIGMA_SCALE_HIGH="${ROBUSTNESS_SIGMA_SCALE_HIGH:-1.15}"
ROBUSTNESS_MIN_EDGE="${ROBUSTNESS_MIN_EDGE:-0.0}"
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
COMPOUND_MAX_OPEN_EXPOSURE_MIN_USD="${COMPOUND_MAX_OPEN_EXPOSURE_MIN_USD:-250}"
COMPOUND_MAX_OPEN_EXPOSURE_MAX_USD="${COMPOUND_MAX_OPEN_EXPOSURE_MAX_USD:-300}"
COMPOUND_DAILY_STOP_LOSS_MIN_ABS_USD="${COMPOUND_DAILY_STOP_LOSS_MIN_ABS_USD:-50}"
COMPOUND_DAILY_STOP_LOSS_MAX_ABS_USD="${COMPOUND_DAILY_STOP_LOSS_MAX_ABS_USD:-50}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SYSTEMD_UNIT="${SYSTEMD_UNIT:-polymarket-weather-monitor.service}"

mkdir -p "$STATE_DIR"

has_systemd_unit() {
  command -v systemctl >/dev/null 2>&1 || return 1
  systemctl --user status "$SYSTEMD_UNIT" >/dev/null 2>&1
}

systemd_is_active() {
  command -v systemctl >/dev/null 2>&1 || return 1
  systemctl --user is-active --quiet "$SYSTEMD_UNIT"
}

is_running() {
  if [[ ! -f "$PID_FILE" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  if ps -p "$pid" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

cmd_start() {
  if has_systemd_unit; then
    systemctl --user start "$SYSTEMD_UNIT"
    echo "monitor started via systemd ($SYSTEMD_UNIT)"
    return 0
  fi

  if is_running; then
    echo "monitor already running (pid=$(cat "$PID_FILE"))"
    return 0
  fi

  nohup bash -lc "
    cd '$ROOT_DIR'
    while true; do
      echo \"[$(date '+%Y-%m-%d %H:%M:%S %z')] tick\" >> '$LOG_FILE'
      '$PYTHON_BIN' '$ROOT_DIR/paper_runner.py' --apply \
        --min-hours-to-expiry '$MIN_HOURS_TO_EXPIRY' \
        --max-positions-per-city '$MAX_POSITIONS_PER_CITY' \
        --max-event-cluster-exposure-usd '$MAX_EVENT_CLUSTER_EXPOSURE_USD' \
        --exit-edge-floor '$EXIT_EDGE_FLOOR' \
        --min-holding-minutes-for-edge-exit '$MIN_HOLDING_MINUTES_FOR_EDGE_EXIT' \
        --confirm-ticks '$CONFIRM_TICKS' \
        --trade-size-usd '$TRADE_SIZE_USD' \
        --max-open-exposure-usd '$MAX_OPEN_EXPOSURE_USD' \
        --daily-stop-loss-usd '$DAILY_STOP_LOSS_USD' \
        --daily-new-open-notional-cap-usd '$DAILY_NEW_OPEN_NOTIONAL_CAP_USD' \
        --paper-bankroll-usd '$PAPER_BANKROLL_USD' \
        --kelly-fraction-core '$KELLY_FRACTION_CORE' \
        --kelly-fraction-tail '$KELLY_FRACTION_TAIL' \
        --max-bet-fraction '$MAX_BET_FRACTION' \
        --tail-size-cap-fraction '$TAIL_SIZE_CAP_FRACTION' \
        --min-edge-for-entry '$MIN_EDGE_FOR_ENTRY' \
        --entry-spread-penalty-mult '$ENTRY_SPREAD_PENALTY_MULT' \
        --min-open-size-usd '$MIN_OPEN_SIZE_USD' \
        --max-entry-participation '$MAX_ENTRY_PARTICIPATION' \
        --robustness-mu-shift-c '$ROBUSTNESS_MU_SHIFT_C' \
        --robustness-sigma-scale-low '$ROBUSTNESS_SIGMA_SCALE_LOW' \
        --robustness-sigma-scale-high '$ROBUSTNESS_SIGMA_SCALE_HIGH' \
        --robustness-min-edge '$ROBUSTNESS_MIN_EDGE' \
        --enable-edge-rotation '$ENABLE_EDGE_ROTATION' \
        --rotation-min-edge-delta '$ROTATION_MIN_EDGE_DELTA' \
        --rotation-min-ev-per-usd-delta '$ROTATION_MIN_EV_PER_USD_DELTA' \
        --rotation-min-holding-minutes '$ROTATION_MIN_HOLDING_MINUTES' \
        --max-rotations-per-run '$MAX_ROTATIONS_PER_RUN' \
        --rotation-require-profit '$ROTATION_REQUIRE_PROFIT' \
        --compound-enabled '$COMPOUND_ENABLED' \
        --compound-trade-size-fraction '$COMPOUND_TRADE_SIZE_FRACTION' \
        --compound-max-open-exposure-fraction '$COMPOUND_MAX_OPEN_EXPOSURE_FRACTION' \
        --compound-daily-stop-loss-fraction '$COMPOUND_DAILY_STOP_LOSS_FRACTION' \
        --compound-trade-size-min-usd '$COMPOUND_TRADE_SIZE_MIN_USD' \
        --compound-trade-size-max-usd '$COMPOUND_TRADE_SIZE_MAX_USD' \
        --compound-max-open-exposure-min-usd '$COMPOUND_MAX_OPEN_EXPOSURE_MIN_USD' \
        --compound-max-open-exposure-max-usd '$COMPOUND_MAX_OPEN_EXPOSURE_MAX_USD' \
        --compound-daily-stop-loss-min-abs-usd '$COMPOUND_DAILY_STOP_LOSS_MIN_ABS_USD' \
        --compound-daily-stop-loss-max-abs-usd '$COMPOUND_DAILY_STOP_LOSS_MAX_ABS_USD' \
        >> '$LOG_FILE' 2>&1 || true
      sleep '$INTERVAL_SEC'
    done
  " >/dev/null 2>&1 &

  echo $! > "$PID_FILE"
  echo "monitor started (pid=$!, interval=${INTERVAL_SEC}s, min_hours_to_expiry=${MIN_HOURS_TO_EXPIRY})"
}

cmd_stop() {
  if has_systemd_unit; then
    systemctl --user stop "$SYSTEMD_UNIT"
    echo "monitor stopped via systemd ($SYSTEMD_UNIT)"
    return 0
  fi

  if ! is_running; then
    rm -f "$PID_FILE"
    echo "monitor not running"
    return 0
  fi
  local pid
  pid="$(cat "$PID_FILE")"
  kill "$pid" >/dev/null 2>&1 || true
  rm -f "$PID_FILE"
  echo "monitor stopped (pid=$pid)"
}

cmd_status() {
  if has_systemd_unit; then
    if systemd_is_active; then
      echo "monitor: running via systemd ($SYSTEMD_UNIT)"
    else
      echo "monitor: stopped via systemd ($SYSTEMD_UNIT)"
    fi
  else
    if is_running; then
      echo "monitor: running (pid=$(cat "$PID_FILE"), interval=${INTERVAL_SEC}s, min_hours_to_expiry=${MIN_HOURS_TO_EXPIRY}, max_positions_per_city=${MAX_POSITIONS_PER_CITY}, max_event_cluster_exposure_usd=${MAX_EVENT_CLUSTER_EXPOSURE_USD}, trade_size_usd=${TRADE_SIZE_USD}, max_open_exposure_usd=${MAX_OPEN_EXPOSURE_USD}, daily_stop_loss_usd=${DAILY_STOP_LOSS_USD}, daily_new_open_notional_cap_usd=${DAILY_NEW_OPEN_NOTIONAL_CAP_USD}, min_open_size_usd=${MIN_OPEN_SIZE_USD}, max_entry_participation=${MAX_ENTRY_PARTICIPATION}, min_edge_for_entry=${MIN_EDGE_FOR_ENTRY}, entry_spread_penalty_mult=${ENTRY_SPREAD_PENALTY_MULT}, exit_edge_floor=${EXIT_EDGE_FLOOR}, confirm_ticks=${CONFIRM_TICKS}, kelly_core=${KELLY_FRACTION_CORE}, kelly_tail=${KELLY_FRACTION_TAIL}, tail_size_cap_fraction=${TAIL_SIZE_CAP_FRACTION}, robustness_mu_shift_c=${ROBUSTNESS_MU_SHIFT_C}, robustness_sigma_low=${ROBUSTNESS_SIGMA_SCALE_LOW}, robustness_sigma_high=${ROBUSTNESS_SIGMA_SCALE_HIGH}, robustness_min_edge=${ROBUSTNESS_MIN_EDGE}, edge_rotation=${ENABLE_EDGE_ROTATION}, compound_enabled=${COMPOUND_ENABLED})"
    else
      echo "monitor: stopped"
    fi
  fi

  if [[ -f "$ROOT_DIR/state/paper_state.json" ]]; then
    "$PYTHON_BIN" - "$ROOT_DIR/state/paper_state.json" <<'PY'
import json
import sys
from pathlib import Path

p = Path(sys.argv[1])
if p.exists():
    d = json.loads(p.read_text())
    print("last_run:", d.get("last_run"))
    print("open_positions:", len(d.get("open_positions", [])))
PY
  fi
}

cmd_run_once() {
  cd "$ROOT_DIR"
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
    --robustness-mu-shift-c "$ROBUSTNESS_MU_SHIFT_C" \
    --robustness-sigma-scale-low "$ROBUSTNESS_SIGMA_SCALE_LOW" \
    --robustness-sigma-scale-high "$ROBUSTNESS_SIGMA_SCALE_HIGH" \
    --robustness-min-edge "$ROBUSTNESS_MIN_EDGE" \
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
    --compound-daily-stop-loss-max-abs-usd "$COMPOUND_DAILY_STOP_LOSS_MAX_ABS_USD"
}

cmd_logs() {
  local n="${2:-120}"

  if has_systemd_unit; then
    journalctl --user -u "$SYSTEMD_UNIT" -n "$n" --no-pager
    return 0
  fi

  if [[ -f "$LOG_FILE" ]]; then
    tail -n "$n" "$LOG_FILE"
  else
    echo "no log file yet: $LOG_FILE"
  fi
}

case "${1:-}" in
  start)
    cmd_start
    ;;
  stop)
    cmd_stop
    ;;
  restart)
    cmd_stop
    cmd_start
    ;;
  status)
    cmd_status
    ;;
  run-once)
    cmd_run_once
    ;;
  logs)
    cmd_logs "$@"
    ;;
  *)
    cat <<USAGE
Usage: $(basename "$0") {start|stop|restart|status|run-once|logs [N]}

Env overrides:
  INTERVAL_SEC=<seconds>                           # default 180
  MIN_HOURS_TO_EXPIRY=<hours>                      # default 0 (paper can still open near expiry)
  MAX_POSITIONS_PER_CITY=<int>                     # default 2
  MAX_EVENT_CLUSTER_EXPOSURE_USD=<float>           # default 100
  EXIT_EDGE_FLOOR=<float>                          # default 0.01
  MIN_HOLDING_MINUTES_FOR_EDGE_EXIT=<int>          # default 10
  CONFIRM_TICKS=<int>                              # default 2
  TRADE_SIZE_USD=<float>                           # default 100
  MAX_OPEN_EXPOSURE_USD=<float>                    # default 250
  DAILY_STOP_LOSS_USD=<float>                      # default -50
  DAILY_NEW_OPEN_NOTIONAL_CAP_USD=<float>          # default 0 (<=0 disables daily turnover cap; live-exposure mode)
  PAPER_BANKROLL_USD=<float>                       # default 1000
  KELLY_FRACTION_CORE=<float>                      # default 0.20
  KELLY_FRACTION_TAIL=<float>                      # default 0.08
  MAX_BET_FRACTION=<float>                         # default 0.10
  TAIL_SIZE_CAP_FRACTION=<float>                   # default 0.5
  MIN_EDGE_FOR_ENTRY=<float>                       # default 0.10
  ENTRY_SPREAD_PENALTY_MULT=<float>                # default 1.0
  MIN_OPEN_SIZE_USD=<float>                        # default 10
  MAX_ENTRY_PARTICIPATION=<float>                  # default 0.20 (0..1)
  ROBUSTNESS_MU_SHIFT_C=<float>                    # default 0.7
  ROBUSTNESS_SIGMA_SCALE_LOW=<float>               # default 0.85
  ROBUSTNESS_SIGMA_SCALE_HIGH=<float>              # default 1.15
  ROBUSTNESS_MIN_EDGE=<float>                      # default 0.0
  ENABLE_EDGE_ROTATION=<0|1>                       # default 1
  ROTATION_MIN_EDGE_DELTA=<float>                  # default 0.05
  ROTATION_MIN_EV_PER_USD_DELTA=<float>            # default 0.08
  ROTATION_MIN_HOLDING_MINUTES=<int>               # default 10
  MAX_ROTATIONS_PER_RUN=<int>                      # default 1
  ROTATION_REQUIRE_PROFIT=<0|1>                    # default 1
  COMPOUND_ENABLED=<0|1>                           # default 1
  COMPOUND_TRADE_SIZE_FRACTION=<float>             # default 0.10
  COMPOUND_MAX_OPEN_EXPOSURE_FRACTION=<float>      # default 0.25
  COMPOUND_DAILY_STOP_LOSS_FRACTION=<float>        # default 0.03
  COMPOUND_TRADE_SIZE_MIN_USD=<float>              # default 10
  COMPOUND_TRADE_SIZE_MAX_USD=<float>              # default 100
  COMPOUND_MAX_OPEN_EXPOSURE_MIN_USD=<float>       # default 250
  COMPOUND_MAX_OPEN_EXPOSURE_MAX_USD=<float>       # default 300
  COMPOUND_DAILY_STOP_LOSS_MIN_ABS_USD=<float>     # default 50
  COMPOUND_DAILY_STOP_LOSS_MAX_ABS_USD=<float>     # default 50
  PYTHON_BIN=<python executable>                   # default python3
  SYSTEMD_UNIT=<unit name>                         # default polymarket-weather-monitor.service

If SYSTEMD_UNIT exists in user systemd, start/stop/status/logs will route through systemd.
USAGE
    exit 1
    ;;
esac
