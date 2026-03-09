#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT_DIR/state"
PID_FILE="$STATE_DIR/monitor.pid"
LOG_FILE="$STATE_DIR/monitor.log"

INTERVAL_SEC="${INTERVAL_SEC:-300}"
MIN_HOURS_TO_EXPIRY="${MIN_HOURS_TO_EXPIRY:-0}"
MAX_POSITIONS_PER_CITY="${MAX_POSITIONS_PER_CITY:-2}"
EXIT_EDGE_FLOOR="${EXIT_EDGE_FLOOR:-0.01}"
MIN_HOLDING_MINUTES_FOR_EDGE_EXIT="${MIN_HOLDING_MINUTES_FOR_EDGE_EXIT:-10}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$STATE_DIR"

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
        --exit-edge-floor '$EXIT_EDGE_FLOOR' \
        --min-holding-minutes-for-edge-exit '$MIN_HOLDING_MINUTES_FOR_EDGE_EXIT' \
        >> '$LOG_FILE' 2>&1 || true
      sleep '$INTERVAL_SEC'
    done
  " >/dev/null 2>&1 &

  echo $! > "$PID_FILE"
  echo "monitor started (pid=$!, interval=${INTERVAL_SEC}s, min_hours_to_expiry=${MIN_HOURS_TO_EXPIRY})"
}

cmd_stop() {
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
  if is_running; then
    echo "monitor: running (pid=$(cat "$PID_FILE"), interval=${INTERVAL_SEC}s, min_hours_to_expiry=${MIN_HOURS_TO_EXPIRY}, max_positions_per_city=${MAX_POSITIONS_PER_CITY}, exit_edge_floor=${EXIT_EDGE_FLOOR})"
  else
    echo "monitor: stopped"
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
    --exit-edge-floor "$EXIT_EDGE_FLOOR" \
    --min-holding-minutes-for-edge-exit "$MIN_HOLDING_MINUTES_FOR_EDGE_EXIT"
}

cmd_logs() {
  local n="${2:-120}"
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
  INTERVAL_SEC=<seconds>                           # default 300
  MIN_HOURS_TO_EXPIRY=<hours>                      # default 0 (paper can still open near expiry)
  MAX_POSITIONS_PER_CITY=<int>                     # default 2
  EXIT_EDGE_FLOOR=<float>                          # default 0.01
  MIN_HOLDING_MINUTES_FOR_EDGE_EXIT=<int>          # default 10
  PYTHON_BIN=<python executable>                   # default python3
USAGE
    exit 1
    ;;
esac
