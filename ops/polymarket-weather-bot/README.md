# Polymarket Weather Bot (Backtest + Paper First)

Execution-first weather workflow for Polymarket.

## Safety Guardrails

- **No live orders in this module**.
- This repo section only does:
  1) weather market discovery
  2) model-vs-odds edge scan
  3) paper position bookkeeping
- Keep secrets in local env file only:
  - `/home/kai/.openclaw/credentials/polymarket.env`

## What it does now

- Pulls markets from `https://polymarket.com/climate-science/weather`
- Resolves market quotes from `gamma-api`
- Parses `highest-temperature-...` weather contracts
- Uses Open-Meteo daily max temperature forecast as model baseline
- Converts model forecast to contract probabilities
- Generates edge-ranked signals (core/tail buckets)
- Opens/updates **paper** positions under your risk constraints

## Risk Parameters (default)

- trade size: `$3`
- max open exposure: `$20`
- daily stop loss: `-$10`
- position mix target: `70% core / 30% tail`
- no new position when `<12h` to expiry

## Usage

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-weather-bot
python3 paper_runner.py --apply
```

Dry run (scan only):

```bash
python3 paper_runner.py
```

Allow paper entries even near expiry (override default 12h buffer):

```bash
python3 paper_runner.py --apply --min-hours-to-expiry 0
```

Custom files:

```bash
python3 paper_runner.py \
  --env /home/kai/.openclaw/credentials/polymarket.env \
  --state ./state/paper_state.json \
  --snapshot ./state/snapshots.jsonl \
  --apply
```

## Output files

- `state/paper_state.json`: open/closed paper positions and PnL
- `state/snapshots.jsonl`: market+model snapshots for later replay/backtest
- `state/monitor.log`: continuous monitor runtime log

## Local dashboard

Open in browser:

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-weather-bot
python3 -m http.server 8787 --bind 127.0.0.1
# then visit http://localhost:8787/portal.html
```

## Continuous monitor (paper)

Control script:

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-weather-bot
./scripts/monitor_ctl.sh start
./scripts/monitor_ctl.sh status
./scripts/monitor_ctl.sh logs 120
./scripts/monitor_ctl.sh stop
```

Default behavior:
- interval: every 300s
- apply mode: on (`--apply`)
- expiry buffer override: `min_hours_to_expiry=0` (paper can still open near expiry)

Optional env overrides when starting:

```bash
INTERVAL_SEC=120 MIN_HOURS_TO_EXPIRY=0 ./scripts/monitor_ctl.sh restart
```

## Backtest status

A robust historical backtest needs historical orderbook/quote time series at entry timestamps.
Current APIs expose current quotes reliably; this script starts snapshot capture now so replay-quality backtest can be built on top of stored data.
