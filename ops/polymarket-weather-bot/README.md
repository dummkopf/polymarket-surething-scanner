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

- trade size cap: `$3`
- max open exposure: `$20`
- daily stop loss: `-$10`
- position mix target: `70% core / 30% tail`
- max positions per city: `2`
- edge-decay auto-close floor: `0.01` (after min hold 10 minutes)
- signal confirmation gate: `confirm_ticks=2`
- fractional Kelly sizing:
  - `kelly_fraction_core=0.20`
  - `kelly_fraction_tail=0.10`
  - `max_bet_fraction=0.01`
  - `min_edge_for_entry=0.01`
- no new position when `<12h` to expiry (can override)

## Paper Allocation Plan (Starting Fund = $1000)

Execution-first baseline for paper phase:

- bankroll baseline: `$1000` (`--paper-bankroll-usd 1000`)
- **active risk budget**: `$120` max open exposure (`--max-open-exposure-usd 120`)
- **per-trade hard cap**: `$10` (`--trade-size-usd 10`, also bounded by Kelly + `max_bet_fraction=0.01`)
- **daily stop loss**: `-$30` (`--daily-stop-loss-usd -30`)
- bucket split target: `80% core / 20% tail` (enforced by signal mix + stricter tail filters)
- tail activation rule: keep `tail_edge_min` high and only take tail when liquidity/spread quality is clean.

Why this shape:
- keeps most capital in reserve (only 12% active)
- allows enough sample size for learning vs. the old `$20` total cap
- still limits single-name/event damage through small per-trade caps and daily stop.

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

Tune strategy controls:

```bash
python3 paper_runner.py --apply \
  --min-hours-to-expiry 0 \
  --max-positions-per-city 2 \
  --exit-edge-floor 0.01 \
  --min-holding-minutes-for-edge-exit 10 \
  --confirm-ticks 2 \
  --trade-size-usd 10 \
  --max-open-exposure-usd 120 \
  --daily-stop-loss-usd -30 \
  --paper-bankroll-usd 1000 \
  --kelly-fraction-core 0.20 \
  --kelly-fraction-tail 0.08 \
  --max-bet-fraction 0.01 \
  --min-edge-for-entry 0.02
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
- city diversification: `max_positions_per_city=2`
- edge decay auto-close: `exit_edge_floor=0.01`
- signal persistence: `confirm_ticks=2`
- fractional Kelly sizing active (core/tail fractions + max bet cap)

Optional env overrides when starting:

```bash
INTERVAL_SEC=120 \
MIN_HOURS_TO_EXPIRY=0 \
MAX_POSITIONS_PER_CITY=2 \
EXIT_EDGE_FLOOR=0.01 \
MIN_HOLDING_MINUTES_FOR_EDGE_EXIT=10 \
CONFIRM_TICKS=2 \
TRADE_SIZE_USD=10 \
MAX_OPEN_EXPOSURE_USD=120 \
DAILY_STOP_LOSS_USD=-30 \
PAPER_BANKROLL_USD=1000 \
KELLY_FRACTION_CORE=0.20 \
KELLY_FRACTION_TAIL=0.08 \
MAX_BET_FRACTION=0.01 \
MIN_EDGE_FOR_ENTRY=0.02 \
./scripts/monitor_ctl.sh restart
```

## GitHub publish safety (no env leaks)

Before push:

- Secrets stay in `/home/kai/.openclaw/credentials/polymarket.env` (outside repo)
- `.env` / `.env.*` / key files are ignored by `.gitignore`
- Runtime files under `state/*.json` and `state/*.jsonl` are ignored
- Optional template: `.env.example`

Quick check:

```bash
git status --short
# ensure no .env / credentials / state/*.json is staged
```

## Research-informed strategy notes

We continuously refine strategy using both practitioner posts (X/community) and academic market-microstructure literature.

Applied takeaways currently implemented:
- Avoid over-reacting to one-tick dislocations via `confirm_ticks` signal persistence
- Cap concentration risk via `max_positions_per_city`
- Enforce deterministic risk exits via `edge_decay` + expiry handling
- Use city-local forecast-day alignment (`timezone=auto`) to avoid UTC date skew in weather contracts
- Use executable-side pricing for both sides (YES ask, NO ask-equivalent) instead of midpoint-only NO pricing
- Anchor forecast uncertainty (`sigma`) to actual time-to-resolution (`endDate - now`)
- Size entries with fractional Kelly + hard max-bet cap
- Keep full replayable snapshots for pseudo-backtest and diagnostics

Kelly formula used (binary share):
- Full Kelly: `f* = (p - q) / (1 - q)`
- Applied fraction: `f = min(max_bet_fraction, max(0, f*) * kelly_fraction_{core|tail})`
- Position size: Kelly size after TTL multiplier, then capped by policy limits.

## Backtest status

A robust historical backtest needs historical orderbook/quote time series at entry timestamps.
Current APIs expose current quotes reliably; this script starts snapshot capture now so replay-quality backtest can be built on top of stored data.
