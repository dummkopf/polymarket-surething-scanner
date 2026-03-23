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

- Pulls markets natively from `gamma-api` via pagination (no HTML scraping dependency)
- Resolves market quotes from `gamma-api`
- Parses `highest-temperature-...` weather contracts
- Uses Open-Meteo daily max temperature forecast as model baseline
- Converts model forecast to contract probabilities
- Runs a robustness gate across small mean/sigma perturbations before promoting signals
- Generates edge-ranked signals (core/tail buckets)
- Opens/updates **paper** positions under your risk constraints

## Risk Parameters (default)

- trade size cap: `$10`
- max open exposure: `$120`
- daily stop loss: `-$30`
- no hard fixed core/tail capital split; allocation is driven by risk-adjusted Kelly ranking
- event cluster cap: `$10` max per city/date cluster
- tail single-position cap: `50%` of normal policy cap (guardrail, not allocation quota)
- max positions per city: `2`
- edge-decay auto-close floor: `0.01` (after min hold 10 minutes)
- signal confirmation gate: `confirm_ticks=2`
- fractional Kelly sizing:
  - `kelly_fraction_core=0.20`
  - `kelly_fraction_tail=0.08`
  - `max_bet_fraction=0.01`
  - `min_edge_for_entry=0.02`
- robustness gate defaults:
  - `robustness_mu_shift_c=0.7`
  - `robustness_sigma_scale_low=0.85`
  - `robustness_sigma_scale_high=1.15`
  - `robustness_min_edge=0.0`
- edge-rotation defaults (upgrade weaker profitable positions):
  - `enable_edge_rotation=1`
  - `rotation_min_edge_delta=0.05`
  - `rotation_min_ev_per_usd_delta=0.08`
  - `rotation_min_holding_minutes=10`
  - `max_rotations_per_run=1`
- compounding defaults (equity-based dynamic caps):
  - `compound_enabled=1`
  - `trade_size ≈ equity * 1%` (bounded by 10~25)
  - `max_open_exposure ≈ equity * 12%` (bounded by 120~300)
  - `daily_stop_loss ≈ -equity * 3%` (bounded by -30~-120)
- no new position when `<12h` to expiry (can override)

## Paper Allocation Plan (Starting Fund = $1000)

Execution-first baseline for paper phase:

- bankroll baseline: `$1000` (`--paper-bankroll-usd 1000`)
- **active risk budget**: `$120` max open exposure (`--max-open-exposure-usd 120`)
- **per-trade hard cap**: `$10` (`--trade-size-usd 10`, also bounded by Kelly + `max_bet_fraction=0.01`)
- **daily stop loss**: `-$30` (`--daily-stop-loss-usd -30`)
- no hard bucket split target in capital allocation; core/tail remains a signal taxonomy, while dollars are allocated by risk-adjusted Kelly, robustness, and TTL.
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
  --max-event-cluster-exposure-usd 10 \
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
  --tail-size-cap-fraction 0.5 \
  --min-edge-for-entry 0.02 \
  --robustness-mu-shift-c 0.7 \
  --robustness-sigma-scale-low 0.85 \
  --robustness-sigma-scale-high 1.15 \
  --robustness-min-edge 0.0
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

- `state/paper_state.json`: open/closed paper positions and PnL, including model audit fields on entry
- `state/snapshots.jsonl`: market+model snapshots for later replay/backtest
- universe discovery now comes straight from Gamma pagination, reducing webpage blind spots
- `state/monitor.log`: continuous monitor runtime log

Audit fields now include:
- forecast mean (`forecast_max_c`)
- sigma (`sigma_c`)
- bucket bounds (`bucket_lower`, `bucket_upper`)
- side price source (`side_price_source`)
- robustness ranges (`robustness_min_prob/max_prob`, `robustness_min_edge/max_edge`, `robustness_pass`)

## Local dashboard

Open in browser:

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-weather-bot
python3 -m http.server 8787 --bind 127.0.0.1
# then visit http://localhost:8787/portal.html
```

## Runtime Profiles (2h stable + 30m low-API)

Use the profile helper:

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-weather-bot
./scripts/apply_runtime_profile.sh stable-2h
# or
./scripts/apply_runtime_profile.sh fast-30m-lowapi
```

Profile meaning:
- `stable-2h`:
  - `INTERVAL_SEC=7200`
  - full 3-model ensemble
  - `MAX_COORD_DATES_PER_TICK=30`
  - designed to stay under Open-Meteo free daily quota more safely
- `fast-30m-lowapi`:
  - `INTERVAL_SEC=1800`
  - single-model ensemble (`gfs_seamless`)
  - `MIN_MODEL_FAMILY_COUNT=1`
  - `MAX_COORD_DATES_PER_TICK=24`
  - keeps external NO-consensus guard enabled while cutting calls

Clear runtime drop-in override:

```bash
./scripts/apply_runtime_profile.sh clear
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
- opening priority now follows risk-adjusted Kelly score, not a fixed core/tail slot quota
- event-cluster concentration is capped per city/date
- tail position size is capped below core policy size as a risk guardrail
- robustness gate active (`mu ± 0.7°C`, `sigma × {0.85, 1.0, 1.15}` by default)

Optional env overrides when starting:

```bash
INTERVAL_SEC=120 \
MIN_HOURS_TO_EXPIRY=0 \
MAX_POSITIONS_PER_CITY=2 \
MAX_EVENT_CLUSTER_EXPOSURE_USD=10 \
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
TAIL_SIZE_CAP_FRACTION=0.5 \
MIN_EDGE_FOR_ENTRY=0.02 \
ROBUSTNESS_MU_SHIFT_C=0.7 \
ROBUSTNESS_SIGMA_SCALE_LOW=0.85 \
ROBUSTNESS_SIGMA_SCALE_HIGH=1.15 \
ROBUSTNESS_MIN_EDGE=0.0 \
ENABLE_EDGE_ROTATION=1 \
ROTATION_MIN_EDGE_DELTA=0.05 \
ROTATION_MIN_EV_PER_USD_DELTA=0.08 \
ROTATION_MIN_HOLDING_MINUTES=10 \
MAX_ROTATIONS_PER_RUN=1 \
ROTATION_REQUIRE_PROFIT=1 \
COMPOUND_ENABLED=1 \
COMPOUND_TRADE_SIZE_FRACTION=0.01 \
COMPOUND_MAX_OPEN_EXPOSURE_FRACTION=0.12 \
COMPOUND_DAILY_STOP_LOSS_FRACTION=0.03 \
COMPOUND_TRADE_SIZE_MIN_USD=10 \
COMPOUND_TRADE_SIZE_MAX_USD=25 \
COMPOUND_MAX_OPEN_EXPOSURE_MIN_USD=120 \
COMPOUND_MAX_OPEN_EXPOSURE_MAX_USD=300 \
COMPOUND_DAILY_STOP_LOSS_MIN_ABS_USD=30 \
COMPOUND_DAILY_STOP_LOSS_MAX_ABS_USD=120 \
./scripts/monitor_ctl.sh restart
```

## Systemd auto-restart (recommended)

Use user-level systemd so paper monitor auto-starts and auto-recovers if it crashes.

Install + enable:

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-weather-bot
./scripts/install_systemd_monitor.sh
```

Check status:

```bash
systemctl --user status polymarket-weather-monitor.service
```

Control:

```bash
systemctl --user restart polymarket-weather-monitor.service
systemctl --user stop polymarket-weather-monitor.service
systemctl --user start polymarket-weather-monitor.service
```

Unit source in repo:
- `systemd/polymarket-weather-monitor.service`

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
- Reject brittle signals with a robustness gate across mean/sigma perturbations
- Store audit fields so every entry can be inspected after the fact
- Size entries with fractional Kelly + hard max-bet cap
- Upgrade-capital rotation: replace weaker profitable positions when edge/EV uplift is large enough
- Equity-linked compounding mode for next-day risk caps (bounded by safety min/max)
- Keep full replayable snapshots for pseudo-backtest and diagnostics

Kelly formula used (binary share):
- Full Kelly: `f* = (p - q) / (1 - q)`
- Applied fraction: `f = min(max_bet_fraction, max(0, f*) * kelly_fraction_{core|tail})`
- Position size: Kelly size after TTL multiplier, then capped by policy limits.

## Backtest status

A robust historical backtest needs historical orderbook/quote time series at entry timestamps.
Current APIs expose current quotes reliably; this script starts snapshot capture now so replay-quality backtest can be built on top of stored data.
