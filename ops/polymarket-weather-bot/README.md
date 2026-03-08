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

## Backtest status

A robust historical backtest needs historical orderbook/quote time series at entry timestamps.
Current APIs expose current quotes reliably; this script starts snapshot capture now so replay-quality backtest can be built on top of stored data.
