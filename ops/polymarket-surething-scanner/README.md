# Polymarket Sure-Thing Scanner

Surething now supports three execution modes with isolated runtime state:

- `paper` — simulated entries, legacy-compatible `state/paper_state.json`
- `shadow` — live-style gating and state isolation, but no external orders
- `live` — authenticated Polymarket execution with preflight checks + circuit breakers

## Run

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-surething-scanner
python3 scanner.py --mode paper
python3 scanner.py --mode shadow
python3 scanner.py --mode live
```

Default mode comes from `config.yaml -> runtime.mode` unless overridden by `--mode` or `SURETHING_TRADING_MODE`.

## Safety model for live mode

Live mode refuses to trade unless all of the following pass:

1. `config.yaml -> live.enabled: true`
2. `POLYMARKET_LIVE_ENABLED=true`
3. Valid Polymarket credentials are available via `.env.live` (or environment)
4. Collateral balance is above the configured floor
5. Remote open orders are empty (default)
6. Strategy-level guardrails pass: confirm-runs, expiry buffer, depth multiple, exposure caps, daily caps, etc.

If consecutive live errors hit the configured threshold, live trading halts automatically.

## State layout

- Shared scan outputs:
  - `state/latest_candidates.json`
  - `state/scan_metrics.json`
  - `state/dashboard.html`
- Mode-isolated trading state:
  - `state/runtime/paper/`
  - `state/runtime/shadow/`
  - `state/runtime/live/`
- Legacy mirrors kept for paper mode only:
  - `state/paper_state.json`
  - `state/daily_stats.json`

## Live env template

Copy the template and fill it in:

```bash
cp .env.live.example .env.live
```

## Dashboard

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-surething-scanner/state
python3 -m http.server 8788
```

Then visit: http://localhost:8788/dashboard.html
