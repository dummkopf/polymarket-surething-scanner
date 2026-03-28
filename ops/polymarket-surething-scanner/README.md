# Polymarket Sure-Thing Scanner

Surething now supports three execution modes with isolated runtime state:

- `paper` — simulated entries, legacy-compatible `state/paper_state.json`
- `shadow` — live-style gating and state isolation, but no external orders
- `live` — authenticated Polymarket execution with preflight checks, remote reconciliation, settlement tracking, and status snapshots

The scanner also applies category/risk filters before execution. By default it now excludes high-randomness narrative/show/podcast wording markets such as “What will be said on the next ... podcast?” because they behave more like long-tail, high-noise event contracts than mass-signal contracts.

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

## New live-operational pieces

### 1) Remote fills + position reconciliation
Each live cycle now:
- syncs remote open orders
- syncs recent trades/fills
- rebuilds live positions from remote Polymarket position data
- writes drift reports to `state/runtime/live/reconciliation_report.json`

### 2) Settlement / archive tracking
Each live cycle also:
- syncs `closed-positions` from Polymarket Data API
- archives resolved positions into local `closed_positions`
- tracks `settled_cash_released_usd`
- tracks `available_for_redeploy_usd`
- flags overdue items in `pending_settlements`
- writes settlement state to `state/runtime/live/settlement_report.json`

### 3) Claim hook (optional)
The official CLOB client does not currently provide a first-class redeem flow. To still support automation safely, the scanner exposes an optional shell hook:

```yaml
live:
  claim_shell_command: ""
```

If you later provide a claim script/command, Surething will call it with these env vars:
- `SURETHING_PENDING_SETTLEMENTS_JSON`
- `SURETHING_PENDING_SETTLEMENTS_COUNT`

### 4) Live status / Telegram-ready output
The scanner writes a live status snapshot to:

- `state/runtime/live/status_snapshot.json`

Render a Chinese hourly update with:

```bash
python3 reporting.py --snapshot state/runtime/live/status_snapshot.json --kind live-hourly
```

This is designed to be used by OpenClaw cron / Telegram broadcasting.

## State layout

- Shared scan outputs:
  - `state/latest_candidates.json`
  - `state/scan_metrics.json`
  - `state/dashboard.html`
- Mode-isolated trading state:
  - `state/runtime/paper/`
  - `state/runtime/shadow/`
  - `state/runtime/live/`
- Live-specific files:
  - `state/runtime/live/execution_journal.jsonl`
  - `state/runtime/live/fills_journal.jsonl`
  - `state/runtime/live/reconciliation_report.json`
  - `state/runtime/live/settlement_report.json`
  - `state/runtime/live/status_snapshot.json`
  - `state/runtime/live/notification_feed.jsonl`
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
