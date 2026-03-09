# Paper Allocation Policy ($1000 Bankroll)

## Objective
Scale paper-learning speed while keeping drawdowns controlled.

## Baseline Allocation

- Starting fund (paper): **$1000**
- Active open exposure cap: **$120** (12% of bankroll)
- Per-trade hard cap: **$10**
- Daily stop-loss: **-$30**
- Mix target: **core 80% / tail 20%**

## Entry Constraints

Must pass all:
1. edge threshold
2. liquidity threshold
3. spread threshold
4. persistence gate (confirm ticks)

## Tail-Specific Constraints

- Higher edge requirement than core
- Lower Kelly fraction than core
- Never exceed 20% of active open exposure at entry

## Operational Params (paper)

```bash
MIN_HOURS_TO_EXPIRY=0
MAX_POSITIONS_PER_CITY=2
EXIT_EDGE_FLOOR=0.01
MIN_HOLDING_MINUTES_FOR_EDGE_EXIT=10
CONFIRM_TICKS=2
TRADE_SIZE_USD=10
MAX_OPEN_EXPOSURE_USD=120
DAILY_STOP_LOSS_USD=-30
PAPER_BANKROLL_USD=1000
KELLY_FRACTION_CORE=0.20
KELLY_FRACTION_TAIL=0.08
MAX_BET_FRACTION=0.01
MIN_EDGE_FOR_ENTRY=0.02
```

## Launch

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-weather-bot
./scripts/monitor_ctl.sh restart
```
