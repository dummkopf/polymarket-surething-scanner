# Agent A Strategy Review — Polymarket Weather Bot

Date: 2026-03-09

## A. 5 Key Strategy Flaws (Current)

1. **No active edge-decay exits**
   - Position can stay open even after edge collapses.
2. **Concentration risk by city**
   - Multiple positions can unintentionally cluster on one city.
3. **Near-expiry behavior can be too aggressive**
   - Allowing entries close to expiry increases volatility and microstructure risk.
4. **Monitoring output lacked explicit close-reason audit**
   - Harder to diagnose whether close came from expiry vs strategy rule.
5. **Risk controls were mostly entry-focused**
   - Exit governance and portfolio-shape constraints were under-defined.

## B. 5 Improvements (Prioritized)

### P1 (High impact, immediate)
1. **Edge-decay auto-close rule**
   - If same-side edge < threshold for held positions, close in paper at mark.
2. **City-level diversification cap**
   - Set max concurrent positions per city.

### P2 (Medium impact)
3. **Explicit monitor parameters for exit behavior**
   - Expose edge floor + min holding minutes as runtime controls.
4. **Structured close reason tracking**
   - Persist `close_reason` / `close_edge` in closed positions.

### P3 (Next sprint)
5. **Session risk mode profiles**
   - Conservative / standard / aggressive presets for quick switching.

## C. Acceptance Criteria (DoD)

1. Runner supports:
   - `--max-positions-per-city`
   - `--exit-edge-floor`
   - `--min-holding-minutes-for-edge-exit`
2. On each `--apply` cycle:
   - Existing positions are evaluated for edge-decay exit
   - New entries respect city cap
3. State records include:
   - `mark_price`, `unrealized_pnl_usd`, `mark_updated_at`
   - closed positions have `close_reason` when edge-based close triggers
4. Summary output includes:
   - `closed_new_expiry`
   - `closed_new_edge`
5. Monitor script can configure these controls via env overrides.

## D. External Research Review (Posts + Academic)

### Practitioner observations (X / community)
- X discussion snapshots often report high win-rate weather traders, but most are survivorship-biased and promotional.
- Practical takeaway: treat social alpha as hypothesis only; never use as direct entry trigger.

Representative links:
- https://x.com/Vlad_Web3/status/2030386653408420090
- https://x.com/mahera777/status/2029877420078088277

### Academic / structural references
- Makers and Takers: The Economics of the Kalshi Prediction Market (UCD, 2025)
  - points to favorite–longshot distortions and microstructure effects.
  - https://www.ucd.ie/economics/t4media/WP2025_19.pdf
- CEPR summary on Kalshi market economics (2026)
  - supports time-to-close efficiency increase and behavioral mispricing persistence.
  - https://cepr.org/voxeu/columns/economics-kalshi-prediction-market

### Strategy implications from research
1. Require signal persistence (`confirm_ticks`) before entry.
2. Keep deterministic exits (expiry > stop/risk > edge decay).
3. Cap concentration (city-level position ceiling).
4. Track reason-coded closes and daily diagnostics for anti-overfit review.
5. Avoid headline/cherry-picked wallet copytrading as a primary signal source.
