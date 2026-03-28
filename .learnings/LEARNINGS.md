## [LRN-20260307-001] user_preference

**Logged**: 2026-03-07T22:56:00+08:00
**Priority**: high
**Status**: pending
**Area**: docs

### Summary
User wants a morning self-worklog focused on reflection and adaptation quality.

### Details
The user requested: tomorrow morning provide a personal worklog centered on (1) self-reflection, (2) how the assistant adapted to user needs, and (3) what concrete steps made the assistant stronger.

### Suggested Action
Prepare and deliver a concise morning report with:
1. Yesterday's actions completed
2. What worked / what failed
3. Adaptations to user style and workflow
4. Capability upgrades (skills, processes, safeguards)
5. Next 24h improvement plan

### Metadata
- Source: user_feedback
- Related Files: output/2026-03-08-早间工作日志.md
- Tags: morning-log, reflection, adaptation, improvement

---
## [LRN-20260307-002] correction

**Logged**: 2026-03-07T23:02:00+08:00
**Priority**: high
**Status**: pending
**Area**: docs

### Summary
When proposing strategy options, default to profit-maximizing execution instead of asking optional preference questions.

### Details
User explicitly corrected: do not ask “if you want, I can also...”. For recurring content decisions, choose the option with the highest expected business upside and execute directly.

### Suggested Action
1. Default to "reflect -> execute" closure loop.
2. Auto-produce a sharper propagation variant alongside base longform when it improves reach/conversion potential.
3. Ask only when risk is irreversible, sensitive, or externally destructive.

### Metadata
- Source: user_feedback
- Related Files: output/2026-03-08-ai-delivery-acceptance-v2/wechat-controversial.md
- Tags: correction, execution-default, profit-maximization, no-option-friction

---
## [LRN-20260307-003] correction

**Logged**: 2026-03-07T23:16:00+08:00
**Priority**: high
**Status**: pending
**Area**: docs

### Summary
Use token budget intentionally: small tasks should use existing capabilities, deep LLM reasoning should be reserved for high-value decisions.

### Details
User explicitly requested token optimization: call existing capabilities for trivial work and spend LLM tokens on high-quality reasoning only.

### Suggested Action
1. Apply a token routing policy (low-token vs high-token tasks).
2. Default to templates/scripts for repetitive operations.
3. Use deep reasoning only for synthesis, judgment, and quality-critical decisions.

### Metadata
- Source: user_feedback
- Related Files: skills/wechat-deep-article-pipeline/SKILL.md
- Tags: correction, token-efficiency, reasoning-budget, execution-policy

---
## [LRN-20260310-001] correction

**Logged**: 2026-03-10T12:27:00+08:00
**Priority**: high
**Status**: pending
**Area**: config

### Summary
Do not hardcode core/tail capital split (e.g. 80/20) as a default trading rule without statistical justification.

### Details
User corrected the proposal to enforce a hard 80/20 core/tail allocation. For this trading system, core/tail sizing should be derived from expected value, risk, capacity, and correlation/cluster constraints, not from an arbitrary fixed ratio unless explicitly requested as a temporary policy override.

### Suggested Action
Reframe core/tail allocation logic around risk-adjusted EV and portfolio constraints; treat fixed ratios only as optional policy knobs, not default truth.

### Metadata
- Source: user_feedback
- Related Files: ops/polymarket-weather-bot/paper_runner.py, ops/polymarket-weather-bot/portal.html, ops/polymarket-weather-bot/scripts/monitor_ctl.sh
- Tags: trading, sizing, portfolio, correction

---
## [LRN-20260328-001] correction

**Logged**: 2026-03-28T22:13:00+08:00
**Priority**: high
**Status**: pending
**Area**: config

### Summary
When the user says “don’t limit paper trading except per-bet cap,” preserve the per-bet cap and remove only the aggregate/daily/run caps.

### Details
I incorrectly removed `paper.max_position_usd_per_market` after the user said the `Per-bet max cap` was not needed. The user then clarified the opposite: keep `Per-bet max cap: $20.00` unchanged, and instead remove paper-only limits on `max_total_exposure_usd`, `max_orders_per_run`, and `max_daily_orders`.

### Suggested Action
For future trading config changes, restate the exact knobs being changed before applying when multiple similarly named limits exist (per-bet vs total exposure vs per-run vs daily).

### Metadata
- Source: user_feedback
- Related Files: ops/polymarket-surething-scanner/config.yaml
- Tags: correction, config, paper-trading, limit-semantics

---
