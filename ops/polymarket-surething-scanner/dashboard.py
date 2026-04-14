from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json(path: Path | None) -> Any:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _fmt_money(value: Any, digits: int = 2) -> str:
    try:
        return f"${float(value):,.{digits}f}"
    except Exception:
        return "NA"


def _fmt_number(value: Any, digits: int = 2) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "NA"


def _fmt_pairs(payload: dict[str, Any], limit: int = 6) -> str:
    if not isinstance(payload, dict) or not payload:
        return "NA"
    items = sorted(payload.items(), key=lambda item: (-float(item[1]), item[0]))[:limit]
    return ", ".join(f"{html.escape(str(key))}: {value}" for key, value in items)


def _fmt_thresholds(payload: dict[str, Any]) -> str:
    if not isinstance(payload, dict) or not payload:
        return "NA"
    ordered_keys = [
        "hours_ahead",
        "quick_yes_price_min",
        "quick_yes_price_max",
        "price_min",
        "price_max",
        "min_depth_usd",
        "depth_price_cap",
        "stale_disagree_threshold",
        "gamma_page_size",
        "books_chunk_size",
        "clob_pause_sec",
    ]
    pairs = []
    for key in ordered_keys:
        if key in payload:
            pairs.append(f"{html.escape(key)}={html.escape(str(payload[key]))}")
    return ", ".join(pairs) if pairs else "NA"


def render_dashboard(
    candidates_path: Path,
    metrics_path: Path,
    output_path: Path,
    trading_state_path: Path | None = None,
    daily_stats_path: Path | None = None,
    runtime_summary_path: Path | None = None,
) -> None:
    candidates = _load_json(candidates_path)
    metrics = _load_json(metrics_path)
    trading_state = _load_json(trading_state_path)
    daily_stats = _load_json(daily_stats_path)
    runtime_summary = _load_json(runtime_summary_path)

    positions = trading_state.get("positions", []) if isinstance(trading_state, dict) else []
    closed_positions = trading_state.get("closed_positions", []) if isinstance(trading_state, dict) else []
    totals = trading_state.get("totals", {}) if isinstance(trading_state, dict) else {}
    last_plan = trading_state.get("last_plan", []) if isinstance(trading_state, dict) else []
    by_day = daily_stats.get("by_day", {}) if isinstance(daily_stats, dict) else {}
    latest_day = sorted(by_day.keys())[-1] if by_day else None
    day_data = by_day.get(latest_day, {}) if latest_day else {}
    mode = runtime_summary.get("mode") or trading_state.get("mode") or "paper"
    thresholds = metrics.get("thresholds", {}) if isinstance(metrics, dict) else {}

    candidate_rows = []
    for candidate in candidates:
        event_slug = candidate.get("event_slug") or candidate.get("slug") or ""
        market_url = f"https://polymarket.com/event/{event_slug}" if event_slug else ""
        try:
            end_dt = datetime.fromisoformat(str(candidate.get("end_date", "")).replace("Z", "+00:00"))
            hours_left = max(0.0, (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0)
            hours_left_text = f"{hours_left:.1f}h"
        except Exception:
            hours_left_text = "NA"
        candidate_rows.append(
            "<tr>"
            f"<td><a href='{html.escape(market_url)}' target='_blank'>open</a></td>"
            f"<td>{html.escape(candidate.get('question', ''))}</td>"
            f"<td>{html.escape(str(candidate.get('category_tag', 'NA')))}</td>"
            f"<td>{candidate.get('best_ask', 'NA')}</td>"
            f"<td>{_fmt_number(candidate.get('volume', 'NA'))}</td>"
            f"<td>{candidate.get('depth_usd', 'NA')}</td>"
            f"<td>{hours_left_text}</td>"
            f"<td>{html.escape(candidate.get('end_date', ''))}</td>"
            f"<td>{_fmt_number(candidate.get('tick_size', 'NA'), 4)}</td>"
            f"<td>{_fmt_number(candidate.get('min_order_size', 'NA'), 5)}</td>"
            f"<td>{html.escape(str(candidate.get('neg_risk', 'NA')))}</td>"
            f"<td>{html.escape(candidate.get('resolution_source', 'NA'))}</td>"
            "</tr>"
        )
    candidate_body = "\n".join(candidate_rows) if candidate_rows else "<tr><td colspan='12'>No candidates</td></tr>"

    exposure_rows = []
    for position in sorted(positions, key=lambda item: float(item.get("size_usd", 0) or 0), reverse=True)[:10]:
        slug = position.get("event_slug") or position.get("slug") or ""
        market_url = f"https://polymarket.com/event/{slug}" if slug else ""
        exposure_rows.append(
            "<tr>"
            f"<td><a href='{html.escape(market_url)}' target='_blank'>open</a></td>"
            f"<td>{html.escape(position.get('question', ''))}</td>"
            f"<td>{_fmt_money(position.get('size_usd', 0))}</td>"
            f"<td>{float(position.get('entry_price', 0) or 0):.4f}</td>"
            f"<td>{float(position.get('last_mark', 0) or 0):.4f}</td>"
            f"<td>{_fmt_money(position.get('unrealized_pnl', 0), 4)}</td>"
            "</tr>"
        )
    exposure_body = "\n".join(exposure_rows) if exposure_rows else "<tr><td colspan='6'>No open positions</td></tr>"

    plan_rows = []
    for item in last_plan[:12]:
        color = "#6ee7b7" if item.get("action") == "open" else "#fca5a5"
        plan_rows.append(
            "<tr>"
            f"<td style='color:{color}'>{html.escape(str(item.get('action', '')))}</td>"
            f"<td>{html.escape(item.get('question', ''))}</td>"
            f"<td>{item.get('best_ask', 'NA')}</td>"
            f"<td>{_fmt_money(item.get('order_size_usd', 0))}</td>"
            f"<td>{html.escape(str(item.get('reason', 'NA')))}</td>"
            "</tr>"
        )
    plan_body = "\n".join(plan_rows) if plan_rows else "<tr><td colspan='5'>No execution plan yet</td></tr>"

    preflight = runtime_summary.get("preflight", {}) if isinstance(runtime_summary, dict) else {}
    reconciliation = runtime_summary.get("reconciliation", {}) if isinstance(runtime_summary, dict) else {}
    settlement = runtime_summary.get("settlement", {}) if isinstance(runtime_summary, dict) else {}
    pending_settlements = trading_state.get("pending_settlements", []) if isinstance(trading_state, dict) else []
    recent_fills = trading_state.get("recent_fills", []) if isinstance(trading_state, dict) else []
    now = datetime.now(timezone.utc).isoformat()
    reusable_freed_capital = float(totals.get("available_for_redeploy_usd", 0) or 0)
    blocked_today = totals.get("blocked_by_cap_this_run", 0)

    content = f"""<!doctype html>
<html><head><meta charset='utf-8'><title>Polymarket Scanner Dashboard</title>
<style>
body {{ font-family: Inter, Arial, sans-serif; padding: 20px; background: #0b1020; color: #dbe4ff; }}
.card {{ background: #121a33; border: 1px solid #1f2a4f; border-radius: 10px; padding: 12px; margin-bottom: 14px; }}
a {{ color: #8ec5ff; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ border-bottom: 1px solid #26345f; padding: 8px; text-align: left; vertical-align: top; }}
small {{ color: #9fb0db; }}
.kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 8px; }}
.kpi {{ background: #0d1530; border-radius: 8px; padding: 10px; }}
.kpi .label {{ color: #9fb0db; font-size: 12px; }}
.kpi .value {{ font-size: 20px; font-weight: 700; margin-top: 4px; }}
.note {{ color: #b9c7ee; margin-top: 8px; font-size: 12px; }}
.badge {{ display: inline-block; border-radius: 999px; padding: 2px 8px; background: #1d4ed8; color: white; font-size: 12px; }}
</style></head><body>
<h2>Polymarket Sure-Thing Scanner</h2>
<div class='card'>
<div><strong>Generated (UTC):</strong> {now}</div>
<div><strong>Execution mode:</strong> <span class='badge'>{html.escape(str(mode))}</span></div>
<div><strong>Total scanned:</strong> {metrics.get('total_scanned', 'NA')}</div>
<div><strong>Quick-pass markets:</strong> {metrics.get('quick_pass_count', 'NA')}</div>
<div><strong>Candidates:</strong> {metrics.get('candidates_count', len(candidates))}</div>
<div><strong>Confirmed candidates:</strong> {runtime_summary.get('confirmed_candidates', 'NA')}</div>
<div><strong>Planned orders this run:</strong> {runtime_summary.get('planned_orders', 'NA')}</div>
<div><strong>Executed orders this run:</strong> {runtime_summary.get('executed_orders', 'NA')}</div>
<div><strong>Book/price stale skips:</strong> {metrics.get('stale_skips', 'NA')}</div>
<div><strong>Scanner rules:</strong> shared across paper / shadow / live</div>
<div><strong>Scanner thresholds:</strong> {_fmt_thresholds(thresholds)}</div>
<div><strong>Blocked reasons:</strong> {_fmt_pairs(runtime_summary.get('blocked_reasons', {}))}</div>
<div><strong>Filter skips:</strong> crypto={metrics.get('crypto_skips', 'NA')}, stock={metrics.get('stock_skips', 'NA')}, sports={metrics.get('sports_skips', 'NA')}, commodity={metrics.get('commodity_skips', 'NA')}, narrative={metrics.get('high_randomness_narrative_skips', 'NA')}</div>
</div>
<div class='card'>
<div><strong>Execution readiness:</strong></div>
<div>Preflight status: {html.escape(str(preflight.get('status', 'NA')))}</div>
<div>Message: {html.escape(str(preflight.get('message', 'OK')))}</div>
<div>Live env enabled: {html.escape(str(preflight.get('env_live_enabled', 'NA')))}</div>
<div>Collateral balance: {_fmt_money(preflight.get('collateral_balance_usd', 'NA'))}</div>
<div>Remote open orders: {html.escape(str(reconciliation.get('remote_open_orders', preflight.get('open_orders_count', 'NA'))))}</div>
<div>Reconciliation status: {html.escape(str(reconciliation.get('status', 'NA')))}</div>
<div>Remote/live drift (local-only): {html.escape(', '.join(reconciliation.get('local_only_tokens', [])[:4]) or 'none')}</div>
<div>Remote/live drift (remote-only): {html.escape(', '.join(reconciliation.get('remote_only_tokens', [])[:4]) or 'none')}</div>
<div>Pending settlements: {html.escape(str(settlement.get('pending_count', len(pending_settlements))))}</div>
<div>Live halted: {html.escape(str(runtime_summary.get('live_halted', False)))}</div>
<div>Live halt reason: {html.escape(str(runtime_summary.get('live_halt_reason', 'NA')))}</div>
</div>
<div class='card'>
<div><strong>Capital / PnL view ({latest_day or 'NA'}):</strong></div>
<div class='kpi-grid'>
  <div class='kpi'><div class='label'>Daily net PnL</div><div class='value'>{_fmt_money(day_data.get('net_pnl_today_usd', totals.get('net_pnl_today_usd', 'NA')))}</div></div>
  <div class='kpi'><div class='label'>Daily realized gain</div><div class='value'>{_fmt_money(day_data.get('realized_pnl_today_usd', totals.get('realized_pnl_today_usd', 'NA')))}</div></div>
  <div class='kpi'><div class='label'>Historical accumulated gain</div><div class='value'>{_fmt_money(day_data.get('historical_realized_pnl_usd', totals.get('realized_pnl_total_usd', 'NA')))}</div></div>
  <div class='kpi'><div class='label'>Actual deployed now</div><div class='value'>{_fmt_money(day_data.get('deployed_now_usd', totals.get('deployed_now_usd', 'NA')))}</div></div>
  <div class='kpi'><div class='label'>Open cost basis</div><div class='value'>{_fmt_money(day_data.get('open_cost_usd', totals.get('open_cost_usd', 'NA')))}</div></div>
  <div class='kpi'><div class='label'>Historical net PnL</div><div class='value'>{_fmt_money(day_data.get('historical_net_pnl_usd', totals.get('historical_net_pnl_usd', 'NA')))}</div></div>
</div>
<div class='note'>Current execution mode is isolated under state/runtime/&lt;mode&gt;. Paper mode still mirrors the old legacy state files for compatibility.</div>
</div>
<div class='card'>
<div><strong>Execution / reuse view:</strong></div>
<div class='kpi-grid'>
  <div class='kpi'><div class='label'>Available for redeploy</div><div class='value'>{_fmt_money(reusable_freed_capital)}</div></div>
  <div class='kpi'><div class='label'>Settled cash released</div><div class='value'>{_fmt_money(totals.get('settled_cash_released_usd', 0))}</div></div>
  <div class='kpi'><div class='label'>Blocked by cap today</div><div class='value'>{blocked_today}</div></div>
  <div class='kpi'><div class='label'>Open positions</div><div class='value'>{len(positions)}</div></div>
  <div class='kpi'><div class='label'>Closed positions</div><div class='value'>{len(closed_positions)}</div></div>
  <div class='kpi'><div class='label'>Pending settlements</div><div class='value'>{len(pending_settlements)}</div></div>
  <div class='kpi'><div class='label'>Recent fills synced</div><div class='value'>{len(recent_fills)}</div></div>
  <div class='kpi'><div class='label'>Per-market cap</div><div class='value'>{_fmt_money(totals.get('max_position_usd_per_market', 0))}</div></div>
  <div class='kpi'><div class='label'>Order size</div><div class='value'>{_fmt_money(totals.get('order_size_usd', 0))}</div></div>
</div>
</div>
<div class='card'>
<div><strong>Daily stats ({latest_day or 'NA'}):</strong></div>
<div>Scans today: {day_data.get('scans', 'NA')}</div>
<div>Latest available bets: {day_data.get('latest_candidates_count', 'NA')}</div>
<div>Total available bets seen today (sum): {day_data.get('total_candidates_seen', 'NA')}</div>
<div>Unique available bets today: {day_data.get('unique_candidates_count', 'NA')}</div>
<div>Planned orders today: {day_data.get('planned_orders', 'NA')}</div>
<div>Orders placed today: {day_data.get('orders_placed', 'NA')}</div>
<div>Blocked reasons today: {_fmt_pairs(day_data.get('blocked_reasons', {}))}</div>
</div>
<div class='card'>
<div><strong>Current mode status:</strong></div>
<div>Open positions: {totals.get('positions', 'NA')}</div>
<div>Unrealized PnL: {_fmt_money(totals.get('unrealized_pnl_usd', 'NA'), 4)}</div>
<div>Equity: {_fmt_money(totals.get('equity_usd', 'NA'), 4)}</div>
<div>Orders this run: {totals.get('orders_this_run', 'NA')}</div>
<div>New markets this run: {totals.get('opened_new_this_run', 'NA')}</div>
<div>Adds to existing this run: {totals.get('added_existing_this_run', 'NA')}</div>
<div>Closed this run: {totals.get('closed_this_run', 'NA')}</div>
<div>Per-bet max cap: {_fmt_money(totals.get('max_position_usd_per_market', 'NA'))}</div>
<div>Cumulative buy notional (historical): {_fmt_money(totals.get('cumulative_buy_usd', 'NA'))}</div>
</div>
<div class='card'>
<div><strong>Top current exposure:</strong></div>
<table>
<thead><tr><th>Link</th><th>Question</th><th>Size USD</th><th>Entry</th><th>Mark</th><th>Unrealized PnL</th></tr></thead>
<tbody>{exposure_body}</tbody>
</table>
</div>
<div class='card'>
<div><strong>Latest execution plan:</strong></div>
<table>
<thead><tr><th>Action</th><th>Question</th><th>Best Ask</th><th>Order Size</th><th>Reason</th></tr></thead>
<tbody>{plan_body}</tbody>
</table>
</div>
<div class='card'>
<table>
<thead><tr><th>Link</th><th>Question</th><th>Category</th><th>Best Ask</th><th>Volume</th><th>Depth USD</th><th>ETA</th><th>End Date</th><th>Tick</th><th>Min Size</th><th>Neg Risk</th><th>Resolution Source</th></tr></thead>
<tbody>{candidate_body}</tbody>
</table>
</div>
<small>Refresh this page after each scan run.</small>
</body></html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
