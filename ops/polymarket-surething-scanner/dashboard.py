from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path


def render_dashboard(
    candidates_path: Path,
    metrics_path: Path,
    output_path: Path,
    paper_state_path: Path | None = None,
    daily_stats_path: Path | None = None,
) -> None:
    candidates = []
    metrics = {}
    paper_state = {}
    daily_stats = {}
    if candidates_path.exists():
        candidates = json.loads(candidates_path.read_text(encoding="utf-8"))
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    if paper_state_path and paper_state_path.exists():
        paper_state = json.loads(paper_state_path.read_text(encoding="utf-8"))
    if daily_stats_path and daily_stats_path.exists():
        daily_stats = json.loads(daily_stats_path.read_text(encoding="utf-8"))

    rows = []
    for c in candidates:
        market_url = f"https://polymarket.com/event/{c.get('slug', '')}" if c.get("slug") else ""
        rows.append(
            "<tr>"
            f"<td><a href='{html.escape(market_url)}' target='_blank'>open</a></td>"
            f"<td>{html.escape(c.get('question', ''))}</td>"
            f"<td>{c.get('best_ask', 'NA')}</td>"
            f"<td>{c.get('depth_usd', 'NA')}</td>"
            f"<td>{html.escape(c.get('end_date', ''))}</td>"
            f"<td>{html.escape(c.get('resolution_source', 'NA'))}</td>"
            "</tr>"
        )

    now = datetime.now(timezone.utc).isoformat()
    body_rows = "\n".join(rows) if rows else "<tr><td colspan='6'>No candidates</td></tr>"

    totals = paper_state.get("totals", {}) if isinstance(paper_state, dict) else {}
    by_day = daily_stats.get("by_day", {}) if isinstance(daily_stats, dict) else {}
    latest_day = sorted(by_day.keys())[-1] if by_day else None
    day_data = by_day.get(latest_day, {}) if latest_day else {}

    content = f"""<!doctype html>
<html><head><meta charset='utf-8'><title>Polymarket Scanner Dashboard</title>
<style>
body {{ font-family: Inter, Arial, sans-serif; padding: 20px; background: #0b1020; color: #dbe4ff; }}
.card {{ background: #121a33; border: 1px solid #1f2a4f; border-radius: 10px; padding: 12px; margin-bottom: 14px; }}
a {{ color: #8ec5ff; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ border-bottom: 1px solid #26345f; padding: 8px; text-align: left; vertical-align: top; }}
small {{ color: #9fb0db; }}
</style></head><body>
<h2>Polymarket Sure-Thing Scanner</h2>
<div class='card'>
<div><strong>Generated (UTC):</strong> {now}</div>
<div><strong>Total scanned:</strong> {metrics.get('total_scanned', 'NA')}</div>
<div><strong>Quick-pass markets:</strong> {metrics.get('quick_pass_count', 'NA')}</div>
<div><strong>Candidates:</strong> {metrics.get('candidates_count', len(candidates))}</div>
<div><strong>Book/price stale skips:</strong> {metrics.get('stale_skips', 'NA')}</div>
<div><strong>Restricted skips:</strong> {metrics.get('restricted_skips', 'NA')}</div>
<div><strong>Crypto skips:</strong> {metrics.get('crypto_skips', 'NA')}</div>
</div>
<div class='card'>
<div><strong>Daily stats ({latest_day or 'NA'}):</strong></div>
<div>Scans today: {day_data.get('scans', 'NA')}</div>
<div>Latest available bets: {day_data.get('latest_candidates_count', 'NA')}</div>
<div>Total available bets seen today (sum): {day_data.get('total_candidates_seen', 'NA')}</div>
<div>Unique available bets today: {day_data.get('unique_candidates_count', 'NA')}</div>
<div>Orders placed today ($1 each): {day_data.get('orders_placed', 'NA')}</div>
</div>
<div class='card'>
<div><strong>Paper trading PnL:</strong></div>
<div>Open positions: {totals.get('positions', 'NA')}</div>
<div>Total invested: ${totals.get('invested_usd', 'NA')}</div>
<div>Unrealized PnL: ${totals.get('unrealized_pnl_usd', 'NA')}</div>
<div>Paper equity: ${totals.get('equity_usd', 'NA')}</div>
<div>Orders this run ($1 each): {totals.get('orders_this_run', totals.get('opened_new_this_run', 'NA'))}</div>
<div>New markets this run: {totals.get('opened_new_this_run', 'NA')}</div>
<div>Adds to existing this run: {totals.get('added_existing_this_run', 'NA')}</div>
</div>
<div class='card'>
<table>
<thead><tr><th>Link</th><th>Question</th><th>Best Ask</th><th>Depth USD</th><th>End Date</th><th>Resolution Source</th></tr></thead>
<tbody>{body_rows}</tbody>
</table>
</div>
<small>Refresh this page after each scan run.</small>
</body></html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
