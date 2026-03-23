from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path


def render_dashboard(candidates_path: Path, metrics_path: Path, output_path: Path) -> None:
    candidates = []
    metrics = {}
    if candidates_path.exists():
        candidates = json.loads(candidates_path.read_text(encoding="utf-8"))
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

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
