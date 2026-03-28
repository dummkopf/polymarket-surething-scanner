from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

CST = timezone(timedelta(hours=8))


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        return str(value)
    except Exception:
        return default


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def money(value: Any) -> str:
    return f"${safe_float(value):.2f}"


def duration_to_human(delta_seconds: float | None) -> str:
    if delta_seconds is None:
        return "NA"
    total = int(max(0, delta_seconds))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return "".join(parts)


def render_live_hourly(snapshot: dict[str, Any], max_positions: int = 10) -> str:
    generated = iso_to_dt(safe_str(snapshot.get("generated_at"))) or datetime.now(timezone.utc)
    totals = snapshot.get("totals", {}) if isinstance(snapshot.get("totals"), dict) else {}
    reconciliation = snapshot.get("reconciliation", {}) if isinstance(snapshot.get("reconciliation"), dict) else {}
    settlement = snapshot.get("settlement", {}) if isinstance(snapshot.get("settlement"), dict) else {}
    positions = snapshot.get("positions", []) if isinstance(snapshot.get("positions"), list) else []
    fills = snapshot.get("recent_fills", []) if isinstance(snapshot.get("recent_fills"), list) else []
    pending = snapshot.get("pending_settlements", []) if isinstance(snapshot.get("pending_settlements"), list) else []

    lines = [f"【Surething LIVE 状态 | {generated.astimezone(CST).strftime('%Y-%m-%d %H:%M')}】"]
    lines.append(
        f"汇总：持仓数 {len(positions)} | 总投入 {money(totals.get('open_cost_usd'))} | 未实现PnL {money(totals.get('unrealized_pnl_usd'))} | 已实现PnL {money(totals.get('realized_pnl_total_usd'))} | 可再部署 {money(totals.get('available_for_redeploy_usd'))}"
    )
    lines.append(
        f"对账：{safe_str(reconciliation.get('status', 'NA'))} | 远端仓位 {reconciliation.get('remote_positions', 'NA')} | 远端挂单 {reconciliation.get('remote_open_orders', 'NA')} | 新fills {reconciliation.get('new_fills', 'NA')} | 待结算 {settlement.get('pending_count', len(pending))}"
    )

    if not positions:
        lines.append("当前无持仓")
    else:
        ranked = sorted(positions, key=lambda item: abs(safe_float(item.get("unrealized_pnl"), 0.0)), reverse=True)
        for position in ranked[:max_positions]:
            expected = iso_to_dt(safe_str(position.get("expected_resolve_at")))
            remain = duration_to_human((expected - datetime.now(timezone.utc)).total_seconds()) if expected else "NA"
            held_from = iso_to_dt(safe_str(position.get("opened_at"))) or iso_to_dt(safe_str(position.get("first_fill_at")))
            held = duration_to_human((datetime.now(timezone.utc) - held_from).total_seconds()) if held_from else "NA"
            market = safe_str(position.get("event_slug") or position.get("slug") or position.get("question"))
            lines.append(
                f"- {market} | 仓位 {money(position.get('size_usd'))} | 均价 {safe_float(position.get('entry_price')):.4f} | mark {safe_float(position.get('last_mark')):.4f} | 未实现 {money(position.get('unrealized_pnl'))} | 持有 {held} | 预计resolve {expected.astimezone(CST).strftime('%m-%d %H:%M') if expected else 'NA'} | 剩余 {remain}"
            )

    if fills:
        lines.append("最近fills：")
        for fill in fills[-5:]:
            ts = iso_to_dt(safe_str(fill.get("timestamp")))
            lines.append(
                f"- [{ts.astimezone(CST).strftime('%H:%M') if ts else 'NA'}] {safe_str(fill.get('question') or fill.get('token_id'))} | {safe_str(fill.get('side'))} | shares {safe_float(fill.get('shares')):.4f} | avg {safe_float(fill.get('price')):.4f} | spent {money(fill.get('spent_usd'))}"
            )

    if pending:
        lines.append("待结算：")
        for item in pending[:5]:
            lines.append(
                f"- {safe_str(item.get('question') or item.get('token_id'))} | 预计resolve {safe_str(item.get('expected_resolve_at'))} | 已超时 {safe_str(item.get('age_past_expected'))}"
            )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Render Surething status updates")
    parser.add_argument("--snapshot", default="state/runtime/live/status_snapshot.json")
    parser.add_argument("--kind", choices=["live-hourly"], default="live-hourly")
    parser.add_argument("--max-positions", type=int, default=10)
    args = parser.parse_args()

    snapshot = load_json(Path(args.snapshot), {})
    if args.kind == "live-hourly":
        print(render_live_hourly(snapshot, max_positions=args.max_positions))


if __name__ == "__main__":
    main()
