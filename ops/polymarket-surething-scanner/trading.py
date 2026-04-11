from __future__ import annotations

import json
import os
import shlex
import subprocess
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams, OpenOrderParams, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

from models import CandidateMarket

CLOB_BASE = "https://clob.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"
CST = timezone(timedelta(hours=8))
VALID_MODES = {"paper", "shadow", "live"}
LIVE_PRICE_DECIMALS = 2
LIVE_SIZE_DECIMALS = 5


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        return str(value)
    except Exception:
        return default


def normalize_usd_amount(value: Any, default: float = 0.0) -> float:
    amount = safe_float(value, default)
    if abs(amount) >= 100000 and abs(amount - round(amount)) < 1e-9:
        return amount / 1_000_000.0
    return amount


def parse_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def first_non_empty(payload: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, "", [], {}):
            return value
    return default


def truncate_list(items: list[Any], limit: int) -> list[Any]:
    if limit <= 0:
        return []
    return items[-limit:]


def now_cst() -> datetime:
    return datetime.now(CST)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso_cst() -> str:
    return now_cst().isoformat()


def now_iso_utc() -> str:
    return now_utc().isoformat()


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def dt_to_cst_string(value: str | None) -> str:
    dt = iso_to_dt(value)
    if dt is None:
        return "NA"
    return dt.astimezone(CST).strftime("%Y-%m-%d %H:%M")


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


def resolve_mode(config: dict[str, Any], cli_mode: str | None = None) -> str:
    runtime = config.get("runtime", {}) if isinstance(config.get("runtime"), dict) else {}
    env_mode = str(os.getenv("SURETHING_TRADING_MODE", "")).strip().lower()
    mode = (cli_mode or env_mode or runtime.get("mode") or "paper").strip().lower()
    if mode not in VALID_MODES:
        raise ValueError(f"unsupported mode: {mode}")
    return mode


def get_mode_paths(base_dir: Path, config: dict[str, Any], mode: str) -> dict[str, Path]:
    runtime = config.get("runtime", {}) if isinstance(config.get("runtime"), dict) else {}
    state_root = runtime.get("state_root", "state/runtime")
    runtime_dir = (base_dir / state_root / mode).resolve()
    shared_dir = (base_dir / state_root).resolve()
    return {
        "runtime_dir": runtime_dir,
        "signal_state": runtime_dir / "signal_state.json",
        "trading_state": runtime_dir / "trading_state.json",
        "daily_stats": runtime_dir / "daily_stats.json",
        "journal": runtime_dir / "execution_journal.jsonl",
        "fills_journal": runtime_dir / "fills_journal.jsonl",
        "reconciliation": runtime_dir / "reconciliation_report.json",
        "settlement": runtime_dir / "settlement_report.json",
        "status_snapshot": runtime_dir / "status_snapshot.json",
        "notifications": runtime_dir / "notification_feed.jsonl",
        "summary": shared_dir / "summary.json",
    }


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


DEFAULT_EXECUTION = {
    "order_size_usd": 1.0,
    "max_position_usd_per_market": 5.0,
    "max_total_exposure_usd": 20.0,
    "max_open_positions": 10,
    "max_orders_per_run": 1,
    "max_daily_orders": 10,
    "min_minutes_to_expiry": 90,
    "min_depth_multiple": 8.0,
    "allow_add_to_existing": False,
    "confirm_scans_required": 2,
    "require_flat_start": False,
}


DEFAULT_LIVE = {
    "enabled": False,
    "env_file": ".env.live",
    "require_env_live_enabled": True,
    "order_type": "FOK",
    "post_only": False,
    "signature_type": 0,
    "min_collateral_balance_usd": 5.0,
    "require_empty_open_orders": True,
    "max_consecutive_errors": 3,
    "data_api_base": DATA_API_BASE,
    "sync_recent_trades_limit": 1000,
    "settlement_grace_minutes": 180,
    "claim_shell_command": "",
    "claim_shell_timeout_sec": 120,
    "profile_address": "",
}


DEFAULT_RUNTIME = {
    "mode": "paper",
    "state_root": "state/runtime",
}


def build_mode_settings(config: dict[str, Any], mode: str) -> dict[str, Any]:
    runtime = config.get("runtime", {}) if isinstance(config.get("runtime"), dict) else {}
    risk = config.get("risk", {}) if isinstance(config.get("risk"), dict) else {}
    paper = config.get("paper", {}) if isinstance(config.get("paper"), dict) else {}
    shadow = config.get("shadow", {}) if isinstance(config.get("shadow"), dict) else {}
    live = config.get("live", {}) if isinstance(config.get("live"), dict) else {}

    merged = deep_merge(DEFAULT_EXECUTION, runtime.get("execution_defaults", {}) if isinstance(runtime.get("execution_defaults"), dict) else {})
    merged = deep_merge(merged, risk)
    if mode == "paper":
        merged = deep_merge(merged, paper)
    elif mode == "shadow":
        merged = deep_merge(merged, deep_merge(paper, shadow))
    elif mode == "live":
        merged = deep_merge(merged, deep_merge(paper, live))

    live_cfg = deep_merge(DEFAULT_LIVE, live)
    runtime_cfg = deep_merge(DEFAULT_RUNTIME, runtime)
    return {
        "mode": mode,
        "runtime": runtime_cfg,
        "execution": merged,
        "live": live_cfg,
    }


def update_signal_state(path: Path, candidates: list[CandidateMarket], confirm_runs_required: int) -> tuple[dict[str, Any], set[str]]:
    state = load_json(path, {"markets": {}, "updated_at": None})
    markets = state.setdefault("markets", {})
    seen_ids = {c.market_id for c in candidates if c.market_id}

    for market_id, record in list(markets.items()):
        if market_id not in seen_ids:
            record["seen_last_run"] = False
            record["consecutive_hits"] = 0
            record["last_absent_at"] = now_iso_cst()

    for candidate in candidates:
        market_id = candidate.market_id
        if not market_id:
            continue
        record = markets.get(market_id, {})
        prev_seen = bool(record.get("seen_last_run", False))
        prev_hits = safe_int(record.get("consecutive_hits", 0))
        hits = prev_hits + 1 if prev_seen else 1
        markets[market_id] = {
            "market_id": market_id,
            "question": candidate.question,
            "seen_last_run": True,
            "consecutive_hits": hits,
            "first_seen_at": record.get("first_seen_at") or now_iso_cst(),
            "last_seen_at": now_iso_cst(),
            "last_best_ask": candidate.best_ask,
            "last_depth_usd": candidate.depth_usd,
            "restricted": candidate.restricted,
        }

    state["updated_at"] = now_iso_cst()
    save_json(path, state)
    confirmed = {
        market_id
        for market_id, record in markets.items()
        if safe_int(record.get("consecutive_hits", 0)) >= max(1, confirm_runs_required)
    }
    return state, confirmed


def default_trading_state(mode: str, settings: dict[str, Any]) -> dict[str, Any]:
    return {
        "mode": mode,
        "positions": [],
        "closed_positions": [],
        "pending_settlements": [],
        "recent_fills": [],
        "seen_trade_ids": [],
        "totals": {},
        "last_run": None,
        "daily_orders": {},
        "order_size_usd": round(safe_float(settings["execution"].get("order_size_usd"), 1.0), 2),
        "max_position_usd_per_market": round(safe_float(settings["execution"].get("max_position_usd_per_market"), 5.0), 2),
        "blocked_reasons": {},
        "last_plan": [],
        "last_preflight": {},
        "live_halted": False,
        "live_halt_reason": None,
        "consecutive_live_errors": 0,
        "available_for_redeploy_usd": 0.0,
        "settled_cash_released_usd": 0.0,
        "last_reconciliation": {},
        "last_settlement": {},
    }


def load_trading_state(path: Path, mode: str, settings: dict[str, Any]) -> dict[str, Any]:
    state = load_json(path, default_trading_state(mode, settings))
    if not isinstance(state, dict):
        state = default_trading_state(mode, settings)
    state.setdefault("positions", [])
    state.setdefault("closed_positions", [])
    state.setdefault("pending_settlements", [])
    state.setdefault("recent_fills", [])
    state.setdefault("seen_trade_ids", [])
    state.setdefault("daily_orders", {})
    state.setdefault("blocked_reasons", {})
    state.setdefault("last_plan", [])
    state.setdefault("last_preflight", {})
    state.setdefault("live_halted", False)
    state.setdefault("live_halt_reason", None)
    state.setdefault("consecutive_live_errors", 0)
    state.setdefault("available_for_redeploy_usd", 0.0)
    state.setdefault("settled_cash_released_usd", 0.0)
    state.setdefault("last_reconciliation", {})
    state.setdefault("last_settlement", {})
    state["mode"] = mode
    return state


def load_daily_stats(path: Path) -> dict[str, Any]:
    state = load_json(path, {"by_day": {}})
    if not isinstance(state, dict):
        state = {"by_day": {}}
    state.setdefault("by_day", {})
    return state


def get_day_bucket(daily_stats: dict[str, Any], day_key: str) -> dict[str, Any]:
    by_day = daily_stats.setdefault("by_day", {})
    return by_day.setdefault(
        day_key,
        {
            "scans": 0,
            "total_candidates_seen": 0,
            "latest_candidates_count": 0,
            "unique_market_ids": [],
            "orders_placed": 0,
            "planned_orders": 0,
            "blocked_reasons": {},
        },
    )


def update_daily_stats(
    path: Path,
    candidates: list[CandidateMarket],
    plan: list[dict[str, Any]],
    executed_orders: int,
    totals: dict[str, Any],
) -> dict[str, Any]:
    state = load_daily_stats(path)
    now = now_cst()
    day_key = now.strftime("%Y-%m-%d")
    day = get_day_bucket(state, day_key)

    day["scans"] = safe_int(day.get("scans", 0)) + 1
    day["latest_candidates_count"] = len(candidates)
    day["total_candidates_seen"] = safe_int(day.get("total_candidates_seen", 0)) + len(candidates)

    unique_ids = set(day.get("unique_market_ids", []))
    unique_ids.update(c.market_id for c in candidates if c.market_id)
    day["unique_market_ids"] = sorted(unique_ids)
    day["unique_candidates_count"] = len(day["unique_market_ids"])
    day["orders_placed"] = safe_int(day.get("orders_placed", 0)) + safe_int(executed_orders)
    day["planned_orders"] = safe_int(day.get("planned_orders", 0)) + sum(1 for item in plan if item.get("action") == "open")

    blocked = Counter(str(item.get("reason", "unknown")) for item in plan if item.get("action") != "open")
    previous_blocked = Counter(day.get("blocked_reasons", {}))
    previous_blocked.update(blocked)
    day["blocked_reasons"] = dict(previous_blocked)

    day["open_cost_usd"] = totals.get("open_cost_usd", 0.0)
    day["deployed_now_usd"] = totals.get("deployed_now_usd", 0.0)
    day["realized_pnl_today_usd"] = totals.get("realized_pnl_today_usd", 0.0)
    day["historical_realized_pnl_usd"] = totals.get("realized_pnl_total_usd", 0.0)
    day["net_pnl_today_usd"] = totals.get("net_pnl_today_usd", 0.0)
    day["historical_net_pnl_usd"] = totals.get("historical_net_pnl_usd", 0.0)
    day["available_for_redeploy_usd"] = totals.get("available_for_redeploy_usd", 0.0)
    day["settled_cash_released_usd"] = totals.get("settled_cash_released_usd", 0.0)

    save_json(path, state)
    return state


def summarize_positions(positions: list[dict[str, Any]], candidates_by_market: dict[str, CandidateMarket]) -> tuple[float, float, float]:
    total_open_cost = 0.0
    total_deployed_now = 0.0
    total_unrealized = 0.0
    for position in positions:
        market_id = str(position.get("market_id", ""))
        candidate = candidates_by_market.get(market_id)
        if candidate is not None:
            position["last_mark"] = round(candidate.best_ask, 4)
            position.setdefault("event_slug", candidate.event_slug)
            position.setdefault("slug", candidate.slug)
        entry = safe_float(position.get("entry_price"), 0.0)
        mark = safe_float(position.get("last_mark"), entry)
        shares = safe_float(position.get("shares"), 0.0)
        size_usd = safe_float(position.get("size_usd"), 0.0)
        remote_unrealized = position.get("remote_unrealized_pnl")
        pnl = safe_float(remote_unrealized, (mark - entry) * shares)
        position["unrealized_pnl"] = round(pnl, 4)
        total_open_cost += size_usd
        total_deployed_now += mark * shares if shares > 0 else size_usd + pnl
        total_unrealized += pnl
    return total_open_cost, total_deployed_now, total_unrealized


def build_execution_plan(
    candidates: list[CandidateMarket],
    state: dict[str, Any],
    settings: dict[str, Any],
    confirmed_ids: set[str],
    preflight: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    execution = settings["execution"]
    mode = settings["mode"]
    preflight = preflight or {}
    positions = state.get("positions", []) if isinstance(state.get("positions"), list) else []
    existing = {str(position.get("market_id", "")): position for position in positions}

    order_size_usd = safe_float(execution.get("order_size_usd"), 1.0)
    max_position_usd = safe_float(execution.get("max_position_usd_per_market"), 5.0)
    max_total_exposure = safe_float(execution.get("max_total_exposure_usd"), 20.0)
    max_open_positions = safe_int(execution.get("max_open_positions"), 10)
    max_orders_per_run = safe_int(execution.get("max_orders_per_run"), 1)
    max_daily_orders = safe_int(execution.get("max_daily_orders"), 10)
    min_minutes_to_expiry = safe_int(execution.get("min_minutes_to_expiry"), 90)
    min_depth_multiple = safe_float(execution.get("min_depth_multiple"), 8.0)
    allow_add = parse_bool(execution.get("allow_add_to_existing"), False)
    confirm_runs_required = safe_int(execution.get("confirm_scans_required"), 2)

    now = now_utc()
    day_key = now_cst().strftime("%Y-%m-%d")
    daily_orders = safe_int(state.get("daily_orders", {}).get(day_key, 0))
    current_exposure = sum(safe_float(position.get("size_usd"), 0.0) for position in positions)
    planned_orders = 0
    planned_exposure = 0.0
    planned_new_positions = 0

    plan: list[dict[str, Any]] = []
    ranked = sorted(candidates, key=lambda candidate: (-candidate.best_ask, -candidate.depth_usd, candidate.end_date))

    for candidate in ranked:
        market_id = candidate.market_id
        current = existing.get(market_id)
        current_size = safe_float(current.get("size_usd"), 0.0) if current else 0.0
        minutes_to_expiry = max(0, int((candidate.end_date - now).total_seconds() // 60))
        shares = round(order_size_usd / candidate.best_ask, LIVE_SIZE_DECIMALS) if candidate.best_ask > 0 else 0.0

        reason = None
        if parse_bool(state.get("live_halted"), False) and mode == "live":
            reason = "live_halted"
        elif preflight.get("status") == "error":
            reason = "preflight_failed"
        elif market_id not in confirmed_ids:
            reason = f"await_confirm_{confirm_runs_required}_runs"
        elif candidate.best_ask <= 0 or shares <= 0:
            reason = "invalid_price"
        elif minutes_to_expiry < min_minutes_to_expiry:
            reason = "too_close_to_expiry"
        elif candidate.depth_usd < order_size_usd * min_depth_multiple:
            reason = "insufficient_depth"
        elif current and not allow_add:
            reason = "already_open"
        elif current_size + order_size_usd > max_position_usd + 1e-9:
            reason = "per_market_cap"
        elif daily_orders + planned_orders >= max_daily_orders:
            reason = "daily_order_cap"
        elif current_exposure + planned_exposure + order_size_usd > max_total_exposure + 1e-9:
            reason = "total_exposure_cap"
        elif (len(positions) + planned_new_positions) >= max_open_positions and not current:
            reason = "max_open_positions"
        elif planned_orders >= max_orders_per_run:
            reason = "max_orders_per_run"

        action = "skip" if reason else "open"
        if action == "open":
            planned_orders += 1
            planned_exposure += order_size_usd
            if not current:
                planned_new_positions += 1

        plan.append(
            {
                "market_id": market_id,
                "condition_id": candidate.condition_id,
                "token_id": candidate.token_id,
                "question": candidate.question,
                "slug": candidate.slug,
                "event_slug": candidate.event_slug,
                "best_ask": round(candidate.best_ask, 4),
                "depth_usd": round(candidate.depth_usd, 2),
                "order_size_usd": round(order_size_usd, 2),
                "shares": shares,
                "minutes_to_expiry": minutes_to_expiry,
                "restricted": candidate.restricted,
                "action": action,
                "reason": reason or "approved",
                "expected_resolve_at": candidate.end_date.isoformat(),
            }
        )
    return plan


def _append_position(positions: list[dict[str, Any]], candidate: CandidateMarket, plan_item: dict[str, Any], allow_add: bool) -> tuple[int, int]:
    market_id = candidate.market_id
    existing = next((position for position in positions if str(position.get("market_id", "")) == market_id), None)
    now_iso = now_iso_cst()
    entry = safe_float(plan_item.get("best_ask"), candidate.best_ask)
    size_usd = safe_float(plan_item.get("order_size_usd"), 0.0)
    shares = safe_float(plan_item.get("shares"), 0.0)

    if existing and allow_add:
        old_size = safe_float(existing.get("size_usd"), 0.0)
        old_shares = safe_float(existing.get("shares"), 0.0)
        new_size = old_size + size_usd
        new_shares = old_shares + shares
        avg_entry = new_size / new_shares if new_shares > 0 else entry
        existing["size_usd"] = round(new_size, 2)
        existing["shares"] = round(new_shares, 8)
        existing["entry_price"] = round(avg_entry, 4)
        existing["last_added_at"] = now_iso
        existing["last_order"] = plan_item
        return 0, 1

    if existing:
        return 0, 0

    positions.append(
        {
            "market_id": market_id,
            "condition_id": candidate.condition_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "slug": candidate.slug,
            "event_slug": candidate.event_slug,
            "opened_at": now_iso,
            "entry_price": round(entry, 4),
            "size_usd": round(size_usd, 2),
            "shares": round(shares, LIVE_SIZE_DECIMALS),
            "last_mark": round(entry, 4),
            "unrealized_pnl": 0.0,
            "restricted": candidate.restricted,
            "last_order": plan_item,
            "expected_resolve_at": candidate.end_date.isoformat(),
        }
    )
    return 1, 0


def finalize_state(
    state: dict[str, Any],
    settings: dict[str, Any],
    candidates: list[CandidateMarket],
    plan: list[dict[str, Any]],
    opened_new: int,
    added_existing: int,
    closed_count: int = 0,
) -> dict[str, Any]:
    positions = state.get("positions", []) if isinstance(state.get("positions"), list) else []
    closed_positions = state.get("closed_positions", []) if isinstance(state.get("closed_positions"), list) else []
    candidates_by_market = {candidate.market_id: candidate for candidate in candidates if candidate.market_id}

    total_open_cost, total_deployed_now, total_unrealized = summarize_positions(positions, candidates_by_market)
    now = now_cst()
    day_key = now.strftime("%Y-%m-%d")
    realized_today = 0.0
    realized_total = 0.0
    for closed in closed_positions:
        realized = safe_float(closed.get("realized_pnl"), 0.0)
        realized_total += realized
        closed_at = safe_str(closed.get("closed_at"))
        if closed_at.startswith(day_key):
            realized_today += realized

    orders_this_run = opened_new + added_existing
    daily_orders = state.setdefault("daily_orders", {})
    daily_orders[day_key] = safe_int(daily_orders.get(day_key, 0)) + orders_this_run

    blocked = Counter(str(item.get("reason", "unknown")) for item in plan if item.get("action") != "open")
    cumulative_buys = sum(safe_float(position.get("size_usd"), 0.0) for position in positions) + sum(
        safe_float(position.get("size_usd"), 0.0) for position in closed_positions
    )

    execution = settings["execution"]
    state["order_size_usd"] = round(safe_float(execution.get("order_size_usd"), 1.0), 2)
    state["max_position_usd_per_market"] = round(safe_float(execution.get("max_position_usd_per_market"), 5.0), 2)
    state["last_run"] = now_iso_cst()
    state["blocked_reasons"] = dict(blocked)
    state["last_plan"] = plan[-50:]
    state["recent_fills"] = truncate_list(state.get("recent_fills", []), 50)
    state["seen_trade_ids"] = truncate_list(state.get("seen_trade_ids", []), 2000)
    state["totals"] = {
        "positions": len(positions),
        "open_cost_usd": round(total_open_cost, 2),
        "deployed_now_usd": round(total_deployed_now, 2),
        "invested_usd": round(total_open_cost, 2),
        "unrealized_pnl_usd": round(total_unrealized, 4),
        "realized_pnl_today_usd": round(realized_today, 4),
        "realized_pnl_total_usd": round(realized_total, 4),
        "net_pnl_today_usd": round(realized_today + total_unrealized, 4),
        "historical_net_pnl_usd": round(realized_total + total_unrealized, 4),
        "equity_usd": round(total_open_cost + realized_total + total_unrealized, 4),
        "opened_new_this_run": opened_new,
        "added_existing_this_run": added_existing,
        "closed_this_run": closed_count,
        "blocked_by_cap_this_run": safe_int(blocked.get("per_market_cap", 0))
        + safe_int(blocked.get("total_exposure_cap", 0))
        + safe_int(blocked.get("daily_order_cap", 0)),
        "orders_this_run": orders_this_run,
        "opened_today": safe_int(daily_orders.get(day_key, 0)),
        "cumulative_buy_usd": round(cumulative_buys, 2),
        "max_position_usd_per_market": round(safe_float(execution.get("max_position_usd_per_market"), 5.0), 2),
        "order_size_usd": round(safe_float(execution.get("order_size_usd"), 1.0), 2),
        "available_for_redeploy_usd": round(safe_float(state.get("available_for_redeploy_usd"), 0.0), 2),
        "settled_cash_released_usd": round(safe_float(state.get("settled_cash_released_usd"), 0.0), 2),
        "pending_settlements": len(state.get("pending_settlements", [])),
        "recent_fills": len(state.get("recent_fills", [])),
    }
    return state


def execute_simulated(
    path: Path,
    candidates: list[CandidateMarket],
    plan: list[dict[str, Any]],
    settings: dict[str, Any],
    preflight: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mode = settings["mode"]
    state = load_trading_state(path, mode, settings)
    positions = state.setdefault("positions", [])
    allow_add = parse_bool(settings["execution"].get("allow_add_to_existing"), False)
    candidates_by_market = {candidate.market_id: candidate for candidate in candidates if candidate.market_id}

    opened_new = 0
    added_existing = 0
    for item in plan:
        if item.get("action") != "open":
            continue
        candidate = candidates_by_market.get(str(item.get("market_id", "")))
        if candidate is None:
            continue
        opened, added = _append_position(positions, candidate, item, allow_add)
        opened_new += opened
        added_existing += added

    if preflight:
        state["last_preflight"] = preflight
    state = finalize_state(state, settings, candidates, plan, opened_new, added_existing)
    save_json(path, state)
    return state


class LiveExecutionError(RuntimeError):
    pass


class LiveTrader:
    def __init__(self, env_file: Path, live_cfg: dict[str, Any]):
        self.env_file = env_file
        if env_file.exists():
            load_dotenv(env_file, override=False)

        self.host = os.getenv("POLYMARKET_HOST", CLOB_BASE)
        self.data_api_base = os.getenv("POLYMARKET_DATA_API_BASE", safe_str(live_cfg.get("data_api_base"), DATA_API_BASE))
        self.chain_id = safe_int(os.getenv("POLYMARKET_CHAIN_ID", 137), 137)
        self.private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
        self.signature_type = safe_int(os.getenv("POLYMARKET_SIGNATURE_TYPE", live_cfg.get("signature_type", 0)), 0)
        self.funder = (os.getenv("POLYMARKET_FUNDER") or safe_str(live_cfg.get("funder"))).strip() or None
        self.api_key = os.getenv("POLYMARKET_API_KEY", "").strip()
        self.api_secret = os.getenv("POLYMARKET_API_SECRET", "").strip()
        self.api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()
        self.live_enabled_env = parse_bool(os.getenv("POLYMARKET_LIVE_ENABLED"), False)
        self.profile_address = (
            os.getenv("POLYMARKET_PROFILE_ADDRESS", "").strip()
            or safe_str(live_cfg.get("profile_address")).strip()
            or self.funder
            or (Account.from_key(self.private_key).address if self.private_key else "")
        )
        self.live_cfg = live_cfg
        self.client: ClobClient | None = None

    def _build_client(self) -> ClobClient:
        if not self.private_key:
            raise LiveExecutionError("missing POLYMARKET_PRIVATE_KEY")
        if self.signature_type in {1, 2} and not self.funder:
            raise LiveExecutionError("POLYMARKET_FUNDER required for proxy/email wallet signatures")

        creds = None
        if self.api_key and self.api_secret and self.api_passphrase:
            creds = ApiCreds(self.api_key, self.api_secret, self.api_passphrase)

        client = ClobClient(
            self.host,
            chain_id=self.chain_id,
            key=self.private_key,
            creds=creds,
            signature_type=self.signature_type,
            funder=self.funder,
        )
        if creds is None:
            client.set_api_creds(client.create_or_derive_api_creds())
        return client

    def _extract_balance(self, allowance_payload: Any) -> float:
        if isinstance(allowance_payload, dict):
            for key in ["balance", "balance_available", "available", "balanceTotal", "balance_total"]:
                if key in allowance_payload:
                    return normalize_usd_amount(allowance_payload.get(key), 0.0)
            for value in allowance_payload.values():
                balance = self._extract_balance(value)
                if balance > 0:
                    return balance
        return 0.0

    def data_api_get(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self.data_api_base.rstrip('/')}/{path.lstrip('/')}"
        with httpx.Client(timeout=20) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            return response.json()

    def preflight(self) -> dict[str, Any]:
        if not parse_bool(self.live_cfg.get("enabled"), False):
            raise LiveExecutionError("live.enabled is false in config")
        if parse_bool(self.live_cfg.get("require_env_live_enabled"), True) and not self.live_enabled_env:
            raise LiveExecutionError("POLYMARKET_LIVE_ENABLED=true not set")

        self.client = self._build_client()
        allowance = self.client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        collateral_balance = self._extract_balance(allowance)
        min_balance = safe_float(self.live_cfg.get("min_collateral_balance_usd"), 0.0)
        if collateral_balance < min_balance:
            raise LiveExecutionError(
                f"insufficient collateral balance: {collateral_balance:.2f} < required {min_balance:.2f}"
            )

        open_orders_count = None
        if parse_bool(self.live_cfg.get("require_empty_open_orders"), True):
            open_orders = self.client.get_orders(OpenOrderParams())
            open_orders_count = len(open_orders) if isinstance(open_orders, list) else 0
            if open_orders_count > 0:
                raise LiveExecutionError(f"remote open orders must be empty before live start (found {open_orders_count})")

        return {
            "status": "ok",
            "host": self.host,
            "data_api_base": self.data_api_base,
            "chain_id": self.chain_id,
            "signature_type": self.signature_type,
            "funder_present": bool(self.funder),
            "api_key_present": bool(self.api_key),
            "profile_address": self.profile_address,
            "env_file": str(self.env_file),
            "env_live_enabled": self.live_enabled_env,
            "collateral_balance_usd": round(collateral_balance, 4),
            "open_orders_count": open_orders_count,
        }

    def place_buy(self, token_id: str, price: float, shares: float) -> dict[str, Any]:
        if self.client is None:
            raise LiveExecutionError("client not initialized")
        normalized_price = round(price, LIVE_PRICE_DECIMALS)
        normalized_shares = round(shares, LIVE_SIZE_DECIMALS)
        order = self.client.create_order(
            OrderArgs(token_id=token_id, price=normalized_price, size=normalized_shares, side=BUY)
        )
        order_type_name = safe_str(self.live_cfg.get("order_type"), "FOK").upper()
        order_type = getattr(OrderType, order_type_name, OrderType.FOK)
        post_only = parse_bool(self.live_cfg.get("post_only"), False)
        if post_only and order_type_name not in {"GTC", "GTD"}:
            post_only = False
        return self.client.post_order(order, order_type, post_only=post_only)

    def fetch_open_orders(self) -> list[dict[str, Any]]:
        if self.client is None:
            raise LiveExecutionError("client not initialized")
        payload = self.client.get_orders(OpenOrderParams())
        return payload if isinstance(payload, list) else []

    def fetch_trades(self) -> list[dict[str, Any]]:
        if self.client is None:
            raise LiveExecutionError("client not initialized")
        payload = self.client.get_trades()
        rows = payload if isinstance(payload, list) else []
        limit = safe_int(self.live_cfg.get("sync_recent_trades_limit"), 1000)
        return rows[-limit:]

    def fetch_positions(self) -> list[dict[str, Any]]:
        if not self.profile_address:
            return []
        payload = self.data_api_get("positions", {"user": self.profile_address, "sizeThreshold": 0})
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return payload["data"]
        return []

    def fetch_closed_positions(self) -> list[dict[str, Any]]:
        if not self.profile_address:
            return []
        payload = self.data_api_get("closed-positions", {"user": self.profile_address, "sizeThreshold": 0})
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return payload["data"]
        return []

    def fetch_total_value(self) -> dict[str, Any]:
        if not self.profile_address:
            return {}
        payload = self.data_api_get("value", {"user": self.profile_address})
        return payload if isinstance(payload, dict) else {}


def response_success(response: Any) -> bool:
    if isinstance(response, dict):
        if response.get("success") is False:
            return False
        status = safe_str(response.get("status") or response.get("state")).lower()
        if status in {"rejected", "cancelled", "canceled", "failed", "error"}:
            return False
        if response.get("error") or response.get("errorMsg") or response.get("message") == "error":
            return False
    return True


def append_notification(path: Path, category: str, level: str, text: str, payload: dict[str, Any] | None = None) -> None:
    event = {
        "ts": now_iso_cst(),
        "category": category,
        "level": level,
        "text": text,
        "payload": payload or {},
    }
    append_jsonl(path, event)


def normalize_trade(raw: dict[str, Any], meta_by_token: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    token_id = safe_str(first_non_empty(raw, ["asset_id", "asset", "token_id", "tokenId"]))
    trade_id = safe_str(first_non_empty(raw, ["id", "tradeID", "trade_id", "match_id", "transactionHash", "tx_hash"]))
    if not token_id and not trade_id:
        return None
    price = safe_float(first_non_empty(raw, ["price", "matched_price", "avgPrice", "avg_price"]), 0.0)
    shares = safe_float(first_non_empty(raw, ["size", "amount", "matched_amount", "shares", "qty"]), 0.0)
    spent = normalize_usd_amount(first_non_empty(raw, ["usdc_size", "notional", "value", "amount_usd"]), 0.0)
    if spent <= 0 and price > 0 and shares > 0:
        spent = price * shares
    side = safe_str(first_non_empty(raw, ["side", "taker_side", "maker_side"])).upper() or "BUY"
    meta = meta_by_token.get(token_id, {})
    timestamp = safe_str(first_non_empty(raw, ["timestamp", "created_at", "createdAt", "matched_at", "time"])) or now_iso_utc()
    return {
        "trade_id": trade_id or f"{token_id}:{timestamp}:{shares}:{price}",
        "token_id": token_id,
        "market_id": safe_str(first_non_empty(raw, ["market", "condition_id", "conditionId"])) or safe_str(meta.get("market_id")),
        "condition_id": safe_str(first_non_empty(raw, ["condition_id", "conditionId"])) or safe_str(meta.get("condition_id")),
        "question": safe_str(meta.get("question") or first_non_empty(raw, ["title", "question"])),
        "event_slug": safe_str(meta.get("event_slug")),
        "slug": safe_str(meta.get("slug")),
        "side": side,
        "price": round(price, 6),
        "shares": round(shares, LIVE_SIZE_DECIMALS),
        "spent_usd": round(spent, 6),
        "timestamp": timestamp,
        "raw": raw,
    }


def normalize_remote_position(raw: dict[str, Any], meta_by_token: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    token_id = safe_str(first_non_empty(raw, ["asset", "asset_id", "assetId", "token_id", "tokenId"]))
    market_id = safe_str(first_non_empty(raw, ["market", "condition_id", "conditionId", "market_id"]))
    meta = meta_by_token.get(token_id, {})
    shares = safe_float(first_non_empty(raw, ["size", "amount", "shares", "position"]), 0.0)
    if shares <= 0:
        return None
    avg_entry = safe_float(first_non_empty(raw, ["avgPrice", "avg_price", "averagePrice", "price"]), 0.0)
    current_value = normalize_usd_amount(first_non_empty(raw, ["currentValue", "current_value", "value", "usdValue"]), 0.0)
    initial_value = normalize_usd_amount(first_non_empty(raw, ["initialValue", "initial_value", "cost_basis", "amountBought"]), 0.0)
    mark = safe_float(first_non_empty(raw, ["curPrice", "current_price", "mark", "price"]), 0.0)
    if mark <= 0 and current_value > 0 and shares > 0:
        mark = current_value / shares
    if initial_value <= 0 and avg_entry > 0 and shares > 0:
        initial_value = avg_entry * shares
    unrealized = normalize_usd_amount(first_non_empty(raw, ["cashPnl", "cash_pnl", "unrealizedPnl", "unrealized_pnl"]), current_value - initial_value)
    return {
        "market_id": market_id or safe_str(meta.get("market_id")),
        "condition_id": safe_str(first_non_empty(raw, ["conditionId", "condition_id"])) or safe_str(meta.get("condition_id")),
        "token_id": token_id,
        "question": safe_str(first_non_empty(raw, ["title", "question", "name"])) or safe_str(meta.get("question")),
        "event_slug": safe_str(meta.get("event_slug")),
        "slug": safe_str(meta.get("slug")),
        "outcome": safe_str(first_non_empty(raw, ["outcome", "label"])),
        "shares": round(shares, 8),
        "entry_price": round(avg_entry, 6),
        "size_usd": round(initial_value, 6),
        "last_mark": round(mark, 6),
        "remote_unrealized_pnl": round(unrealized, 6),
        "current_value_usd": round(current_value, 6),
        "expected_resolve_at": safe_str(meta.get("expected_resolve_at")),
        "raw": raw,
    }


def normalize_remote_closed_position(raw: dict[str, Any], meta_by_token: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    token_id = safe_str(first_non_empty(raw, ["asset", "asset_id", "assetId", "token_id", "tokenId"]))
    market_id = safe_str(first_non_empty(raw, ["market", "condition_id", "conditionId", "market_id"]))
    meta = meta_by_token.get(token_id, {})
    initial_value = normalize_usd_amount(first_non_empty(raw, ["initialValue", "initial_value", "cost_basis", "amountBought"]), 0.0)
    payout = normalize_usd_amount(first_non_empty(raw, ["currentValue", "current_value", "redeemed", "value", "amountSold", "amountReceived"]), 0.0)
    realized = normalize_usd_amount(first_non_empty(raw, ["cashPnl", "cash_pnl", "realizedPnl", "realized_pnl"]), payout - initial_value)
    if not token_id and not market_id:
        return None
    return {
        "market_id": market_id or safe_str(meta.get("market_id")),
        "condition_id": safe_str(first_non_empty(raw, ["conditionId", "condition_id"])) or safe_str(meta.get("condition_id")),
        "token_id": token_id,
        "question": safe_str(first_non_empty(raw, ["title", "question", "name"])) or safe_str(meta.get("question")),
        "event_slug": safe_str(meta.get("event_slug")),
        "slug": safe_str(meta.get("slug")),
        "outcome": safe_str(first_non_empty(raw, ["outcome", "label"])),
        "size_usd": round(initial_value, 6),
        "payout_usd": round(payout, 6),
        "realized_pnl": round(realized, 6),
        "closed_at": safe_str(first_non_empty(raw, ["updatedAt", "closedAt", "closed_at", "timestamp"])) or now_iso_cst(),
        "raw": raw,
    }


def build_meta_maps(state: dict[str, Any], candidates: list[CandidateMarket]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    meta_by_token: dict[str, dict[str, Any]] = {}
    meta_by_market: dict[str, dict[str, Any]] = {}

    for candidate in candidates:
        meta = {
            "market_id": candidate.market_id,
            "condition_id": candidate.condition_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "event_slug": candidate.event_slug,
            "slug": candidate.slug,
            "expected_resolve_at": candidate.end_date.isoformat(),
        }
        meta_by_token[candidate.token_id] = meta
        meta_by_market[candidate.market_id] = meta

    for bucket in [state.get("positions", []), state.get("closed_positions", []), state.get("recent_fills", [])]:
        if not isinstance(bucket, list):
            continue
        for item in bucket:
            if not isinstance(item, dict):
                continue
            meta = {
                "market_id": safe_str(item.get("market_id")),
                "condition_id": safe_str(item.get("condition_id")),
                "token_id": safe_str(item.get("token_id")),
                "question": safe_str(item.get("question")),
                "event_slug": safe_str(item.get("event_slug")),
                "slug": safe_str(item.get("slug")),
                "expected_resolve_at": safe_str(item.get("expected_resolve_at")),
            }
            if meta["token_id"]:
                meta_by_token[meta["token_id"]] = {**meta_by_token.get(meta["token_id"], {}), **meta}
            if meta["market_id"]:
                meta_by_market[meta["market_id"]] = {**meta_by_market.get(meta["market_id"], {}), **meta}
    return meta_by_token, meta_by_market


def append_new_fills(state: dict[str, Any], fills_path: Path, raw_trades: list[dict[str, Any]], meta_by_token: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set(safe_str(item) for item in state.get("seen_trade_ids", []))
    new_rows: list[dict[str, Any]] = []
    for raw in raw_trades:
        normalized = normalize_trade(raw, meta_by_token)
        if normalized is None:
            continue
        trade_id = safe_str(normalized.get("trade_id"))
        if trade_id in seen:
            continue
        seen.add(trade_id)
        append_jsonl(fills_path, normalized)
        new_rows.append(normalized)

    if new_rows:
        state["recent_fills"] = truncate_list((state.get("recent_fills", []) or []) + new_rows, 50)
        state["seen_trade_ids"] = truncate_list(list(seen), 2000)
    return new_rows


def reconcile_positions_from_remote(
    state: dict[str, Any],
    remote_positions_raw: list[dict[str, Any]],
    candidates: list[CandidateMarket],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    meta_by_token, _ = build_meta_maps(state, candidates)
    old_positions = state.get("positions", []) if isinstance(state.get("positions"), list) else []
    old_by_token = {safe_str(item.get("token_id")): item for item in old_positions if safe_str(item.get("token_id"))}

    new_positions: list[dict[str, Any]] = []
    remote_tokens: set[str] = set()
    for raw in remote_positions_raw:
        normalized = normalize_remote_position(raw, meta_by_token)
        if normalized is None:
            continue
        token_id = safe_str(normalized.get("token_id"))
        remote_tokens.add(token_id)
        existing = old_by_token.get(token_id, {})
        merged = {
            **existing,
            **normalized,
            "opened_at": safe_str(existing.get("opened_at")) or safe_str(existing.get("first_fill_at")) or now_iso_cst(),
            "first_fill_at": safe_str(existing.get("first_fill_at")) or safe_str(existing.get("opened_at")) or now_iso_cst(),
            "reconciled_at": now_iso_cst(),
        }
        if safe_float(merged.get("entry_price"), 0.0) <= 0 and safe_float(merged.get("size_usd"), 0.0) > 0 and safe_float(merged.get("shares"), 0.0) > 0:
            merged["entry_price"] = round(safe_float(merged.get("size_usd")) / safe_float(merged.get("shares")), 6)
        new_positions.append(merged)

    local_tokens = {safe_str(item.get("token_id")) for item in old_positions if safe_str(item.get("token_id"))}
    report = {
        "local_position_count_before": len(old_positions),
        "remote_position_count": len(new_positions),
        "remote_only_tokens": sorted(token for token in remote_tokens - local_tokens if token),
        "local_only_tokens": sorted(token for token in local_tokens - remote_tokens if token),
    }
    return new_positions, report


def archive_closed_positions(
    state: dict[str, Any],
    remote_closed_raw: list[dict[str, Any]],
    candidates: list[CandidateMarket],
) -> tuple[int, float, list[dict[str, Any]]]:
    meta_by_token, _ = build_meta_maps(state, candidates)
    open_positions = state.get("positions", []) if isinstance(state.get("positions"), list) else []
    open_by_token = {safe_str(item.get("token_id")): item for item in open_positions if safe_str(item.get("token_id"))}
    existing_closed = {
        f"{safe_str(item.get('token_id'))}:{safe_str(item.get('closed_at'))}:{safe_float(item.get('payout_usd'), 0.0)}"
        for item in state.get("closed_positions", [])
        if isinstance(item, dict)
    }
    archived: list[dict[str, Any]] = []
    released = 0.0

    for raw in remote_closed_raw:
        normalized = normalize_remote_closed_position(raw, meta_by_token)
        if normalized is None:
            continue
        token_id = safe_str(normalized.get("token_id"))
        key = f"{token_id}:{safe_str(normalized.get('closed_at'))}:{safe_float(normalized.get('payout_usd'), 0.0)}"
        if key in existing_closed:
            continue
        local = open_by_token.get(token_id)
        if local is None and token_id not in {safe_str(fill.get("token_id")) for fill in state.get("recent_fills", []) if isinstance(fill, dict)}:
            continue
        closed = {
            **(local or {}),
            **normalized,
            "close_reason": "remote_settled_archive",
        }
        archived.append(closed)
        existing_closed.add(key)
        released += max(0.0, safe_float(closed.get("payout_usd"), 0.0))

    if archived:
        archived_token_ids = {safe_str(item.get("token_id")) for item in archived}
        state["positions"] = [item for item in open_positions if safe_str(item.get("token_id")) not in archived_token_ids]
        state["closed_positions"] = truncate_list((state.get("closed_positions", []) or []) + archived, 500)
        state["settled_cash_released_usd"] = round(safe_float(state.get("settled_cash_released_usd"), 0.0) + released, 6)
    return len(archived), released, archived


def build_pending_settlements(state: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    grace_minutes = safe_int(settings["live"].get("settlement_grace_minutes"), 180)
    pending: list[dict[str, Any]] = []
    now = now_utc()
    for position in state.get("positions", []):
        if not isinstance(position, dict):
            continue
        expected = iso_to_dt(safe_str(position.get("expected_resolve_at")))
        if expected is None:
            continue
        if expected + timedelta(minutes=grace_minutes) > now:
            continue
        pending.append(
            {
                "market_id": safe_str(position.get("market_id")),
                "condition_id": safe_str(position.get("condition_id")),
                "token_id": safe_str(position.get("token_id")),
                "question": safe_str(position.get("question")),
                "expected_resolve_at": expected.isoformat(),
                "age_past_expected": duration_to_human((now - expected).total_seconds()),
                "status": "await_settlement_or_claim",
            }
        )
    return pending


def maybe_run_claim_hook(
    pending_settlements: list[dict[str, Any]],
    settings: dict[str, Any],
    notifications_path: Path,
) -> dict[str, Any]:
    command = safe_str(settings["live"].get("claim_shell_command")).strip()
    if not command or not pending_settlements:
        return {"attempted": False, "count": 0}
    payload = json.dumps(pending_settlements, ensure_ascii=False)
    env = os.environ.copy()
    env["SURETHING_PENDING_SETTLEMENTS_JSON"] = payload
    env["SURETHING_PENDING_SETTLEMENTS_COUNT"] = str(len(pending_settlements))
    timeout = safe_int(settings["live"].get("claim_shell_timeout_sec"), 120)
    try:
        proc = subprocess.run(command, shell=True, env=env, capture_output=True, text=True, timeout=timeout)
        result = {
            "attempted": True,
            "count": len(pending_settlements),
            "returncode": proc.returncode,
            "stdout": proc.stdout[-2000:],
            "stderr": proc.stderr[-2000:],
            "command": command,
        }
        level = "info" if proc.returncode == 0 else "error"
        append_notification(
            notifications_path,
            "claim_hook",
            level,
            f"claim hook executed for {len(pending_settlements)} settlement(s)",
            result,
        )
        return result
    except Exception as exc:
        result = {"attempted": True, "count": len(pending_settlements), "command": command, "error": str(exc)}
        append_notification(notifications_path, "claim_hook", "error", "claim hook execution failed", result)
        return result


def parse_total_value(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return {
        "total_value_usd": round(normalize_usd_amount(first_non_empty(payload, ["value", "totalValue", "total_value"]), 0.0), 6),
        "cash_balance_usd": round(
            normalize_usd_amount(first_non_empty(payload, ["cash", "cashBalance", "cash_balance", "available", "availableBalance"]), 0.0),
            6,
        ),
    }


def write_status_snapshot(path: Path, state: dict[str, Any], summary: dict[str, Any], preflight: dict[str, Any]) -> dict[str, Any]:
    snapshot = {
        "generated_at": now_iso_cst(),
        "mode": state.get("mode"),
        "preflight": preflight,
        "totals": state.get("totals", {}),
        "reconciliation": state.get("last_reconciliation", {}),
        "settlement": state.get("last_settlement", {}),
        "positions": truncate_list(state.get("positions", []), 25),
        "pending_settlements": truncate_list(state.get("pending_settlements", []), 25),
        "recent_fills": truncate_list(state.get("recent_fills", []), 20),
        "summary": summary,
    }
    save_json(path, snapshot)
    return snapshot


def sync_live_state(
    state: dict[str, Any],
    trader: LiveTrader,
    candidates: list[CandidateMarket],
    settings: dict[str, Any],
    paths: dict[str, Path],
    preflight: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    report: dict[str, Any] = {"status": "ok", "synced_at": now_iso_cst()}
    settlement_report: dict[str, Any] = {"status": "ok", "synced_at": now_iso_cst()}
    new_fills: list[dict[str, Any]] = []

    try:
        open_orders = trader.fetch_open_orders()
        trades = trader.fetch_trades()
        remote_positions_raw = trader.fetch_positions()
        remote_closed_raw = trader.fetch_closed_positions()
        total_value = parse_total_value(trader.fetch_total_value())
    except Exception as exc:
        report = {"status": "error", "synced_at": now_iso_cst(), "message": str(exc)}
        state["last_reconciliation"] = report
        state["last_settlement"] = settlement_report
        save_json(paths["reconciliation"], report)
        save_json(paths["settlement"], settlement_report)
        append_notification(paths["notifications"], "reconciliation", "error", "live reconciliation failed", report)
        return state, report, settlement_report, new_fills

    meta_by_token, _ = build_meta_maps(state, candidates)
    new_fills = append_new_fills(state, paths["fills_journal"], trades, meta_by_token)
    if new_fills:
        for fill in new_fills:
            append_notification(
                paths["notifications"],
                "fill",
                "info",
                f"new remote fill synced: {fill.get('question') or fill.get('token_id')}",
                fill,
            )

    reconciled_positions, drift_report = reconcile_positions_from_remote(state, remote_positions_raw, candidates)
    archived_count, released, archived_rows = archive_closed_positions(state, remote_closed_raw, candidates)
    state["positions"] = reconciled_positions
    state["pending_settlements"] = build_pending_settlements(state, settings)

    if total_value.get("cash_balance_usd", 0.0) > 0:
        state["available_for_redeploy_usd"] = round(total_value["cash_balance_usd"], 6)
    else:
        state["available_for_redeploy_usd"] = round(
            max(
                safe_float(state.get("available_for_redeploy_usd"), 0.0),
                safe_float(preflight.get("collateral_balance_usd"), 0.0),
                safe_float(state.get("settled_cash_released_usd"), 0.0),
            ),
            6,
        )

    claim_result = maybe_run_claim_hook(state["pending_settlements"], settings, paths["notifications"])

    report = {
        "status": "ok",
        "synced_at": now_iso_cst(),
        "profile_address": trader.profile_address,
        "remote_open_orders": len(open_orders),
        "remote_trades_considered": len(trades),
        "remote_positions": len(reconciled_positions),
        "new_fills": len(new_fills),
        **drift_report,
    }
    settlement_report = {
        "status": "ok",
        "synced_at": now_iso_cst(),
        "archived_count": archived_count,
        "released_cash_usd": round(released, 6),
        "pending_count": len(state.get("pending_settlements", [])),
        "claim_hook": claim_result,
    }

    if report.get("remote_only_tokens") or report.get("local_only_tokens"):
        report["status"] = "warn"
        append_notification(paths["notifications"], "reconciliation", "warn", "live drift detected", report)

    if archived_count:
        append_notification(
            paths["notifications"],
            "settlement",
            "info",
            f"archived {archived_count} settled position(s)",
            {"released_cash_usd": round(released, 6), "positions": archived_rows},
        )

    state["last_reconciliation"] = report
    state["last_settlement"] = settlement_report
    save_json(paths["reconciliation"], report)
    save_json(paths["settlement"], settlement_report)
    return state, report, settlement_report, new_fills


def execute_live(
    path: Path,
    journal_path: Path,
    notifications_path: Path,
    candidates: list[CandidateMarket],
    plan: list[dict[str, Any]],
    settings: dict[str, Any],
    preflight: dict[str, Any],
    trader: LiveTrader,
) -> tuple[dict[str, Any], int]:
    mode = settings["mode"]
    state = load_trading_state(path, mode, settings)
    errors = safe_int(state.get("consecutive_live_errors", 0))
    state["last_preflight"] = preflight
    accepted_orders = 0

    for item in plan:
        if item.get("action") != "open":
            continue
        journal_event = {
            "ts": now_iso_cst(),
            "mode": mode,
            "market_id": item.get("market_id"),
            "condition_id": item.get("condition_id"),
            "token_id": item.get("token_id"),
            "question": item.get("question"),
            "price": item.get("best_ask"),
            "shares": item.get("shares"),
            "order_size_usd": item.get("order_size_usd"),
        }
        try:
            response = trader.place_buy(
                token_id=safe_str(item.get("token_id")),
                price=safe_float(item.get("best_ask"), 0.0),
                shares=safe_float(item.get("shares"), 0.0),
            )
            journal_event["response"] = response
            if not response_success(response):
                raise LiveExecutionError(f"order rejected: {response}")
            accepted_orders += 1
            errors = 0
            journal_event["status"] = "submitted"
            append_notification(
                notifications_path,
                "order_submission",
                "info",
                f"live order submitted: {safe_str(item.get('question'))}",
                journal_event,
            )
        except Exception as exc:
            errors += 1
            journal_event["status"] = "error"
            journal_event["error"] = str(exc)
            append_notification(
                notifications_path,
                "order_submission",
                "error",
                f"live order submission failed: {safe_str(item.get('question'))}",
                journal_event,
            )
            append_jsonl(journal_path, journal_event)
            max_errors = safe_int(settings["live"].get("max_consecutive_errors"), 3)
            if errors >= max_errors:
                state["live_halted"] = True
                state["live_halt_reason"] = f"consecutive live errors >= {max_errors}"
                break
            continue
        append_jsonl(journal_path, journal_event)

    state["consecutive_live_errors"] = errors
    save_json(path, state)
    return state, accepted_orders


def mirror_legacy_outputs(base_dir: Path, config: dict[str, Any], trading_state_path: Path, daily_stats_path: Path) -> None:
    out = config.get("output", {}) if isinstance(config.get("output"), dict) else {}
    paper_state_path = (base_dir / out.get("paper_state_json", "state/paper_state.json")).resolve()
    daily_path = (base_dir / out.get("daily_stats_json", "state/daily_stats.json")).resolve()
    if trading_state_path.exists():
        paper_state_path.parent.mkdir(parents=True, exist_ok=True)
        paper_state_path.write_text(trading_state_path.read_text(encoding="utf-8"), encoding="utf-8")
    if daily_stats_path.exists():
        daily_path.parent.mkdir(parents=True, exist_ok=True)
        daily_path.write_text(daily_stats_path.read_text(encoding="utf-8"), encoding="utf-8")


def run_trading_cycle(
    base_dir: Path,
    config: dict[str, Any],
    candidates: list[CandidateMarket],
    cli_mode: str | None = None,
) -> dict[str, Any]:
    mode = resolve_mode(config, cli_mode)
    settings = build_mode_settings(config, mode)
    paths = get_mode_paths(base_dir, config, mode)
    paths["runtime_dir"].mkdir(parents=True, exist_ok=True)

    signal_state, confirmed_ids = update_signal_state(
        paths["signal_state"],
        candidates,
        safe_int(settings["execution"].get("confirm_scans_required"), 2),
    )

    state = load_trading_state(paths["trading_state"], mode, settings)
    preflight: dict[str, Any] = {"status": "skipped", "mode": mode}
    live_trader = None
    reconciliation_report: dict[str, Any] = {}
    settlement_report: dict[str, Any] = {}

    if mode == "live":
        env_file = (base_dir / safe_str(settings["live"].get("env_file"), ".env.live")).resolve()
        live_trader = LiveTrader(env_file, settings["live"])
        try:
            preflight = live_trader.preflight()
        except Exception as exc:
            preflight = {"status": "error", "message": str(exc), "env_file": str(env_file)}
            state["last_preflight"] = preflight
            save_json(paths["trading_state"], state)
    elif mode == "shadow":
        preflight = {"status": "shadow", "message": "shadow mode: no external orders submitted"}

    if mode == "live" and live_trader is not None and preflight.get("status") == "ok":
        state, reconciliation_report, settlement_report, _ = sync_live_state(
            state,
            live_trader,
            candidates,
            settings,
            paths,
            preflight,
        )
        save_json(paths["trading_state"], state)

    plan = build_execution_plan(candidates, state, settings, confirmed_ids, preflight)

    executed_orders = 0
    if mode == "live" and live_trader is not None and preflight.get("status") == "ok":
        state, executed_orders = execute_live(
            paths["trading_state"],
            paths["journal"],
            paths["notifications"],
            candidates,
            plan,
            settings,
            preflight,
            live_trader,
        )
        state, reconciliation_report, settlement_report, _ = sync_live_state(
            state,
            live_trader,
            candidates,
            settings,
            paths,
            preflight,
        )
        state = finalize_state(state, settings, candidates, plan, executed_orders, 0, settlement_report.get("archived_count", 0))
        save_json(paths["trading_state"], state)
    else:
        state = execute_simulated(paths["trading_state"], candidates, plan, settings, preflight)
        executed_orders = safe_int(state.get("totals", {}).get("orders_this_run", 0))

    daily_stats = update_daily_stats(
        paths["daily_stats"],
        candidates,
        plan,
        executed_orders,
        state.get("totals", {}) if isinstance(state.get("totals"), dict) else {},
    )

    if mode == "paper":
        mirror_legacy_outputs(base_dir, config, paths["trading_state"], paths["daily_stats"])

    summary = {
        "mode": mode,
        "last_run": now_iso_cst(),
        "confirmed_candidates": len(confirmed_ids),
        "signal_state_path": str(paths["signal_state"]),
        "trading_state_path": str(paths["trading_state"]),
        "daily_stats_path": str(paths["daily_stats"]),
        "reconciliation_path": str(paths["reconciliation"]),
        "settlement_path": str(paths["settlement"]),
        "status_snapshot_path": str(paths["status_snapshot"]),
        "preflight": preflight,
        "planned_orders": sum(1 for item in plan if item.get("action") == "open"),
        "executed_orders": executed_orders,
        "blocked_reasons": state.get("blocked_reasons", {}),
        "live_halted": bool(state.get("live_halted")),
        "live_halt_reason": state.get("live_halt_reason"),
        "reconciliation": reconciliation_report or state.get("last_reconciliation", {}),
        "settlement": settlement_report or state.get("last_settlement", {}),
        "available_for_redeploy_usd": round(safe_float(state.get("available_for_redeploy_usd"), 0.0), 2),
    }
    save_json(paths["summary"], summary)
    status_snapshot = write_status_snapshot(paths["status_snapshot"], state, summary, preflight)

    return {
        "mode": mode,
        "settings": settings,
        "paths": {key: str(value) for key, value in paths.items()},
        "signal_state": signal_state,
        "confirmed_ids": sorted(confirmed_ids),
        "plan": plan,
        "state": state,
        "daily_stats": daily_stats,
        "summary": summary,
        "status_snapshot": status_snapshot,
    }
