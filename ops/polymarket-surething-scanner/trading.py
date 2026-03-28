from __future__ import annotations

import json
import os
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams, OpenOrderParams, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

from models import CandidateMarket

CLOB_BASE = "https://clob.polymarket.com"
CST = timezone(timedelta(hours=8))
VALID_MODES = {"paper", "shadow", "live"}


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


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


def now_cst() -> datetime:
    return datetime.now(CST)


def now_iso_cst() -> str:
    return now_cst().isoformat()


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


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
    }


def load_trading_state(path: Path, mode: str, settings: dict[str, Any]) -> dict[str, Any]:
    state = load_json(path, default_trading_state(mode, settings))
    if not isinstance(state, dict):
        state = default_trading_state(mode, settings)
    state.setdefault("positions", [])
    state.setdefault("closed_positions", [])
    state.setdefault("daily_orders", {})
    state.setdefault("blocked_reasons", {})
    state.setdefault("last_plan", [])
    state.setdefault("last_preflight", {})
    state.setdefault("live_halted", False)
    state.setdefault("live_halt_reason", None)
    state.setdefault("consecutive_live_errors", 0)
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
        entry = safe_float(position.get("entry_price"), 0.0)
        mark = safe_float(position.get("last_mark"), entry)
        shares = safe_float(position.get("shares"), 0.0)
        size_usd = safe_float(position.get("size_usd"), 0.0)
        pnl = (mark - entry) * shares
        position["unrealized_pnl"] = round(pnl, 4)
        total_open_cost += size_usd
        total_deployed_now += mark * shares
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

    now = datetime.now(timezone.utc)
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
        shares = round(order_size_usd / candidate.best_ask, 8) if candidate.best_ask > 0 else 0.0

        reason = None
        if parse_bool(state.get("live_halted"), False) and mode == "live":
            reason = "live_halted"
        elif preflight.get("status") == "error":
            reason = "preflight_failed"
        elif market_id not in confirmed_ids:
            reason = f"await_confirm_{confirm_runs_required}_runs"
        elif candidate.restricted and mode in {"shadow", "live"}:
            reason = "restricted_market"
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
            "token_id": candidate.token_id,
            "question": candidate.question,
            "slug": candidate.slug,
            "event_slug": candidate.event_slug,
            "opened_at": now_iso,
            "entry_price": round(entry, 4),
            "size_usd": round(size_usd, 2),
            "shares": round(shares, 8),
            "last_mark": round(entry, 4),
            "unrealized_pnl": 0.0,
            "restricted": candidate.restricted,
            "last_order": plan_item,
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
        closed_at = str(closed.get("closed_at") or "")
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
    executed_plan_items: list[dict[str, Any]] = []
    for item in plan:
        if item.get("action") != "open":
            continue
        candidate = candidates_by_market.get(str(item.get("market_id", "")))
        if candidate is None:
            continue
        opened, added = _append_position(positions, candidate, item, allow_add)
        opened_new += opened
        added_existing += added
        executed_plan_items.append(item)

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
        self.chain_id = safe_int(os.getenv("POLYMARKET_CHAIN_ID", 137), 137)
        self.private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
        self.signature_type = safe_int(os.getenv("POLYMARKET_SIGNATURE_TYPE", live_cfg.get("signature_type", 0)), 0)
        self.funder = (os.getenv("POLYMARKET_FUNDER") or str(live_cfg.get("funder") or "")).strip() or None
        self.api_key = os.getenv("POLYMARKET_API_KEY", "").strip()
        self.api_secret = os.getenv("POLYMARKET_API_SECRET", "").strip()
        self.api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()
        self.live_enabled_env = parse_bool(os.getenv("POLYMARKET_LIVE_ENABLED"), False)
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
                    return safe_float(allowance_payload.get(key), 0.0)
            for value in allowance_payload.values():
                balance = self._extract_balance(value)
                if balance > 0:
                    return balance
        return 0.0

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
            "chain_id": self.chain_id,
            "signature_type": self.signature_type,
            "funder_present": bool(self.funder),
            "api_key_present": bool(self.api_key),
            "env_file": str(self.env_file),
            "env_live_enabled": self.live_enabled_env,
            "collateral_balance_usd": round(collateral_balance, 4),
            "open_orders_count": open_orders_count,
        }

    def place_buy(self, token_id: str, price: float, shares: float) -> dict[str, Any]:
        if self.client is None:
            raise LiveExecutionError("client not initialized")
        order = self.client.create_order(OrderArgs(token_id=token_id, price=price, size=shares, side=BUY))
        order_type_name = str(self.live_cfg.get("order_type", "FOK")).upper()
        order_type = getattr(OrderType, order_type_name, OrderType.FOK)
        post_only = parse_bool(self.live_cfg.get("post_only"), False)
        if post_only and order_type_name not in {"GTC", "GTD"}:
            post_only = False
        return self.client.post_order(order, order_type, post_only=post_only)


def response_success(response: Any) -> bool:
    if isinstance(response, dict):
        if response.get("success") is False:
            return False
        status = str(response.get("status") or response.get("state") or "").lower()
        if status in {"rejected", "cancelled", "canceled", "failed", "error"}:
            return False
        if response.get("error") or response.get("errorMsg") or response.get("message") == "error":
            return False
    return True


def execute_live(
    path: Path,
    journal_path: Path,
    candidates: list[CandidateMarket],
    plan: list[dict[str, Any]],
    settings: dict[str, Any],
    preflight: dict[str, Any],
    trader: LiveTrader,
) -> dict[str, Any]:
    mode = settings["mode"]
    state = load_trading_state(path, mode, settings)
    positions = state.setdefault("positions", [])
    allow_add = parse_bool(settings["execution"].get("allow_add_to_existing"), False)
    candidates_by_market = {candidate.market_id: candidate for candidate in candidates if candidate.market_id}

    opened_new = 0
    added_existing = 0
    errors = safe_int(state.get("consecutive_live_errors", 0))
    state["last_preflight"] = preflight

    for item in plan:
        if item.get("action") != "open":
            continue
        market_id = str(item.get("market_id", ""))
        candidate = candidates_by_market.get(market_id)
        if candidate is None:
            continue
        journal_event = {
            "ts": now_iso_cst(),
            "mode": mode,
            "market_id": market_id,
            "token_id": item.get("token_id"),
            "question": item.get("question"),
            "price": item.get("best_ask"),
            "shares": item.get("shares"),
            "order_size_usd": item.get("order_size_usd"),
        }
        try:
            response = trader.place_buy(
                token_id=str(item.get("token_id")),
                price=safe_float(item.get("best_ask"), 0.0),
                shares=safe_float(item.get("shares"), 0.0),
            )
            journal_event["response"] = response
            if not response_success(response):
                raise LiveExecutionError(f"order rejected: {response}")
            opened, added = _append_position(positions, candidate, item, allow_add)
            opened_new += opened
            added_existing += added
            errors = 0
            journal_event["status"] = "filled_assumed"
        except Exception as exc:
            errors += 1
            journal_event["status"] = "error"
            journal_event["error"] = str(exc)
            append_jsonl(journal_path, journal_event)
            max_errors = safe_int(settings["live"].get("max_consecutive_errors"), 3)
            if errors >= max_errors:
                state["live_halted"] = True
                state["live_halt_reason"] = f"consecutive live errors >= {max_errors}"
                break
            continue
        append_jsonl(journal_path, journal_event)

    state["consecutive_live_errors"] = errors
    state = finalize_state(state, settings, candidates, plan, opened_new, added_existing)
    save_json(path, state)
    return state


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

    if mode == "live":
        env_file = (base_dir / str(settings["live"].get("env_file", ".env.live"))).resolve()
        live_trader = LiveTrader(env_file, settings["live"])
        try:
            preflight = live_trader.preflight()
        except Exception as exc:
            preflight = {"status": "error", "message": str(exc), "env_file": str(env_file)}
            state["last_preflight"] = preflight
            save_json(paths["trading_state"], state)
    elif mode == "shadow":
        preflight = {"status": "shadow", "message": "shadow mode: no external orders submitted"}

    plan = build_execution_plan(candidates, state, settings, confirmed_ids, preflight)

    if mode == "live" and live_trader is not None and preflight.get("status") == "ok":
        state = execute_live(paths["trading_state"], paths["journal"], candidates, plan, settings, preflight, live_trader)
    else:
        state = execute_simulated(paths["trading_state"], candidates, plan, settings, preflight)

    daily_stats = update_daily_stats(
        paths["daily_stats"],
        candidates,
        plan,
        safe_int(state.get("totals", {}).get("orders_this_run", 0)),
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
        "preflight": preflight,
        "planned_orders": sum(1 for item in plan if item.get("action") == "open"),
        "executed_orders": safe_int(state.get("totals", {}).get("orders_this_run", 0)),
        "blocked_reasons": state.get("blocked_reasons", {}),
        "live_halted": bool(state.get("live_halted")),
        "live_halt_reason": state.get("live_halt_reason"),
    }
    save_json(paths["summary"], summary)

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
    }
