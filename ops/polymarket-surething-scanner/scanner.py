from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml

from dashboard import render_dashboard
from models import CandidateMarket

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
CST = timezone(timedelta(hours=8))


def load_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def parse_json_field(value: Any, default: list[Any] | None = None) -> list[Any]:
    if default is None:
        default = []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else default
        except Exception:
            return default
    return default


def parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def is_restricted_market(m: dict[str, Any]) -> bool:
    if bool(m.get("restricted", False)):
        return True
    events = m.get("events")
    if isinstance(events, list):
        for e in events:
            if isinstance(e, dict) and bool(e.get("restricted", False)):
                return True
    return False


def is_crypto_market(m: dict[str, Any]) -> bool:
    hay = " ".join(
        [
            str(m.get("category", "")),
            str(m.get("slug", "")),
            str(m.get("question", "")),
            str(m.get("description", "")),
            str(m.get("seriesSlug", "")),
        ]
    ).lower()

    events = m.get("events")
    if isinstance(events, list):
        for e in events:
            if not isinstance(e, dict):
                continue
            hay += " " + " ".join(
                [
                    str(e.get("slug", "")),
                    str(e.get("title", "")),
                    str(e.get("seriesSlug", "")),
                    str(e.get("ticker", "")),
                ]
            ).lower()

    crypto_tokens = [
        "crypto",
        "bitcoin",
        "btc",
        "ethereum",
        "eth",
        "solana",
        "sol",
        "doge",
        "xrp",
        "ada",
        "bnb",
    ]
    return any(t in hay for t in crypto_tokens)


async def fetch_markets(client: httpx.AsyncClient, page_size: int) -> list[dict[str, Any]]:
    all_markets: list[dict[str, Any]] = []
    offset = 0
    while True:
        try:
            r = await client.get(
                f"{GAMMA_BASE}/markets",
                params={"closed": "false", "active": "true", "limit": page_size, "offset": offset},
                timeout=20,
            )
            r.raise_for_status()
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            all_markets.extend(batch)
            offset += page_size
        except Exception:
            break
    return all_markets


async def fetch_books(client: httpx.AsyncClient, token_ids: list[str]) -> dict[str, dict[str, Any]]:
    books: dict[str, dict[str, Any]] = {}
    if not token_ids:
        return books

    chunk = 60
    for i in range(0, len(token_ids), chunk):
        sub = token_ids[i : i + chunk]
        try:
            r = await client.post(
                f"{CLOB_BASE}/books",
                json=[{"token_id": t} for t in sub],
                timeout=30,
            )
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, list):
                for item in payload:
                    token = item.get("asset_id") or item.get("token_id")
                    if token:
                        books[str(token)] = item
            elif isinstance(payload, dict):
                for k, v in payload.items():
                    books[str(k)] = v
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return books


async def fetch_price(client: httpx.AsyncClient, token_id: str) -> float | None:
    try:
        r = await client.get(f"{CLOB_BASE}/price", params={"token_id": token_id, "side": "BUY"}, timeout=12)
        r.raise_for_status()
        data = r.json()
        p = data.get("price") if isinstance(data, dict) else None
        return float(p) if p is not None else None
    except Exception:
        return None


def get_best_ask(book: dict[str, Any]) -> float | None:
    asks = book.get("asks") if isinstance(book, dict) else None
    if not isinstance(asks, list) or not asks:
        return None
    prices = []
    for a in asks:
        try:
            prices.append(float(a.get("price")))
        except Exception:
            continue
    return min(prices) if prices else None


def depth_usd_upto(book: dict[str, Any], max_price: float) -> float:
    asks = book.get("asks") if isinstance(book, dict) else None
    if not isinstance(asks, list):
        return 0.0
    total_notional = 0.0
    for a in asks:
        try:
            p = float(a.get("price"))
            s = float(a.get("size"))
            if p <= max_price:
                total_notional += p * s
        except Exception:
            continue
    return total_notional


async def run_scan(config_path: Path) -> tuple[list[CandidateMarket], dict[str, Any]]:
    cfg = load_config(config_path)
    sc = cfg["scanner"]

    horizon = datetime.now(timezone.utc) + timedelta(hours=float(sc["hours_ahead"]))
    price_min = float(sc["price_min"])
    price_max = float(sc["price_max"])
    min_depth = float(sc["min_depth_usd"])
    page_size = int(sc["gamma_page_size"])
    stale_threshold = float(sc.get("stale_disagree_threshold", 0.05))

    candidates: list[CandidateMarket] = []
    quick_pass: list[dict[str, Any]] = []
    stale_skips = 0
    restricted_skips = 0
    crypto_skips = 0

    async with httpx.AsyncClient() as client:
        markets = await fetch_markets(client, page_size)

        for m in markets:
            if m.get("closed") or not m.get("active") or not m.get("enableOrderBook"):
                continue
            if is_restricted_market(m):
                restricted_skips += 1
                continue
            if is_crypto_market(m):
                crypto_skips += 1
                continue
            end_date = parse_iso(m.get("endDate"))
            if not end_date:
                continue
            now = datetime.now(timezone.utc)
            if end_date <= now or end_date > horizon:
                continue

            prices = parse_json_field(m.get("outcomePrices"))
            token_ids = parse_json_field(m.get("clobTokenIds"))
            if len(prices) < 1 or len(token_ids) < 1:
                continue

            try:
                yes_price = float(prices[0])
            except Exception:
                continue

            if yes_price < 0.90 or yes_price > 0.98:
                continue

            quick_pass.append({"market": m, "yes_token": str(token_ids[0])})

        books = await fetch_books(client, [x["yes_token"] for x in quick_pass])

        for item in quick_pass:
            m = item["market"]
            token_id = item["yes_token"]
            b = books.get(token_id)
            if not b:
                continue
            best_ask = get_best_ask(b)
            if best_ask is None:
                continue

            px = await fetch_price(client, token_id)
            if px is not None and abs(px - best_ask) > stale_threshold:
                stale_skips += 1
                continue

            depth_usd = depth_usd_upto(b, 0.97)
            if best_ask < price_min or best_ask > price_max or depth_usd < min_depth:
                continue

            end_date = parse_iso(m.get("endDate"))
            if not end_date:
                continue

            events = m.get("events") if isinstance(m.get("events"), list) else []
            event_slug = ""
            if events and isinstance(events[0], dict):
                event_slug = str(events[0].get("slug") or "")
            if not event_slug:
                event_slug = str(m.get("eventSlug") or "")
            if not event_slug:
                # Fallback for single-market events where market slug == event slug.
                event_slug = str(m.get("slug") or "")

            candidates.append(
                CandidateMarket(
                    market_id=str(m.get("id", "")),
                    condition_id=str(m.get("conditionId", "")),
                    token_id=token_id,
                    question=str(m.get("question", "")),
                    description=str(m.get("description", "")),
                    end_date=end_date,
                    best_ask=round(best_ask, 4),
                    depth_usd=round(depth_usd, 2),
                    resolution_source=str(m.get("resolutionSource", "")),
                    category_tag=str(m.get("category", "")),
                    volume=float(m.get("volume", 0) or 0),
                    slug=str(m.get("slug", "")),
                    event_slug=event_slug,
                )
            )

    metrics = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "total_scanned": len(markets) if 'markets' in locals() else 0,
        "quick_pass_count": len(quick_pass),
        "candidates_count": len(candidates),
        "stale_skips": stale_skips,
        "restricted_skips": restricted_skips,
        "crypto_skips": crypto_skips,
    }
    return candidates, metrics


def update_daily_stats(daily_stats_path: Path, candidates: list[CandidateMarket]) -> dict[str, Any]:
    now_cst = datetime.now(CST)
    day_key = now_cst.strftime("%Y-%m-%d")

    state = load_json(daily_stats_path, {"by_day": {}})
    by_day = state.setdefault("by_day", {})
    day = by_day.setdefault(
        day_key,
        {
            "scans": 0,
            "total_candidates_seen": 0,
            "latest_candidates_count": 0,
            "unique_market_ids": [],
            "orders_placed": 0,
        },
    )

    day["scans"] = int(day.get("scans", 0)) + 1
    day["latest_candidates_count"] = len(candidates)
    day["total_candidates_seen"] = int(day.get("total_candidates_seen", 0)) + len(candidates)

    unique_ids = set(day.get("unique_market_ids", []))
    unique_ids.update(c.market_id for c in candidates if c.market_id)
    day["unique_market_ids"] = sorted(unique_ids)
    day["unique_candidates_count"] = len(day["unique_market_ids"])

    daily_stats_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state


def update_paper_state(paper_state_path: Path, candidates: list[CandidateMarket], daily_stats_state: dict[str, Any]) -> dict[str, Any]:
    now_cst = datetime.now(CST)
    now_iso = now_cst.isoformat()
    day_key = now_cst.strftime("%Y-%m-%d")

    state = load_json(
        paper_state_path,
        {
            "positions": [],
            "totals": {},
            "last_run": None,
            "daily_orders": {},
            "order_size_usd": 1.0,
        },
    )
    positions = state.setdefault("positions", [])
    existing = {str(p.get("market_id", "")): p for p in positions}

    candidates_by_market = {c.market_id: c for c in candidates if c.market_id}
    opened_new = 0
    added_existing = 0

    for mid, c in candidates_by_market.items():
        entry = float(c.best_ask)
        if entry <= 0:
            continue
        size_usd = 1.0
        add_shares = size_usd / entry

        if mid in existing:
            p = existing[mid]
            old_shares = float(p.get("shares", 0) or 0)
            old_size = float(p.get("size_usd", 0) or 0)
            new_shares = old_shares + add_shares
            new_size = old_size + size_usd
            avg_entry = (old_size + size_usd) / new_shares if new_shares > 0 else entry

            p["shares"] = round(new_shares, 8)
            p["size_usd"] = round(new_size, 2)
            p["entry_price"] = round(avg_entry, 4)
            p["last_added_at"] = now_iso
            added_existing += 1
            continue

        positions.append(
            {
                "market_id": c.market_id,
                "token_id": c.token_id,
                "question": c.question,
                "slug": c.slug,
                "opened_at": now_iso,
                "entry_price": round(entry, 4),
                "size_usd": round(size_usd, 2),
                "shares": round(add_shares, 8),
                "last_mark": round(entry, 4),
                "unrealized_pnl": 0.0,
            }
        )
        opened_new += 1
    # update marks + pnl using latest candidate best ask if available
    total_invested = 0.0
    total_unrealized = 0.0
    for p in positions:
        mid = str(p.get("market_id", ""))
        c = candidates_by_market.get(mid)
        if c is not None:
            p["last_mark"] = round(float(c.best_ask), 4)
        entry = float(p.get("entry_price", 0) or 0)
        mark = float(p.get("last_mark", entry) or entry)
        shares = float(p.get("shares", 0) or 0)
        size_usd = float(p.get("size_usd", 0) or 0)
        pnl = (mark - entry) * shares
        p["unrealized_pnl"] = round(pnl, 4)
        total_invested += size_usd
        total_unrealized += pnl

    orders_this_run = opened_new + added_existing

    state["positions"] = positions
    state["last_run"] = now_iso
    daily_orders = state.setdefault("daily_orders", {})
    daily_orders[day_key] = int(daily_orders.get(day_key, 0)) + orders_this_run
    state["totals"] = {
        "positions": len(positions),
        "invested_usd": round(total_invested, 2),
        "unrealized_pnl_usd": round(total_unrealized, 4),
        "equity_usd": round(total_invested + total_unrealized, 4),
        "opened_new_this_run": opened_new,
        "added_existing_this_run": added_existing,
        "orders_this_run": orders_this_run,
        "opened_today": int(daily_orders.get(day_key, 0)),
    }

    # sync daily order count into daily stats view
    by_day = daily_stats_state.setdefault("by_day", {})
    day = by_day.setdefault(day_key, {})
    day["orders_placed"] = int(daily_orders.get(day_key, 0))

    paper_state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state


def persist_outputs(base_dir: Path, config: dict[str, Any], candidates: list[CandidateMarket], metrics: dict[str, Any]) -> None:
    out = config["output"]
    candidates_path = (base_dir / out["candidates_json"]).resolve()
    metrics_path = (base_dir / out["metrics_json"]).resolve()
    dashboard_path = (base_dir / out["dashboard_html"]).resolve()
    paper_state_path = (base_dir / out.get("paper_state_json", "state/paper_state.json")).resolve()
    daily_stats_path = (base_dir / out.get("daily_stats_json", "state/daily_stats.json")).resolve()

    candidates_path.parent.mkdir(parents=True, exist_ok=True)
    candidates_path.write_text(json.dumps([c.to_dict() for c in candidates], ensure_ascii=False, indent=2), encoding="utf-8")
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

    daily_stats_state = update_daily_stats(daily_stats_path, candidates)
    update_paper_state(paper_state_path, candidates, daily_stats_state)
    daily_stats_path.write_text(json.dumps(daily_stats_state, ensure_ascii=False, indent=2), encoding="utf-8")

    render_dashboard(candidates_path, metrics_path, dashboard_path, paper_state_path, daily_stats_path)


def main() -> None:
    base = Path(__file__).resolve().parent
    cfg_path = base / "config.yaml"
    config = load_config(cfg_path)

    candidates, metrics = asyncio.run(run_scan(cfg_path))
    persist_outputs(base, config, candidates, metrics)
    print(json.dumps({"metrics": metrics, "candidates": [c.to_dict() for c in candidates]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
