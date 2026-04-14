from __future__ import annotations

import argparse
import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml

from dashboard import render_dashboard
from models import CandidateMarket
from trading import resolve_mode, run_trading_cycle

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


def load_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def build_scanner_settings(config: dict[str, Any], mode: str) -> dict[str, Any]:
    _ = mode
    return dict(config.get("scanner", {}) or {})


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


def build_market_haystack(market: dict[str, Any]) -> str:
    hay = " ".join(
        [
            str(market.get("category", "")),
            str(market.get("slug", "")),
            str(market.get("question", "")),
            str(market.get("description", "")),
            str(market.get("seriesSlug", "")),
        ]
    ).lower()

    events = market.get("events")
    if isinstance(events, list):
        for event in events:
            if not isinstance(event, dict):
                continue
            hay += " " + " ".join(
                [
                    str(event.get("slug", "")),
                    str(event.get("title", "")),
                    str(event.get("seriesSlug", "")),
                    str(event.get("ticker", "")),
                ]
            ).lower()
    return hay


def is_crypto_market(market: dict[str, Any]) -> bool:
    hay = build_market_haystack(market)
    tokens = set(re.findall(r"[a-z0-9]+", hay))
    crypto_words = ["crypto", "bitcoin", "ethereum", "solana", "doge", "cardano", "ripple"]
    crypto_tickers = {"btc", "eth", "sol", "xrp", "ada", "bnb"}
    return any(word in hay for word in crypto_words) or any(token in tokens for token in crypto_tickers)


def is_stock_related_market(market: dict[str, Any]) -> bool:
    hay = build_market_haystack(market)
    tokens = set(re.findall(r"[a-z0-9]+", hay))
    stock_words = {
        "stock",
        "stocks",
        "share",
        "shares",
        "equity",
        "equities",
        "nasdaq",
        "nyse",
        "s&p",
        "sp500",
        "dow",
        "dowjones",
        "earnings",
        "ipo",
        "etf",
        "tesla",
        "nvidia",
        "apple",
        "microsoft",
        "amazon",
        "meta",
        "google",
        "alphabet",
        "amd",
        "netflix",
    }
    stock_phrases = ["stock market", "share price", "earnings report", "earnings call"]
    return any(phrase in hay for phrase in stock_phrases) or any(token in tokens for token in stock_words)


def is_sports_related_market(market: dict[str, Any]) -> bool:
    hay = build_market_haystack(market)
    tokens = set(re.findall(r"[a-z0-9]+", hay))
    sports_words = {
        "sport",
        "sports",
        "nba",
        "nfl",
        "mlb",
        "nhl",
        "ufc",
        "fifa",
        "uefa",
        "champions",
        "premier",
        "league",
        "laliga",
        "bundesliga",
        "serie",
        "tennis",
        "golf",
        "soccer",
        "football",
        "baseball",
        "basketball",
        "hockey",
        "cricket",
        "rugby",
        "match",
        "tournament",
        "playoff",
        "finals",
        "worldcup",
        "olympics",
    }
    sports_phrases = ["super bowl", "world cup", "grand slam", "stanley cup"]
    return any(phrase in hay for phrase in sports_phrases) or any(token in tokens for token in sports_words)


def is_commodity_related_market(market: dict[str, Any]) -> bool:
    hay = build_market_haystack(market)
    tokens = set(re.findall(r"[a-z0-9]+", hay))
    commodity_words = {
        "commodity",
        "commodities",
        "gold",
        "silver",
        "oil",
        "crude",
        "brent",
        "wti",
        "naturalgas",
        "gas",
        "copper",
        "platinum",
        "palladium",
        "xauusd",
        "xagusd",
        "usdjpy",
        "eurusd",
        "gbpusd",
        "audusd",
        "nzdusd",
        "usdchf",
        "usdcad",
        "forex",
        "fx",
    }
    directional_phrases = ["up or down", "higher or lower", "go up or down", "price of"]
    return any(phrase in hay for phrase in directional_phrases) or any(token in tokens for token in commodity_words)


def is_high_randomness_narrative_market(market: dict[str, Any]) -> bool:
    hay = build_market_haystack(market)
    question = str(market.get("question", "")).lower()
    event_title = ""
    events = market.get("events")
    if isinstance(events, list) and events and isinstance(events[0], dict):
        event_title = str(events[0].get("title", "")).lower()

    conversational_context = any(
        phrase in hay
        for phrase in [
            "podcast",
            "episode",
            "livestream",
            "stream",
            "interview",
            "debate show",
            "town hall",
            "press conference",
            "earnings call",
            "spaces",
            "ama",
        ]
    )
    subjective_prompt = any(
        phrase in hay
        for phrase in [
            "what will be said",
            "will be said",
            "say the word",
            "mention the word",
            "be mentioned",
            "mention",
            "talk about",
            "discuss",
            "bring up",
            "reference",
        ]
    )
    next_show_pattern = question.startswith("what will be said on the next ") or (
        question.startswith("will ") and " on the next " in question
    )
    all_in_like = "all-in podcast" in hay or "all in podcast" in hay or event_title.endswith("podcast")

    return (conversational_context and subjective_prompt) or next_show_pattern or all_in_like


async def fetch_markets(client: httpx.AsyncClient, page_size: int) -> list[dict[str, Any]]:
    all_markets: list[dict[str, Any]] = []
    offset = 0
    while True:
        try:
            response = await client.get(
                f"{GAMMA_BASE}/markets",
                params={"closed": "false", "active": "true", "limit": page_size, "offset": offset},
                timeout=20,
            )
            response.raise_for_status()
            batch = response.json()
            if not isinstance(batch, list) or not batch:
                break
            all_markets.extend(batch)
            offset += page_size
        except Exception:
            break
    return all_markets


async def fetch_books(
    client: httpx.AsyncClient,
    token_ids: list[str],
    pause_sec: float,
    chunk_size: int,
) -> dict[str, dict[str, Any]]:
    books: dict[str, dict[str, Any]] = {}
    if not token_ids:
        return books

    for start in range(0, len(token_ids), chunk_size):
        batch = token_ids[start : start + chunk_size]
        try:
            response = await client.post(
                f"{CLOB_BASE}/books",
                json=[{"token_id": token_id} for token_id in batch],
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, list):
                for item in payload:
                    token = item.get("asset_id") or item.get("token_id")
                    if token:
                        books[str(token)] = item
            elif isinstance(payload, dict):
                for key, value in payload.items():
                    books[str(key)] = value
        except Exception:
            pass
        await asyncio.sleep(max(0.0, pause_sec))
    return books


async def fetch_price(client: httpx.AsyncClient, token_id: str) -> float | None:
    try:
        response = await client.get(f"{CLOB_BASE}/price", params={"token_id": token_id, "side": "BUY"}, timeout=12)
        response.raise_for_status()
        payload = response.json()
        price = payload.get("price") if isinstance(payload, dict) else None
        return float(price) if price is not None else None
    except Exception:
        return None


def get_best_ask(book: dict[str, Any]) -> float | None:
    asks = book.get("asks") if isinstance(book, dict) else None
    if not isinstance(asks, list) or not asks:
        return None
    prices = []
    for ask in asks:
        try:
            prices.append(float(ask.get("price")))
        except Exception:
            continue
    return min(prices) if prices else None


def depth_usd_upto(book: dict[str, Any], max_price: float) -> float:
    asks = book.get("asks") if isinstance(book, dict) else None
    if not isinstance(asks, list):
        return 0.0
    total_notional = 0.0
    for ask in asks:
        try:
            price = float(ask.get("price"))
            size = float(ask.get("size"))
            if price <= max_price:
                total_notional += price * size
        except Exception:
            continue
    return total_notional


async def run_scan(config_path: Path, mode: str) -> tuple[list[CandidateMarket], dict[str, Any]]:
    config = load_config(config_path)
    scanner_cfg = build_scanner_settings(config, mode)

    horizon = datetime.now(timezone.utc) + timedelta(hours=float(scanner_cfg["hours_ahead"]))
    price_min = float(scanner_cfg["price_min"])
    price_max = float(scanner_cfg["price_max"])
    min_depth = float(scanner_cfg["min_depth_usd"])
    page_size = int(scanner_cfg["gamma_page_size"])
    books_chunk_size = int(scanner_cfg.get("books_chunk_size", 60))
    pause_sec = float(scanner_cfg.get("clob_pause_sec", 0.5))
    stale_threshold = float(scanner_cfg.get("stale_disagree_threshold", 0.05))
    quick_yes_price_min = float(scanner_cfg.get("quick_yes_price_min", 0.90))
    quick_yes_price_max = float(scanner_cfg.get("quick_yes_price_max", 0.98))
    depth_price_cap = float(scanner_cfg.get("depth_price_cap", 0.97))
    exclude_crypto = bool(scanner_cfg.get("exclude_crypto", True))
    exclude_stock_related = bool(scanner_cfg.get("exclude_stock_related", True))
    exclude_sports_related = bool(scanner_cfg.get("exclude_sports_related", True))
    exclude_commodity_related = bool(scanner_cfg.get("exclude_commodity_related", True))
    exclude_high_randomness_narrative = bool(scanner_cfg.get("exclude_high_randomness_narrative", True))

    candidates: list[CandidateMarket] = []
    quick_pass: list[dict[str, Any]] = []
    stale_skips = 0
    crypto_skips = 0
    stock_skips = 0
    sports_skips = 0
    commodity_skips = 0
    high_randomness_narrative_skips = 0

    async with httpx.AsyncClient() as client:
        markets = await fetch_markets(client, page_size)

        for market in markets:
            if market.get("closed") or not market.get("active") or not market.get("enableOrderBook"):
                continue
            if exclude_crypto and is_crypto_market(market):
                crypto_skips += 1
                continue
            if exclude_stock_related and is_stock_related_market(market):
                stock_skips += 1
                continue
            if exclude_sports_related and is_sports_related_market(market):
                sports_skips += 1
                continue
            if exclude_commodity_related and is_commodity_related_market(market):
                commodity_skips += 1
                continue
            if exclude_high_randomness_narrative and is_high_randomness_narrative_market(market):
                high_randomness_narrative_skips += 1
                continue

            end_date = parse_iso(market.get("endDate"))
            if not end_date:
                continue
            now = datetime.now(timezone.utc)
            if end_date <= now or end_date > horizon:
                continue

            prices = parse_json_field(market.get("outcomePrices"))
            token_ids = parse_json_field(market.get("clobTokenIds"))
            if len(prices) < 1 or len(token_ids) < 1:
                continue

            try:
                yes_price = float(prices[0])
            except Exception:
                continue

            if yes_price < quick_yes_price_min or yes_price > quick_yes_price_max:
                continue

            quick_pass.append({"market": market, "yes_token": str(token_ids[0]), "quick_yes_price": round(yes_price, 4)})

        books = await fetch_books(
            client,
            [item["yes_token"] for item in quick_pass],
            pause_sec=pause_sec,
            chunk_size=max(1, books_chunk_size),
        )

        for item in quick_pass:
            market = item["market"]
            token_id = item["yes_token"]
            book = books.get(token_id)
            if not book:
                continue
            best_ask = get_best_ask(book)
            if best_ask is None:
                continue

            price = await fetch_price(client, token_id)
            if price is not None and abs(price - best_ask) > stale_threshold:
                stale_skips += 1
                continue

            depth_usd = depth_usd_upto(book, depth_price_cap)
            if best_ask < price_min or best_ask > price_max or depth_usd < min_depth:
                continue

            end_date = parse_iso(market.get("endDate"))
            if not end_date:
                continue

            events = market.get("events") if isinstance(market.get("events"), list) else []
            event_slug = ""
            if events and isinstance(events[0], dict):
                event_slug = str(events[0].get("slug") or "")
            if not event_slug:
                event_slug = str(market.get("eventSlug") or "")
            if not event_slug:
                event_slug = str(market.get("slug") or "")

            candidates.append(
                CandidateMarket(
                    market_id=str(market.get("id", "")),
                    condition_id=str(market.get("conditionId", "")),
                    token_id=token_id,
                    question=str(market.get("question", "")),
                    description=str(market.get("description", "")),
                    end_date=end_date,
                    best_ask=round(best_ask, 4),
                    depth_usd=round(depth_usd, 2),
                    resolution_source=str(market.get("resolutionSource", "")),
                    category_tag=str(market.get("category", "")),
                    volume=float(market.get("volume", 0) or 0),
                    slug=str(market.get("slug", "")),
                    event_slug=event_slug,
                    tick_size=float(book.get("tick_size") or 0.01),
                    min_order_size=float(book.get("min_order_size") or 0.0),
                    neg_risk=bool(book.get("neg_risk", False)),
                )
            )

    metrics = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "total_scanned": len(markets) if "markets" in locals() else 0,
        "quick_pass_count": len(quick_pass),
        "candidates_count": len(candidates),
        "stale_skips": stale_skips,
        "crypto_skips": crypto_skips,
        "stock_skips": stock_skips,
        "sports_skips": sports_skips,
        "commodity_skips": commodity_skips,
        "high_randomness_narrative_skips": high_randomness_narrative_skips,
        "exclude_crypto": exclude_crypto,
        "exclude_stock_related": exclude_stock_related,
        "exclude_sports_related": exclude_sports_related,
        "exclude_commodity_related": exclude_commodity_related,
        "exclude_high_randomness_narrative": exclude_high_randomness_narrative,
        "scan_mode": mode,
        "thresholds": {
            "hours_ahead": float(scanner_cfg["hours_ahead"]),
            "quick_yes_price_min": quick_yes_price_min,
            "quick_yes_price_max": quick_yes_price_max,
            "price_min": price_min,
            "price_max": price_max,
            "min_depth_usd": min_depth,
            "depth_price_cap": depth_price_cap,
            "stale_disagree_threshold": stale_threshold,
            "gamma_page_size": page_size,
            "books_chunk_size": books_chunk_size,
            "clob_pause_sec": pause_sec,
        },
    }
    return candidates, metrics


def persist_outputs(
    base_dir: Path,
    config: dict[str, Any],
    candidates: list[CandidateMarket],
    metrics: dict[str, Any],
    trading_result: dict[str, Any],
) -> None:
    output_cfg = config["output"]
    candidates_path = (base_dir / output_cfg["candidates_json"]).resolve()
    metrics_path = (base_dir / output_cfg["metrics_json"]).resolve()
    dashboard_path = (base_dir / output_cfg["dashboard_html"]).resolve()

    candidates_path.parent.mkdir(parents=True, exist_ok=True)
    candidates_path.write_text(json.dumps([candidate.to_dict() for candidate in candidates], ensure_ascii=False, indent=2), encoding="utf-8")
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

    render_dashboard(
        candidates_path,
        metrics_path,
        dashboard_path,
        trading_state_path=Path(trading_result["paths"]["trading_state"]),
        daily_stats_path=Path(trading_result["paths"]["daily_stats"]),
        runtime_summary_path=Path(trading_result["paths"]["summary"]),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket Sure-Thing scanner / trader")
    parser.add_argument("--mode", choices=["paper", "shadow", "live"], default=None, help="execution mode override")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent
    config_path = base_dir / "config.yaml"
    config = load_config(config_path)
    mode = resolve_mode(config, args.mode)

    candidates, metrics = asyncio.run(run_scan(config_path, mode=mode))
    trading_result = run_trading_cycle(base_dir, config, candidates, cli_mode=mode)
    persist_outputs(base_dir, config, candidates, metrics, trading_result)

    print(
        json.dumps(
            {
                "metrics": metrics,
                "trading": trading_result["summary"],
                "candidates": [candidate.to_dict() for candidate in candidates],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
