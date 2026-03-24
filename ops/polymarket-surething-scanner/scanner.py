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


def load_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


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


def persist_outputs(base_dir: Path, config: dict[str, Any], candidates: list[CandidateMarket], metrics: dict[str, Any]) -> None:
    out = config["output"]
    candidates_path = (base_dir / out["candidates_json"]).resolve()
    metrics_path = (base_dir / out["metrics_json"]).resolve()
    dashboard_path = (base_dir / out["dashboard_html"]).resolve()

    candidates_path.parent.mkdir(parents=True, exist_ok=True)
    candidates_path.write_text(json.dumps([c.to_dict() for c in candidates], ensure_ascii=False, indent=2), encoding="utf-8")
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    render_dashboard(candidates_path, metrics_path, dashboard_path)


def main() -> None:
    base = Path(__file__).resolve().parent
    cfg_path = base / "config.yaml"
    config = load_config(cfg_path)

    candidates, metrics = asyncio.run(run_scan(cfg_path))
    persist_outputs(base, config, candidates, metrics)
    print(json.dumps({"metrics": metrics, "candidates": [c.to_dict() for c in candidates]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
