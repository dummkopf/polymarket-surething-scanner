#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import dotenv_values

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


UTC = timezone.utc
CN_TZ = ZoneInfo("Asia/Shanghai") if ZoneInfo else UTC

WEATHER_SECTION_URL = "https://polymarket.com/climate-science/weather"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"

MONTH_TO_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

CITY_COORDS = {
    "nyc": (40.7128, -74.0060),
    "new-york-city": (40.7128, -74.0060),
    "seoul": (37.5665, 126.9780),
    "london": (51.5072, -0.1276),
    "wellington": (-41.2866, 174.7756),
    "toronto": (43.6511, -79.3470),
    "atlanta": (33.7490, -84.3880),
    "dallas": (32.7767, -96.7970),
    "ankara": (39.9334, 32.8597),
    "paris": (48.8566, 2.3522),
    "lucknow": (26.8467, 80.9462),
    "chicago": (41.8781, -87.6298),
    "buenos-aires": (-34.6037, -58.3816),
    "seattle": (47.6062, -122.3321),
}


@dataclass
class Config:
    trade_size_usd: float = 3.0
    max_open_exposure_usd: float = 20.0
    daily_stop_loss_usd: float = -10.0

    target_core_ratio: float = 0.7
    target_tail_ratio: float = 0.3

    min_hours_to_expiry: float = 12.0
    min_liquidity: float = 500.0
    max_yes_spread: float = 0.03

    core_prob_min: float = 0.75
    core_edge_min: float = 0.03

    tail_prob_max: float = 0.25
    tail_edge_min: float = 0.08
    tail_price_max: float = 0.20

    request_timeout_sec: int = 20


@dataclass
class ParsedContract:
    slug: str
    city_slug: str
    target_date: str  # YYYY-MM-DD
    unit: str  # C/F
    lower: Optional[float]
    upper: Optional[float]


@dataclass
class Signal:
    slug: str
    question: str
    city_slug: str
    target_date: str
    side: str  # YES/NO
    category: str  # core/tail

    side_prob: float
    side_price: float
    edge: float

    yes_price: float
    no_price: float
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    yes_spread: Optional[float]

    forecast_max_c: float
    sigma_c: float

    end_date: str
    liquidity: float


def iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def parse_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def norm_cdf(x: float, mu: float, sigma: float) -> float:
    if sigma <= 0:
        return 1.0 if x >= mu else 0.0
    z = (x - mu) / (sigma * math.sqrt(2))
    return 0.5 * (1 + math.erf(z))


def sigma_by_horizon_days(h: float) -> float:
    # Conservative baseline for daily max-temp forecast uncertainty.
    # 0d: ~1.6C, 1d: ~2.0C, 2d: ~2.4C
    h = max(0.0, h)
    return min(4.0, 1.6 + 0.4 * h)


def load_env(env_path: Path) -> Dict[str, str]:
    values = dotenv_values(env_path)
    return {k: v for k, v in values.items() if isinstance(v, str)}


def ensure_dirs(paths: List[Path]) -> None:
    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)


def fetch_weather_slugs(timeout_sec: int) -> List[str]:
    html = requests.get(WEATHER_SECTION_URL, timeout=timeout_sec).text

    slugs = set()
    slugs.update(re.findall(r"marketSlug=([a-z0-9\-]+)", html))
    slugs.update(re.findall(r"/event/[a-z0-9\-]+/([a-z0-9\-]+)", html))

    # Keep weather contract slugs only (temperature bins for now).
    filtered = sorted(s for s in slugs if s.startswith("highest-temperature-in-"))
    return filtered


def fetch_market_by_slug(slug: str, timeout_sec: int) -> Optional[Dict[str, Any]]:
    resp = requests.get(GAMMA_MARKETS_URL, params={"slug": slug}, timeout=timeout_sec)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list) and data:
        return data[0]
    return None


def parse_temperature_slug(slug: str) -> Optional[ParsedContract]:
    pattern = (
        r"^highest-temperature-in-(?P<city>[a-z0-9\-]+)-on-"
        r"(?P<month>[a-z]+)-(?P<day>\d{1,2})-(?P<year>\d{4})-"
        r"(?P<bucket>[a-z0-9\-]+)$"
    )
    m = re.match(pattern, slug)
    if not m:
        return None

    city_slug = m.group("city")
    month_txt = m.group("month")
    day = int(m.group("day"))
    year = int(m.group("year"))
    bucket = m.group("bucket")

    month = MONTH_TO_NUM.get(month_txt)
    if month is None:
        return None

    try:
        date_obj = datetime(year, month, day)
    except ValueError:
        return None

    unit = "C"
    lower: Optional[float] = None
    upper: Optional[float] = None

    # e.g. 52-53f / 11-12c
    m_range = re.match(r"^(\d+)-(\d+)([fc])$", bucket)
    if m_range:
        a = float(m_range.group(1))
        b = float(m_range.group(2))
        unit = m_range.group(3).upper()
        # integer range bucket => [a-0.5, b+0.5)
        lower = a - 0.5
        upper = b + 0.5
    else:
        # e.g. 51forbelow / 19corhigher
        m_below = re.match(r"^(\d+)([fc])orbelow$", bucket)
        m_above = re.match(r"^(\d+)([fc])orhigher$", bucket)
        m_exact = re.match(r"^(\d+)([fc])$", bucket)

        if m_below:
            t = float(m_below.group(1))
            unit = m_below.group(2).upper()
            lower = None
            upper = t + 0.5
        elif m_above:
            t = float(m_above.group(1))
            unit = m_above.group(2).upper()
            lower = t - 0.5
            upper = None
        elif m_exact:
            t = float(m_exact.group(1))
            unit = m_exact.group(2).upper()
            lower = t - 0.5
            upper = t + 0.5
        else:
            return None

    return ParsedContract(
        slug=slug,
        city_slug=city_slug,
        target_date=date_obj.strftime("%Y-%m-%d"),
        unit=unit,
        lower=lower,
        upper=upper,
    )


def geocode_city(city_slug: str, timeout_sec: int) -> Optional[Tuple[float, float]]:
    if city_slug in CITY_COORDS:
        return CITY_COORDS[city_slug]

    city_name = city_slug.replace("-", " ")
    try:
        resp = requests.get(
            OPEN_METEO_GEOCODE_URL,
            params={"name": city_name, "count": 1, "language": "en", "format": "json"},
            timeout=timeout_sec,
        )
        resp.raise_for_status()
        results = resp.json().get("results") or []
        if not results:
            return None
        lat = float(results[0]["latitude"])
        lon = float(results[0]["longitude"])
        CITY_COORDS[city_slug] = (lat, lon)
        return lat, lon
    except Exception:
        return None


def fetch_forecast_max_temp_c(
    city_slug: str,
    target_date: str,
    timeout_sec: int,
    cache: Dict[Tuple[str, str], Optional[float]],
) -> Optional[float]:
    key = (city_slug, target_date)
    if key in cache:
        return cache[key]

    coords = geocode_city(city_slug, timeout_sec)
    if not coords:
        cache[key] = None
        return None

    lat, lon = coords
    try:
        resp = requests.get(
            OPEN_METEO_FORECAST_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max",
                "timezone": "UTC",
                "forecast_days": 16,
            },
            timeout=timeout_sec,
        )
        resp.raise_for_status()
        daily = resp.json().get("daily") or {}
        dates = daily.get("time") or []
        temps = daily.get("temperature_2m_max") or []

        date_to_temp = {d: t for d, t in zip(dates, temps)}
        value = parse_float(date_to_temp.get(target_date))
        cache[key] = value
        return value
    except Exception:
        cache[key] = None
        return None


def prob_yes_from_contract(
    forecast_max_c: float,
    sigma_c: float,
    contract: ParsedContract,
) -> float:
    if contract.unit == "F":
        mu = forecast_max_c * 9 / 5 + 32
        sigma = sigma_c * 9 / 5
    else:
        mu = forecast_max_c
        sigma = sigma_c

    lower = contract.lower
    upper = contract.upper

    if lower is None and upper is None:
        return 0.0

    if lower is None:
        return max(0.0, min(1.0, norm_cdf(upper, mu, sigma)))

    if upper is None:
        return max(0.0, min(1.0, 1 - norm_cdf(lower, mu, sigma)))

    p = norm_cdf(upper, mu, sigma) - norm_cdf(lower, mu, sigma)
    return max(0.0, min(1.0, p))


def parse_iso_to_utc(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def market_prices(market: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    yes_bid = parse_float(market.get("bestBid"))
    yes_ask = parse_float(market.get("bestAsk"))

    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None

    yes_px = None
    no_px = None
    if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
        yes_px = parse_float(outcome_prices[0])
        no_px = parse_float(outcome_prices[1])

    if yes_ask is None:
        yes_ask = yes_px
    if yes_bid is None:
        yes_bid = yes_px

    if no_px is None and yes_px is not None:
        no_px = max(0.0, min(1.0, 1 - yes_px))

    yes_spread = None
    if yes_ask is not None and yes_bid is not None:
        yes_spread = yes_ask - yes_bid

    return yes_bid, yes_ask, yes_spread, no_px


def build_signals(config: Config) -> Tuple[List[Signal], Dict[str, Dict[str, Any]], Dict[str, int]]:
    now = datetime.now(UTC)
    forecast_cache: Dict[Tuple[str, str], Optional[float]] = {}
    market_map: Dict[str, Dict[str, Any]] = {}

    counters = {
        "total_slugs": 0,
        "fetched": 0,
        "parseable": 0,
        "future_window": 0,
        "model_ready": 0,
        "quality_pass": 0,
        "signals": 0,
    }

    signals: List[Signal] = []
    slugs = fetch_weather_slugs(config.request_timeout_sec)
    counters["total_slugs"] = len(slugs)

    for slug in slugs:
        market = fetch_market_by_slug(slug, config.request_timeout_sec)
        if not market:
            continue
        counters["fetched"] += 1
        market_map[slug] = market

        parsed = parse_temperature_slug(slug)
        if not parsed:
            continue
        counters["parseable"] += 1

        if not market.get("active") or market.get("closed"):
            continue

        end_date_raw = market.get("endDate") or ""
        end_date = parse_iso_to_utc(end_date_raw)
        if not end_date:
            continue

        hours_to_expiry = (end_date - now).total_seconds() / 3600
        if hours_to_expiry < config.min_hours_to_expiry:
            continue
        counters["future_window"] += 1

        liquidity = parse_float(market.get("liquidity")) or 0.0
        if liquidity < config.min_liquidity:
            continue

        yes_bid, yes_ask, yes_spread, no_price = market_prices(market)
        if yes_ask is None or no_price is None:
            continue

        if yes_spread is not None and yes_spread > config.max_yes_spread:
            continue

        forecast_max_c = fetch_forecast_max_temp_c(
            parsed.city_slug,
            parsed.target_date,
            config.request_timeout_sec,
            forecast_cache,
        )
        if forecast_max_c is None:
            continue
        counters["model_ready"] += 1

        horizon_days = max(0.0, (datetime.fromisoformat(parsed.target_date).replace(tzinfo=UTC) - now).total_seconds() / 86400)
        sigma_c = sigma_by_horizon_days(horizon_days)
        p_yes = prob_yes_from_contract(forecast_max_c, sigma_c, parsed)

        yes_edge = p_yes - yes_ask
        no_prob = 1 - p_yes
        no_edge = no_prob - no_price

        # Pick side with stronger positive edge.
        if yes_edge >= no_edge:
            side = "YES"
            side_prob = p_yes
            side_price = yes_ask
            edge = yes_edge
        else:
            side = "NO"
            side_prob = no_prob
            side_price = no_price
            edge = no_edge

        if side_prob >= config.core_prob_min and edge >= config.core_edge_min:
            category = "core"
        elif side_prob <= config.tail_prob_max and edge >= config.tail_edge_min and side_price <= config.tail_price_max:
            category = "tail"
        else:
            continue

        counters["quality_pass"] += 1

        signal = Signal(
            slug=slug,
            question=str(market.get("question") or ""),
            city_slug=parsed.city_slug,
            target_date=parsed.target_date,
            side=side,
            category=category,
            side_prob=side_prob,
            side_price=side_price,
            edge=edge,
            yes_price=yes_ask,
            no_price=no_price,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            yes_spread=yes_spread,
            forecast_max_c=forecast_max_c,
            sigma_c=sigma_c,
            end_date=end_date_raw,
            liquidity=liquidity,
        )
        signals.append(signal)

    counters["signals"] = len(signals)

    signals.sort(key=lambda s: s.edge, reverse=True)
    return signals, market_map, counters


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "created_at": iso_now(),
            "open_positions": [],
            "closed_positions": [],
            "last_run": None,
            "last_note": None,
        }
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: Dict[str, Any]) -> None:
    ensure_dirs([path])
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def append_snapshot(path: Path, payload: Dict[str, Any]) -> None:
    ensure_dirs([path])
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def today_cn_str(ts_utc: Optional[str] = None) -> str:
    if ts_utc:
        t = parse_iso_to_utc(ts_utc)
        if t is None:
            t = datetime.now(UTC)
    else:
        t = datetime.now(UTC)
    return t.astimezone(CN_TZ).strftime("%Y-%m-%d")


def current_price_for_side(market: Dict[str, Any], side: str) -> Optional[float]:
    yes_bid, yes_ask, _, no_price = market_prices(market)
    yes_mid = None
    if yes_bid is not None and yes_ask is not None:
        yes_mid = (yes_bid + yes_ask) / 2
    elif yes_ask is not None:
        yes_mid = yes_ask
    elif yes_bid is not None:
        yes_mid = yes_bid

    if side == "YES":
        return yes_mid
    if side == "NO":
        if no_price is not None:
            return no_price
        if yes_mid is not None:
            return 1 - yes_mid
    return None


def settle_price_for_side(market: Dict[str, Any], side: str) -> Optional[float]:
    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None

    if not (isinstance(outcome_prices, list) and len(outcome_prices) >= 2):
        return None

    yes_final = parse_float(outcome_prices[0])
    no_final = parse_float(outcome_prices[1])
    if yes_final is None or no_final is None:
        return None

    # Snap near-binary values for cleaner settlement.
    if yes_final >= 0.999:
        yes_final = 1.0
        no_final = 0.0
    elif yes_final <= 0.001:
        yes_final = 0.0
        no_final = 1.0

    return yes_final if side == "YES" else no_final


def calc_exposure(open_positions: List[Dict[str, Any]]) -> float:
    return round(sum(float(p.get("size_usd", 0.0)) for p in open_positions), 6)


def calc_realized_today(closed_positions: List[Dict[str, Any]], day_cn: str) -> float:
    total = 0.0
    for p in closed_positions:
        closed_at = p.get("closed_at")
        if not closed_at:
            continue
        if today_cn_str(closed_at) == day_cn:
            total += float(p.get("realized_pnl_usd", 0.0))
    return round(total, 6)


def calc_unrealized(open_positions: List[Dict[str, Any]], market_map: Dict[str, Dict[str, Any]]) -> float:
    total = 0.0
    for p in open_positions:
        slug = p.get("slug")
        side = p.get("side")
        shares = float(p.get("shares", 0.0))
        entry = float(p.get("entry_price", 0.0))

        m = market_map.get(slug)
        if not m:
            continue
        px = current_price_for_side(m, side)
        if px is None:
            continue
        total += shares * (px - entry)
    return round(total, 6)


def close_expired_positions(state: Dict[str, Any], market_map: Dict[str, Dict[str, Any]]) -> int:
    now = datetime.now(UTC)
    still_open = []
    closed_new = 0

    for p in state.get("open_positions", []):
        slug = p.get("slug")
        side = p.get("side")
        end_date = parse_iso_to_utc(str(p.get("end_date") or ""))

        m = market_map.get(slug)
        market_closed = bool(m.get("closed")) if m else False
        expired = bool(end_date and now >= end_date)

        if not market_closed and not expired:
            still_open.append(p)
            continue

        settle_px = settle_price_for_side(m, side) if m else None
        if settle_px is None:
            # Keep position open until settlement data exists.
            still_open.append(p)
            continue

        shares = float(p.get("shares", 0.0))
        entry = float(p.get("entry_price", 0.0))
        pnl = shares * (settle_px - entry)

        p2 = dict(p)
        p2["closed_at"] = iso_now()
        p2["settle_price"] = settle_px
        p2["realized_pnl_usd"] = round(pnl, 6)
        state["closed_positions"].append(p2)
        closed_new += 1

    state["open_positions"] = still_open
    return closed_new


def update_open_position_marks(state: Dict[str, Any], market_map: Dict[str, Dict[str, Any]]) -> None:
    ts = iso_now()
    for p in state.get("open_positions", []):
        slug = p.get("slug")
        side = p.get("side")
        shares = float(p.get("shares", 0.0))
        entry = float(p.get("entry_price", 0.0))

        m = market_map.get(slug)
        px = current_price_for_side(m, side) if m else None
        if px is None:
            p["mark_price"] = None
            p["unrealized_pnl_usd"] = None
            p["mark_updated_at"] = ts
            continue

        pnl = shares * (px - entry)
        p["mark_price"] = round(px, 6)
        p["unrealized_pnl_usd"] = round(pnl, 6)
        p["mark_updated_at"] = ts


def select_signals_for_opening(signals: List[Signal], config: Config, slots_total: int) -> List[Signal]:
    if slots_total <= 0:
        return []

    core = [s for s in signals if s.category == "core"]
    tail = [s for s in signals if s.category == "tail"]

    core_slots = max(0, min(slots_total, round(slots_total * config.target_core_ratio)))
    tail_slots = max(0, slots_total - core_slots)

    picks = core[:core_slots] + tail[:tail_slots]

    # Fill remaining slots by global edge ranking.
    if len(picks) < slots_total:
        picked_keys = {(s.slug, s.side) for s in picks}
        for s in signals:
            key = (s.slug, s.side)
            if key in picked_keys:
                continue
            picks.append(s)
            picked_keys.add(key)
            if len(picks) >= slots_total:
                break

    return picks[:slots_total]


def apply_paper_positions(
    state: Dict[str, Any],
    signals: List[Signal],
    market_map: Dict[str, Dict[str, Any]],
    config: Config,
) -> Dict[str, Any]:
    open_positions: List[Dict[str, Any]] = state.get("open_positions", [])
    closed_positions: List[Dict[str, Any]] = state.get("closed_positions", [])

    exposure = calc_exposure(open_positions)
    free_exposure = max(0.0, config.max_open_exposure_usd - exposure)
    slots_left = int(free_exposure // config.trade_size_usd)

    day_cn = today_cn_str()
    realized_today = calc_realized_today(closed_positions, day_cn)
    unrealized = calc_unrealized(open_positions, market_map)

    stop_triggered = (realized_today + unrealized) <= config.daily_stop_loss_usd

    opened = 0
    skipped_existing = 0
    blocked_by_risk = 0

    if stop_triggered:
        return {
            "opened": 0,
            "skipped_existing": 0,
            "blocked_by_risk": len(signals),
            "stop_triggered": True,
            "realized_today": realized_today,
            "unrealized": unrealized,
        }

    picks = select_signals_for_opening(signals, config, slots_left)

    existing_keys = {(p.get("slug"), p.get("side")) for p in open_positions}

    for s in picks:
        key = (s.slug, s.side)
        if key in existing_keys:
            skipped_existing += 1
            continue

        if s.side_price <= 0:
            blocked_by_risk += 1
            continue

        shares = config.trade_size_usd / s.side_price

        pos = {
            "position_id": str(uuid.uuid4()),
            "opened_at": iso_now(),
            "slug": s.slug,
            "question": s.question,
            "city_slug": s.city_slug,
            "target_date": s.target_date,
            "end_date": s.end_date,
            "side": s.side,
            "category": s.category,
            "size_usd": config.trade_size_usd,
            "entry_price": round(s.side_price, 6),
            "shares": round(shares, 6),
            "model_prob": round(s.side_prob, 6),
            "edge": round(s.edge, 6),
            "forecast_max_c": round(s.forecast_max_c, 4),
            "sigma_c": round(s.sigma_c, 4),
            "liquidity": round(s.liquidity, 4),
            "yes_ask": s.yes_ask,
            "yes_bid": s.yes_bid,
            "yes_spread": s.yes_spread,
            "no_price": s.no_price,
        }
        open_positions.append(pos)
        existing_keys.add(key)
        opened += 1

    state["open_positions"] = open_positions

    return {
        "opened": opened,
        "skipped_existing": skipped_existing,
        "blocked_by_risk": blocked_by_risk,
        "stop_triggered": False,
        "realized_today": realized_today,
        "unrealized": unrealized,
    }


def summarize(signals: List[Signal], state: Dict[str, Any], counters: Dict[str, int], apply_result: Dict[str, Any], closed_new: int) -> Dict[str, Any]:
    open_positions = state.get("open_positions", [])
    closed_positions = state.get("closed_positions", [])

    summary = {
        "ts": iso_now(),
        "scan": counters,
        "signals_top": [
            {
                "slug": s.slug,
                "side": s.side,
                "category": s.category,
                "edge": round(s.edge, 6),
                "prob": round(s.side_prob, 6),
                "price": round(s.side_price, 6),
                "target_date": s.target_date,
                "end_date": s.end_date,
                "liquidity": round(s.liquidity, 2),
            }
            for s in signals[:10]
        ],
        "paper": {
            "closed_new": closed_new,
            "opened_new": apply_result.get("opened", 0),
            "open_positions": len(open_positions),
            "closed_positions": len(closed_positions),
            "open_exposure_usd": round(calc_exposure(open_positions), 6),
            "realized_today_usd": apply_result.get("realized_today", 0.0),
            "unrealized_usd": apply_result.get("unrealized", 0.0),
            "stop_triggered": bool(apply_result.get("stop_triggered", False)),
        },
    }
    return summary


def validate_env_has_trading_keys(env_map: Dict[str, str]) -> List[str]:
    # we only verify presence; no secrets printed.
    required_any_pairs = [
        ("POLY_PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY"),
        ("POLY_CLOB_API_KEY", "POLYMARKET_API_KEY"),
        ("POLY_CLOB_SECRET", "POLYMARKET_API_SECRET"),
        ("POLY_CLOB_PASSPHRASE", "POLYMARKET_API_PASSPHRASE"),
        ("POLYGON_RPC_URL", "POLYGON_RPC_URL"),
    ]
    missing = []
    for k1, k2 in required_any_pairs:
        if not env_map.get(k1) and not env_map.get(k2):
            missing.append(f"{k1}|{k2}")
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket weather model-vs-odds paper runner")
    parser.add_argument(
        "--env",
        default="/home/kai/.openclaw/credentials/polymarket.env",
        help="Path to local credentials env file",
    )
    parser.add_argument(
        "--state",
        default="/home/kai/.openclaw/workspace/ops/polymarket-weather-bot/state/paper_state.json",
        help="Path to paper state json",
    )
    parser.add_argument(
        "--snapshot",
        default="/home/kai/.openclaw/workspace/ops/polymarket-weather-bot/state/snapshots.jsonl",
        help="Path to snapshot jsonl",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply paper position open/close updates (default: scan only)",
    )
    parser.add_argument(
        "--min-hours-to-expiry",
        type=float,
        default=None,
        help="Override expiry buffer in hours (default: Config.min_hours_to_expiry)",
    )
    args = parser.parse_args()

    config = Config()
    if args.min_hours_to_expiry is not None:
        config.min_hours_to_expiry = max(0.0, float(args.min_hours_to_expiry))

    env_path = Path(args.env)
    state_path = Path(args.state)
    snapshot_path = Path(args.snapshot)

    env_map = load_env(env_path) if env_path.exists() else {}
    missing = validate_env_has_trading_keys(env_map)

    signals, market_map, counters = build_signals(config)

    state = load_state(state_path)

    closed_new = close_expired_positions(state, market_map)

    if args.apply:
        apply_result = apply_paper_positions(state, signals, market_map, config)
    else:
        apply_result = {
            "opened": 0,
            "skipped_existing": 0,
            "blocked_by_risk": 0,
            "stop_triggered": False,
            "realized_today": calc_realized_today(state.get("closed_positions", []), today_cn_str()),
            "unrealized": calc_unrealized(state.get("open_positions", []), market_map),
        }

    update_open_position_marks(state, market_map)

    state["last_run"] = iso_now()
    state["last_note"] = {
        "apply": bool(args.apply),
        "env_missing": missing,
        "scan_total": counters.get("total_slugs", 0),
        "signals": len(signals),
    }

    save_state(state_path, state)

    snapshot_payload = {
        "ts": iso_now(),
        "scan": counters,
        "signals": [asdict(s) for s in signals],
    }
    append_snapshot(snapshot_path, snapshot_payload)

    summary = summarize(signals, state, counters, apply_result, closed_new)
    summary["env_missing_keys"] = missing
    summary["apply_mode"] = bool(args.apply)
    summary["config"] = {
        "min_hours_to_expiry": config.min_hours_to_expiry,
    }

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
