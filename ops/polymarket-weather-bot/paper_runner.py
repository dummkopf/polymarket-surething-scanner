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

    # Portfolio/risk refinement
    max_positions_per_city: int = 2
    exit_edge_floor: float = 0.01
    min_holding_minutes_for_edge_exit: int = 10

    # Signal confirmation + sizing by time-to-expiry
    confirm_ticks: int = 2

    # Fractional Kelly controls
    paper_bankroll_usd: float = 1000.0
    kelly_fraction_core: float = 0.20
    kelly_fraction_tail: float = 0.10
    max_bet_fraction: float = 0.01
    min_edge_for_entry: float = 0.01

    # Model robustness gate: require edge to remain positive under
    # small forecast mean shifts and sigma perturbations.
    robustness_mu_shift_c: float = 0.7
    robustness_sigma_scale_low: float = 0.85
    robustness_sigma_scale_high: float = 1.15
    robustness_min_edge: float = 0.0

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
    # Executable NO ask used for entry/edge comparisons.
    no_price: float
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    yes_spread: Optional[float]

    forecast_max_c: float
    sigma_c: float
    bucket_lower: Optional[float]
    bucket_upper: Optional[float]
    side_price_source: str
    robustness_mu_shift_c: float
    robustness_sigma_low_c: float
    robustness_sigma_high_c: float
    robustness_min_prob: float
    robustness_max_prob: float
    robustness_min_edge: float
    robustness_max_edge: float
    robustness_pass: bool

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


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


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


def fetch_weather_markets(timeout_sec: int) -> List[Dict[str, Any]]:
    """Fetch weather markets natively from Gamma API.

    Discovery flow:
    1) paginate Gamma `series`
    2) keep `*-daily-weather` series
    3) for active/open events in those series, fetch exact `events?slug=...`
       and read the embedded `markets`

    This avoids HTML scraping blind spots and avoids scanning the entire open
    market universe just to find weather contracts.
    """
    series_limit = 300
    offset = 0
    weather_series: List[Dict[str, Any]] = []
    seen_series = set()

    while True:
        resp = requests.get(
            "https://gamma-api.polymarket.com/series",
            params={"closed": "false", "limit": series_limit, "offset": offset},
            timeout=timeout_sec,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not isinstance(batch, list) or not batch:
            break

        for series in batch:
            slug = str(series.get("slug") or "")
            if not slug.endswith("-daily-weather"):
                continue
            if slug in seen_series:
                continue
            seen_series.add(slug)
            weather_series.append(series)

        if len(batch) < series_limit:
            break
        offset += series_limit
        if offset > 5000:
            break

    markets: List[Dict[str, Any]] = []
    seen_market_slugs = set()
    for series in weather_series:
        for event_stub in (series.get("events") or []):
            if not isinstance(event_stub, dict):
                continue
            if not event_stub.get("active") or event_stub.get("closed"):
                continue
            event_slug = str(event_stub.get("slug") or "")
            if not event_slug:
                continue

            try:
                event_resp = requests.get(
                    "https://gamma-api.polymarket.com/events",
                    params={"slug": event_slug, "limit": 1},
                    timeout=timeout_sec,
                )
                event_resp.raise_for_status()
                event_rows = event_resp.json()
            except Exception:
                continue

            if not isinstance(event_rows, list) or not event_rows:
                continue
            event = event_rows[0]
            for market in (event.get("markets") or []):
                if not isinstance(market, dict):
                    continue
                market_slug = str(market.get("slug") or "")
                if not market_slug.startswith("highest-temperature-in-"):
                    continue
                if market_slug in seen_market_slugs:
                    continue
                seen_market_slugs.add(market_slug)
                markets.append(market)

    markets.sort(key=lambda m: str(m.get("slug") or ""))
    return markets


def fetch_weather_slugs(timeout_sec: int) -> List[str]:
    return [str(m.get("slug") or "") for m in fetch_weather_markets(timeout_sec)]


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
                # Contract date is city-local. Using auto timezone avoids UTC date skew.
                "timezone": "auto",
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


def side_prob_from_p_yes(p_yes: float, side: str) -> float:
    return p_yes if side == "YES" else (1.0 - p_yes)


def side_price_source_label(market: Dict[str, Any], side: str) -> str:
    yes_bid = parse_float(market.get("bestBid"))
    yes_ask = parse_float(market.get("bestAsk"))

    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None

    yes_mid_from_outcome = None
    no_mid_from_outcome = None
    if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
        yes_mid_from_outcome = parse_float(outcome_prices[0])
        no_mid_from_outcome = parse_float(outcome_prices[1])

    if side == "YES":
        if yes_ask is not None:
            return "yes_ask_direct"
        if yes_mid_from_outcome is not None:
            return "yes_mid_outcome_fallback"
        if yes_bid is not None:
            return "yes_bid_fallback"
        return "unavailable"

    if side == "NO":
        if yes_bid is not None:
            return "no_ask_synthetic_from_yes_bid"
        if no_mid_from_outcome is not None:
            return "no_mid_outcome_fallback"
        if yes_ask is not None or yes_mid_from_outcome is not None:
            return "no_synthetic_from_yes_mid"
        return "unavailable"

    return "unknown"


def calc_robustness_metrics(
    forecast_max_c: float,
    sigma_c: float,
    contract: ParsedContract,
    side: str,
    side_price: float,
    config: Config,
) -> Dict[str, Any]:
    mu_shift_c = max(0.0, float(config.robustness_mu_shift_c))
    sigma_base_c = max(0.05, float(sigma_c))
    sigma_low_c = max(0.05, sigma_base_c * max(0.01, float(config.robustness_sigma_scale_low)))
    sigma_high_c = max(0.05, sigma_base_c * max(0.01, float(config.robustness_sigma_scale_high)))

    sigma_candidates = []
    for candidate in (sigma_low_c, sigma_base_c, sigma_high_c):
        if not any(abs(candidate - existing) < 1e-9 for existing in sigma_candidates):
            sigma_candidates.append(candidate)

    probs: List[float] = []
    edges: List[float] = []
    for mu_shift in (-mu_shift_c, 0.0, mu_shift_c):
        mu_candidate_c = forecast_max_c + mu_shift
        for sigma_candidate_c in sigma_candidates:
            p_yes = prob_yes_from_contract(mu_candidate_c, sigma_candidate_c, contract)
            side_prob = side_prob_from_p_yes(p_yes, side)
            probs.append(side_prob)
            edges.append(side_prob - side_price)

    min_prob = min(probs) if probs else 0.0
    max_prob = max(probs) if probs else 0.0
    min_edge = min(edges) if edges else -1.0
    max_edge = max(edges) if edges else -1.0

    return {
        "mu_shift_c": mu_shift_c,
        "sigma_low_c": sigma_low_c,
        "sigma_high_c": sigma_high_c,
        "min_prob": min_prob,
        "max_prob": max_prob,
        "min_edge": min_edge,
        "max_edge": max_edge,
        "pass": min_edge >= float(config.robustness_min_edge),
    }


def parse_iso_to_utc(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def market_prices(
    market: Dict[str, Any],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Return executable price tuple:
      yes_bid, yes_ask, yes_spread, no_bid, no_ask, no_mid

    Notes:
    - bestBid/bestAsk are treated as YES-side executable book levels.
    - NO executable levels are derived via complement when possible:
        no_ask ~= 1 - yes_bid
        no_bid ~= 1 - yes_ask
    - outcomePrices are used as midpoint fallback only.
    """
    yes_bid = parse_float(market.get("bestBid"))
    yes_ask = parse_float(market.get("bestAsk"))

    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None

    yes_mid_from_outcome = None
    no_mid_from_outcome = None
    if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
        yes_mid_from_outcome = parse_float(outcome_prices[0])
        no_mid_from_outcome = parse_float(outcome_prices[1])

    if yes_ask is None:
        yes_ask = yes_mid_from_outcome
    if yes_bid is None:
        yes_bid = yes_mid_from_outcome

    yes_mid = None
    if yes_bid is not None and yes_ask is not None:
        yes_mid = (yes_bid + yes_ask) / 2
    elif yes_mid_from_outcome is not None:
        yes_mid = yes_mid_from_outcome
    elif yes_bid is not None:
        yes_mid = yes_bid
    elif yes_ask is not None:
        yes_mid = yes_ask

    no_mid = None
    if no_mid_from_outcome is not None:
        no_mid = no_mid_from_outcome
    elif yes_mid is not None:
        no_mid = 1 - yes_mid

    no_ask = (1 - yes_bid) if yes_bid is not None else no_mid
    no_bid = (1 - yes_ask) if yes_ask is not None else no_mid

    if yes_bid is not None:
        yes_bid = clamp01(yes_bid)
    if yes_ask is not None:
        yes_ask = clamp01(yes_ask)
    if no_bid is not None:
        no_bid = clamp01(no_bid)
    if no_ask is not None:
        no_ask = clamp01(no_ask)
    if no_mid is not None:
        no_mid = clamp01(no_mid)

    yes_spread = None
    if yes_ask is not None and yes_bid is not None:
        yes_spread = max(0.0, yes_ask - yes_bid)

    return yes_bid, yes_ask, yes_spread, no_bid, no_ask, no_mid


def build_signals(config: Config) -> Tuple[List[Signal], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, int]]:
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
    radar_rows: List[Dict[str, Any]] = []
    universe_rows: List[Dict[str, Any]] = []
    weather_markets = fetch_weather_markets(config.request_timeout_sec)
    counters["total_slugs"] = len(weather_markets)

    for market in weather_markets:
        slug = str(market.get("slug") or "")
        if not slug:
            continue
        counters["fetched"] += 1
        market_map[slug] = market

        parsed = parse_temperature_slug(slug)
        if not parsed:
            continue
        counters["parseable"] += 1

        row = {
            "slug": slug,
            "question": str(market.get("question") or ""),
            "city_slug": parsed.city_slug,
            "target_date": parsed.target_date,
            "status": "watch-only",
            "brief": "parseable, pending checks",
        }

        if not market.get("active") or market.get("closed"):
            row["status"] = "inactive"
            row["brief"] = "inactive/closed market"
            universe_rows.append(row)
            continue

        end_date_raw = market.get("endDate") or ""
        end_date = parse_iso_to_utc(end_date_raw)
        if not end_date:
            row["status"] = "data-missing"
            row["brief"] = "missing endDate"
            universe_rows.append(row)
            continue

        row["end_date"] = end_date_raw
        hours_to_expiry = (end_date - now).total_seconds() / 3600
        if hours_to_expiry < config.min_hours_to_expiry:
            row["status"] = "out-of-window"
            row["brief"] = "outside expiry window"
            universe_rows.append(row)
            continue
        counters["future_window"] += 1

        liquidity = parse_float(market.get("liquidity")) or 0.0
        row["liquidity"] = round(liquidity, 6)
        if liquidity < config.min_liquidity:
            row["status"] = "watch-only"
            row["brief"] = "below liquidity floor"
            universe_rows.append(row)
            continue

        yes_bid, yes_ask, yes_spread, no_bid, no_ask, no_mid = market_prices(market)
        row["yes_spread"] = None if yes_spread is None else round(yes_spread, 6)
        if yes_ask is None or no_ask is None:
            row["status"] = "data-missing"
            row["brief"] = "missing executable prices"
            universe_rows.append(row)
            continue

        if yes_spread is not None and yes_spread > config.max_yes_spread:
            row["status"] = "watch-only"
            row["brief"] = "spread too wide"
            universe_rows.append(row)
            continue

        forecast_max_c = fetch_forecast_max_temp_c(
            parsed.city_slug,
            parsed.target_date,
            config.request_timeout_sec,
            forecast_cache,
        )
        if forecast_max_c is None:
            row["status"] = "data-missing"
            row["brief"] = "weather forecast unavailable"
            universe_rows.append(row)
            continue
        counters["model_ready"] += 1

        # Sigma should be anchored to actual time-to-resolution (endDate), not target-date midnight.
        horizon_days = max(0.0, (end_date - now).total_seconds() / 86400)
        sigma_c = sigma_by_horizon_days(horizon_days)
        p_yes = prob_yes_from_contract(forecast_max_c, sigma_c, parsed)

        yes_edge = p_yes - yes_ask
        no_prob = 1 - p_yes
        no_edge = no_prob - no_ask

        # Pick side with stronger positive edge.
        if yes_edge >= no_edge:
            side = "YES"
            side_prob = p_yes
            side_price = yes_ask
            edge = yes_edge
        else:
            side = "NO"
            side_prob = no_prob
            side_price = no_ask
            edge = no_edge

        side_price_source = side_price_source_label(market, side)
        robustness = calc_robustness_metrics(
            forecast_max_c=forecast_max_c,
            sigma_c=sigma_c,
            contract=parsed,
            side=side,
            side_price=side_price,
            config=config,
        )

        base_core = side_prob >= config.core_prob_min and edge >= config.core_edge_min
        base_tail = (
            side_prob <= config.tail_prob_max
            and edge >= config.tail_edge_min
            and side_price <= config.tail_price_max
        )

        category = None
        if base_core and robustness["pass"]:
            category = "core"
        elif base_tail and robustness["pass"]:
            category = "tail"

        if category:
            brief = "meets quality + robustness gates"
        elif base_core or base_tail:
            brief = "fails robustness gate"
        else:
            brief = "reliable weather+odds, but below quality threshold"

        candidate_row = {
            "slug": slug,
            "question": str(market.get("question") or ""),
            "city_slug": parsed.city_slug,
            "target_date": parsed.target_date,
            "side": side,
            "side_prob": round(side_prob, 6),
            "side_price": round(side_price, 6),
            "edge": round(edge, 6),
            "forecast_max_c": round(forecast_max_c, 6),
            "sigma_c": round(sigma_c, 6),
            "bucket_lower": parsed.lower,
            "bucket_upper": parsed.upper,
            "side_price_source": side_price_source,
            "robustness_mu_shift_c": round(float(robustness["mu_shift_c"]), 6),
            "robustness_sigma_low_c": round(float(robustness["sigma_low_c"]), 6),
            "robustness_sigma_high_c": round(float(robustness["sigma_high_c"]), 6),
            "robustness_min_prob": round(float(robustness["min_prob"]), 6),
            "robustness_max_prob": round(float(robustness["max_prob"]), 6),
            "robustness_min_edge": round(float(robustness["min_edge"]), 6),
            "robustness_max_edge": round(float(robustness["max_edge"]), 6),
            "robustness_pass": bool(robustness["pass"]),
            "liquidity": round(liquidity, 6),
            "yes_spread": None if yes_spread is None else round(yes_spread, 6),
            "no_bid": None if no_bid is None else round(no_bid, 6),
            "no_ask": None if no_ask is None else round(no_ask, 6),
            "no_mid": None if no_mid is None else round(no_mid, 6),
            "end_date": end_date_raw,
            "status": "quality-pass" if category else "watch-only",
            "brief": brief,
        }
        radar_rows.append(candidate_row)
        universe_rows.append(candidate_row)

        if not category:
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
            no_price=no_ask,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            yes_spread=yes_spread,
            forecast_max_c=forecast_max_c,
            sigma_c=sigma_c,
            bucket_lower=parsed.lower,
            bucket_upper=parsed.upper,
            side_price_source=side_price_source,
            robustness_mu_shift_c=float(robustness["mu_shift_c"]),
            robustness_sigma_low_c=float(robustness["sigma_low_c"]),
            robustness_sigma_high_c=float(robustness["sigma_high_c"]),
            robustness_min_prob=float(robustness["min_prob"]),
            robustness_max_prob=float(robustness["max_prob"]),
            robustness_min_edge=float(robustness["min_edge"]),
            robustness_max_edge=float(robustness["max_edge"]),
            robustness_pass=bool(robustness["pass"]),
            end_date=end_date_raw,
            liquidity=liquidity,
        )
        signals.append(signal)

    counters["signals"] = len(signals)

    signals.sort(key=lambda s: s.edge, reverse=True)
    radar_rows.sort(key=lambda r: float(r.get("edge", 0.0)), reverse=True)
    return signals, radar_rows, universe_rows, market_map, counters


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "created_at": iso_now(),
            "open_positions": [],
            "closed_positions": [],
            "signal_confirm_counts": {},
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
    yes_bid, yes_ask, _, no_bid, no_ask, no_mid = market_prices(market)

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
        if no_bid is not None and no_ask is not None:
            return (no_bid + no_ask) / 2
        if no_mid is not None:
            return no_mid
        if yes_mid is not None:
            return 1 - yes_mid

    return None


def current_edge_for_position(
    slug: str,
    side: str,
    market: Dict[str, Any],
    config: Config,
    forecast_cache: Dict[Tuple[str, str], Optional[float]],
    now: datetime,
) -> Optional[float]:
    parsed = parse_temperature_slug(slug)
    if parsed is None:
        return None

    _, yes_ask, _, _, no_ask, _ = market_prices(market)
    if side == "YES":
        side_ask = yes_ask
    elif side == "NO":
        side_ask = no_ask
    else:
        return None

    if side_ask is None:
        return None

    forecast_max_c = fetch_forecast_max_temp_c(
        parsed.city_slug,
        parsed.target_date,
        config.request_timeout_sec,
        forecast_cache,
    )
    if forecast_max_c is None:
        return None

    end_date = parse_iso_to_utc(str(market.get("endDate") or ""))
    if end_date is not None:
        horizon_days = max(0.0, (end_date - now).total_seconds() / 86400)
    else:
        horizon_days = max(
            0.0,
            (datetime.fromisoformat(parsed.target_date).replace(tzinfo=UTC) - now).total_seconds() / 86400,
        )

    sigma_c = sigma_by_horizon_days(horizon_days)
    p_yes = prob_yes_from_contract(forecast_max_c, sigma_c, parsed)
    side_prob = p_yes if side == "YES" else (1 - p_yes)
    return float(side_prob - side_ask)


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


def calc_realized_total(closed_positions: List[Dict[str, Any]]) -> float:
    total = 0.0
    for p in closed_positions:
        total += float(p.get("realized_pnl_usd", 0.0))
    return round(total, 6)


def kelly_full_fraction(prob: float, price: float) -> float:
    # Binary share with unit payout. Full Kelly for long position.
    # f* = (p - q) / (1 - q)
    if price <= 0 or price >= 1:
        return 0.0
    return (prob - price) / (1.0 - price)


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


def close_edge_decay_positions(
    state: Dict[str, Any],
    signals: List[Signal],
    market_map: Dict[str, Dict[str, Any]],
    config: Config,
) -> int:
    """Close open paper positions when their same-side edge decays below a floor."""
    now = datetime.now(UTC)
    signal_edge = {(s.slug, s.side): float(s.edge) for s in signals}
    forecast_cache: Dict[Tuple[str, str], Optional[float]] = {}

    still_open: List[Dict[str, Any]] = []
    closed_new = 0

    for p in state.get("open_positions", []):
        slug = str(p.get("slug") or "")
        side = str(p.get("side") or "")
        key = (slug, side)

        edge_now = signal_edge.get(key)
        if edge_now is None:
            m = market_map.get(slug)
            if m is not None:
                edge_now = current_edge_for_position(
                    slug=slug,
                    side=side,
                    market=m,
                    config=config,
                    forecast_cache=forecast_cache,
                    now=now,
                )

        if edge_now is None:
            still_open.append(p)
            continue

        opened_at = parse_iso_to_utc(str(p.get("opened_at") or ""))
        held_minutes = 0.0
        if opened_at is not None:
            held_minutes = max(0.0, (now - opened_at).total_seconds() / 60)

        if held_minutes < float(config.min_holding_minutes_for_edge_exit):
            still_open.append(p)
            continue

        if edge_now >= float(config.exit_edge_floor):
            still_open.append(p)
            continue

        m = market_map.get(slug)
        px = current_price_for_side(m, side) if m else None
        if px is None:
            still_open.append(p)
            continue

        shares = float(p.get("shares", 0.0))
        entry = float(p.get("entry_price", 0.0))
        pnl = shares * (px - entry)

        p2 = dict(p)
        p2["closed_at"] = iso_now()
        p2["settle_price"] = round(px, 6)
        p2["realized_pnl_usd"] = round(pnl, 6)
        p2["close_reason"] = "edge_decay"
        p2["close_edge"] = round(edge_now, 6)

        state["closed_positions"].append(p2)
        closed_new += 1

    state["open_positions"] = still_open
    return closed_new


def update_signal_confirm_counts(state: Dict[str, Any], signals: List[Signal]) -> Dict[str, int]:
    counts: Dict[str, int] = state.get("signal_confirm_counts", {})
    if not isinstance(counts, dict):
        counts = {}

    current_keys = {f"{s.slug}|{s.side}" for s in signals}

    # reset keys not present in current cycle
    for k in list(counts.keys()):
        if k not in current_keys:
            counts[k] = 0

    # increment present keys
    for s in signals:
        k = f"{s.slug}|{s.side}"
        counts[k] = int(counts.get(k, 0)) + 1

    state["signal_confirm_counts"] = counts
    return counts


def ttl_bucket_and_multiplier(end_date_raw: str) -> Tuple[str, float]:
    end_date = parse_iso_to_utc(end_date_raw)
    if not end_date:
        return "unknown", 1.0

    now = datetime.now(UTC)
    h = (end_date - now).total_seconds() / 3600

    if h >= 24:
        return "24h+", 1.0
    if h >= 12:
        return "12-24h", 0.7
    if h >= 6:
        return "6-12h", 0.4
    return "<6h", 0.2


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
    realized_total = calc_realized_total(closed_positions)
    unrealized = calc_unrealized(open_positions, market_map)

    # Equity reference for Kelly sizing in paper mode.
    bankroll_equity = max(1.0, float(config.paper_bankroll_usd) + realized_total + unrealized)

    stop_triggered = (realized_today + unrealized) <= config.daily_stop_loss_usd

    opened = 0
    skipped_existing = 0
    blocked_by_risk = 0
    blocked_by_confirm = 0
    blocked_by_edge_gate = 0
    blocked_by_kelly = 0

    if stop_triggered:
        return {
            "opened": 0,
            "skipped_existing": 0,
            "blocked_by_risk": len(signals),
            "blocked_by_confirm": 0,
            "blocked_by_edge_gate": 0,
            "blocked_by_kelly": 0,
            "stop_triggered": True,
            "realized_today": realized_today,
            "unrealized": unrealized,
        }

    confirm_counts: Dict[str, int] = state.get("signal_confirm_counts", {})
    confirmed_signals: List[Signal] = []
    for s in signals:
        k = f"{s.slug}|{s.side}"
        if int(confirm_counts.get(k, 0)) >= int(config.confirm_ticks):
            confirmed_signals.append(s)
        else:
            blocked_by_confirm += 1

    picks = select_signals_for_opening(confirmed_signals, config, slots_left)

    existing_keys = {(p.get("slug"), p.get("side")) for p in open_positions}
    city_counts: Dict[str, int] = {}
    for p in open_positions:
        c = str(p.get("city_slug") or "")
        if not c:
            continue
        city_counts[c] = city_counts.get(c, 0) + 1

    for s in picks:
        key = (s.slug, s.side)
        if key in existing_keys:
            skipped_existing += 1
            continue

        if city_counts.get(s.city_slug, 0) >= int(config.max_positions_per_city):
            blocked_by_risk += 1
            continue

        if s.side_price <= 0:
            blocked_by_risk += 1
            continue

        if float(s.edge) < float(config.min_edge_for_entry):
            blocked_by_edge_gate += 1
            continue

        kelly_frac = (
            float(config.kelly_fraction_core)
            if s.category == "core"
            else float(config.kelly_fraction_tail)
        )
        full_kelly = kelly_full_fraction(float(s.side_prob), float(s.side_price))
        used_kelly = min(
            float(config.max_bet_fraction),
            max(0.0, full_kelly) * max(0.0, kelly_frac),
        )
        if used_kelly <= 0:
            blocked_by_kelly += 1
            continue

        ttl_bucket, ttl_mult = ttl_bucket_and_multiplier(s.end_date)

        # Kelly-implied size, then apply TTL multiplier and hard exposure cap.
        kelly_size_usd = bankroll_equity * used_kelly
        size_cap_by_policy = float(config.trade_size_usd) * float(ttl_mult)
        size_usd = min(kelly_size_usd * float(ttl_mult), size_cap_by_policy, free_exposure)

        if size_usd <= 0.25:
            blocked_by_kelly += 1
            continue

        shares = size_usd / s.side_price

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
            "size_usd": round(size_usd, 6),
            "ttl_bucket": ttl_bucket,
            "size_multiplier": round(ttl_mult, 4),
            "kelly_full_fraction": round(full_kelly, 6),
            "kelly_fraction_used": round(used_kelly, 6),
            "kelly_size_usd_pre_ttl": round(kelly_size_usd, 6),
            "bankroll_equity_usd_at_entry": round(bankroll_equity, 6),
            "entry_price": round(s.side_price, 6),
            "shares": round(shares, 6),
            "model_prob": round(s.side_prob, 6),
            "edge": round(s.edge, 6),
            "forecast_max_c": round(s.forecast_max_c, 4),
            "sigma_c": round(s.sigma_c, 4),
            "bucket_lower": s.bucket_lower,
            "bucket_upper": s.bucket_upper,
            "side_price_source": s.side_price_source,
            "robustness_mu_shift_c": round(s.robustness_mu_shift_c, 4),
            "robustness_sigma_low_c": round(s.robustness_sigma_low_c, 4),
            "robustness_sigma_high_c": round(s.robustness_sigma_high_c, 4),
            "robustness_min_prob": round(s.robustness_min_prob, 6),
            "robustness_max_prob": round(s.robustness_max_prob, 6),
            "robustness_min_edge": round(s.robustness_min_edge, 6),
            "robustness_max_edge": round(s.robustness_max_edge, 6),
            "robustness_pass": bool(s.robustness_pass),
            "liquidity": round(s.liquidity, 4),
            "yes_ask": s.yes_ask,
            "yes_bid": s.yes_bid,
            "yes_spread": s.yes_spread,
            "no_price": s.no_price,
        }
        open_positions.append(pos)
        existing_keys.add(key)
        city_counts[s.city_slug] = city_counts.get(s.city_slug, 0) + 1
        free_exposure = max(0.0, free_exposure - size_usd)
        opened += 1

    state["open_positions"] = open_positions

    return {
        "opened": opened,
        "skipped_existing": skipped_existing,
        "blocked_by_risk": blocked_by_risk,
        "blocked_by_confirm": blocked_by_confirm,
        "blocked_by_edge_gate": blocked_by_edge_gate,
        "blocked_by_kelly": blocked_by_kelly,
        "stop_triggered": False,
        "realized_today": realized_today,
        "unrealized": unrealized,
    }


def summarize(
    signals: List[Signal],
    state: Dict[str, Any],
    counters: Dict[str, int],
    apply_result: Dict[str, Any],
    closed_new_expiry: int,
    closed_new_edge: int,
) -> Dict[str, Any]:
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
                "robust_min_edge": round(s.robustness_min_edge, 6),
                "robust_pass": bool(s.robustness_pass),
                "target_date": s.target_date,
                "end_date": s.end_date,
                "liquidity": round(s.liquidity, 2),
            }
            for s in signals[:10]
        ],
        "paper": {
            "closed_new": int(closed_new_expiry + closed_new_edge),
            "closed_new_expiry": int(closed_new_expiry),
            "closed_new_edge": int(closed_new_edge),
            "opened_new": apply_result.get("opened", 0),
            "blocked_by_confirm": apply_result.get("blocked_by_confirm", 0),
            "blocked_by_edge_gate": apply_result.get("blocked_by_edge_gate", 0),
            "blocked_by_kelly": apply_result.get("blocked_by_kelly", 0),
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
        "--trade-size-usd",
        type=float,
        default=None,
        help="Hard per-trade size cap in USD (default: Config.trade_size_usd)",
    )
    parser.add_argument(
        "--max-open-exposure-usd",
        type=float,
        default=None,
        help="Max total open exposure in USD (default: Config.max_open_exposure_usd)",
    )
    parser.add_argument(
        "--daily-stop-loss-usd",
        type=float,
        default=None,
        help="Daily stop loss threshold in USD, usually negative (default: Config.daily_stop_loss_usd)",
    )
    parser.add_argument(
        "--min-hours-to-expiry",
        type=float,
        default=None,
        help="Override expiry buffer in hours (default: Config.min_hours_to_expiry)",
    )
    parser.add_argument(
        "--max-positions-per-city",
        type=int,
        default=None,
        help="Max concurrent open positions per city (default: Config.max_positions_per_city)",
    )
    parser.add_argument(
        "--exit-edge-floor",
        type=float,
        default=None,
        help="Close open position when same-side edge falls below this floor (default: Config.exit_edge_floor)",
    )
    parser.add_argument(
        "--min-holding-minutes-for-edge-exit",
        type=int,
        default=None,
        help="Minimum holding time before edge-decay auto-close can trigger",
    )
    parser.add_argument(
        "--confirm-ticks",
        type=int,
        default=None,
        help="Require signal persistence for N cycles before entry",
    )
    parser.add_argument(
        "--paper-bankroll-usd",
        type=float,
        default=None,
        help="Paper bankroll baseline used for fractional Kelly sizing",
    )
    parser.add_argument(
        "--kelly-fraction-core",
        type=float,
        default=None,
        help="Fractional Kelly multiplier for core signals",
    )
    parser.add_argument(
        "--kelly-fraction-tail",
        type=float,
        default=None,
        help="Fractional Kelly multiplier for tail signals",
    )
    parser.add_argument(
        "--max-bet-fraction",
        type=float,
        default=None,
        help="Hard cap of bankroll fraction per trade",
    )
    parser.add_argument(
        "--min-edge-for-entry",
        type=float,
        default=None,
        help="Global minimum edge threshold required for entry",
    )
    parser.add_argument(
        "--robustness-mu-shift-c",
        type=float,
        default=None,
        help="Forecast mean perturbation in C used by robustness gate",
    )
    parser.add_argument(
        "--robustness-sigma-scale-low",
        type=float,
        default=None,
        help="Lower sigma multiplier used by robustness gate",
    )
    parser.add_argument(
        "--robustness-sigma-scale-high",
        type=float,
        default=None,
        help="Upper sigma multiplier used by robustness gate",
    )
    parser.add_argument(
        "--robustness-min-edge",
        type=float,
        default=None,
        help="Require scenario worst-case edge to stay above this threshold",
    )
    args = parser.parse_args()

    config = Config()
    if args.trade_size_usd is not None:
        config.trade_size_usd = max(0.25, float(args.trade_size_usd))
    if args.max_open_exposure_usd is not None:
        config.max_open_exposure_usd = max(0.25, float(args.max_open_exposure_usd))
    if args.daily_stop_loss_usd is not None:
        config.daily_stop_loss_usd = float(args.daily_stop_loss_usd)
    if args.min_hours_to_expiry is not None:
        config.min_hours_to_expiry = max(0.0, float(args.min_hours_to_expiry))
    if args.max_positions_per_city is not None:
        config.max_positions_per_city = max(1, int(args.max_positions_per_city))
    if args.exit_edge_floor is not None:
        config.exit_edge_floor = max(0.0, float(args.exit_edge_floor))
    if args.min_holding_minutes_for_edge_exit is not None:
        config.min_holding_minutes_for_edge_exit = max(0, int(args.min_holding_minutes_for_edge_exit))
    if args.confirm_ticks is not None:
        config.confirm_ticks = max(1, int(args.confirm_ticks))
    if args.paper_bankroll_usd is not None:
        config.paper_bankroll_usd = max(1.0, float(args.paper_bankroll_usd))
    if args.kelly_fraction_core is not None:
        config.kelly_fraction_core = max(0.0, float(args.kelly_fraction_core))
    if args.kelly_fraction_tail is not None:
        config.kelly_fraction_tail = max(0.0, float(args.kelly_fraction_tail))
    if args.max_bet_fraction is not None:
        config.max_bet_fraction = max(0.0, float(args.max_bet_fraction))
    if args.min_edge_for_entry is not None:
        config.min_edge_for_entry = max(0.0, float(args.min_edge_for_entry))
    if args.robustness_mu_shift_c is not None:
        config.robustness_mu_shift_c = max(0.0, float(args.robustness_mu_shift_c))
    if args.robustness_sigma_scale_low is not None:
        config.robustness_sigma_scale_low = max(0.01, float(args.robustness_sigma_scale_low))
    if args.robustness_sigma_scale_high is not None:
        config.robustness_sigma_scale_high = max(0.01, float(args.robustness_sigma_scale_high))
    if args.robustness_min_edge is not None:
        config.robustness_min_edge = float(args.robustness_min_edge)

    env_path = Path(args.env)
    state_path = Path(args.state)
    snapshot_path = Path(args.snapshot)

    env_map = load_env(env_path) if env_path.exists() else {}
    missing = validate_env_has_trading_keys(env_map)

    signals, radar_rows, universe_rows, market_map, counters = build_signals(config)

    state = load_state(state_path)
    update_signal_confirm_counts(state, signals)

    closed_new_expiry = close_expired_positions(state, market_map)
    closed_new_edge = 0

    if args.apply:
        closed_new_edge = close_edge_decay_positions(state, signals, market_map, config)
        apply_result = apply_paper_positions(state, signals, market_map, config)
    else:
        apply_result = {
            "opened": 0,
            "skipped_existing": 0,
            "blocked_by_risk": 0,
            "blocked_by_confirm": 0,
            "blocked_by_edge_gate": 0,
            "blocked_by_kelly": 0,
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
        "radar": radar_rows,
        "universe": universe_rows,
    }
    append_snapshot(snapshot_path, snapshot_payload)

    summary = summarize(
        signals,
        state,
        counters,
        apply_result,
        closed_new_expiry,
        closed_new_edge,
    )
    summary["env_missing_keys"] = missing
    summary["apply_mode"] = bool(args.apply)
    summary["config"] = {
        "trade_size_usd": config.trade_size_usd,
        "max_open_exposure_usd": config.max_open_exposure_usd,
        "daily_stop_loss_usd": config.daily_stop_loss_usd,
        "min_hours_to_expiry": config.min_hours_to_expiry,
        "max_positions_per_city": config.max_positions_per_city,
        "exit_edge_floor": config.exit_edge_floor,
        "min_holding_minutes_for_edge_exit": config.min_holding_minutes_for_edge_exit,
        "confirm_ticks": config.confirm_ticks,
        "paper_bankroll_usd": config.paper_bankroll_usd,
        "kelly_fraction_core": config.kelly_fraction_core,
        "kelly_fraction_tail": config.kelly_fraction_tail,
        "max_bet_fraction": config.max_bet_fraction,
        "min_edge_for_entry": config.min_edge_for_entry,
        "robustness_mu_shift_c": config.robustness_mu_shift_c,
        "robustness_sigma_scale_low": config.robustness_sigma_scale_low,
        "robustness_sigma_scale_high": config.robustness_sigma_scale_high,
        "robustness_min_edge": config.robustness_min_edge,
    }

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
