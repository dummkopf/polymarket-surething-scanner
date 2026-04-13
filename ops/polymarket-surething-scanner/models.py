from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass
class CandidateMarket:
    market_id: str
    condition_id: str
    token_id: str
    question: str
    description: str
    end_date: datetime
    best_ask: float
    depth_usd: float
    resolution_source: str
    category_tag: str
    volume: float
    slug: str
    event_slug: str
    restricted: bool = False
    tick_size: float = 0.01
    min_order_size: float = 0.0
    neg_risk: bool = False

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["end_date"] = self.end_date.isoformat()
        return d
