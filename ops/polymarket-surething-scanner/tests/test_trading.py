from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from models import CandidateMarket
from trading import (
    build_execution_plan,
    build_mode_settings,
    execute_simulated,
    load_json,
    load_trading_state,
    resolve_mode,
    update_signal_state,
)


def make_candidate(market_id: str = "m1", restricted: bool = False, hours_ahead: int = 8) -> CandidateMarket:
    return CandidateMarket(
        market_id=market_id,
        condition_id=f"cond-{market_id}",
        token_id=f"token-{market_id}",
        question=f"Question {market_id}",
        description="desc",
        end_date=datetime.now(timezone.utc) + timedelta(hours=hours_ahead),
        best_ask=0.95,
        depth_usd=200.0,
        resolution_source="source",
        category_tag="news",
        volume=1000.0,
        slug=f"slug-{market_id}",
        event_slug=f"event-{market_id}",
        restricted=restricted,
    )


class TradingTests(unittest.TestCase):
    def test_resolve_mode_defaults_to_paper(self) -> None:
        self.assertEqual(resolve_mode({}), "paper")

    def test_signal_confirmation_requires_two_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "signal.json"
            candidate = make_candidate()
            _, confirmed = update_signal_state(path, [candidate], confirm_runs_required=2)
            self.assertEqual(confirmed, set())
            _, confirmed = update_signal_state(path, [candidate], confirm_runs_required=2)
            self.assertEqual(confirmed, {"m1"})

    def test_live_plan_blocks_restricted_market(self) -> None:
        config = {
            "runtime": {"mode": "live"},
            "live": {"enabled": True},
        }
        settings = build_mode_settings(config, "live")
        state = load_trading_state(Path("/tmp/does-not-exist.json"), "live", settings)
        candidate = make_candidate(restricted=True)
        plan = build_execution_plan([candidate], state, settings, confirmed_ids={candidate.market_id}, preflight={"status": "ok"})
        self.assertEqual(plan[0]["action"], "skip")
        self.assertEqual(plan[0]["reason"], "restricted_market")

    def test_simulated_execution_does_not_add_existing_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "trading_state.json"
            config = {"runtime": {"mode": "paper"}, "paper": {"allow_add_to_existing": False}}
            settings = build_mode_settings(config, "paper")
            candidate = make_candidate()
            plan = [
                {
                    "market_id": candidate.market_id,
                    "token_id": candidate.token_id,
                    "question": candidate.question,
                    "slug": candidate.slug,
                    "event_slug": candidate.event_slug,
                    "best_ask": candidate.best_ask,
                    "depth_usd": candidate.depth_usd,
                    "order_size_usd": 1.0,
                    "shares": round(1.0 / candidate.best_ask, 8),
                    "minutes_to_expiry": 100,
                    "restricted": False,
                    "action": "open",
                    "reason": "approved",
                }
            ]
            execute_simulated(state_path, [candidate], plan, settings)
            execute_simulated(state_path, [candidate], plan, settings)
            state = load_json(state_path, {})
            self.assertEqual(len(state.get("positions", [])), 1)
            self.assertEqual(state.get("totals", {}).get("added_existing_this_run"), 0)


if __name__ == "__main__":
    unittest.main()
