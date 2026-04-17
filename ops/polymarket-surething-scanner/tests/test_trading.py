from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import CandidateMarket
from reporting import render_live_hourly
from trading import (
    append_new_fills,
    archive_closed_positions,
    build_execution_plan,
    build_mode_settings,
    build_pending_settlements,
    load_json,
    load_trading_state,
    reconcile_positions_from_remote,
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

    def test_live_plan_allows_restricted_market(self) -> None:
        config = {
            "runtime": {"mode": "live"},
            "live": {"enabled": True},
        }
        settings = build_mode_settings(config, "live")
        state = load_trading_state(Path("/tmp/does-not-exist.json"), "live", settings)
        candidate = make_candidate(restricted=True)
        plan = build_execution_plan([candidate], state, settings, confirmed_ids={candidate.market_id}, preflight={"status": "ok"})
        self.assertNotEqual(plan[0]["reason"], "restricted_market")

    def test_append_new_fills_dedupes_trade_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fills_path = Path(tmp) / "fills.jsonl"
            state = {"recent_fills": [], "seen_trade_ids": []}
            trades = [{"id": "t1", "asset_id": "token-m1", "price": 0.95, "size": 2}]
            meta = {"token-m1": {"market_id": "m1", "question": "Question m1"}}
            rows = append_new_fills(state, fills_path, trades, meta)
            self.assertEqual(len(rows), 1)
            rows = append_new_fills(state, fills_path, trades, meta)
            self.assertEqual(len(rows), 0)

    def test_reconcile_positions_from_remote_rebuilds_local(self) -> None:
        candidate = make_candidate()
        state = {
            "positions": [
                {
                    "market_id": candidate.market_id,
                    "condition_id": candidate.condition_id,
                    "token_id": candidate.token_id,
                    "question": candidate.question,
                    "entry_price": 0.94,
                    "size_usd": 9.4,
                    "shares": 10,
                    "opened_at": "2026-03-28T01:00:00+00:00",
                }
            ]
        }
        remote = [
            {
                "asset": candidate.token_id,
                "market": candidate.market_id,
                "conditionId": candidate.condition_id,
                "size": 10,
                "avgPrice": 0.95,
                "currentValue": 9.8,
                "curPrice": 0.98,
                "title": candidate.question,
            }
        ]
        new_positions, report = reconcile_positions_from_remote(state, remote, [candidate])
        self.assertEqual(len(new_positions), 1)
        self.assertEqual(report["remote_only_tokens"], [])
        self.assertEqual(report["local_only_tokens"], [])
        self.assertAlmostEqual(new_positions[0]["entry_price"], 0.95)
        self.assertAlmostEqual(new_positions[0]["last_mark"], 0.98)

    def test_archive_closed_positions_moves_matching_open_position(self) -> None:
        candidate = make_candidate()
        state = {
            "positions": [
                {
                    "market_id": candidate.market_id,
                    "condition_id": candidate.condition_id,
                    "token_id": candidate.token_id,
                    "question": candidate.question,
                    "entry_price": 0.95,
                    "size_usd": 9.5,
                    "shares": 10,
                    "opened_at": "2026-03-28T01:00:00+00:00",
                    "expected_resolve_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                }
            ],
            "closed_positions": [],
            "recent_fills": [{"token_id": candidate.token_id}],
            "settled_cash_released_usd": 0.0,
        }
        remote_closed = [
            {
                "asset": candidate.token_id,
                "market": candidate.market_id,
                "conditionId": candidate.condition_id,
                "currentValue": 10.0,
                "initialValue": 9.5,
                "cashPnl": 0.5,
                "updatedAt": "2026-03-28T10:00:00+00:00",
            }
        ]
        count, released, archived = archive_closed_positions(state, remote_closed, [candidate])
        self.assertEqual(count, 1)
        self.assertAlmostEqual(released, 10.0)
        self.assertEqual(len(state["positions"]), 0)
        self.assertEqual(len(state["closed_positions"]), 1)
        self.assertEqual(len(archived), 1)

    def test_build_pending_settlements_marks_overdue_positions(self) -> None:
        config = {"runtime": {"mode": "live"}, "live": {"settlement_grace_minutes": 60}}
        settings = build_mode_settings(config, "live")
        state = {
            "positions": [
                {
                    "market_id": "m1",
                    "token_id": "t1",
                    "question": "Question",
                    "expected_resolve_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                }
            ]
        }
        pending = build_pending_settlements(state, settings)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["status"], "await_settlement_or_claim")

    def test_render_live_hourly_outputs_summary(self) -> None:
        snapshot = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "totals": {
                "open_cost_usd": 12.0,
                "unrealized_pnl_usd": 0.6,
                "realized_pnl_total_usd": 1.2,
                "available_for_redeploy_usd": 8.5,
            },
            "reconciliation": {"status": "ok", "remote_positions": 1, "remote_open_orders": 0, "new_fills": 1},
            "settlement": {"pending_count": 0},
            "positions": [
                {
                    "question": "Question m1",
                    "event_slug": "event-m1",
                    "size_usd": 12.0,
                    "entry_price": 0.95,
                    "last_mark": 0.98,
                    "unrealized_pnl": 0.6,
                    "opened_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                    "expected_resolve_at": (datetime.now(timezone.utc) + timedelta(hours=5)).isoformat(),
                }
            ],
            "recent_fills": [
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "question": "Question m1",
                    "side": "BUY",
                    "shares": 10,
                    "price": 0.95,
                    "spent_usd": 9.5,
                }
            ],
            "pending_settlements": [],
        }
        text = render_live_hourly(snapshot)
        self.assertIn("Surething LIVE 状态", text)
        self.assertIn("可再部署", text)
        self.assertIn("最近fills", text)


if __name__ == "__main__":
    unittest.main()
