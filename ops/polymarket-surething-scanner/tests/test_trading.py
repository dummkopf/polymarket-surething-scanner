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
    build_stop_loss_plan,
    execute_stop_loss,
    load_json,
    load_trading_state,
    reconcile_positions_from_remote,
    resolve_mode,
    update_signal_state,
    update_stop_loss_state,
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


class StopLossTests(unittest.TestCase):
    STOP_CFG = {
        "enabled": True,
        "absolute_price": 0.68,
        "relative_drop": 0.25,
        "confirm_scans_required": 2,
        "min_bid_depth_usd": 20.0,
        "order_type": "FOK",
        "dry_run": True,
    }

    def make_position(self, token_id: str = "tok-1", entry: float = 0.93, shares: float = 10.0) -> dict:
        return {
            "market_id": "m1",
            "condition_id": "cond-1",
            "token_id": token_id,
            "question": "Question m1",
            "entry_price": entry,
            "shares": shares,
            "size_usd": round(entry * shares, 4),
            "opened_at": "2026-04-18T00:00:00+00:00",
        }

    def test_no_trigger_when_price_above_both_thresholds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sl.json"
            pos = [self.make_position(entry=0.93)]
            bids = {"tok-1": {"best_bid": 0.75, "bid_depth_usd": 500.0}}
            _, statuses = update_stop_loss_state(path, pos, bids, self.STOP_CFG)
            self.assertEqual(statuses[0]["triggered_reasons"], [])
            self.assertEqual(statuses[0]["consecutive_hits"], 0)
            self.assertFalse(statuses[0]["confirmed"])

    def test_absolute_threshold_requires_two_scans_to_confirm(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sl.json"
            pos = [self.make_position(entry=0.93)]
            bids = {"tok-1": {"best_bid": 0.68, "bid_depth_usd": 500.0}}
            _, statuses = update_stop_loss_state(path, pos, bids, self.STOP_CFG)
            self.assertIn("absolute_price", statuses[0]["triggered_reasons"])
            self.assertEqual(statuses[0]["consecutive_hits"], 1)
            self.assertFalse(statuses[0]["confirmed"])
            _, statuses = update_stop_loss_state(path, pos, bids, self.STOP_CFG)
            self.assertEqual(statuses[0]["consecutive_hits"], 2)
            self.assertTrue(statuses[0]["confirmed"])

    def test_relative_drop_triggers_for_high_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sl.json"
            pos = [self.make_position(entry=0.99)]
            bids = {"tok-1": {"best_bid": 0.73, "bid_depth_usd": 500.0}}
            _, statuses = update_stop_loss_state(path, pos, bids, self.STOP_CFG)
            self.assertIn("relative_drop", statuses[0]["triggered_reasons"])
            self.assertNotIn("absolute_price", statuses[0]["triggered_reasons"])

    def test_recovery_resets_confirmation_counter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sl.json"
            pos = [self.make_position(entry=0.93)]
            bids_below = {"tok-1": {"best_bid": 0.65, "bid_depth_usd": 500.0}}
            bids_recover = {"tok-1": {"best_bid": 0.85, "bid_depth_usd": 500.0}}
            _, statuses = update_stop_loss_state(path, pos, bids_below, self.STOP_CFG)
            self.assertEqual(statuses[0]["consecutive_hits"], 1)
            _, statuses = update_stop_loss_state(path, pos, bids_recover, self.STOP_CFG)
            self.assertEqual(statuses[0]["consecutive_hits"], 0)
            _, statuses = update_stop_loss_state(path, pos, bids_below, self.STOP_CFG)
            self.assertEqual(statuses[0]["consecutive_hits"], 1)
            self.assertFalse(statuses[0]["confirmed"])

    def test_thin_book_does_not_count_toward_trigger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sl.json"
            pos = [self.make_position(entry=0.93)]
            bids = {"tok-1": {"best_bid": 0.50, "bid_depth_usd": 5.0}}  # thin book
            _, statuses = update_stop_loss_state(path, pos, bids, self.STOP_CFG)
            self.assertTrue(statuses[0]["thin_book"])
            self.assertEqual(statuses[0]["triggered_reasons"], [])
            self.assertEqual(statuses[0]["consecutive_hits"], 0)

    def test_build_plan_includes_dry_run_flag(self) -> None:
        status = {
            "token_id": "tok-1",
            "confirmed": True,
            "triggered_reasons": ["absolute_price"],
            "best_bid": 0.68,
            "bid_depth_usd": 500.0,
        }
        pos = [self.make_position()]
        plan = build_stop_loss_plan([status], pos, self.STOP_CFG)
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]["action"], "stop_loss_dry_run")
        self.assertTrue(plan[0]["dry_run"])
        self.assertAlmostEqual(plan[0]["expected_proceeds_usd"], 10.0 * 0.68, places=4)

    def test_build_plan_skips_unconfirmed(self) -> None:
        status = {"token_id": "tok-1", "confirmed": False, "triggered_reasons": ["absolute_price"], "best_bid": 0.68}
        plan = build_stop_loss_plan([status], [self.make_position()], self.STOP_CFG)
        self.assertEqual(plan, [])

    def test_build_plan_disabled_returns_empty(self) -> None:
        status = {"token_id": "tok-1", "confirmed": True, "triggered_reasons": ["absolute_price"], "best_bid": 0.68}
        cfg = {**self.STOP_CFG, "enabled": False}
        plan = build_stop_loss_plan([status], [self.make_position()], cfg)
        self.assertEqual(plan, [])

    def test_execute_dry_run_does_not_modify_positions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = Path(tmp) / "journal.jsonl"
            notifications = Path(tmp) / "notifications.jsonl"
            pos = self.make_position()
            state = {"positions": [pos], "closed_positions": []}
            plan = [{
                "action": "stop_loss_dry_run",
                "token_id": "tok-1",
                "market_id": "m1",
                "question": "Question m1",
                "entry_price": 0.93,
                "shares": 10.0,
                "size_usd": 9.3,
                "best_bid": 0.68,
                "bid_depth_usd": 500.0,
                "reasons": ["absolute_price"],
                "order_type": "FOK",
                "dry_run": True,
                "expected_proceeds_usd": 6.8,
            }]
            state, executed, dry_run_count, rows = execute_stop_loss(
                state, plan, journal, notifications, trader=None, mode="live"
            )
            self.assertEqual(executed, 0)
            self.assertEqual(dry_run_count, 1)
            self.assertEqual(len(state["positions"]), 1)  # position untouched
            self.assertEqual(len(state["closed_positions"]), 0)
            self.assertEqual(rows[0]["status"], "dry_run")

    def test_execute_paper_simulates_close(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = Path(tmp) / "journal.jsonl"
            notifications = Path(tmp) / "notifications.jsonl"
            pos = self.make_position()
            state = {"positions": [pos], "closed_positions": []}
            plan = [{
                "action": "stop_loss",
                "token_id": "tok-1",
                "market_id": "m1",
                "question": "Question m1",
                "entry_price": 0.93,
                "shares": 10.0,
                "size_usd": 9.3,
                "best_bid": 0.68,
                "reasons": ["absolute_price"],
                "order_type": "FOK",
                "dry_run": False,
                "expected_proceeds_usd": 6.8,
            }]
            state, executed, dry_run_count, rows = execute_stop_loss(
                state, plan, journal, notifications, trader=None, mode="paper"
            )
            self.assertEqual(executed, 1)
            self.assertEqual(dry_run_count, 0)
            self.assertEqual(len(state["positions"]), 0)
            self.assertEqual(len(state["closed_positions"]), 1)
            closed = state["closed_positions"][0]
            self.assertAlmostEqual(closed["payout_usd"], 6.8, places=4)
            self.assertAlmostEqual(closed["realized_pnl"], 6.8 - 9.3, places=4)
            self.assertEqual(closed["close_reason"], "stop_loss_simulated")


if __name__ == "__main__":
    unittest.main()
