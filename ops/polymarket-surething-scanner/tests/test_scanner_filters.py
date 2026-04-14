from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scanner import build_scanner_settings, is_commodity_related_market, is_high_randomness_narrative_market


class ScannerFilterTests(unittest.TestCase):
    def test_paper_uses_base_scanner_thresholds(self) -> None:
        config = {
            "scanner": {"price_min": 0.92, "quick_yes_price_min": 0.90},
            "paper": {"scanner": {"price_min": 0.90, "quick_yes_price_min": 0.88}},
        }
        settings = build_scanner_settings(config, "paper")
        self.assertEqual(settings["price_min"], 0.92)
        self.assertEqual(settings["quick_yes_price_min"], 0.90)

    def test_live_uses_base_scanner_thresholds(self) -> None:
        config = {
            "scanner": {"price_min": 0.92, "quick_yes_price_min": 0.90},
            "paper": {"scanner": {"price_min": 0.90, "quick_yes_price_min": 0.88}},
        }
        settings = build_scanner_settings(config, "live")
        self.assertEqual(settings["price_min"], 0.92)
        self.assertEqual(settings["quick_yes_price_min"], 0.90)

    def test_filters_all_in_podcast_word_mention_market(self) -> None:
        market = {
            "question": "What will be said on the next All-In Podcast? (March 27)",
            "description": "Will Iran be said on the next All-In Podcast?",
            "slug": "what-will-be-said-on-the-next-all-in-podcast-march-27",
            "events": [{"title": "All-In Podcast"}],
        }
        self.assertTrue(is_high_randomness_narrative_market(market))

    def test_filters_commodity_direction_market(self) -> None:
        market = {
            "question": "Silver (XAGUSD) Up or Down on April 14?",
            "description": "Resolves based on the XAGUSD price move on the day.",
            "slug": "silver-xagusd-up-or-down-on-april-14",
            "events": [{"title": "Silver direction"}],
        }
        self.assertTrue(is_commodity_related_market(market))

    def test_does_not_filter_mass_participation_event_market(self) -> None:
        market = {
            "question": "Which app will be #1 on the US App Store on March 31?",
            "description": "Resolves by official ranking.",
            "slug": "which-app-will-be-number-1-on-the-us-app-store-on-march-31",
            "events": [{"title": "US App Store ranking"}],
        }
        self.assertFalse(is_high_randomness_narrative_market(market))


if __name__ == "__main__":
    unittest.main()
