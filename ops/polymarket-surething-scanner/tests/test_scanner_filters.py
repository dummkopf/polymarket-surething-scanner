from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scanner import is_high_randomness_narrative_market


class ScannerFilterTests(unittest.TestCase):
    def test_filters_all_in_podcast_word_mention_market(self) -> None:
        market = {
            "question": "What will be said on the next All-In Podcast? (March 27)",
            "description": "Will Iran be said on the next All-In Podcast?",
            "slug": "what-will-be-said-on-the-next-all-in-podcast-march-27",
            "events": [{"title": "All-In Podcast"}],
        }
        self.assertTrue(is_high_randomness_narrative_market(market))

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
