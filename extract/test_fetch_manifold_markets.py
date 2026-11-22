import importlib.util
import unittest
from pathlib import Path

SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_manifold_markets.py"
SPEC = importlib.util.spec_from_file_location("fetch_manifold_markets", SCRIPT_PATH)
manifold = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(manifold)

# Live market observed 2026-11-22: "never closes" sentinel, just past year 9999.
SENTINEL_CLOSE_MS = 253402322399999


class MsTests(unittest.TestCase):
    def test_none_and_zero(self):
        self.assertIsNone(manifold._ms(None))
        self.assertIsNone(manifold._ms(0))

    def test_valid(self):
        # 2026-11-22T18:24:00Z
        self.assertEqual(
            manifold._ms(1795371840000), "2026-11-22T18:24:00+00:00"
        )

    def test_sentinel_past_year_9999_is_none(self):
        self.assertIsNone(manifold._ms(SENTINEL_CLOSE_MS))

    def test_pre_year_1_is_none(self):
        self.assertIsNone(manifold._ms(-70000000000000))


class NormTests(unittest.TestCase):
    def test_norm_tolerates_sentinel_close_time(self):
        # Regression: a market with an out-of-range closeTime used to raise
        # ValueError and kill the whole run with 0 records.
        market = {
            "id": "QIUPORhE5S",
            "question": "In which order will the three remaining millennia go?",
            "closeTime": SENTINEL_CLOSE_MS,
            "createdTime": 1795371840000,
            "outcomeType": "MULTIPLE_CHOICE",
            "probability": None,
        }
        rec = manifold._norm(market, "2026-11-22T18:39:00+00:00")
        self.assertEqual(rec["id"], "QIUPORhE5S")
        self.assertIsNone(rec["close_time"])
        self.assertEqual(rec["created"], "2026-11-22T18:24:00+00:00")
        self.assertIsNone(rec["probability"])


if __name__ == "__main__":
    unittest.main()
