import unittest

from src.leadlag_evaluation import (
    group_verified_signals,
    summarize_verified_signals,
    wilson_interval,
)
from src.reporter import _scoreboard_verdict


class LeadLagEvaluationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rows = [
            {
                "created_date": "2026-07-01",
                "target_date": "2026-07-02",
                "leader": "US",
                "follower": "KR",
                "sector": "정보기술",
                "lag": 1,
                "predicted_direction": 1,
                "follower_return": 1.0,
                "hit": 1,
            },
            {
                "created_date": "2026-07-02",
                "target_date": "2026-07-03",
                "leader": "US",
                "follower": "KR",
                "sector": "정보기술",
                "lag": 1,
                "predicted_direction": -1,
                "follower_return": 2.0,
                "hit": 0,
            },
            {
                "created_date": "2026-07-03",
                "target_date": "2026-07-06",
                "leader": "DE",
                "follower": "JP",
                "sector": "산업재",
                "lag": 2,
                "predicted_direction": 1,
                "follower_return": 0.5,
                "hit": 1,
            },
            {
                "created_date": "2026-07-06",
                "target_date": "2026-07-07",
                "leader": "DE",
                "follower": "JP",
                "sector": "산업재",
                "lag": 2,
                "predicted_direction": 1,
                "follower_return": -0.5,
                "hit": 0,
            },
        ]

    def test_summary_includes_wilson_baseline_and_bias(self) -> None:
        summary = summarize_verified_signals(self.rows)

        self.assertEqual(summary["total"], 4)
        self.assertEqual(summary["hits"], 2)
        self.assertEqual(summary["hit_rate"], 0.5)
        self.assertEqual(summary["naive_direction"], "up")
        self.assertEqual(summary["naive_rate"], 0.75)
        self.assertEqual(summary["excess_vs_naive"], -0.25)
        self.assertEqual(summary["prediction_up_share"], 0.75)
        self.assertEqual(summary["outcome_up_share"], 0.75)
        self.assertLess(summary["ci95_low"], 0.5)
        self.assertGreater(summary["ci95_high"], 0.5)

    def test_groups_cover_country_sector_lag_and_direction(self) -> None:
        for dimension in (
            "country_pair",
            "leader",
            "follower",
            "sector",
            "lag",
            "direction",
        ):
            groups = group_verified_signals(self.rows, dimension)
            self.assertTrue(groups)
            self.assertEqual(sum(group["total"] for group in groups), 4)

    def test_wilson_empty_sample(self) -> None:
        self.assertEqual(wilson_interval(0, 0), (None, None))

    def test_scoreboard_rejects_accuracy_below_naive_baseline(self) -> None:
        stats = summarize_verified_signals(self.rows * 3)
        verdict = _scoreboard_verdict(stats)

        self.assertIn("기준선 미달", verdict)
        self.assertIn("naive", verdict)


if __name__ == "__main__":
    unittest.main()
