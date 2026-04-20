import unittest

import pandas as pd

from src.collectors.base import BaseCollector


class DummyCollector(BaseCollector):
    country_code = "US"

    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        raise NotImplementedError


class CollectorAggregationTests(unittest.TestCase):
    def test_aggregate_sectors_includes_weekly_return_average(self) -> None:
        collector = DummyCollector()
        df = pd.DataFrame([
            {
                "ticker": "AAA",
                "name": "Alpha",
                "sector": "정보기술",
                "daily_return": 2.0,
                "weekly_return": 6.0,
                "volume": 100,
                "avg_volume_20d": 80,
            },
            {
                "ticker": "BBB",
                "name": "Beta",
                "sector": "정보기술",
                "daily_return": -1.0,
                "weekly_return": -2.0,
                "volume": 120,
                "avg_volume_20d": 100,
            },
            {
                "ticker": "CCC",
                "name": "Gamma",
                "sector": "정보기술",
                "daily_return": 0.5,
                "weekly_return": None,
                "volume": 90,
                "avg_volume_20d": 90,
            },
        ])

        rows = collector._aggregate_sectors(df, "2026-04-20", "US")

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["weekly_return"], 2.0, places=4)
        self.assertEqual(rows[0]["stock_count"], 3)
