import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.collectors.finnhub_collector as finnhub_module
import src.database as database
from src.collectors.base import BaseCollector
from src.collectors.finnhub_collector import FinnhubCollector


class DummyCollector(BaseCollector):
    country_code = "US"

    def __init__(self, rows: list[dict]):
        self._rows = rows

    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        return pd.DataFrame(self._rows)


class UniversePrefilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tempdir.name) / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.summary_db_path = self.data_dir / "marketbot.db"
        self.raw_db_path = self.data_dir / "marketbot_raw.db"

        self.patchers = [
            patch.object(database, "DATA_DIR", self.data_dir),
            patch.object(database, "DB_PATH", self.summary_db_path),
            patch.object(database, "RAW_DB_PATH", self.raw_db_path),
        ]
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.tempdir.cleanup()

    def _seed_universe(self, rows: list[dict]) -> None:
        database.init_db()
        conn = database.get_connection()
        try:
            database.upsert_instrument_universe(conn, "US", rows)
            conn.commit()
        finally:
            conn.close()

    def test_base_collector_run_updates_instrument_universe(self) -> None:
        collector = DummyCollector(
            [
                {
                    "ticker": "AAA",
                    "name": "Alpha",
                    "sector": "Tech",
                    "market_cap": 800_000_000,
                    "close_price": 101.0,
                    "daily_return": 2.0,
                    "weekly_return": 5.0,
                    "volume": 1_000_000,
                    "avg_volume_20d": 900_000,
                },
                {
                    "ticker": "BBB",
                    "name": "Beta",
                    "sector": "Tech",
                    "market_cap": 900_000_000,
                    "close_price": 50.0,
                    "daily_return": 60.0,
                    "weekly_return": 10.0,
                    "volume": 1_000_000,
                    "avg_volume_20d": 950_000,
                },
            ]
        )

        result = collector.run(date="2026-04-20")

        self.assertTrue(result)
        conn = database.get_connection()
        try:
            rows = conn.execute(
                """
                SELECT ticker, last_seen_date, last_is_filtered, last_is_abnormal
                FROM instrument_universe
                ORDER BY ticker
                """
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["ticker"], "AAA")
        self.assertEqual(rows[0]["last_seen_date"], "2026-04-20")
        self.assertEqual(rows[0]["last_is_filtered"], 0)
        self.assertEqual(rows[0]["last_is_abnormal"], 0)
        self.assertEqual(rows[1]["ticker"], "BBB")
        self.assertEqual(rows[1]["last_seen_date"], "2026-04-20")
        self.assertEqual(rows[1]["last_is_filtered"], 0)
        self.assertEqual(rows[1]["last_is_abnormal"], 1)

    def test_prefilter_stocks_uses_cached_priority_order(self) -> None:
        self._seed_universe(
            [
                {
                    "date": "2026-04-20",
                    "ticker": "AAA",
                    "name": "Alpha",
                    "sector": "Tech",
                    "market_cap": 700_000_000,
                    "close_price": 10.0,
                    "volume": 100_000,
                    "avg_volume_20d": 90_000,
                    "is_filtered": 0,
                    "is_abnormal": 1,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "BBB",
                    "name": "Beta",
                    "sector": "Tech",
                    "market_cap": 900_000_000,
                    "close_price": 12.0,
                    "volume": 500_000,
                    "avg_volume_20d": 450_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "CCC",
                    "name": "Gamma",
                    "sector": "Tech",
                    "market_cap": 850_000_000,
                    "close_price": 8.0,
                    "volume": 300_000,
                    "avg_volume_20d": 250_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "DDD",
                    "name": "Delta",
                    "sector": "Tech",
                    "market_cap": 1_000_000_000,
                    "close_price": 7.0,
                    "volume": 900_000,
                    "avg_volume_20d": 850_000,
                    "is_filtered": 1,
                    "is_abnormal": 0,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "EEE",
                    "name": "Epsilon",
                    "sector": "Tech",
                    "market_cap": 750_000_000,
                    "close_price": 6.0,
                    "volume": 400_000,
                    "avg_volume_20d": 380_000,
                    "is_filtered": 1,
                    "is_abnormal": 0,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "FFF",
                    "name": "Zeta",
                    "sector": "Tech",
                    "market_cap": 650_000_000,
                    "close_price": 5.0,
                    "volume": 200_000,
                    "avg_volume_20d": 180_000,
                    "is_filtered": 1,
                    "is_abnormal": 0,
                },
            ]
        )
        collector = FinnhubCollector("US")

        stocks = [
            {"symbol": "AAA"},
            {"symbol": "BBB"},
            {"symbol": "CCC"},
            {"symbol": "DDD"},
            {"symbol": "EEE"},
            {"symbol": "FFF"},
        ]

        with patch.dict(
            finnhub_module.UNIVERSE_PREFILTER_TARGET_COUNT,
            {"US": 3},
            clear=True,
        ):
            with patch.dict(
                finnhub_module.UNIVERSE_PREFILTER_FULL_REFRESH_WEEKDAY,
                {},
                clear=True,
            ):
                selected = collector._prefilter_stocks(stocks, "2026-04-21")

        self.assertEqual([row["symbol"] for row in selected], ["AAA", "BBB", "CCC"])

    def test_prefilter_stocks_skips_on_full_refresh_day(self) -> None:
        self._seed_universe(
            [
                {
                    "date": "2026-04-20",
                    "ticker": ticker,
                    "name": ticker,
                    "sector": "Tech",
                    "market_cap": 700_000_000,
                    "close_price": 10.0,
                    "volume": 100_000,
                    "avg_volume_20d": 90_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                }
                for ticker in ["AAA", "BBB", "CCC", "DDD"]
            ]
        )
        collector = FinnhubCollector("US")
        stocks = [{"symbol": ticker} for ticker in ["AAA", "BBB", "CCC", "DDD"]]

        with patch.dict(
            finnhub_module.UNIVERSE_PREFILTER_TARGET_COUNT,
            {"US": 2},
            clear=True,
        ):
            with patch.dict(
                finnhub_module.UNIVERSE_PREFILTER_FULL_REFRESH_WEEKDAY,
                {"US": 0},
                clear=True,
            ):
                selected = collector._prefilter_stocks(stocks, "2026-04-20")

        self.assertEqual(selected, stocks)
