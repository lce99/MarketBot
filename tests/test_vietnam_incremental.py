import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.collectors.vietnam as vietnam_module
import src.database as database
from src.collectors.vietnam import VietnamCollector


class VietnamIncrementalTests(unittest.TestCase):
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
            database.upsert_instrument_universe(conn, "VN", rows)
            conn.commit()
        finally:
            conn.close()

    def _seed_abnormals(self, rows: list[dict]) -> None:
        database.init_db()
        conn = database.get_connection()
        try:
            database.upsert_abnormal_stocks(conn, rows)
            conn.commit()
        finally:
            conn.close()

    def test_select_listing_candidates_uses_active_abnormal_and_large_caps(self) -> None:
        self._seed_universe(
            [
                {
                    "date": "2026-04-20",
                    "ticker": "AAA",
                    "name": "Alpha",
                    "sector": "금융",
                    "market_cap": 400_000_000_000,
                    "close_price": 10.0,
                    "volume": 100_000,
                    "avg_volume_20d": 80_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "BBB",
                    "name": "Beta",
                    "sector": "금융",
                    "market_cap": 350_000_000_000,
                    "close_price": 9.0,
                    "volume": 90_000,
                    "avg_volume_20d": 75_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "CCC",
                    "name": "Gamma",
                    "sector": "정보기술",
                    "market_cap": 900_000_000_000,
                    "close_price": 12.0,
                    "volume": 70_000,
                    "avg_volume_20d": 60_000,
                    "is_filtered": 1,
                    "is_abnormal": 0,
                },
            ]
        )
        self._seed_abnormals(
            [
                {
                    "date": "2026-04-20",
                    "ticker": "DDD",
                    "name": "Delta",
                    "country": "VN",
                    "sector": "산업재",
                    "market_cap": 250_000_000_000,
                    "close_price": 8.0,
                    "daily_return": 51.0,
                    "volume": 120_000,
                    "avg_volume_20d": 60_000,
                }
            ]
        )
        listing = pd.DataFrame(
            [
                {"ticker": "AAA"},
                {"ticker": "BBB"},
                {"ticker": "CCC"},
                {"ticker": "DDD"},
                {"ticker": "EEE"},
            ]
        )
        collector = VietnamCollector()

        with patch.dict(
            vietnam_module.VN_INCREMENTAL_FULL_REFRESH_WEEKDAY,
            {},
            clear=True,
        ):
            with patch.object(vietnam_module, "VN_INCREMENTAL_LARGE_CAP_COUNT", 1):
                with patch.object(vietnam_module, "VN_INCREMENTAL_MIN_CANDIDATES", 1):
                    result = collector._select_listing_candidates(listing, "2026-04-21")

        self.assertEqual(
            set(result["ticker"]),
            {"AAA", "BBB", "CCC", "DDD"},
        )

    def test_select_listing_candidates_returns_full_listing_on_refresh_day(self) -> None:
        self._seed_universe(
            [
                {
                    "date": "2026-04-20",
                    "ticker": "AAA",
                    "name": "Alpha",
                    "sector": "금융",
                    "market_cap": 400_000_000_000,
                    "close_price": 10.0,
                    "volume": 100_000,
                    "avg_volume_20d": 80_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                }
            ]
        )
        listing = pd.DataFrame(
            [
                {"ticker": "AAA"},
                {"ticker": "BBB"},
            ]
        )
        collector = VietnamCollector()

        with patch.dict(
            vietnam_module.VN_INCREMENTAL_FULL_REFRESH_WEEKDAY,
            {"VN": 1},
            clear=True,
        ):
            result = collector._select_listing_candidates(listing, "2026-04-21")

        pd.testing.assert_frame_equal(result.reset_index(drop=True), listing)
