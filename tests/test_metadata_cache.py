import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import src.database as database
from src.collectors.finnhub_collector import FinnhubCollector
from src.collectors.yfinance_collector import YfinanceCollector


class MetadataCacheTests(unittest.TestCase):
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

    def _seed_metadata(self, country: str, rows: list[dict], refreshed_at: str) -> None:
        database.init_db()
        conn = database.get_connection()
        try:
            database.upsert_instrument_metadata(conn, country, rows, source="test")
            conn.execute(
                """
                UPDATE instrument_metadata
                SET last_refreshed_at = ?
                WHERE country = ?
                """,
                (refreshed_at, country),
            )
            conn.commit()
        finally:
            conn.close()

    def test_yfinance_collector_uses_cached_metadata_without_info_calls(self) -> None:
        self._seed_metadata(
            "JP",
            [
                {
                    "ticker": "6758.T",
                    "name": "Sony Group Corp.",
                    "sector": "정보기술",
                    "market_cap": 15_000_000_000_000,
                }
            ],
            refreshed_at="2026-04-20T00:00:00",
        )
        collector = YfinanceCollector("JP", ["6758.T"])
        df = pd.DataFrame(
            [
                {
                    "ticker": "6758.T",
                    "name": "6758",
                    "sector": "기타",
                    "market_cap": None,
                    "close_price": 11000.0,
                    "daily_return": 1.0,
                    "weekly_return": 5.0,
                    "volume": 1_500_000.0,
                    "avg_volume_20d": 1_200_000.0,
                }
            ]
        )

        with patch("src.collectors.yfinance_collector.yf.Ticker") as mock_ticker:
            result = collector._add_sector_and_cap(df.copy(), "2026-04-21")

        mock_ticker.assert_not_called()
        self.assertEqual(result.iloc[0]["name"], "Sony Group Corp.")
        self.assertEqual(result.iloc[0]["sector"], "정보기술")
        self.assertEqual(result.iloc[0]["market_cap"], 15_000_000_000_000)

    def test_finnhub_collector_uses_cached_metadata_without_profile_calls(self) -> None:
        self._seed_metadata(
            "US",
            [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "sector": "정보기술",
                    "market_cap": 3_200_000_000_000,
                }
            ],
            refreshed_at="2026-04-20T00:00:00",
        )
        collector = FinnhubCollector("US")
        collector._client = Mock()
        df = pd.DataFrame(
            [
                {
                    "ticker": "AAPL",
                    "name": "AAPL",
                    "sector": "기타",
                    "market_cap": None,
                    "close_price": 210.0,
                    "daily_return": 5.0,
                    "volume": 110_000_000.0,
                    "avg_volume_20d": 97_333_333.0,
                }
            ]
        )

        result = collector._add_market_caps(df.copy(), "2026-04-21")

        collector._client.company_profile2.assert_not_called()
        self.assertEqual(result.iloc[0]["name"], "Apple Inc.")
        self.assertEqual(result.iloc[0]["sector"], "정보기술")
        self.assertEqual(result.iloc[0]["market_cap"], 3_200_000_000_000)

    def test_upsert_instrument_universe_preserves_known_sector(self) -> None:
        database.init_db()
        conn = database.get_connection()
        try:
            database.upsert_instrument_universe(
                conn,
                "KR",
                [
                    {
                        "date": "2026-04-20",
                        "ticker": "005930",
                        "name": "삼성전자",
                        "country": "KR",
                        "sector": "정보기술",
                        "market_cap": 330_000_000_000_000.0,
                        "close_price": 100.0,
                        "daily_return": 0.5,
                        "volume": 1_000_000.0,
                        "avg_volume_20d": 900_000.0,
                        "is_filtered": 0,
                        "is_abnormal": 0,
                    }
                ],
            )
            database.upsert_instrument_universe(
                conn,
                "KR",
                [
                    {
                        "date": "2026-04-21",
                        "ticker": "005930",
                        "name": "",
                        "country": "KR",
                        "sector": "기타",
                        "market_cap": None,
                        "close_price": 101.0,
                        "daily_return": 1.0,
                        "volume": 1_100_000.0,
                        "avg_volume_20d": None,
                        "is_filtered": 0,
                        "is_abnormal": 0,
                    }
                ],
            )
            conn.commit()

            row = conn.execute(
                """
                SELECT name, sector, market_cap, avg_volume_20d
                FROM instrument_universe
                WHERE country = 'KR' AND ticker = '005930'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(row["name"], "삼성전자")
        self.assertEqual(row["sector"], "정보기술")
        self.assertEqual(row["market_cap"], 330_000_000_000_000.0)
        self.assertEqual(row["avg_volume_20d"], 900_000.0)
