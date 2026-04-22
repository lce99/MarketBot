import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.database as database
from src.collectors import benchmark


class BenchmarkCollectorTests(unittest.TestCase):
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

    def test_collect_benchmarks_retries_empty_download(self) -> None:
        empty_frame = pd.DataFrame()
        valid_frame = pd.DataFrame(
            {
                "Close": [100.0, 101.0],
            },
            index=pd.to_datetime(["2026-04-18", "2026-04-21"]),
        )

        with patch.object(
            benchmark,
            "BENCHMARK_TICKERS",
            {
                "US_IT": {
                    "ticker": "XLK",
                    "country": "US",
                    "sector": "정보기술",
                }
            },
        ):
            with patch(
                "src.collectors.benchmark.yf.download",
                side_effect=[empty_frame, valid_frame],
            ) as mock_download:
                with patch("src.collectors.benchmark.time.sleep", return_value=None):
                    saved_rows = benchmark.collect_benchmarks("2026-04-21")

        self.assertEqual(saved_rows, 1)
        self.assertEqual(mock_download.call_count, 2)

        conn = database.get_connection()
        try:
            row = conn.execute(
                """
                SELECT ticker, daily_return
                FROM benchmark_daily
                WHERE date = '2026-04-21'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(row["ticker"], "XLK")
        self.assertAlmostEqual(row["daily_return"], 1.0, places=4)
