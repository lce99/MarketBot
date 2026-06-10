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

    def _multi_ticker_frame(self, *, ticker_level: int) -> pd.DataFrame:
        index = pd.to_datetime(["2026-04-18", "2026-04-21"])
        closes = {"XLK": [100.0, 101.0], "XLF": [50.0, 49.0]}
        if ticker_level == 0:
            columns = pd.MultiIndex.from_product([["XLK", "XLF"], ["Close"]])
        else:
            columns = pd.MultiIndex.from_product([["Close"], ["XLK", "XLF"]])
        frame = pd.DataFrame(index=index, columns=columns, dtype=float)
        for ticker, values in closes.items():
            key = (ticker, "Close") if ticker_level == 0 else ("Close", ticker)
            frame[key] = values
        return frame

    def _run_multi_ticker_collection(self, data: pd.DataFrame) -> int:
        tickers = {
            "US_IT": {"ticker": "XLK", "country": "US", "sector": "정보기술"},
            "US_FIN": {"ticker": "XLF", "country": "US", "sector": "금융"},
        }
        with patch.object(benchmark, "BENCHMARK_TICKERS", tickers):
            with patch(
                "src.collectors.benchmark.yf.download",
                return_value=data,
            ):
                return benchmark.collect_benchmarks("2026-04-21")

    def test_collect_benchmarks_handles_ticker_level_columns(self) -> None:
        saved_rows = self._run_multi_ticker_collection(
            self._multi_ticker_frame(ticker_level=0)
        )
        self.assertEqual(saved_rows, 2)

    def test_collect_benchmarks_handles_field_level_columns(self) -> None:
        """yfinance 기본 multi-ticker 응답은 레벨 0이 가격 필드다."""
        saved_rows = self._run_multi_ticker_collection(
            self._multi_ticker_frame(ticker_level=1)
        )
        self.assertEqual(saved_rows, 2)

        conn = database.get_connection()
        try:
            rows = conn.execute(
                """
                SELECT ticker, daily_return
                FROM benchmark_daily
                WHERE date = '2026-04-21'
                ORDER BY ticker
                """
            ).fetchall()
        finally:
            conn.close()

        by_ticker = {row["ticker"]: row["daily_return"] for row in rows}
        self.assertAlmostEqual(by_ticker["XLK"], 1.0, places=4)
        self.assertAlmostEqual(by_ticker["XLF"], -2.0, places=4)
