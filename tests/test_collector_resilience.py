import sys
import types
import unittest
from unittest.mock import patch

import pandas as pd

from src.collectors.korea import KoreaCollector
from src.collectors.vietnam import VietnamCollector


class CollectorResilienceTests(unittest.TestCase):
    def test_korea_fetch_market_retries_invalid_pykrx_response(self) -> None:
        collector = KoreaCollector()
        invalid_ohlcv = pd.DataFrame({"foo": [1]}, index=["005930"])
        valid_ohlcv = pd.DataFrame(
            [
                {
                    "ticker": "005930",
                    "종가": 110.0,
                    "거래량": 1_200_000.0,
                    "등락률": 5.0,
                    "시가총액": 330_000_000_000_000.0,
                }
            ]
        ).set_index("ticker")

        with patch(
            "src.collectors.korea.krx.get_market_ohlcv",
            side_effect=[invalid_ohlcv, valid_ohlcv],
        ) as mock_get_market_ohlcv:
            with patch(
                "src.collectors.korea.krx.get_market_ticker_name",
                return_value="삼성전자",
            ):
                with patch.object(
                    collector,
                    "_build_sector_map",
                    return_value={"005930": "전기전자"},
                ):
                    with patch("src.collectors.korea.time.sleep", return_value=None):
                        actual = collector._fetch_market("20260420", "KOSPI")

        self.assertEqual(mock_get_market_ohlcv.call_count, 2)
        self.assertIsNotNone(actual)
        self.assertEqual(len(actual), 1)
        self.assertEqual(actual.iloc[0]["ticker"], "005930")
        self.assertEqual(actual.iloc[0]["sector"], "정보기술")

    def test_vietnam_load_listing_supports_listing_api(self) -> None:
        class FakeListing:
            def __init__(self, source=None):
                self.source = source

            def all_symbols(self):
                return pd.DataFrame(
                    [
                        {"symbol": "VCB", "organ_name": "Vietcombank"},
                        {"symbol": "FPT", "organ_name": "FPT Corp"},
                    ]
                )

            def symbols_by_industries(self):
                return pd.DataFrame(
                    [
                        {"symbol": "VCB", "industry_name": "Banks"},
                        {"symbol": "FPT", "industry_name": "Technology"},
                    ]
                )

        class FakeVnstock:
            def stock(self, symbol=None, source=None):
                return types.SimpleNamespace(symbol=symbol, source=source)

        fake_vnstock = types.ModuleType("vnstock")
        fake_vnstock.Listing = FakeListing
        fake_vnstock.Vnstock = FakeVnstock

        with patch.dict(sys.modules, {"vnstock": fake_vnstock}):
            collector = VietnamCollector()
            listing = collector._load_listing()

        self.assertEqual(list(listing["ticker"]), ["VCB", "FPT"])
        self.assertEqual(list(listing["name"]), ["Vietcombank", "FPT Corp"])
        self.assertEqual(list(listing["industry"]), ["Banks", "Technology"])

    def test_vietnam_load_listing_falls_back_to_cached_universe(self) -> None:
        class BrokenListing:
            def __init__(self, source=None):
                raise ValueError(f"listing unavailable: {source}")

        class BrokenQuote:
            def __init__(self, symbol=None, source=None):
                raise ValueError(f"quote unavailable: {symbol}/{source}")

        class BrokenVnstock:
            def stock(self, symbol=None, source=None):
                raise AttributeError("legacy listing unavailable")

        fake_vnstock = types.ModuleType("vnstock")
        fake_vnstock.Listing = BrokenListing
        fake_vnstock.Quote = BrokenQuote
        fake_vnstock.Vnstock = BrokenVnstock
        fake_conn = types.SimpleNamespace(close=lambda: None)

        with patch.dict(sys.modules, {"vnstock": fake_vnstock}):
            with patch("src.collectors.vietnam.get_connection", return_value=fake_conn):
                with patch(
                    "src.collectors.vietnam.get_instrument_universe",
                    return_value=[
                        {
                            "ticker": "VCB",
                            "name": "Vietcombank",
                            "sector": "금융",
                            "market_cap": 500000000000.0,
                        },
                        {
                            "ticker": "FPT",
                            "name": "FPT Corp",
                            "sector": "정보기술",
                            "market_cap": 300000000000.0,
                        },
                    ],
                ):
                    collector = VietnamCollector()
                    listing = collector._load_listing()

        self.assertEqual(list(listing["ticker"]), ["VCB", "FPT"])
        self.assertEqual(list(listing["sector"]), ["금융", "정보기술"])

    def test_korea_fetch_all_stocks_stops_after_transport_failure(self) -> None:
        collector = KoreaCollector()
        invalid_ohlcv = pd.DataFrame({"foo": [1]}, index=["005930"])

        with patch.object(
            collector,
            "_candidate_trading_dates",
            return_value=["20260421", "20260420"],
        ):
            with patch.object(collector, "_resolve_weekly_reference_date", return_value=None):
                with patch(
                    "src.collectors.korea.krx.get_market_ohlcv",
                    return_value=invalid_ohlcv,
                ) as mock_get_market_ohlcv:
                    with patch("src.collectors.korea.time.sleep", return_value=None):
                        actual = collector.fetch_all_stocks("2026-04-21")

        self.assertTrue(actual.empty)
        self.assertEqual(mock_get_market_ohlcv.call_count, 4)

    def test_korea_fetch_market_falls_back_to_finance_data_reader(self) -> None:
        collector = KoreaCollector()
        invalid_ohlcv = pd.DataFrame({"foo": [1]}, index=["005930"])

        def stock_listing(symbol):
            if symbol == "KRX-MARCAP":
                return pd.DataFrame(
                    [
                        {
                            "Code": "005930",
                            "Name": "삼성전자",
                            "Close": 110.0,
                            "Volume": 1_200_000.0,
                            "ChagesRatio": 5.0,
                            "Marcap": 330_000_000.0,
                            "Market": "KOSPI",
                        }
                    ]
                )
            if symbol == "KOSPI":
                return pd.DataFrame(
                    [
                        {
                            "Code": "005930",
                            "Name": "삼성전자",
                            "Sector": "전기전자",
                        }
                    ]
                )
            return pd.DataFrame()

        fake_fdr = types.ModuleType("FinanceDataReader")
        fake_fdr.StockListing = stock_listing

        with patch.dict(sys.modules, {"FinanceDataReader": fake_fdr}):
            with patch(
                "src.collectors.korea.krx.get_market_ohlcv",
                return_value=invalid_ohlcv,
            ):
                with patch.object(
                    collector,
                    "_load_cached_universe_map",
                    return_value={
                        "005930": {
                            "ticker": "005930",
                            "name": "삼성전자",
                            "sector": "정보기술",
                            "avg_volume_20d": 900_000.0,
                        }
                    },
                ):
                    with patch.object(
                        collector,
                        "_load_cached_close_map",
                        return_value={"005930": 100.0},
                    ):
                        with patch("src.collectors.korea.time.sleep", return_value=None):
                            actual = collector._fetch_market(
                                "20260421",
                                "KOSPI",
                                weekly_reference_date="20260414",
                            )

        self.assertEqual(len(actual), 1)
        self.assertEqual(actual.iloc[0]["ticker"], "005930")
        self.assertEqual(actual.iloc[0]["sector"], "정보기술")
        self.assertEqual(actual.iloc[0]["market_cap"], 330_000_000_000_000.0)
        self.assertEqual(actual.iloc[0]["weekly_return"], 10.0)
