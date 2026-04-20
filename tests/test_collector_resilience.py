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
