import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from src.collectors.china import ChinaCollector
from src.collectors.finnhub_collector import FinnhubCollector
from src.collectors.korea import KoreaCollector
from src.collectors.vietnam import VietnamCollector
from src.collectors.yfinance_collector import YfinanceCollector


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "collector_contracts"
EXPECTED_COLUMNS = [
    "ticker",
    "name",
    "sector",
    "market_cap",
    "close_price",
    "daily_return",
    "weekly_return",
    "volume",
    "avg_volume_20d",
]


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / f"{name}.json").read_text(encoding="utf-8"))


def build_download_frame(price_rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(price_rows)
    frame["date"] = pd.to_datetime(frame["date"])
    tickers = list(dict.fromkeys(frame["ticker"].tolist()))
    index = sorted(frame["date"].unique())
    columns = {}

    for ticker in tickers:
        subset = frame[frame["ticker"] == ticker].set_index("date").reindex(index)
        columns[(ticker, "Close")] = subset["close"]
        columns[(ticker, "Volume")] = subset["volume"]

    result = pd.DataFrame(columns, index=index)
    result.index.name = "Date"
    result.columns = pd.MultiIndex.from_tuples(result.columns)
    return result


class CollectorContractTests(unittest.TestCase):
    def assert_contract_frame(
        self,
        actual: pd.DataFrame,
        expected_records: list[dict],
    ) -> None:
        expected = pd.DataFrame(expected_records, columns=EXPECTED_COLUMNS)
        self.assertEqual(list(actual.columns), EXPECTED_COLUMNS)

        actual_sorted = actual[EXPECTED_COLUMNS].sort_values("ticker").reset_index(drop=True)
        expected_sorted = expected.sort_values("ticker").reset_index(drop=True)
        assert_frame_equal(
            actual_sorted,
            expected_sorted,
            check_dtype=False,
            check_exact=False,
            atol=1e-4,
            rtol=1e-4,
        )

    def test_korea_collector_contract_from_pykrx_fixtures(self) -> None:
        fixture = load_fixture("korea")
        current_ohlcv = pd.DataFrame(fixture["current_ohlcv"]).set_index("ticker")
        weekly_ohlcv = pd.DataFrame(fixture["weekly_ohlcv"]).set_index("ticker")
        collector = KoreaCollector()

        def get_market_ohlcv_side_effect(date_fmt, market=None):
            if date_fmt == "20260420" and market == "KOSPI":
                return current_ohlcv.copy()
            if date_fmt == "20260413" and market == "KOSPI":
                return weekly_ohlcv.copy()
            return pd.DataFrame()

        with patch.object(collector, "_candidate_trading_dates", return_value=["20260420"]):
            with patch.object(collector, "_resolve_weekly_reference_date", return_value="20260413"):
                with patch("src.collectors.korea.krx.get_market_ohlcv", side_effect=get_market_ohlcv_side_effect):
                    with patch(
                        "src.collectors.korea.krx.get_market_ticker_name",
                        side_effect=lambda ticker: fixture["ticker_names"][ticker],
                    ):
                        with patch.object(
                            collector,
                            "_build_sector_map",
                            side_effect=lambda *_args, **_kwargs: fixture["sector_map"],
                        ):
                            with patch("src.collectors.korea.time.sleep", return_value=None):
                                actual = collector.fetch_all_stocks("2026-04-20")

        self.assertEqual(collector.effective_date, "2026-04-20")
        self.assert_contract_frame(
            actual,
            [
                {
                    "ticker": "005930",
                    "name": "삼성전자",
                    "sector": "정보기술",
                    "market_cap": 330000000000000.0,
                    "close_price": 110.0,
                    "daily_return": 5.0,
                    "weekly_return": 10.0,
                    "volume": 1200000.0,
                    "avg_volume_20d": None,
                },
                {
                    "ticker": "000660",
                    "name": "SK하이닉스",
                    "sector": "정보기술",
                    "market_cap": 160000000000000.0,
                    "close_price": 220.0,
                    "daily_return": 10.0,
                    "weekly_return": 10.0,
                    "volume": 800000.0,
                    "avg_volume_20d": None,
                },
            ],
        )

    def test_china_collector_contract_from_tushare_fixtures(self) -> None:
        fixture = load_fixture("china")

        class FakePro:
            def daily(self, trade_date):
                return pd.DataFrame(fixture["daily_by_trade_date"].get(trade_date, []))

            def stock_basic(self, **_kwargs):
                return pd.DataFrame(fixture["stock_basic"])

            def daily_basic(self, **_kwargs):
                return pd.DataFrame(fixture["daily_basic"])

        fake_tushare = types.ModuleType("tushare")
        fake_tushare.set_token = lambda _token: None
        fake_tushare.pro_api = lambda: FakePro()

        with patch.dict(sys.modules, {"tushare": fake_tushare}):
            with patch("src.collectors.china.TUSHARE_TOKEN", "test-token"):
                with patch(
                    "src.collectors.china.recent_dates",
                    return_value=[
                        "2026-04-20",
                        "2026-04-19",
                        "2026-04-18",
                        "2026-04-17",
                        "2026-04-16",
                        "2026-04-15",
                        "2026-04-14",
                    ],
                ):
                    with patch("src.collectors.china.time.sleep", return_value=None):
                        collector = ChinaCollector()
                        actual = collector.fetch_all_stocks("2026-04-20")

        self.assertEqual(collector.effective_date, "2026-04-20")
        self.assert_contract_frame(
            actual,
            [
                {
                    "ticker": "000001.SZ",
                    "name": "平安银行",
                    "sector": "금융",
                    "market_cap": 15000000000.0,
                    "close_price": 120.0,
                    "daily_return": 5.0,
                    "weekly_return": 20.0,
                    "volume": 1500000.0,
                    "avg_volume_20d": None,
                },
                {
                    "ticker": "002475.SZ",
                    "name": "立讯精密",
                    "sector": "정보기술",
                    "market_cap": 9000000000.0,
                    "close_price": 55.0,
                    "daily_return": -1.5,
                    "weekly_return": 10.0,
                    "volume": 900000.0,
                    "avg_volume_20d": None,
                },
            ],
        )

    def test_finnhub_collector_contract_from_symbol_and_price_fixtures(self) -> None:
        fixture = load_fixture("finnhub")
        collector = FinnhubCollector("US")
        collector._client = Mock()
        collector._client.stock_symbols.return_value = fixture["symbols"]

        with patch(
            "src.collectors.finnhub_collector.yf.download",
            return_value=build_download_frame(fixture["prices"]),
        ):
            with patch.object(collector, "_prefilter_stocks", side_effect=lambda stocks, _date: stocks):
                with patch.object(collector, "_add_market_caps", side_effect=lambda df, _date: df):
                    with patch("src.collectors.finnhub_collector.time.sleep", return_value=None):
                        actual = collector.fetch_all_stocks("2026-04-20")

        self.assertEqual(collector.effective_date, "2026-04-20")
        self.assert_contract_frame(
            actual,
            [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "sector": "정보기술",
                    "market_cap": None,
                    "close_price": 210.0,
                    "daily_return": 5.0,
                    "weekly_return": 16.6666666667,
                    "volume": 110000000.0,
                    "avg_volume_20d": 97333333.3333,
                },
                {
                    "ticker": "JPM",
                    "name": "JPMorgan Chase & Co.",
                    "sector": "금융",
                    "market_cap": None,
                    "close_price": 105.0,
                    "daily_return": 0.9615384615,
                    "weekly_return": 5.0,
                    "volume": 55000000.0,
                    "avg_volume_20d": 52500000.0,
                },
            ],
        )

    def test_yfinance_collector_contract_from_download_and_info_fixtures(self) -> None:
        fixture = load_fixture("yfinance")

        class FakeTicker:
            def __init__(self, ticker):
                self.info = fixture["info"].get(ticker, {})

        with patch(
            "src.collectors.yfinance_collector.yf.download",
            return_value=build_download_frame(fixture["prices"]),
        ):
            with patch("src.collectors.yfinance_collector.yf.Ticker", side_effect=FakeTicker):
                with patch("src.collectors.yfinance_collector.time.sleep", return_value=None):
                    collector = YfinanceCollector("JP", ["6758.T", "8306.T"])
                    with patch.object(collector, "_get_cached_metadata", return_value={}):
                        with patch.object(collector, "_upsert_metadata", return_value=None):
                            actual = collector.fetch_all_stocks("2026-04-20")

        self.assertEqual(collector.effective_date, "2026-04-20")
        self.assert_contract_frame(
            actual,
            [
                {
                    "ticker": "6758.T",
                    "name": "Sony Group Corp.",
                    "sector": "정보기술",
                    "market_cap": 15000000000000.0,
                    "close_price": 11000.0,
                    "daily_return": 1.8518518519,
                    "weekly_return": 10.0,
                    "volume": 1500000.0,
                    "avg_volume_20d": 1250000.0,
                },
                {
                    "ticker": "8306.T",
                    "name": "Mitsubishi UFJ Financial",
                    "sector": "금융",
                    "market_cap": 12000000000000.0,
                    "close_price": 1050.0,
                    "daily_return": 0.9615384615,
                    "weekly_return": 5.0,
                    "volume": 600000.0,
                    "avg_volume_20d": 550000.0,
                },
            ],
        )

    def test_vietnam_collector_contract_from_vnstock_fixtures(self) -> None:
        fixture = load_fixture("vietnam")

        class FakeVnstock:
            def stock(self, symbol=None, source=None):
                if symbol is None:
                    return types.SimpleNamespace(
                        listing=types.SimpleNamespace(
                            all_symbols=lambda: pd.DataFrame(fixture["listing"])
                        )
                    )

                return types.SimpleNamespace(
                    quote=types.SimpleNamespace(
                        history=lambda start, end, _symbol=symbol: pd.DataFrame(
                            fixture["histories"][_symbol]
                        )
                    )
                )

        fake_vnstock = types.ModuleType("vnstock")
        fake_vnstock.Vnstock = FakeVnstock

        with patch.dict(sys.modules, {"vnstock": fake_vnstock}):
            with patch("src.collectors.vietnam.time.sleep", return_value=None):
                collector = VietnamCollector()
                with patch.object(
                    collector,
                    "_select_listing_candidates",
                    side_effect=lambda listing, _date: listing,
                ):
                    actual = collector.fetch_all_stocks("2026-04-20")

        self.assertEqual(collector.effective_date, "2026-04-20")
        self.assert_contract_frame(
            actual,
            [
                {
                    "ticker": "VCB",
                    "name": "Vietcombank",
                    "sector": "금융",
                    "market_cap": 500000000000.0,
                    "close_price": 85.0,
                    "daily_return": 1.1904761905,
                    "weekly_return": 6.25,
                    "volume": 1500000.0,
                    "avg_volume_20d": 1250000.0,
                },
                {
                    "ticker": "FPT",
                    "name": "FPT Corp",
                    "sector": "정보기술",
                    "market_cap": 300000000000.0,
                    "close_price": 110.0,
                    "daily_return": 5.7692307692,
                    "weekly_return": 10.0,
                    "volume": 2500000.0,
                    "avg_volume_20d": 2250000.0,
                },
            ],
        )
