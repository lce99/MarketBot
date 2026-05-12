import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import report as report_script
from src import reporter
import src.database as database


class ReportSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tempdir.name) / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "marketbot.db"
        self.raw_db_path = self.data_dir / "marketbot_raw.db"

        self.patchers = [
            patch.dict(
                os.environ,
                {
                    "MARKETBOT_WATCHLIST": "",
                    "MARKETBOT_WATCHLIST_PATH": str(self.data_dir / "watchlist.json"),
                },
            ),
            patch.object(database, "DATA_DIR", self.data_dir),
            patch.object(database, "DB_PATH", self.db_path),
            patch.object(database, "RAW_DB_PATH", self.raw_db_path),
        ]
        for patcher in self.patchers:
            patcher.start()

        database.init_db()
        self._seed_database()

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.tempdir.cleanup()

    def _seed_database(self) -> None:
        conn = database.get_connection()
        now = "2026-04-20T00:00:00"

        database.upsert_sector_performance(
            conn,
            [
                {
                    "date": "2026-04-20",
                    "country": "US",
                    "sector": "정보기술",
                    "daily_return": 1.2,
                    "weekly_return": 0.0,
                    "breadth": 0.75,
                    "volume_change": 8.0,
                    "stock_count": 120,
                    "top_gainers": [{"name": "NVIDIA", "return": 4.2}],
                    "top_losers": [{"name": "Intel", "return": -1.1}],
                    "collected_at": now,
                },
                {
                    "date": "2026-04-20",
                    "country": "KR",
                    "sector": "정보기술",
                    "daily_return": 0.4,
                    "weekly_return": 10.0,
                    "breadth": 0.55,
                    "volume_change": 3.0,
                    "stock_count": 80,
                    "top_gainers": [{"name": "삼성전자", "return": 2.0}],
                    "top_losers": [{"name": "LG전자", "return": -0.7}],
                    "collected_at": now,
                },
                {
                    "date": "2026-04-20",
                    "country": "US",
                    "sector": "금융",
                    "daily_return": -0.9,
                    "weekly_return": -1.8,
                    "breadth": 0.30,
                    "volume_change": 6.0,
                    "stock_count": 60,
                    "top_gainers": [{"name": "JPMorgan", "return": 0.5}],
                    "top_losers": [{"name": "Goldman Sachs", "return": -2.2}],
                    "collected_at": now,
                },
                {
                    "date": "2026-04-20",
                    "country": "KR",
                    "sector": "금융",
                    "daily_return": -0.2,
                    "weekly_return": -0.4,
                    "breadth": 0.45,
                    "volume_change": 2.0,
                    "stock_count": 40,
                    "top_gainers": [{"name": "KB금융", "return": 0.7}],
                    "top_losers": [{"name": "신한지주", "return": -1.0}],
                    "collected_at": now,
                },
            ],
        )

        database.upsert_benchmark_daily(
            conn,
            [
                {
                    "date": "2026-04-20",
                    "ticker": "XLK",
                    "name": "US_IT",
                    "country": "US",
                    "sector": "정보기술",
                    "close_price": 200.0,
                    "daily_return": 0.9,
                    "weekly_return": 3.4,
                },
                {
                    "date": "2026-04-20",
                    "ticker": "XLF",
                    "name": "US_FIN",
                    "country": "US",
                    "sector": "금융",
                    "close_price": 48.0,
                    "daily_return": -0.4,
                    "weekly_return": -0.8,
                },
                {
                    "date": "2026-04-19",
                    "ticker": "^KS11",
                    "name": "KR_KOSPI",
                    "country": "KR",
                    "sector": None,
                    "close_price": 2800.0,
                    "daily_return": 0.15,
                    "weekly_return": 1.1,
                },
            ],
        )

        database.replace_abnormal_stocks(
            conn,
            "2026-04-20",
            "KR",
            [
                {
                    "date": "2026-04-20",
                    "ticker": "005930",
                    "name": "삼성전자",
                    "country": "KR",
                    "sector": "정보기술",
                    "market_cap": 350_000_000_000_000,
                    "close_price": 70000,
                    "daily_return": 52.0,
                    "volume": 1000000,
                    "avg_volume_20d": 800000,
                    "is_filtered": 0,
                    "is_abnormal": 1,
                }
            ],
        )

        conn.commit()
        conn.close()

    def test_prepare_report_data_writes_trend_scores(self) -> None:
        report_script.prepare_report_data(date="2026-04-20")

        conn = database.get_connection()
        count = conn.execute(
            "SELECT COUNT(*) FROM trend_scores WHERE date = ?",
            ("2026-04-20",),
        ).fetchone()[0]
        conn.close()

        self.assertEqual(count, 2)

    def test_prepare_report_data_defaults_to_latest_sector_date(self) -> None:
        conn = database.get_connection()
        database.upsert_sector_performance(
            conn,
            [
                {
                    "date": "2026-04-21",
                    "country": "US",
                    "sector": "정보기술",
                    "daily_return": 2.0,
                    "weekly_return": 4.0,
                    "breadth": 0.8,
                    "volume_change": 5.0,
                    "stock_count": 100,
                    "top_gainers": [{"name": "NVIDIA", "return": 5.0}],
                    "top_losers": [],
                    "collected_at": "2026-04-21T00:00:00",
                }
            ],
        )
        conn.commit()
        conn.close()

        report_script.prepare_report_data()

        conn = database.get_connection()
        latest_count = conn.execute(
            "SELECT COUNT(*) FROM trend_scores WHERE date = ?",
            ("2026-04-21",),
        ).fetchone()[0]
        stale_count = conn.execute(
            "SELECT COUNT(*) FROM trend_scores WHERE date > ?",
            ("2026-04-21",),
        ).fetchone()[0]
        conn.close()

        self.assertEqual(latest_count, 1)
        self.assertEqual(stale_count, 0)

    def test_prepare_report_data_averages_zero_weekly_returns(self) -> None:
        report_script.prepare_report_data(date="2026-04-20")

        conn = database.get_connection()
        trend_score = conn.execute(
            "SELECT trend_score FROM trend_scores WHERE date = ? AND sector = ?",
            ("2026-04-20", "정보기술"),
        ).fetchone()[0]
        conn.close()

        self.assertAlmostEqual(trend_score, 51.4, places=2)

    def test_format_daily_report_includes_benchmarks(self) -> None:
        report_script.prepare_report_data(date="2026-04-20")
        messages = reporter.format_daily_report(date="2026-04-20")
        joined = "\n".join(messages)

        self.assertIn("글로벌 섹터 데일리 리포트", joined)
        self.assertIn("기준일 2026-04-20", joined)
        self.assertIn("핵심 결론", joined)
        self.assertIn("관심 후보", joined)
        self.assertIn("관찰점수", joined)
        self.assertIn("미국", joined)
        self.assertIn("XLK +0.90% · 대비 +0.30%", joined)
        self.assertIn("기준선 KOSPI +0.15% (04-19)", joined)
        self.assertIn("비정상 급등/급락 1종목", joined)

    def test_watch_candidates_are_scored_and_risky_leaders_are_separated(self) -> None:
        conn = database.get_connection()
        database.upsert_sector_performance(
            conn,
            [
                {
                    "date": "2026-04-22",
                    "country": "US",
                    "sector": "정보기술",
                    "daily_return": 2.0,
                    "weekly_return": 4.0,
                    "breadth": 0.80,
                    "volume_change": 7.0,
                    "stock_count": 120,
                    "top_gainers": [{"name": "NVIDIA", "return": 4.2}],
                    "top_losers": [],
                    "collected_at": "2026-04-22T00:00:00",
                },
                {
                    "date": "2026-04-22",
                    "country": "KR",
                    "sector": "헬스케어",
                    "daily_return": 1.0,
                    "weekly_return": 1.0,
                    "breadth": 0.20,
                    "volume_change": 10.0,
                    "stock_count": 20,
                    "top_gainers": [{"name": "테마바이오", "return": 35.0}],
                    "top_losers": [],
                    "collected_at": "2026-04-22T00:00:00",
                },
            ],
        )
        conn.commit()
        conn.close()

        report_script.prepare_report_data(date="2026-04-22")
        header = reporter.format_daily_report(date="2026-04-22")[0]

        self.assertIn("NVIDIA 관찰점수", header)
        self.assertIn("벤치 대비", header)
        self.assertIn("제외/주의 신호", header)
        self.assertIn("테마바이오", header)
        self.assertIn("상승 확산 약함", header)
        self.assertIn("대표 종목 과열", header)

    def test_daily_report_includes_personal_watchlist_when_configured(self) -> None:
        conn = database.get_connection()
        database.upsert_instrument_universe(
            conn,
            "US",
            [
                {
                    "date": "2026-04-20",
                    "ticker": "NVDA",
                    "name": "NVIDIA",
                    "country": "US",
                    "sector": "정보기술",
                    "market_cap": 2_000_000_000_000,
                    "close_price": 900.0,
                    "daily_return": 4.2,
                    "volume": 1_000_000,
                    "avg_volume_20d": 800_000,
                    "is_filtered": 0,
                    "is_abnormal": 0,
                }
            ],
        )
        conn.commit()
        conn.close()

        watchlist_json = json.dumps(
            [{"country": "US", "ticker": "NVDA", "name": "NVIDIA"}],
            ensure_ascii=False,
        )
        with patch.dict(os.environ, {"MARKETBOT_WATCHLIST": watchlist_json}):
            report_script.prepare_report_data(date="2026-04-20")
            header = reporter.format_daily_report(date="2026-04-20")[0]
            watch_report = reporter.format_watchlist_report(date="2026-04-20")

        self.assertIn("내 관심 종목", header)
        self.assertIn("NVIDIA (NVDA)", header)
        self.assertIn("정보기술 우호", header)
        self.assertIn("벤치 대비", header)
        self.assertIn("NVIDIA (NVDA)", watch_report)

    def test_auto_daily_report_marks_stale_latest_data(self) -> None:
        report_script.prepare_report_data()
        messages = reporter.format_daily_report()
        header = messages[0]

        self.assertIn("데이터 신뢰도: 낮음", header)
        self.assertIn("기준일", header)
        self.assertIn("핵심 결론", header)

    def test_format_country_detail_returns_expected_market_section(self) -> None:
        detail = reporter.format_country_detail("KR", date="2026-04-20")

        self.assertIn("한국 섹터 상세", detail)
        self.assertIn("기준선 KOSPI +0.15% (04-19) · 주간 +1.10%", detail)
        self.assertIn("KOSPI +0.15% (04-19) · 대비 +0.25%", detail)
        self.assertIn("금융", detail)

    def test_format_sector_detail_uses_country_and_sector_benchmarks(self) -> None:
        detail = reporter.format_sector_detail("정보기술", date="2026-04-20")

        self.assertIn("XLK +0.90% · 대비 +0.30%", detail)
        self.assertIn("KOSPI +0.15% (04-19) · 대비 +0.25%", detail)
