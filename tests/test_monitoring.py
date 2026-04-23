import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.database as database
from src.monitor import format_failure_alert, format_status_report, get_operational_status


class MonitoringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tempdir.name) / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "marketbot.db"

        self.patchers = [
            patch.object(database, "DATA_DIR", self.data_dir),
            patch.object(database, "DB_PATH", self.db_path),
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

        database.upsert_sector_performance(
            conn,
            [
                {
                    "date": "2026-04-18",
                    "country": "US",
                    "sector": "정보기술",
                    "daily_return": 1.0,
                    "weekly_return": 3.0,
                    "breadth": 0.7,
                    "volume_change": 5.0,
                    "stock_count": 50,
                    "top_gainers": [],
                    "top_losers": [],
                    "collected_at": "2026-04-18T00:00:00",
                },
                {
                    "date": "2026-04-10",
                    "country": "VN",
                    "sector": "금융",
                    "daily_return": -0.3,
                    "weekly_return": -1.2,
                    "breadth": 0.4,
                    "volume_change": 2.0,
                    "stock_count": 20,
                    "top_gainers": [],
                    "top_losers": [],
                    "collected_at": "2026-04-10T00:00:00",
                },
            ],
        )

        database.log_collection(conn, "US", "success", total=100, filtered=10, abnormal=0)
        database.log_collection(conn, "VN", "success", total=80, filtered=10, abnormal=0)
        database.log_collection(
            conn,
            "VN",
            "failed",
            error="vnstock API 호출 한도를 초과했습니다.",
            failure_code="provider_rate_limited",
            failure_stage="fetch_history",
            run_mode="incremental",
            provider="vnstock",
            raw_error_excerpt="Rate limit exceeded",
        )
        conn.execute(
            "UPDATE collection_log SET timestamp = ? WHERE market = ? AND status = ?",
            ("2026-04-18T08:00:00", "US", "success"),
        )
        conn.execute(
            "UPDATE collection_log SET timestamp = ? WHERE market = ? AND status = ?",
            ("2026-04-10T08:00:00", "VN", "success"),
        )
        conn.execute(
            "UPDATE collection_log SET timestamp = ? WHERE market = ? AND status = ?",
            ("2026-04-20T08:30:00", "VN", "failed"),
        )
        conn.commit()
        conn.close()

    def test_get_operational_status_marks_error_and_no_data(self) -> None:
        snapshot = get_operational_status(as_of_date="2026-04-20", stale_after_days=4)
        markets = {market["code"]: market for market in snapshot["markets"]}

        self.assertEqual(markets["US"]["state"], "OK")
        self.assertEqual(markets["VN"]["state"], "ERROR")
        self.assertEqual(markets["VN"]["last_failure_code"], "provider_rate_limited")
        self.assertEqual(markets["KR"]["state"], "NO_DATA")

    def test_format_status_report_mentions_failed_and_no_data_markets(self) -> None:
        text = format_status_report(as_of_date="2026-04-20")

        self.assertIn("최근 실패 로그", text)
        self.assertIn("베트남 (VN) ERROR", text)
        self.assertIn("API 한도 초과", text)
        self.assertIn("한국 (KR) NO_DATA", text)

    def test_format_failure_alert_focuses_on_failed_markets(self) -> None:
        text = format_failure_alert(["VN"], as_of_date="2026-04-20")

        self.assertIn("실패 시장: VN", text)
        self.assertIn("VN", text)
        self.assertIn("API 한도 초과", text)
