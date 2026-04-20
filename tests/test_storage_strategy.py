import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.database as database


class StorageStrategyTests(unittest.TestCase):
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

    def _create_legacy_summary_db(self) -> None:
        database.init_db()
        conn = database.get_connection()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS stock_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                name TEXT,
                country TEXT NOT NULL,
                sector TEXT NOT NULL,
                market_cap REAL,
                close_price REAL,
                daily_return REAL,
                volume REAL,
                avg_volume_20d REAL,
                is_filtered INTEGER DEFAULT 0,
                is_abnormal INTEGER DEFAULT 0,
                UNIQUE(date, ticker)
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO stock_daily (
                date, ticker, name, country, sector, market_cap,
                close_price, daily_return, volume, avg_volume_20d,
                is_filtered, is_abnormal
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "2026-04-20",
                    "005930",
                    "삼성전자",
                    "KR",
                    "정보기술",
                    350_000_000_000_000,
                    70000,
                    52.0,
                    1_000_000,
                    800_000,
                    0,
                    1,
                ),
                (
                    "2026-04-20",
                    "000660",
                    "SK하이닉스",
                    "KR",
                    "정보기술",
                    120_000_000_000_000,
                    190000,
                    3.5,
                    850_000,
                    700_000,
                    0,
                    0,
                ),
            ],
        )
        conn.commit()
        conn.close()

    def test_checkpoint_db_moves_legacy_stock_rows_to_raw_db(self) -> None:
        self._create_legacy_summary_db()

        result = database.checkpoint_db()

        summary_conn = database.get_connection()
        raw_conn = database.get_raw_connection()
        try:
            legacy_table = summary_conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name = 'stock_daily'
                """
            ).fetchone()
            raw_count = raw_conn.execute(
                "SELECT COUNT(*) FROM stock_daily"
            ).fetchone()[0]
            abnormal_count = summary_conn.execute(
                "SELECT COUNT(*) FROM abnormal_stock_summary"
            ).fetchone()[0]
        finally:
            summary_conn.close()
            raw_conn.close()

        self.assertIsNone(legacy_table)
        self.assertEqual(raw_count, 2)
        self.assertEqual(abnormal_count, 1)
        self.assertEqual(result["migrated_rows"], 2)
        self.assertEqual(result["backfilled_abnormal_rows"], 1)
        self.assertTrue(result["vacuumed_summary"])

    def test_get_abnormal_stocks_falls_back_to_legacy_table_before_migration(self) -> None:
        self._create_legacy_summary_db()

        conn = database.get_connection()
        try:
            rows = database.get_abnormal_stocks(conn, date="2026-04-20")
        finally:
            conn.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ticker"], "005930")
