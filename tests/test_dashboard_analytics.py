import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from scripts.export_dashboard import export
from scripts.refresh_dashboard_analytics import correct_provenance, refresh
from src import database
from src.collectors.yfinance_collector import YfinanceCollector


class DashboardAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        folder = Path(self.tempdir.name)
        self.db = folder / 'summary.db'
        self.patchers = [patch.object(database, 'DATA_DIR', folder), patch.object(database, 'DB_PATH', self.db),
                         patch.object(database, 'RAW_DB_PATH', folder / 'raw.db')]
        for item in self.patchers:
            item.start()
        database.init_db()

    def tearDown(self):
        self.doCleanups()
        for item in reversed(self.patchers):
            item.stop()
        self.tempdir.cleanup()

    def test_refresh_uses_observation_date_and_does_not_invent_new_signals(self):
        conn = database.get_connection()
        self.addCleanup(conn.close)
        conn.executemany("INSERT INTO sector_performance (date,country,sector,daily_return,breadth,stock_count,collected_at) VALUES (?,?,'정보기술',1,0.6,10,'2026-09-30T00:00:00')",
                         [('2026-09-29','US'),('2026-09-30','KR'),('2040-01-01','CN')])
        database.upsert_flow_signals(conn, [dict(created_date='2026-09-23',sector='정보기술',leader=code,
            follower='US',lag=1,leader_return=2,predicted_direction=1,correlation=0.9) for code in ['KR','CN']])
        conn.commit()
        conn.close()
        result = refresh()
        self.assertEqual(result['asOfDate'], '2026-09-30')
        self.assertEqual(result['inputDates']['US'], '2026-09-29')
        self.assertEqual(result['trendInputMarkets'], ['KR'])
        self.assertEqual(result['signalsCreated'], 0)
        snapshot = export(self.db, now=datetime(2026,9,30,tzinfo=timezone.utc))
        self.assertEqual(snapshot['analytics']['flowSignals']['latestDate'], '2026-09-23')
        self.assertTrue(snapshot['analytics']['flowSignals']['stale'])
        self.assertEqual(snapshot['analytics']['flowSignals']['evaluatedThrough'], '2026-09-30')
        self.assertFalse(snapshot['analytics']['trends']['stale'])
        self.assertEqual(snapshot['analytics']['latestRefresh']['signalsCreated'], 0)
        conn = database.get_connection()
        self.addCleanup(conn.close)
        self.assertEqual(conn.execute("SELECT status FROM flow_signals WHERE leader='CN'").fetchone()[0], 'pending')
        self.assertEqual(conn.execute("SELECT MAX(date) FROM sector_performance WHERE country='CN'").fetchone()[0], '2040-01-01')
        conn.close()

    def test_verified_provenance_correction_is_precise_and_idempotent(self):
        conn = database.get_connection()
        self.addCleanup(conn.close)
        conn.executemany("INSERT INTO collection_log (timestamp,market,status,provider) VALUES (?,'JP','success','finnhub')",
                         [('older',),('verified',)])
        corrections = [dict(market='JP',timestamp='verified',expectedProvider='finnhub',provider='yfinance')]
        self.assertEqual(correct_provenance(conn, corrections), 1)
        self.assertEqual(correct_provenance(conn, corrections), 0)
        self.assertEqual(conn.execute("SELECT provider FROM collection_log WHERE timestamp='older'").fetchone()[0], 'finnhub')
        self.assertEqual(conn.execute("SELECT provider FROM collection_log WHERE timestamp='verified'").fetchone()[0], 'yfinance')
        self.assertEqual(YfinanceCollector('JP', []).get_provider_name(), 'yfinance')
        conn.close()

    def test_correction_refuses_unverified_row(self):
        conn = database.get_connection()
        self.addCleanup(conn.close)
        with self.assertRaises(ValueError):
            correct_provenance(conn, [dict(market='DE',timestamp='missing',expectedProvider='finnhub',provider='yfinance')])
        conn.close()
