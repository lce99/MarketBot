import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import io

from scripts.export_dashboard import export, main


class DashboardExportTests(unittest.TestCase):
    def test_china_history_is_preserved_but_excluded_from_scope_and_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.db"
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE sector_performance (date TEXT, country TEXT, sector TEXT, daily_return REAL)")
            conn.executemany("INSERT INTO sector_performance VALUES (?, ?, 'Technology', 1.0)",
                             [("2026-09-29", "US"), ("2040-01-01", "CN")])
            conn.commit()
            conn.close()
            before = path.read_bytes()
            snapshot = export(path, now=datetime(2026, 9, 30, tzinfo=timezone.utc))
            self.assertEqual(snapshot["latestDate"], "2026-09-29")
            self.assertEqual(snapshot["coverage"], {"targetMarkets": 6, "freshMarkets": 1, "observedMarkets": 1,
                "activeMarkets": ["US", "KR", "JP", "VN", "IN", "DE"], "excludedMarkets": ["CN"]})
            self.assertNotIn("CN", [m['code'] for m in snapshot['markets']])
            self.assertNotIn("CN", [s['country'] for s in snapshot['sectorHistory']])
            self.assertEqual(path.read_bytes(), before)

    def test_new_export_timestamp_preserves_old_observation_dates_and_stale_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.db"
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE sector_performance (date TEXT, country TEXT, sector TEXT, daily_return REAL)")
            dates = {"US": "2026-09-23", **{code: "2026-09-24" for code in ("KR", "JP", "VN", "IN", "DE")}}
            conn.executemany("INSERT INTO sector_performance VALUES (?, ?, 'Technology', 1.0)",
                             [(day, code) for code, day in dates.items()])
            conn.commit()
            conn.close()
            before = path.read_bytes()
            snapshot = export(path, now=datetime(2026, 9, 30, tzinfo=timezone.utc), source_commit="fixture")
            self.assertEqual(snapshot["generatedAt"], "2026-09-30T00:00:00+00:00")
            self.assertEqual(snapshot["latestDate"], "2026-09-24")
            for market in snapshot["markets"]:
                self.assertTrue(market["stale"])
                self.assertEqual(market["latestDate"], dates.get(market["code"]))
            self.assertEqual(path.read_bytes(), before)

    def test_successful_export_log_names_stale_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            db = path / "summary.db"
            sqlite3.connect(db).close()
            output = io.StringIO()
            with patch("sys.argv", ["export_dashboard", "--db", str(db), "--output", str(path / "snapshot.json")]), \
                 patch("sys.stdout", output):
                main()
            self.assertIn("stale/missing markets: US,KR,JP,VN,IN,DE", output.getvalue())
            self.assertTrue((path / "snapshot.json").exists())


if __name__ == "__main__":
    unittest.main()
