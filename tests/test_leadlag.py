import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import src.database as database
from src import leadlag
from src.database import (
    get_connection,
    get_flow_signal_stats,
    get_flow_signals,
    init_db,
)


def _weekdays(start: str, count: int) -> list[str]:
    """count개의 평일 날짜 목록 (start 포함, 과거→미래 순)."""
    dates = []
    current = datetime.strptime(start, "%Y-%m-%d")
    while len(dates) < count:
        if current.weekday() < 5:
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def _recent_weekdays(count: int) -> list[str]:
    """오늘로 끝나는 평일 날짜 목록.

    시그널 만료(LEADLAG_SIGNAL_EXPIRE_AFTER_DAYS)가 동작하지 않도록
    마지막 날짜를 현재 시각 근처로 맞춘다.
    """
    dates: list[str] = []
    current = datetime.utcnow()
    while len(dates) < count:
        if current.weekday() < 5:
            dates.append(current.strftime("%Y-%m-%d"))
        current -= timedelta(days=1)
    return list(reversed(dates))


class LeadLagTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tempdir.name) / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        self.patchers = [
            patch.object(database, "DATA_DIR", data_dir),
            patch.object(database, "DB_PATH", data_dir / "marketbot.db"),
            patch.object(database, "RAW_DB_PATH", data_dir / "marketbot_raw.db"),
        ]
        for patcher in self.patchers:
            patcher.start()
        init_db()

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.tempdir.cleanup()

    def insert_sector_returns(self, rows: list[tuple[str, str, str, float]]) -> None:
        conn = get_connection()
        try:
            conn.executemany(
                """
                INSERT INTO sector_performance (
                    date, country, sector, daily_return,
                    breadth, stock_count, collected_at
                )
                VALUES (?, ?, ?, ?, 0.6, 10, '2026-01-01T00:00:00')
                ON CONFLICT(date, country, sector) DO UPDATE SET
                    daily_return = excluded.daily_return
                """,
                rows,
            )
            conn.commit()
        finally:
            conn.close()

    def seed_us_leads_kr(self, *, days: int = 30) -> list[str]:
        """KR 정보기술이 US 정보기술을 정확히 1거래일 따라가는 데이터."""
        dates = _recent_weekdays(days)
        us_returns = [((i * 7) % 11 - 5) / 2.0 for i in range(days)]  # -2.5 ~ +2.5
        rows = []
        for i, date in enumerate(dates):
            rows.append((date, "US", "정보기술", us_returns[i]))
            if i > 0:
                rows.append((date, "KR", "정보기술", us_returns[i - 1]))
            else:
                rows.append((date, "KR", "정보기술", 0.1))
        self.insert_sector_returns(rows)
        return dates


class LeadLagScoreTests(LeadLagTestBase):
    def test_detects_us_leading_kr_by_one_day(self) -> None:
        dates = self.seed_us_leads_kr()
        scores = leadlag.compute_lead_lag_scores(date=dates[-1])

        by_pair = {
            (row["leader"], row["follower"]): row
            for row in scores
            if row["sector"] == "정보기술"
        }
        us_to_kr = by_pair[("US", "KR")]
        self.assertEqual(us_to_kr["lag"], 1)
        self.assertGreater(us_to_kr["correlation"], 0.95)
        self.assertGreaterEqual(us_to_kr["n_obs"], 15)

        # 반대 방향(KR→US)이 같은 강도로 잡히면 안 된다.
        kr_to_us = by_pair.get(("KR", "US"))
        if kr_to_us is not None:
            self.assertLess(kr_to_us["correlation"], us_to_kr["correlation"])

    def test_no_lookahead_lag_for_late_closing_leader(self) -> None:
        # US는 KR보다 늦게 마감하므로 US→KR 페어에 lag 0이 허용되면 안 된다.
        self.assertNotIn(0, leadlag._allowed_lags("US", "KR"))
        # KR은 US보다 먼저 마감하므로 KR→US는 lag 0(같은 날 추종)이 허용된다.
        self.assertIn(0, leadlag._allowed_lags("KR", "US"))


class FlowSignalTests(LeadLagTestBase):
    def test_signal_created_verified_and_scored(self) -> None:
        dates = self.seed_us_leads_kr()
        latest = dates[-1]

        # 마지막 날 US가 임계값 이상으로 움직이도록 보장
        self.insert_sector_returns([(latest, "US", "정보기술", 2.0)])

        leadlag.compute_lead_lag_scores(date=latest)
        signals = leadlag.generate_flow_signals(date=latest)

        target = [
            s for s in signals if s["leader"] == "US" and s["follower"] == "KR"
        ]
        self.assertEqual(len(target), 1)
        self.assertEqual(target[0]["predicted_direction"], 1)

        # 아직 후행국 데이터가 없으므로 pending 유지
        outcome = leadlag.verify_flow_signals()
        self.assertEqual(outcome["verified"], 0)

        # 다음 거래일 KR 데이터 도착 → 채점
        next_date = _weekdays(latest, 2)[1]
        self.insert_sector_returns([(next_date, "KR", "정보기술", 1.5)])
        outcome = leadlag.verify_flow_signals()
        self.assertEqual(outcome["verified"], 1)

        conn = get_connection()
        try:
            verified = get_flow_signals(conn, status="verified")
            self.assertEqual(len(verified), 1)
            self.assertEqual(verified[0]["hit"], 1)
            self.assertEqual(verified[0]["target_date"], next_date)

            stats = get_flow_signal_stats(conn)
            self.assertEqual(stats["total"], 1)
            self.assertEqual(stats["hits"], 1)
        finally:
            conn.close()

    def test_wrong_direction_scores_miss(self) -> None:
        dates = self.seed_us_leads_kr()
        latest = dates[-1]
        self.insert_sector_returns([(latest, "US", "정보기술", 2.0)])

        leadlag.compute_lead_lag_scores(date=latest)
        leadlag.generate_flow_signals(date=latest)

        next_date = _weekdays(latest, 2)[1]
        self.insert_sector_returns([(next_date, "KR", "정보기술", -0.8)])
        leadlag.verify_flow_signals()

        conn = get_connection()
        try:
            verified = get_flow_signals(conn, status="verified")
            us_kr = [
                s for s in verified if s["leader"] == "US" and s["follower"] == "KR"
            ]
            self.assertEqual(len(us_kr), 1)
            self.assertEqual(us_kr[0]["hit"], 0)
        finally:
            conn.close()


class FlowReportTests(LeadLagTestBase):
    def test_flow_report_renders(self) -> None:
        from src.reporter import format_flow_report

        dates = self.seed_us_leads_kr()
        latest = dates[-1]
        self.insert_sector_returns([(latest, "US", "정보기술", 2.0)])
        leadlag.update_lead_lag(date=latest)

        report = format_flow_report(date=latest)
        self.assertIn("글로벌 자금 흐름", report)
        self.assertIn("강한 선행 관계", report)
        self.assertIn("정보기술", report)
        self.assertIn("미국", report)

    def test_daily_report_includes_flow_section(self) -> None:
        from src.reporter import format_daily_report

        dates = self.seed_us_leads_kr()
        latest = dates[-1]
        self.insert_sector_returns([(latest, "US", "정보기술", 2.0)])
        leadlag.update_lead_lag(date=latest)

        messages = format_daily_report(date=latest)
        header = messages[0]
        self.assertIn("자금 흐름 시그널", header)
        self.assertIn("/flow", header)


if __name__ == "__main__":
    unittest.main()
