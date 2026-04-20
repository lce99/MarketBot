import sys
import unittest
from unittest.mock import Mock, patch

from scripts import collect


class CollectCliSmokeTests(unittest.TestCase):
    def test_main_exits_when_collector_reports_no_data(self) -> None:
        collector = Mock()
        collector.run.return_value = False

        with patch.object(sys, "argv", [
            "collect.py",
            "--market",
            "KR",
            "--date",
            "2026-04-20",
        ]):
            with patch.object(collect, "get_collector", return_value=collector):
                with patch.object(collect, "send_failure_alert") as mock_alert:
                    with self.assertRaises(SystemExit) as ctx:
                        collect.main()

        self.assertEqual(str(ctx.exception), "수집 실패/데이터 없음 시장: KR")
        collector.run.assert_called_once_with(date="2026-04-20")
        mock_alert.assert_called_once_with(["KR"], "2026-04-20")

    def test_main_succeeds_when_collector_reports_success(self) -> None:
        collector = Mock()
        collector.run.return_value = True

        with patch.object(sys, "argv", [
            "collect.py",
            "--market",
            "KR",
            "--date",
            "2026-04-20",
        ]):
            with patch.object(collect, "get_collector", return_value=collector):
                collect.main()

        collector.run.assert_called_once_with(date="2026-04-20")

    def test_main_exits_for_unsupported_market(self) -> None:
        with patch.object(sys, "argv", [
            "collect.py",
            "--market",
            "FOO",
            "--date",
            "2026-04-20",
        ]):
            with self.assertRaises(SystemExit) as ctx:
                collect.main()

        self.assertEqual(str(ctx.exception), "수집 실패/데이터 없음 시장: FOO")
