"""리포트 전송 스크립트 - GitHub Actions에서 호출

사용법:
    python -m scripts.report
    python -m scripts.report --date 2026-02-06
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def prepare_report_data(date: str | None = None) -> None:
    """리포트용 파생 데이터를 계산해 DB에 저장한다."""
    from src.analyzer import compute_trend_scores

    compute_trend_scores(date=date)
    logger.info("리포트용 파생 데이터 준비 완료")


def configure_stdout() -> None:
    """로컬 콘솔 출력 시 UTF-8 인코딩을 강제한다."""
    stream = getattr(sys, "stdout", None)
    if stream and hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8")


async def main():
    parser = argparse.ArgumentParser(description="MarketBot 리포트 전송")
    parser.add_argument("--date", default=None, help="리포트 날짜 (YYYY-MM-DD)")
    parser.add_argument(
        "--skip-analyze",
        action="store_true",
        help="트렌드 스코어 계산을 건너뛰고 기존 DB 기준으로 전송",
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="리포트 전송 없이 파생 데이터만 계산하고 종료",
    )
    args = parser.parse_args()
    report_date = args.date or None

    # 트렌드 스코어 계산
    if not args.skip_analyze:
        prepare_report_data(date=report_date)

    if args.prepare_only:
        return

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        # 토큰 없으면 콘솔 출력
        logger.warning("TELEGRAM 토큰 미설정. 콘솔 출력합니다.")
        configure_stdout()
        from src.reporter import format_daily_report
        messages = format_daily_report(date=report_date)
        for msg in messages:
            print(msg)
        return

    from src.bot import send_auto_report
    await send_auto_report(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, date=report_date)
    logger.info("리포트 전송 완료")


if __name__ == "__main__":
    asyncio.run(main())
