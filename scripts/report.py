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


async def main():
    parser = argparse.ArgumentParser(description="MarketBot 리포트 전송")
    parser.add_argument("--date", default=None, help="리포트 날짜 (YYYY-MM-DD)")
    parser.add_argument(
        "--analyze", action="store_true", default=True,
        help="트렌드 스코어 계산 후 리포트 (기본: True)"
    )
    args = parser.parse_args()

    # 트렌드 스코어 계산
    if args.analyze:
        from src.analyzer import compute_trend_scores
        compute_trend_scores(date=args.date)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        # 토큰 없으면 콘솔 출력
        logger.warning("TELEGRAM 토큰 미설정. 콘솔 출력합니다.")
        from src.reporter import format_daily_report
        messages = format_daily_report(date=args.date)
        for msg in messages:
            print(msg)
        return

    from src.bot import send_auto_report
    await send_auto_report(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    logger.info("리포트 전송 완료")


if __name__ == "__main__":
    asyncio.run(main())
