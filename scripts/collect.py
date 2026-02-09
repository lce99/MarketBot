"""수집 실행 스크립트 - GitHub Actions 또는 로컬에서 실행

사용법:
    python -m scripts.collect --market KR
    python -m scripts.collect --market US
    python -m scripts.collect --market ALL
    python -m scripts.collect --market KR --date 2026-02-07
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config import COUNTRIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def get_collector(market: str):
    """국가 코드에 맞는 수집기 인스턴스 반환."""
    if market == "KR":
        from src.collectors.korea import KoreaCollector
        return KoreaCollector()
    elif market == "US":
        from src.collectors.finnhub_collector import USCollector
        return USCollector()
    elif market == "JP":
        from src.collectors.finnhub_collector import JPCollector
        return JPCollector()
    elif market == "DE":
        from src.collectors.finnhub_collector import DECollector
        return DECollector()
    elif market == "IN":
        from src.collectors.finnhub_collector import INCollector
        return INCollector()
    elif market == "CN":
        from src.collectors.china import ChinaCollector
        return ChinaCollector()
    elif market == "VN":
        from src.collectors.vietnam import VietnamCollector
        return VietnamCollector()
    elif market == "BENCHMARK":
        return None  # 벤치마크는 별도 처리
    else:
        raise ValueError(f"지원하지 않는 시장: {market}")


def main():
    parser = argparse.ArgumentParser(description="MarketBot 데이터 수집")
    parser.add_argument(
        "--market", required=True,
        help="시장 코드 (KR, US, CN, JP, VN, IN, DE) 또는 ALL"
    )
    parser.add_argument(
        "--date", default=None,
        help="수집 날짜 (YYYY-MM-DD). 미지정 시 오늘."
    )
    args = parser.parse_args()

    date = args.date or datetime.utcnow().strftime("%Y-%m-%d")

    if args.market == "ALL":
        markets = list(COUNTRIES.keys())
    else:
        markets = [m.strip().upper() for m in args.market.split(",")]

    for market in markets:
        try:
            if market == "BENCHMARK":
                from src.collectors.benchmark import collect_benchmarks
                collect_benchmarks(date)
                logger.info("[BENCHMARK] 수집 성공")
            else:
                collector = get_collector(market)
                collector.run(date=date)
                logger.info(f"[{market}] 수집 성공")
        except ValueError as e:
            logger.warning(str(e))
        except Exception as e:
            logger.error(f"[{market}] 수집 실패: {e}", exc_info=True)


if __name__ == "__main__":
    main()
