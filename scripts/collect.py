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
from src.monitor import format_failure_alert, send_admin_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def positive_int(value: str) -> int:
    """Argparse type that accepts only positive integers."""
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("양수만 입력할 수 있습니다.")
    return parsed


def get_collector(market: str):
    """국가 코드에 맞는 수집기 인스턴스 반환."""
    if market == "KR":
        from src.collectors.korea import KoreaCollector
        return KoreaCollector()
    elif market == "US":
        from src.collectors.finnhub_collector import USCollector
        return USCollector()
    elif market == "JP":
        from src.collectors.yfinance_collector import JPCollector
        return JPCollector()
    elif market == "DE":
        from src.collectors.yfinance_collector import DECollector
        return DECollector()
    elif market == "IN":
        from src.collectors.yfinance_collector import INCollector
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


def configure_collector(collector, market: str, args) -> None:
    """Apply optional runtime controls to collectors that support them."""
    requested_mode = args.mode.lower() if args.mode else None
    wants_manual_controls = (
        requested_mode is not None
        or args.max_tickers is not None
        or args.resume_from_checkpoint
    )
    if not wants_manual_controls:
        return

    configure = getattr(collector, "configure_collection", None)
    if configure is None:
        raise ValueError(
            f"{market} 시장은 --mode/--max-tickers/--resume-from-checkpoint를 지원하지 않습니다."
        )

    configure(
        mode=requested_mode,
        max_tickers=args.max_tickers,
        resume_from_checkpoint=args.resume_from_checkpoint,
    )


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
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="실제 수집 전에 사전 점검만 수행한다.",
    )
    parser.add_argument(
        "--mode",
        choices=("full", "incremental", "seed"),
        help="VN 수집 모드 강제 지정.",
    )
    parser.add_argument(
        "--max-tickers",
        type=positive_int,
        help="1회 실행에서 처리할 최대 종목 수.",
    )
    parser.add_argument(
        "--resume-from-checkpoint",
        action="store_true",
        help="저장된 checkpoint부터 수집을 재개한다.",
    )
    args = parser.parse_args()

    date = args.date or datetime.utcnow().strftime("%Y-%m-%d")

    if args.market == "ALL":
        markets = list(COUNTRIES.keys())
    else:
        markets = [m.strip().upper() for m in args.market.split(",")]

    failed_markets: list[str] = []

    for market in markets:
        try:
            if market == "BENCHMARK":
                from src.collectors.benchmark import collect_benchmarks
                saved_rows = collect_benchmarks(date)
                logger.info(f"[BENCHMARK] 수집 성공 ({saved_rows}개)")
            else:
                collector = get_collector(market)
                configure_collector(collector, market, args)
                if args.preflight_only:
                    success = collector.run_preflight(date=date)
                    if success:
                        logger.info(f"[{market}] preflight 성공")
                    continue

                success = collector.run(date=date)
                if not success:
                    failed_markets.append(market)
                    logger.error(f"[{market}] 수집 실패: 데이터 없음")
                    continue
                logger.info(f"[{market}] 수집 성공")
        except ValueError as e:
            logger.error(str(e))
            failed_markets.append(market)
        except Exception as e:
            logger.error(f"[{market}] 수집 실패: {e}", exc_info=True)
            failed_markets.append(market)

    if failed_markets:
        send_failure_alert(failed_markets, date)
        failed_list = ", ".join(failed_markets)
        if args.preflight_only:
            raise SystemExit(f"preflight 실패 시장: {failed_list}")
        raise SystemExit(f"수집 실패/데이터 없음 시장: {failed_list}")


def send_failure_alert(failed_markets: list[str], date: str) -> None:
    """실패 시장이 있으면 관리자용 텔레그램 알림을 전송한다."""
    alert_text = format_failure_alert(failed_markets, as_of_date=date)
    send_admin_alert(alert_text)


if __name__ == "__main__":
    main()
