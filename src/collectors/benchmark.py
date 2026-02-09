"""벤치마크 수집기 - yfinance로 섹터 ETF/인덱스 수집"""

import logging
from datetime import datetime, timedelta

import yfinance as yf

from src.config import BENCHMARK_TICKERS
from src.database import get_connection, init_db, upsert_benchmark_daily

logger = logging.getLogger(__name__)


def collect_benchmarks(date: str | None = None):
    """모든 벤치마크 티커의 일간 데이터를 수집하여 DB에 저장."""
    if date is None:
        date = datetime.utcnow().strftime("%Y-%m-%d")

    dt = datetime.strptime(date, "%Y-%m-%d")
    start = (dt - timedelta(days=10)).strftime("%Y-%m-%d")
    end = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    tickers_str = " ".join(
        info["ticker"] for info in BENCHMARK_TICKERS.values()
    )

    logger.info(f"벤치마크 수집: {len(BENCHMARK_TICKERS)}개 티커")

    try:
        data = yf.download(
            tickers_str,
            start=start,
            end=end,
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as e:
        logger.error(f"벤치마크 다운로드 실패: {e}")
        return

    if data.empty:
        logger.warning("벤치마크 데이터 없음")
        return

    init_db()
    conn = get_connection()
    rows = []

    for key, info in BENCHMARK_TICKERS.items():
        ticker = info["ticker"]
        try:
            # 멀티티커일 때 컬럼 접근
            if len(BENCHMARK_TICKERS) == 1:
                ticker_data = data
            else:
                if ticker not in data.columns.get_level_values(0):
                    continue
                ticker_data = data[ticker]

            if ticker_data.empty:
                continue

            # 최신 데이터
            latest = ticker_data.dropna(subset=["Close"]).iloc[-1]
            close_price = float(latest["Close"])

            # 일간 수익률
            daily_return = None
            valid_data = ticker_data.dropna(subset=["Close"])
            if len(valid_data) >= 2:
                prev = float(valid_data.iloc[-2]["Close"])
                if prev > 0:
                    daily_return = ((close_price - prev) / prev) * 100

            # 주간 수익률 (5거래일 전 대비)
            weekly_return = None
            if len(valid_data) >= 6:
                week_ago = float(valid_data.iloc[-6]["Close"])
                if week_ago > 0:
                    weekly_return = ((close_price - week_ago) / week_ago) * 100

            rows.append({
                "date": date,
                "ticker": ticker,
                "name": key,
                "country": info["country"],
                "sector": info.get("sector"),
                "close_price": round(close_price, 2),
                "daily_return": round(daily_return, 4) if daily_return else None,
                "weekly_return": round(weekly_return, 4) if weekly_return else None,
            })
        except Exception as e:
            logger.debug(f"벤치마크 {ticker} 실패: {e}")
            continue

    if rows:
        upsert_benchmark_daily(conn, rows)
        conn.commit()
        logger.info(f"벤치마크 저장: {len(rows)}개")
    conn.close()
