"""벤치마크 수집기 - yfinance로 섹터 ETF/인덱스 수집"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from src.config import BENCHMARK_TICKERS
from src.database import get_connection, init_db, upsert_benchmark_daily

logger = logging.getLogger(__name__)


def _download_with_retries(
    tickers_str: str,
    start: str,
    end: str,
    attempts: int = 3,
) -> object:
    """Download benchmark prices with backoff for transient yfinance failures."""
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            data = yf.download(
                tickers_str,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                threads=False,
                group_by="ticker",
            )
            if not data.empty:
                return data
            last_error = RuntimeError("벤치마크 데이터 없음")
        except Exception as exc:
            last_error = exc

        if attempt < attempts:
            sleep_seconds = attempt * 20
            logger.warning(
                f"벤치마크 다운로드 재시도 {attempt}/{attempts - 1}: {last_error}"
            )
            time.sleep(sleep_seconds)

    if last_error is None:
        raise RuntimeError("벤치마크 다운로드 실패")
    raise last_error


def _extract_ticker_frame(data: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    """멀티티커 다운로드 결과에서 단일 티커 프레임을 추출.

    yfinance는 버전/옵션에 따라 컬럼 레벨 0이 티커일 수도, 가격 필드(Close 등)일
    수도 있어 두 방향 모두 지원한다.
    """
    if not isinstance(data.columns, pd.MultiIndex):
        return data

    if ticker in data.columns.get_level_values(0):
        return data[ticker]
    if ticker in data.columns.get_level_values(-1):
        return data.xs(ticker, axis=1, level=-1)
    return None


def collect_benchmarks(date: str | None = None) -> int:
    """모든 벤치마크 티커의 일간 데이터를 수집하여 DB에 저장.

    Returns:
        저장된 벤치마크 row 수.
    """
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
        data = _download_with_retries(tickers_str, start, end)
    except Exception as e:
        logger.error(f"벤치마크 다운로드 실패: {e}")
        raise RuntimeError("벤치마크 다운로드 실패") from e

    if data.empty:
        logger.warning("벤치마크 데이터 없음")
        raise RuntimeError("벤치마크 데이터 없음")

    init_db()
    conn = get_connection()
    rows = []

    for key, info in BENCHMARK_TICKERS.items():
        ticker = info["ticker"]
        try:
            ticker_data = _extract_ticker_frame(data, ticker)
            if ticker_data is None or ticker_data.empty:
                continue

            valid_data = ticker_data.dropna(subset=["Close"])
            if valid_data.empty:
                continue

            # 최신 데이터
            latest = valid_data.iloc[-1]
            close_price = float(latest["Close"])

            # 일간 수익률
            daily_return = None
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
                "daily_return": round(daily_return, 4) if daily_return is not None else None,
                "weekly_return": round(weekly_return, 4) if weekly_return is not None else None,
            })
        except Exception as e:
            logger.debug(f"벤치마크 {ticker} 실패: {e}")
            continue

    if rows:
        upsert_benchmark_daily(conn, rows)
        conn.commit()
        logger.info(f"벤치마크 저장: {len(rows)}개")
    conn.close()
    if not rows:
        raise RuntimeError("벤치마크 저장 대상 없음")
    return len(rows)
