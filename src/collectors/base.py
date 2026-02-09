"""BaseCollector - 모든 국가별 수집기의 베이스 클래스"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd

from src.config import COUNTRIES
from src.database import (
    get_connection,
    init_db,
    log_collection,
    upsert_sector_performance,
    upsert_stock_daily,
)
from src.filter import apply_filters

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """국가별 수집기의 추상 베이스 클래스.

    서브클래스는 country_code와 fetch_all_stocks()를 구현해야 함.
    """

    country_code: str  # "US", "KR", "CN", ...

    @abstractmethod
    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """해당 시장의 전체 종목 데이터를 수집.

        Returns:
            DataFrame 필수 컬럼:
                ticker, name, sector, market_cap,
                close_price, daily_return, volume, avg_volume_20d
        """
        ...

    def run(self, date: str | None = None):
        """수집 → 필터 → 섹터 집계 → DB 저장 전체 파이프라인 실행."""
        if date is None:
            date = datetime.utcnow().strftime("%Y-%m-%d")

        country = self.country_code
        info = COUNTRIES[country]
        logger.info(f"[{info['flag']} {info['name_kr']}] 수집 시작: {date}")

        init_db()
        conn = get_connection()

        try:
            # 1) 전종목 수집
            df = self.fetch_all_stocks(date)
            if df.empty:
                log_collection(conn, country, "failed", error="데이터 없음")
                conn.commit()
                logger.warning(f"[{country}] 데이터 없음")
                return

            total = len(df)
            logger.info(f"[{country}] 수집 완료: {total}개 종목")

            # 2) 필터 적용
            df = apply_filters(df, country)
            filtered_count = int(df["is_filtered"].sum())
            abnormal_count = int(df["is_abnormal"].sum())
            logger.info(
                f"[{country}] 필터: {filtered_count}개 제외, "
                f"{abnormal_count}개 비정상"
            )

            # 3) 개별 종목 DB 저장
            stock_rows = []
            for _, row in df.iterrows():
                stock_rows.append({
                    "date": date,
                    "ticker": row["ticker"],
                    "name": row.get("name", ""),
                    "country": country,
                    "sector": row.get("sector", "기타"),
                    "market_cap": row.get("market_cap"),
                    "close_price": row.get("close_price"),
                    "daily_return": row.get("daily_return"),
                    "volume": row.get("volume"),
                    "avg_volume_20d": row.get("avg_volume_20d"),
                    "is_filtered": int(row.get("is_filtered", 0)),
                    "is_abnormal": int(row.get("is_abnormal", 0)),
                })
            upsert_stock_daily(conn, stock_rows)

            # 4) 섹터별 집계 (필터 통과 + 비정상 아닌 종목만)
            active = df[(df["is_filtered"] == 0) & (df["is_abnormal"] == 0)]
            sector_rows = self._aggregate_sectors(active, date, country)
            upsert_sector_performance(conn, sector_rows)

            # 5) 수집 로그
            log_collection(
                conn, country, "success",
                total=total, filtered=filtered_count, abnormal=abnormal_count,
            )
            conn.commit()
            logger.info(
                f"[{country}] 저장 완료: "
                f"{len(sector_rows)}개 섹터, {len(stock_rows)}개 종목"
            )

        except Exception as e:
            log_collection(conn, country, "failed", error=str(e))
            conn.commit()
            logger.error(f"[{country}] 수집 실패: {e}", exc_info=True)
            raise
        finally:
            conn.close()

    def _aggregate_sectors(self, df: pd.DataFrame, date: str,
                           country: str) -> list[dict]:
        """필터 통과 종목들을 섹터별로 집계."""
        now = datetime.utcnow().isoformat()
        results = []

        for sector, group in df.groupby("sector"):
            if len(group) == 0:
                continue

            daily_returns = group["daily_return"].dropna()
            avg_return = float(daily_returns.mean()) if len(daily_returns) > 0 else 0.0
            breadth = float((daily_returns > 0).sum() / len(daily_returns)) if len(daily_returns) > 0 else 0.0

            # 거래량 변화율 (avg_volume_20d 대비)
            vol_change = 0.0
            if "volume" in group.columns and "avg_volume_20d" in group.columns:
                valid = group.dropna(subset=["volume", "avg_volume_20d"])
                valid = valid[valid["avg_volume_20d"] > 0]
                if len(valid) > 0:
                    ratio = valid["volume"] / valid["avg_volume_20d"]
                    vol_change = float((ratio.mean() - 1) * 100)

            # 상위 상승/하락 종목
            sorted_g = group.dropna(subset=["daily_return"]).sort_values(
                "daily_return", ascending=False
            )
            top_gainers = [
                {"name": r["name"], "return": round(r["daily_return"], 2)}
                for _, r in sorted_g.head(3).iterrows()
            ]
            top_losers = [
                {"name": r["name"], "return": round(r["daily_return"], 2)}
                for _, r in sorted_g.tail(3).iterrows()
            ]

            results.append({
                "date": date,
                "country": country,
                "sector": sector,
                "daily_return": round(avg_return, 4),
                "weekly_return": None,  # 주간은 analyzer에서 계산
                "breadth": round(breadth, 4),
                "volume_change": round(vol_change, 2),
                "stock_count": len(group),
                "top_gainers": top_gainers,
                "top_losers": top_losers,
                "collected_at": now,
            })

        return results
