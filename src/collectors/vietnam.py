"""베트남 시장 수집기 - vnstock 기반 HOSE/HNX 전종목 수집

vnstock은 무료, 무제한.
"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd

from src.collectors.base import BaseCollector
from src.collectors.date_utils import compute_period_return_from_closes

logger = logging.getLogger(__name__)

# vnstock 업종 → GICS 매핑 (영문 industry name 기반)
VN_SECTOR_MAP = {
    "Banks": "금융",
    "Financial Services": "금융",
    "Insurance": "금융",
    "Securities": "금융",
    "Real Estate": "부동산",
    "Construction": "산업재",
    "Building Materials": "소재",
    "Steel": "소재",
    "Chemicals": "소재",
    "Technology": "정보기술",
    "Information Technology": "정보기술",
    "Software": "정보기술",
    "Telecommunications": "커뮤니케이션",
    "Media": "커뮤니케이션",
    "Healthcare": "헬스케어",
    "Pharmaceuticals": "헬스케어",
    "Food & Beverage": "필수소비재",
    "Consumer Staples": "필수소비재",
    "Consumer Goods": "필수소비재",
    "Retail": "경기소비재",
    "Consumer Discretionary": "경기소비재",
    "Automobiles": "경기소비재",
    "Textiles": "경기소비재",
    "Oil & Gas": "에너지",
    "Energy": "에너지",
    "Electricity": "유틸리티",
    "Utilities": "유틸리티",
    "Water": "유틸리티",
    "Transportation": "산업재",
    "Logistics": "산업재",
    "Industrial": "산업재",
    "Agriculture": "필수소비재",
    "Aquaculture": "필수소비재",
    "Mining": "소재",
    "Rubber": "소재",
    "Plastics": "소재",
}


class VietnamCollector(BaseCollector):
    country_code = "VN"

    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """HOSE + HNX 전종목 수집 via vnstock."""
        try:
            from vnstock import Vnstock
        except ImportError:
            logger.error("vnstock 미설치. pip install vnstock")
            return pd.DataFrame()

        try:
            stock = Vnstock()
            target_date = datetime.strptime(date, "%Y-%m-%d")
            start_date = (target_date - timedelta(days=14)).strftime("%Y-%m-%d")
            end_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

            # 1) 전종목 리스트
            listing = stock.stock().listing.all_symbols()
            if listing is None or listing.empty:
                logger.warning("[VN] 종목 리스트 없음")
                return pd.DataFrame()

            logger.info(f"[VN] 전체 종목: {len(listing)}개")

            # 2) 각 종목의 일간 데이터 수집
            rows = []
            used_dates: list[str] = []
            for _, info in listing.iterrows():
                ticker = info.get("ticker") or info.get("symbol") or ""
                if not ticker:
                    continue

                try:
                    # vnstock으로 최근 며칠 데이터를 가져와 최신 거래일 봉을 선택
                    hist = stock.stock(symbol=ticker, source="VCI").quote.history(
                        start=start_date, end=end_date
                    )
                    hist = self._prepare_history(hist, target_date)
                    if hist.empty:
                        continue

                    latest = hist.iloc[-1]
                    close_price = (
                        float(latest["close"])
                        if pd.notna(latest.get("close"))
                        else None
                    )
                    volume = (
                        float(latest["volume"])
                        if pd.notna(latest.get("volume"))
                        else None
                    )
                    daily_return = self._compute_daily_return(hist)
                    weekly_return = self._compute_weekly_return(hist)
                    avg_volume_20d = self._compute_avg_volume(hist)

                    # 섹터
                    industry = info.get("industry", "") or info.get("sector", "") or ""
                    sector = VN_SECTOR_MAP.get(industry, "기타")

                    market_cap = info.get("market_cap")
                    if pd.isna(market_cap) or market_cap in ("", None):
                        market_cap = None
                    else:
                        try:
                            market_cap = float(market_cap)
                        except (TypeError, ValueError):
                            market_cap = None

                    if "_date" in hist.columns:
                        used_dates.append(hist.iloc[-1]["_date"].strftime("%Y-%m-%d"))

                    rows.append({
                        "ticker": ticker,
                        "name": info.get("name", ticker),
                        "sector": sector,
                        "market_cap": market_cap,
                        "close_price": close_price,
                        "daily_return": daily_return,
                        "weekly_return": weekly_return,
                        "volume": volume,
                        "avg_volume_20d": avg_volume_20d,
                    })

                    time.sleep(0.1)  # 부하 방지
                except Exception as e:
                    logger.debug(f"[VN] {ticker} 스킵: {e}")
                    continue

                if len(rows) % 100 == 0:
                    logger.info(f"[VN] {len(rows)}개 수집 중...")

            df = pd.DataFrame(rows)
            if used_dates:
                self.effective_date = max(used_dates)
            logger.info(f"[VN] 수집 완료: {len(df)}개 종목")
            return df

        except Exception as e:
            logger.error(f"[VN] 수집 실패: {e}", exc_info=True)
            return pd.DataFrame()

    def _prepare_history(self, hist: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
        """최근 구간에서 목표일 이전의 최신 거래일 데이터만 남긴다."""
        if hist is None or hist.empty:
            return pd.DataFrame()

        hist = hist.copy()
        date_col = next(
            (col for col in ("time", "date", "tradingDate") if col in hist.columns),
            None,
        )
        if date_col:
            hist["_date"] = pd.to_datetime(hist[date_col], errors="coerce")
            hist = hist.dropna(subset=["_date"])
            hist = hist[hist["_date"].dt.date <= target_date.date()]
            hist = hist.sort_values("_date")

        return hist.reset_index(drop=True)

    def _compute_daily_return(self, hist: pd.DataFrame) -> float | None:
        """종가 기준 일간 수익률을 계산한다."""
        if "close" not in hist.columns:
            return None

        valid = hist.dropna(subset=["close"])
        return compute_period_return_from_closes(valid["close"].tolist(), periods_back=1)

    def _compute_weekly_return(self, hist: pd.DataFrame) -> float | None:
        """종가 기준 주간 수익률을 계산한다."""
        if "close" not in hist.columns:
            return None

        valid = hist.dropna(subset=["close"])
        return compute_period_return_from_closes(valid["close"].tolist(), periods_back=5)

    def _compute_avg_volume(self, hist: pd.DataFrame) -> float | None:
        """최근 20거래일 평균 거래량을 계산한다."""
        if "volume" not in hist.columns:
            return None

        volumes = hist["volume"].dropna().tail(20)
        if len(volumes) == 0:
            return None

        return float(volumes.mean())
