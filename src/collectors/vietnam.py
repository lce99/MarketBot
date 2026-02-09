"""베트남 시장 수집기 - vnstock 기반 HOSE/HNX 전종목 수집

vnstock은 무료, 무제한.
"""

import logging
import time

import pandas as pd

from src.collectors.base import BaseCollector

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

            # 1) 전종목 리스트
            listing = stock.stock().listing.all_symbols()
            if listing is None or listing.empty:
                logger.warning("[VN] 종목 리스트 없음")
                return pd.DataFrame()

            logger.info(f"[VN] 전체 종목: {len(listing)}개")

            # 2) 각 종목의 일간 데이터 수집
            rows = []
            for _, info in listing.iterrows():
                ticker = info.get("ticker") or info.get("symbol") or ""
                if not ticker:
                    continue

                try:
                    # vnstock으로 최근 데이터 가져오기
                    hist = stock.stock(symbol=ticker, source="VCI").quote.history(
                        start=date, end=date
                    )
                    if hist is None or hist.empty:
                        continue

                    latest = hist.iloc[-1]
                    close_price = float(latest.get("close", 0))
                    volume = float(latest.get("volume", 0))

                    # 등락률
                    daily_return = None
                    if "change" in hist.columns:
                        daily_return = float(latest["change"])

                    # 섹터
                    industry = info.get("industry", "") or info.get("sector", "") or ""
                    sector = VN_SECTOR_MAP.get(industry, "기타")

                    rows.append({
                        "ticker": ticker,
                        "name": info.get("name", ticker),
                        "sector": sector,
                        "market_cap": info.get("market_cap"),
                        "close_price": close_price,
                        "daily_return": daily_return,
                        "volume": volume,
                        "avg_volume_20d": None,
                    })

                    time.sleep(0.1)  # 부하 방지
                except Exception as e:
                    logger.debug(f"[VN] {ticker} 스킵: {e}")
                    continue

                if len(rows) % 100 == 0:
                    logger.info(f"[VN] {len(rows)}개 수집 중...")

            df = pd.DataFrame(rows)
            logger.info(f"[VN] 수집 완료: {len(df)}개 종목")
            return df

        except Exception as e:
            logger.error(f"[VN] 수집 실패: {e}", exc_info=True)
            return pd.DataFrame()
