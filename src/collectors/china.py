"""중국 시장 수집기 - tushare 기반 SSE/SZSE 전종목 수집

tushare 무료 토큰으로 사용 가능.
필요: TUSHARE_TOKEN 환경변수 설정.
"""

import logging
import time

import pandas as pd

from src.collectors.base import BaseCollector
from src.config import TUSHARE_TOKEN

logger = logging.getLogger(__name__)

# tushare 업종 → GICS 매핑
CN_SECTOR_MAP = {
    # SW (신완) 1차 업종 분류
    "银行": "금융",
    "非银金融": "금융",
    "房地产": "부동산",
    "医药生物": "헬스케어",
    "电子": "정보기술",
    "计算机": "정보기술",
    "通信": "커뮤니케이션",
    "传媒": "커뮤니케이션",
    "食品饮料": "필수소비재",
    "家用电器": "경기소비재",
    "汽车": "경기소비재",
    "商贸零售": "경기소비재",
    "纺织服装": "경기소비재",
    "轻工制造": "산업재",
    "机械设备": "산업재",
    "电力设备": "산업재",
    "建筑装饰": "산업재",
    "建筑材料": "소재",
    "国防军工": "산업재",
    "交通运输": "산업재",
    "化工": "소재",
    "钢铁": "소재",
    "有色金属": "소재",
    "采掘": "에너지",
    "石油石化": "에너지",
    "煤炭": "에너지",
    "公用事业": "유틸리티",
    "电力": "유틸리티",
    "农林牧渔": "필수소비재",
    "社会服务": "산업재",
    "美容护理": "경기소비재",
    "环保": "산업재",
    "综合": "산업재",
}


class ChinaCollector(BaseCollector):
    country_code = "CN"

    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """SSE + SZSE 전종목 수집 via tushare."""
        try:
            import tushare as ts
        except ImportError:
            logger.error("tushare 미설치. pip install tushare")
            return pd.DataFrame()

        if not TUSHARE_TOKEN:
            logger.error("TUSHARE_TOKEN 환경변수 미설정")
            return pd.DataFrame()

        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()

        date_fmt = date.replace("-", "")  # "20260206"

        try:
            # 1) 일간 데이터 (전종목)
            df_daily = pro.daily(trade_date=date_fmt)
            if df_daily is None or df_daily.empty:
                logger.warning(f"[CN] 데이터 없음 ({date_fmt})")
                return pd.DataFrame()
            time.sleep(1)

            # 2) 종목 기본정보 (이름, 업종)
            df_basic = pro.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,name,industry,market,list_date"
            )
            time.sleep(1)

            # 3) 일간 지표 (시총, PE 등)
            df_indicator = pro.daily_basic(
                trade_date=date_fmt,
                fields="ts_code,close,turnover_rate,volume_ratio,total_mv,circ_mv"
            )
            time.sleep(1)

            # 조인
            merged = df_daily.merge(
                df_basic[["ts_code", "name", "industry"]],
                on="ts_code", how="left"
            )
            if df_indicator is not None and not df_indicator.empty:
                merged = merged.merge(
                    df_indicator[["ts_code", "total_mv"]],
                    on="ts_code", how="left"
                )

            # DataFrame 구성
            rows = []
            for _, row in merged.iterrows():
                industry = row.get("industry", "") or ""
                sector = CN_SECTOR_MAP.get(industry, "기타")

                # tushare의 total_mv는 만 위안 단위
                market_cap = None
                if pd.notna(row.get("total_mv")):
                    market_cap = float(row["total_mv"]) * 10000

                daily_return = None
                if pd.notna(row.get("pct_chg")):
                    daily_return = float(row["pct_chg"])

                rows.append({
                    "ticker": row["ts_code"],
                    "name": row.get("name", ""),
                    "sector": sector,
                    "market_cap": market_cap,
                    "close_price": float(row["close"]) if pd.notna(row.get("close")) else None,
                    "daily_return": daily_return,
                    "volume": float(row["vol"]) if pd.notna(row.get("vol")) else None,
                    "avg_volume_20d": None,
                })

            df = pd.DataFrame(rows)
            logger.info(f"[CN] 전종목: {len(df)}개")
            return df

        except Exception as e:
            logger.error(f"[CN] 수집 실패: {e}", exc_info=True)
            return pd.DataFrame()
