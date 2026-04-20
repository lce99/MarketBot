"""중국 시장 수집기 - tushare 기반 SSE/SZSE 전종목 수집

tushare 무료 토큰으로 사용 가능.
필요: TUSHARE_TOKEN 환경변수 설정.
"""

import logging
import time
from datetime import datetime

import pandas as pd

from src.collectors.base import BaseCollector
from src.collectors.date_utils import compute_return_pct, recent_dates
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

        try:
            # 1) 최근 거래일 스냅샷 확보. 첫 번째는 현재, 여섯 번째는 주간 비교 기준
            snapshots = self._fetch_recent_daily_snapshots(
                pro,
                date,
                limit=6,
                lookback_days=21,
            )
            if not snapshots:
                logger.warning(f"[CN] 최근 21일 내 데이터 없음 ({date})")
                return pd.DataFrame()

            trade_date, df_daily = snapshots[0]
            if df_daily is None or df_daily.empty:
                logger.warning(f"[CN] 최근 21일 내 데이터 없음 ({date})")
                return pd.DataFrame()
            time.sleep(1)

            self.effective_date = datetime.strptime(
                trade_date, "%Y%m%d"
            ).strftime("%Y-%m-%d")
            weekly_reference = (
                snapshots[min(5, len(snapshots) - 1)][1]
                if len(snapshots) > 1
                else pd.DataFrame()
            )
            weekly_close_map = {}
            if weekly_reference is not None and not weekly_reference.empty:
                weekly_close_map = (
                    weekly_reference.set_index("ts_code")["close"].to_dict()
                )

            # 2) 종목 기본정보 (이름, 업종)
            try:
                df_basic = pro.stock_basic(
                    exchange="",
                    list_status="L",
                    fields="ts_code,name,industry,market,list_date"
                )
                time.sleep(1)
            except Exception as e:
                logger.warning(f"[CN] stock_basic 조회 실패, 기본값으로 진행: {e}")
                df_basic = pd.DataFrame()

            # 3) 일간 지표 (시총, PE 등)
            try:
                df_indicator = pro.daily_basic(
                    trade_date=trade_date,
                    fields="ts_code,close,turnover_rate,volume_ratio,total_mv,circ_mv"
                )
                time.sleep(1)
            except Exception as e:
                logger.warning(f"[CN] daily_basic 조회 실패, 시총 없이 진행: {e}")
                df_indicator = pd.DataFrame()

            # 조인
            merged = df_daily.copy()
            if not df_basic.empty:
                merged = merged.merge(
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
                weekly_return = compute_return_pct(
                    row.get("close"),
                    weekly_close_map.get(row["ts_code"]),
                )

                name = row.get("name", "")
                if pd.isna(name) or not name:
                    name = row["ts_code"]

                rows.append({
                    "ticker": row["ts_code"],
                    "name": name,
                    "sector": sector,
                    "market_cap": market_cap,
                    "close_price": float(row["close"]) if pd.notna(row.get("close")) else None,
                    "daily_return": daily_return,
                    "weekly_return": weekly_return,
                    "volume": float(row["vol"]) if pd.notna(row.get("vol")) else None,
                    "avg_volume_20d": None,
                })

            df = pd.DataFrame(rows)
            logger.info(f"[CN] 전종목: {len(df)}개")
            return df

        except Exception as e:
            logger.error(f"[CN] 수집 실패: {e}", exc_info=True)
            return pd.DataFrame()

    def _fetch_recent_daily_snapshots(
        self,
        pro,
        date: str,
        limit: int = 6,
        lookback_days: int = 21,
    ) -> list[tuple[str, pd.DataFrame]]:
        """최근 며칠 내 실제로 데이터가 존재하는 거래일 스냅샷을 모은다."""
        snapshots: list[tuple[str, pd.DataFrame]] = []
        seen: set[str] = set()

        for candidate in recent_dates(date, lookback_days=lookback_days):
            candidate_fmt = candidate.replace("-", "")
            try:
                df_daily = pro.daily(trade_date=candidate_fmt)
            except Exception as e:
                logger.warning(f"[CN] {candidate_fmt} 일간 데이터 조회 실패: {e}")
                time.sleep(1)
                continue

            if (
                df_daily is not None
                and not df_daily.empty
                and candidate_fmt not in seen
            ):
                snapshots.append((candidate_fmt, df_daily))
                seen.add(candidate_fmt)
                if len(snapshots) >= limit:
                    break

            time.sleep(0.3)

        return snapshots
