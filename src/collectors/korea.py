"""한국 시장 수집기 - pykrx 기반 KOSPI/KOSDAQ 전종목 수집"""

from contextlib import contextmanager
import logging
import time

import pandas as pd
from pykrx import stock as krx

from src.collectors.base import BaseCollector
from src.collectors.date_utils import compute_return_pct, recent_dates
from src.config import KR_SECTOR_MAP

logger = logging.getLogger(__name__)

# pykrx 업종 인덱스명 → GICS 섹터 매핑
KRX_INDEX_TO_GICS = {
    "음식료·담배": "필수소비재",
    "섬유·의류": "경기소비재",
    "종이·목재": "소재",
    "화학": "소재",
    "제약": "헬스케어",
    "비금속": "소재",
    "금속": "소재",
    "기계·장비": "산업재",
    "전기전자": "정보기술",
    "의료·정밀기기": "헬스케어",
    "운송장비·부품": "산업재",
    "유통": "경기소비재",
    "전기·가스": "유틸리티",
    "건설": "산업재",
    "운송·창고": "산업재",
    "통신": "커뮤니케이션",
    "금융": "금융",
    "증권": "금융",
    "보험": "금융",
    "일반서비스": "산업재",
    "제조": "산업재",
    "부동산": "부동산",
    "IT 서비스": "정보기술",
    "오락·문화": "커뮤니케이션",
    "출판·매체복제": "커뮤니케이션",
    "기타제조": "산업재",
}


class KoreaCollector(BaseCollector):
    country_code = "KR"

    @contextmanager
    def _suppress_pykrx_info_logging(self):
        """Suppress pykrx's malformed root-level info logging during API calls."""
        root_logger = logging.getLogger()
        previous_level = root_logger.level
        root_logger.setLevel(max(logging.WARNING, previous_level))
        try:
            yield
        finally:
            root_logger.setLevel(previous_level)

    def _call_pykrx(
        self,
        label: str,
        func,
        *args,
        retries: int = 3,
        retry_delay: float = 1.0,
        validator=None,
        **kwargs,
    ):
        """Call pykrx with retries and optional response validation."""
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                with self._suppress_pykrx_info_logging():
                    result = func(*args, **kwargs)
                if validator is not None:
                    validator(result)
                return result
            except Exception as exc:
                last_exc = exc
                if attempt >= retries:
                    break
                logger.warning(
                    f"[KR] {label} 실패, 재시도 {attempt}/{retries - 1}: {exc}"
                )
                time.sleep(retry_delay * attempt)

        raise last_exc

    def _validate_ohlcv_frame(
        self,
        frame: pd.DataFrame,
        required_columns: tuple[str, ...] = ("종가", "거래량"),
    ) -> None:
        """Ensure pykrx returned the expected OHLCV structure."""
        if frame is None or frame.empty:
            return

        missing_columns = [
            column for column in required_columns if column not in frame.columns
        ]
        if missing_columns:
            raise ValueError(f"pykrx invalid columns: {missing_columns}")

    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """KOSPI + KOSDAQ 전종목 일간 데이터 수집."""
        for date_fmt in self._candidate_trading_dates(date):
            weekly_reference_date = self._resolve_weekly_reference_date(date_fmt)
            all_data = []
            for market in ["KOSPI", "KOSDAQ"]:
                df = self._fetch_market(
                    date_fmt,
                    market,
                    weekly_reference_date=weekly_reference_date,
                )
                if df is not None and not df.empty:
                    all_data.append(df)

            if not all_data:
                continue

            result = pd.concat(all_data, ignore_index=True)
            self.effective_date = (
                f"{date_fmt[:4]}-{date_fmt[4:6]}-{date_fmt[6:8]}"
            )
            logger.info(f"[KR] 전체: {len(result)}개 종목")
            return result

        logger.warning(f"[KR] 최근 7일 내 사용 가능한 데이터 없음 ({date})")
        return pd.DataFrame()

    def _candidate_trading_dates(self, date: str) -> list[str]:
        """요청일 기준 최근 거래일 후보를 중복 없이 반환."""
        return self._recent_trading_dates(date, lookback_days=7)

    def _recent_trading_dates(self, date: str, lookback_days: int) -> list[str]:
        """요청일 기준 최근 거래일 후보를 중복 없이 반환."""
        candidates: list[str] = []
        seen: set[str] = set()

        for raw_date in recent_dates(date, lookback_days=lookback_days):
            compact = raw_date.replace("-", "")
            try:
                business_date = self._call_pykrx(
                    "거래일 확인",
                    krx.get_nearest_business_day_in_a_week,
                    compact,
                    retries=2,
                    retry_delay=0.5,
                )
            except Exception:
                business_date = compact

            if business_date not in seen:
                seen.add(business_date)
                candidates.append(business_date)

        return candidates

    def _resolve_weekly_reference_date(self, date_fmt: str) -> str | None:
        """현재 거래일 대비 약 5거래일 전 날짜를 반환."""
        iso_date = f"{date_fmt[:4]}-{date_fmt[4:6]}-{date_fmt[6:8]}"
        recent_trading_dates = self._recent_trading_dates(iso_date, lookback_days=21)
        if len(recent_trading_dates) <= 1:
            return None

        idx = min(5, len(recent_trading_dates) - 1)
        reference_date = recent_trading_dates[idx]
        return reference_date if reference_date != date_fmt else None

    def _fetch_market(
        self,
        date_fmt: str,
        market: str,
        weekly_reference_date: str | None = None,
    ) -> pd.DataFrame | None:
        """특정 시장 전종목 데이터 수집."""
        try:
            # 1) 전종목 OHLCV (등락률, 시가총액 포함)
            ohlcv = self._call_pykrx(
                f"{market} OHLCV",
                krx.get_market_ohlcv,
                date_fmt,
                market=market,
                validator=self._validate_ohlcv_frame,
            )
            if ohlcv.empty:
                logger.warning(f"[KR] {market} 데이터 없음 ({date_fmt})")
                return None
            time.sleep(1)

            weekly_reference = None
            if weekly_reference_date:
                weekly_reference = self._call_pykrx(
                    f"{market} 주간 비교 OHLCV",
                    krx.get_market_ohlcv,
                    weekly_reference_date,
                    market=market,
                    validator=lambda frame: self._validate_ohlcv_frame(
                        frame,
                        required_columns=("종가",),
                    ),
                )
                time.sleep(0.5)

            # 2) 업종 인덱스 → 종목별 섹터 매핑
            sector_map = self._build_sector_map(date_fmt, market)
            time.sleep(0.5)

            # 3) DataFrame 구성
            rows = []
            for ticker in ohlcv.index:
                try:
                    name = krx.get_market_ticker_name(ticker)
                    row_data = ohlcv.loc[ticker]

                    close_price = float(row_data["종가"])
                    volume = float(row_data["거래량"])
                    daily_return = (
                        float(row_data["등락률"])
                        if "등락률" in ohlcv.columns
                        else None
                    )
                    weekly_return = None
                    if (
                        weekly_reference is not None
                        and ticker in weekly_reference.index
                        and "종가" in weekly_reference.columns
                    ):
                        prev_close = float(weekly_reference.loc[ticker]["종가"])
                        weekly_return = compute_return_pct(close_price, prev_close)
                    market_cap = (
                        float(row_data["시가총액"])
                        if "시가총액" in ohlcv.columns
                        else None
                    )

                    # 섹터 매핑: 업종 인덱스 기반
                    raw_sector = sector_map.get(ticker, "기타")
                    sector = KRX_INDEX_TO_GICS.get(
                        raw_sector, KR_SECTOR_MAP.get(raw_sector, "기타")
                    )

                    rows.append({
                        "ticker": ticker,
                        "name": name,
                        "sector": sector,
                        "market_cap": market_cap,
                        "close_price": close_price,
                        "daily_return": daily_return,
                        "weekly_return": weekly_return,
                        "volume": volume,
                        "avg_volume_20d": None,
                    })
                except Exception as e:
                    logger.debug(f"[KR] {ticker} 스킵: {e}")
                    continue

            df = pd.DataFrame(rows)
            logger.info(f"[KR] {market}: {len(df)}개 종목")
            return df

        except Exception as e:
            logger.error(f"[KR] {market} 수집 실패: {e}", exc_info=True)
            return None

    def _build_sector_map(self, date_fmt: str, market: str) -> dict[str, str]:
        """업종 인덱스 구성종목 조회 → 종목→업종 매핑."""
        sector_map: dict[str, str] = {}

        # 종합/규모 지수는 스킵 (코스피, 코스닥, 대형주 등)
        skip_prefixes = ("코스피", "코스닥", "KOSPI", "KOSDAQ")

        try:
            idx_list = self._call_pykrx(
                f"{market} 업종 인덱스",
                krx.get_index_ticker_list,
                date_fmt,
                market=market,
                retries=2,
                retry_delay=0.5,
            )
            for idx_ticker in idx_list:
                idx_name = krx.get_index_ticker_name(idx_ticker)

                if any(idx_name.startswith(p) for p in skip_prefixes):
                    continue
                if idx_name not in KRX_INDEX_TO_GICS:
                    continue

                try:
                    components = self._call_pykrx(
                        f"{market} 업종 구성종목 {idx_name}",
                        krx.get_index_portfolio_deposit_file,
                        idx_ticker,
                        date_fmt,
                        retries=2,
                        retry_delay=0.5,
                    )
                    if components:
                        for stock_ticker in components:
                            if stock_ticker not in sector_map:
                                sector_map[stock_ticker] = idx_name
                    time.sleep(0.3)
                except Exception:
                    continue

        except Exception as e:
            logger.warning(f"[KR] 업종 매핑 실패: {e}")

        logger.info(f"[KR] {market} 업종 매핑: {len(sector_map)}개 종목")
        return sector_map
