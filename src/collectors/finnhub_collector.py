"""Finnhub 기반 수집기 - 미국, 일본, 독일, 인도 시장

Finnhub 무료: 60콜/분, 일 86,400콜.
전종목 수집 전략:
1. /stock/symbol로 전체 종목 리스트 (섹터 포함) — 1콜
2. /stock/profile2로 시총 확인 — 종목당 1콜 (필요시)
3. /quote로 현재가/등락 — 종목당 1콜

Rate limit 관리를 위해 배치 처리 + sleep.
"""

import logging
import time
from datetime import datetime

import finnhub
import pandas as pd
import yfinance as yf

from src.collectors.base import BaseCollector
from src.config import COUNTRIES, FINNHUB_API_KEY, SECTOR_EN_TO_KR

logger = logging.getLogger(__name__)

# Finnhub 거래소 코드 → 국가 매핑
EXCHANGE_MAP = {
    "US": "US",
    "JP": "T",   # Tokyo
    "DE": "DE",  # XETRA는 Finnhub에서 "HE" 또는 "DE"
    "IN": "NS",  # NSE
}

# Finnhub의 finnhubIndustry → GICS 섹터 매핑
FINNHUB_SECTOR_TO_GICS = {
    "Technology": "정보기술",
    "Financial Services": "금융",
    "Healthcare": "헬스케어",
    "Consumer Cyclical": "경기소비재",
    "Consumer Defensive": "필수소비재",
    "Industrials": "산업재",
    "Energy": "에너지",
    "Basic Materials": "소재",
    "Utilities": "유틸리티",
    "Real Estate": "부동산",
    "Communication Services": "커뮤니케이션",
    # yfinance 용
    "Information Technology": "정보기술",
    "Financials": "금융",
    "Health Care": "헬스케어",
    "Consumer Discretionary": "경기소비재",
    "Consumer Staples": "필수소비재",
    "Materials": "소재",
}


class FinnhubCollector(BaseCollector):
    """Finnhub API 기반 수집기. 국가 코드를 설정해서 사용."""

    def __init__(self, country_code: str):
        self.country_code = country_code
        self._client = finnhub.Client(api_key=FINNHUB_API_KEY)
        self._call_count = 0
        self._last_reset = time.time()

    def _rate_limit(self):
        """60콜/분 제한 준수."""
        self._call_count += 1
        if self._call_count >= 55:  # 약간 여유
            elapsed = time.time() - self._last_reset
            if elapsed < 60:
                sleep_time = 60 - elapsed + 1
                logger.info(f"Rate limit 대기: {sleep_time:.0f}초")
                time.sleep(sleep_time)
            self._call_count = 0
            self._last_reset = time.time()

    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """전종목 수집. yfinance로 가격 데이터, Finnhub으로 종목 리스트."""
        exchange = EXCHANGE_MAP.get(self.country_code, "US")
        country_info = COUNTRIES[self.country_code]

        # 1) 종목 리스트 가져오기 (Finnhub)
        logger.info(f"[{self.country_code}] 종목 리스트 조회 (exchange={exchange})")
        self._rate_limit()
        try:
            symbols = self._client.stock_symbols(exchange)
        except Exception as e:
            logger.error(f"[{self.country_code}] 종목 리스트 실패: {e}")
            return pd.DataFrame()

        if not symbols:
            return pd.DataFrame()

        # 보통주만 필터 (type == "Common Stock" 또는 "EQS")
        stocks = [
            s for s in symbols
            if s.get("type") in ("Common Stock", "EQS", "Equity", "")
            and s.get("symbol")
            and "." not in s.get("symbol", "x.x")  # 우선주 등 제외 (US)
        ]

        # US의 경우 워런트, 유닛 등 제외
        if self.country_code == "US":
            stocks = [
                s for s in stocks
                if not any(suffix in s.get("symbol", "")
                          for suffix in ["/W", "/U", "/R", "-W", "-U"])
            ]

        logger.info(f"[{self.country_code}] 대상 종목: {len(stocks)}개")

        # 2) yfinance로 배치 가격 데이터 수집 (Finnhub보다 효율적)
        tickers = [s["symbol"] for s in stocks]
        sector_info = {s["symbol"]: s for s in stocks}

        # yfinance 배치 다운로드 (한번에 수백개 가능)
        df = self._batch_fetch_yfinance(tickers, date, sector_info)

        return df

    def _batch_fetch_yfinance(self, tickers: list[str], date: str,
                               sector_info: dict) -> pd.DataFrame:
        """yfinance로 배치 가격 데이터 + 섹터 정보 수집.

        yfinance.download()은 한번에 수백개 티커를 처리 가능.
        """
        # 날짜 범위: 전일 ~ 당일 (등락률 계산용)
        end_date = date
        # 시작일은 2일 전 (주말/휴일 고려)
        from datetime import timedelta
        dt = datetime.strptime(date, "%Y-%m-%d")
        start_date = (dt - timedelta(days=5)).strftime("%Y-%m-%d")

        # 배치 크기: yfinance는 한번에 ~500개 처리 가능
        batch_size = 200
        all_rows = []

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            batch_str = " ".join(batch)

            try:
                data = yf.download(
                    batch_str,
                    start=start_date,
                    end=(dt + timedelta(days=1)).strftime("%Y-%m-%d"),
                    group_by="ticker",
                    auto_adjust=True,
                    progress=False,
                    threads=True,
                )

                if data.empty:
                    continue

                for ticker in batch:
                    try:
                        if len(batch) == 1:
                            ticker_data = data
                        else:
                            if ticker not in data.columns.get_level_values(0):
                                continue
                            ticker_data = data[ticker]

                        if ticker_data.empty:
                            continue

                        # 최신 날짜 데이터
                        latest = ticker_data.iloc[-1]
                        close_price = float(latest["Close"])
                        volume = float(latest["Volume"])

                        # 등락률 계산
                        daily_return = None
                        if len(ticker_data) >= 2:
                            prev_close = float(ticker_data.iloc[-2]["Close"])
                            if prev_close > 0:
                                daily_return = ((close_price - prev_close) / prev_close) * 100

                        # 섹터 정보 (Finnhub 데이터 기반)
                        info = sector_info.get(ticker, {})
                        finnhub_sector = info.get("type2", "") or ""
                        sector = FINNHUB_SECTOR_TO_GICS.get(finnhub_sector, "기타")

                        all_rows.append({
                            "ticker": ticker,
                            "name": info.get("description", ticker),
                            "sector": sector,
                            "market_cap": None,  # 나중에 별도 조회
                            "close_price": close_price,
                            "daily_return": daily_return,
                            "volume": volume,
                            "avg_volume_20d": None,
                        })
                    except Exception as e:
                        logger.debug(f"[{self.country_code}] {ticker} 처리 실패: {e}")
                        continue

            except Exception as e:
                logger.warning(
                    f"[{self.country_code}] 배치 {i}-{i+batch_size} 실패: {e}"
                )
                continue

            logger.info(
                f"[{self.country_code}] 배치 {i+len(batch)}/{len(tickers)} 완료"
            )
            time.sleep(1)  # yfinance 부하 방지

        df = pd.DataFrame(all_rows)

        # 시총이 없으면 Finnhub profile2로 보충 (상위 종목만, rate limit 고려)
        if not df.empty:
            df = self._add_market_caps(df)

        return df

    def _add_market_caps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finnhub profile2로 시가총액 추가. rate limit 고려하여 상위 종목만."""
        # 거래량 상위 종목만 시총 조회 (rate limit 절약)
        top_by_volume = df.nlargest(min(500, len(df)), "volume")

        market_caps = {}
        for _, row in top_by_volume.iterrows():
            ticker = row["ticker"]
            try:
                self._rate_limit()
                profile = self._client.company_profile2(symbol=ticker)
                if profile and "marketCapitalization" in profile:
                    # Finnhub은 백만 단위로 반환
                    market_caps[ticker] = profile["marketCapitalization"] * 1_000_000
                    # 섹터도 보충
                    if row["sector"] == "기타" and profile.get("finnhubIndustry"):
                        sector = FINNHUB_SECTOR_TO_GICS.get(
                            profile["finnhubIndustry"], "기타"
                        )
                        df.loc[df["ticker"] == ticker, "sector"] = sector
            except Exception:
                continue

        # 시총 반영
        df["market_cap"] = df["ticker"].map(market_caps)
        return df


class USCollector(FinnhubCollector):
    def __init__(self):
        super().__init__("US")


class JPCollector(FinnhubCollector):
    def __init__(self):
        super().__init__("JP")


class DECollector(FinnhubCollector):
    def __init__(self):
        super().__init__("DE")


class INCollector(FinnhubCollector):
    def __init__(self):
        super().__init__("IN")
