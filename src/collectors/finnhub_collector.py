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
from datetime import datetime, timedelta

import finnhub
import pandas as pd
import yfinance as yf

from src.collectors.base import BaseCollector
from src.collectors.date_utils import compute_period_return_from_closes
from src.config import (
    COUNTRIES,
    FINNHUB_API_KEY,
    SECTOR_EN_TO_KR,
    UNIVERSE_PREFILTER_FULL_REFRESH_WEEKDAY,
    UNIVERSE_PREFILTER_TARGET_COUNT,
)
from src.database import get_connection, get_instrument_universe

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

    metadata_source = "finnhub"

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

    def _prefilter_stocks(self, stocks: list[dict], date: str) -> list[dict]:
        """Reuse the latest universe snapshot before expensive yfinance fetches."""
        target_count = UNIVERSE_PREFILTER_TARGET_COUNT.get(self.country_code)
        if not target_count or len(stocks) <= target_count:
            return stocks

        refresh_weekday = UNIVERSE_PREFILTER_FULL_REFRESH_WEEKDAY.get(
            self.country_code
        )
        try:
            requested_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            requested_date = None

        if (
            requested_date is not None
            and refresh_weekday is not None
            and requested_date.weekday() == refresh_weekday
        ):
            logger.info(
                f"[{self.country_code}] weekly full refresh day, skip prefilter"
            )
            return stocks

        try:
            conn = get_connection()
            try:
                cached_rows = get_instrument_universe(conn, self.country_code)
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(
                f"[{self.country_code}] universe cache unavailable, use full fetch: {exc}"
            )
            return stocks

        if not cached_rows:
            logger.info(f"[{self.country_code}] universe cache empty, use full fetch")
            return stocks

        stock_by_ticker = {
            stock["symbol"]: stock for stock in stocks if stock.get("symbol")
        }
        cached_rows = [
            row for row in cached_rows if row.get("ticker") in stock_by_ticker
        ]
        if not cached_rows:
            logger.info(
                f"[{self.country_code}] cache does not match current symbols, use full fetch"
            )
            return stocks

        min_cached_rows = min(len(stocks), max(200, target_count // 2))
        if len(cached_rows) < min_cached_rows:
            logger.info(
                f"[{self.country_code}] cache too small ({len(cached_rows)}/{len(stocks)}), use full fetch"
            )
            return stocks

        if requested_date is not None:
            fresh_rows = []
            for row in cached_rows:
                last_seen_date = row.get("last_seen_date")
                if not last_seen_date:
                    continue
                try:
                    age_days = (
                        requested_date - datetime.strptime(last_seen_date, "%Y-%m-%d")
                    ).days
                except ValueError:
                    continue
                if age_days <= 14:
                    fresh_rows.append(row)

            if len(fresh_rows) < min_cached_rows:
                logger.info(
                    f"[{self.country_code}] cache too stale ({len(fresh_rows)}/{len(cached_rows)} fresh), use full fetch"
                )
                return stocks
            cached_rows = fresh_rows

        def sort_key(row: dict) -> tuple:
            abnormal_rank = 0 if int(row.get("last_is_abnormal") or 0) == 1 else 1
            filtered_rank = 0 if int(row.get("last_is_filtered") or 0) == 0 else 1
            volume_rank = -(float(row.get("last_volume") or 0.0))
            market_cap_rank = -(float(row.get("market_cap") or 0.0))
            return (
                abnormal_rank,
                filtered_rank,
                volume_rank,
                market_cap_rank,
                row.get("ticker", ""),
            )

        selected = []
        selected_tickers = set()
        for row in sorted(cached_rows, key=sort_key):
            ticker = row.get("ticker")
            if not ticker or ticker in selected_tickers:
                continue
            selected.append(stock_by_ticker[ticker])
            selected_tickers.add(ticker)
            if len(selected) >= target_count:
                break

        if len(selected) < target_count:
            for stock in stocks:
                ticker = stock.get("symbol")
                if not ticker or ticker in selected_tickers:
                    continue
                selected.append(stock)
                selected_tickers.add(ticker)
                if len(selected) >= target_count:
                    break

        logger.info(
            f"[{self.country_code}] universe prefilter {len(stocks)} -> {len(selected)}"
        )
        return selected

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

        logger.info(f"[{self.country_code}] 대상 종목(원본): {len(stocks)}개")
        stocks = self._prefilter_stocks(stocks, date)
        logger.info(f"[{self.country_code}] 다운로드 후보: {len(stocks)}개")

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
        # 5거래일 이상 확보하기 위해 2주치 데이터를 요청
        dt = datetime.strptime(date, "%Y-%m-%d")
        start_date = (dt - timedelta(days=14)).strftime("%Y-%m-%d")

        # 배치 크기: yfinance는 한번에 ~500개 처리 가능
        batch_size = 200
        all_rows = []
        used_dates: set[str] = set()

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

                        valid_data = ticker_data.dropna(subset=["Close"])
                        if valid_data.empty:
                            continue

                        # 최신 날짜 데이터
                        latest = valid_data.iloc[-1]
                        close_price = float(latest["Close"])
                        volume = (
                            float(latest["Volume"])
                            if pd.notna(latest["Volume"])
                            else None
                        )
                        used_dates.add(valid_data.index[-1].strftime("%Y-%m-%d"))

                        # 등락률 계산
                        daily_return = None
                        if len(valid_data) >= 2:
                            prev_close = float(valid_data.iloc[-2]["Close"])
                            if prev_close > 0:
                                daily_return = ((close_price - prev_close) / prev_close) * 100
                        weekly_return = compute_period_return_from_closes(
                            valid_data["Close"].tolist()
                        )

                        avg_volume_20d = None
                        valid_volume = ticker_data["Volume"].dropna().tail(20)
                        if len(valid_volume) > 0:
                            avg_volume_20d = float(valid_volume.mean())

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
                            "weekly_return": weekly_return,
                            "volume": volume,
                            "avg_volume_20d": avg_volume_20d,
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
        if used_dates:
            self.effective_date = max(used_dates)

        # 시총이 없으면 Finnhub profile2로 보충 (상위 종목만, rate limit 고려)
        if not df.empty:
            df = self._add_market_caps(df, date)

        return df

    def _apply_metadata_to_df(
        self,
        df: pd.DataFrame,
        names: dict[str, str],
        sectors: dict[str, str],
        market_caps: dict[str, float],
    ) -> pd.DataFrame:
        """Overlay cached/refreshed metadata onto the current quote snapshot."""
        if names:
            mapped_names = df["ticker"].map(names)
            df["name"] = mapped_names.where(mapped_names.notna(), df["name"])
        if sectors:
            mapped_sectors = df["ticker"].map(sectors)
            df["sector"] = mapped_sectors.where(mapped_sectors.notna(), df["sector"])
        if market_caps:
            mapped_caps = df["ticker"].map(market_caps)
            df["market_cap"] = mapped_caps.where(mapped_caps.notna(), df["market_cap"])
        return df

    def _add_market_caps(self, df: pd.DataFrame, date: str) -> pd.DataFrame:
        """Use cached metadata and only refresh Finnhub profiles when needed."""
        tickers = df["ticker"].tolist()
        cached_metadata = self._get_cached_metadata(tickers)
        refresh_due = self._is_metadata_refresh_due(date)

        names = {}
        sectors = {}
        market_caps = {}
        for ticker, row in cached_metadata.items():
            if row.get("name"):
                names[ticker] = row["name"]
            if row.get("sector"):
                sectors[ticker] = row["sector"]
            if row.get("market_cap") is not None:
                market_caps[ticker] = float(row["market_cap"])

        # 거래량 상위 종목만 metadata refresh 대상으로 유지한다.
        top_by_volume = df.nlargest(min(500, len(df)), "volume")
        metadata_rows = []

        for _, row in top_by_volume.iterrows():
            ticker = row["ticker"]
            cached_row = cached_metadata.get(ticker)
            if (
                not refresh_due
                and cached_row is not None
                and self._metadata_row_is_fresh(cached_row, date)
            ):
                continue

            try:
                self._rate_limit()
                profile = self._client.company_profile2(symbol=ticker)
                if not profile:
                    continue

                name = profile.get("name") or names.get(ticker) or row["name"]
                raw_sector = profile.get("finnhubIndustry")
                sector = FINNHUB_SECTOR_TO_GICS.get(raw_sector, None) if raw_sector else None
                market_cap = market_caps.get(ticker)
                if profile.get("marketCapitalization") is not None:
                    market_cap = float(profile["marketCapitalization"]) * 1_000_000

                if name:
                    names[ticker] = name
                if sector:
                    sectors[ticker] = sector
                if market_cap is not None:
                    market_caps[ticker] = market_cap

                metadata_rows.append(
                    {
                        "ticker": ticker,
                        "name": name,
                        "sector": sector or sectors.get(ticker) or row["sector"],
                        "market_cap": market_cap,
                    }
                )
            except Exception:
                continue

        if metadata_rows:
            self._upsert_metadata(metadata_rows)

        return self._apply_metadata_to_df(df, names, sectors, market_caps)


class USCollector(FinnhubCollector):
    def __init__(self):
        super().__init__("US")
