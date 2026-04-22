"""Korea market collector.

Primary source: pykrx
Fallback source: FinanceDataReader KRX-MARCAP snapshot
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
import logging
import time

import pandas as pd
from pykrx import stock as krx

from src.collectors.base import BaseCollector
from src.collectors.date_utils import compute_return_pct, recent_dates
from src.config import KR_SECTOR_MAP, SECTORS
from src.database import get_connection, get_instrument_universe, get_raw_connection

logger = logging.getLogger(__name__)


KRX_INDEX_TO_GICS = {
    "음식료품담배": "필수소비재",
    "섬유의류": "경기소비재",
    "종이목재": "소재",
    "화학": "소재",
    "제약": "헬스케어",
    "비금속광물": "소재",
    "금속": "소재",
    "기계장비": "산업재",
    "전기전자": "정보기술",
    "의료정밀기기": "헬스케어",
    "운송장비부품": "산업재",
    "유통": "경기소비재",
    "전기가스": "유틸리티",
    "건설": "산업재",
    "운수창고": "산업재",
    "통신": "커뮤니케이션",
    "금융": "금융",
    "증권": "금융",
    "보험": "금융",
    "일반서비스": "산업재",
    "제조": "산업재",
    "부동산": "부동산",
    "IT 서비스": "정보기술",
    "오락문화": "커뮤니케이션",
    "출판매체복제": "커뮤니케이션",
    "기타제조": "산업재",
}


class KoreaCollector(BaseCollector):
    country_code = "KR"

    @contextmanager
    def _suppress_pykrx_info_logging(self):
        """Suppress pykrx's broken root-level info logging."""
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
        """Call pykrx with retries and optional validation."""
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                with self._suppress_pykrx_info_logging():
                    result = func(*args, **kwargs)
                if validator is not None:
                    validator(result)
                return result
            except Exception as exc:  # pragma: no cover - retry path is tested
                last_exc = exc
                if attempt >= retries:
                    break
                logger.warning(
                    f"[KR] {label} 실패, 재시도 {attempt}/{retries - 1}: {exc}"
                )
                time.sleep(retry_delay * attempt)

        raise last_exc

    def _is_transport_error(self, exc: Exception) -> bool:
        """Detect pykrx/network failures where retrying older dates will not help."""
        message = str(exc)
        markers = (
            "None of [Index(",
            "Expecting value",
            "JSONDecodeError",
            "pykrx invalid columns",
            "index -1 is out of bounds",
        )
        return any(marker in message for marker in markers)

    def _validate_ohlcv_frame(
        self,
        frame: pd.DataFrame,
        required_columns: tuple[str, ...] = ("종가", "거래량"),
    ) -> None:
        """Validate that pykrx returned an OHLCV-like frame."""
        if frame is None or frame.empty:
            return

        missing_columns = [
            column for column in required_columns if column not in frame.columns
        ]
        if missing_columns:
            raise ValueError(f"pykrx invalid columns: {missing_columns}")

    def _iso_from_compact(self, date_fmt: str | None) -> str | None:
        if not date_fmt:
            return None
        return f"{date_fmt[:4]}-{date_fmt[4:6]}-{date_fmt[6:8]}"

    def _load_cached_universe_map(self) -> dict[str, dict]:
        """Load cached KR universe rows keyed by ticker."""
        try:
            conn = get_connection()
            try:
                rows = get_instrument_universe(conn, self.country_code)
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(f"[KR] cached universe unavailable: {exc}")
            return {}

        return {
            row["ticker"]: row
            for row in rows
            if row.get("ticker")
        }

    def _load_cached_close_map(
        self,
        tickers: list[str],
        end_date: str | None,
        lookback_days: int = 7,
    ) -> dict[str, float]:
        """Load the latest cached close at or before one reference date."""
        if not tickers or not end_date:
            return {}

        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_date = (end_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        placeholders = ", ".join("?" for _ in tickers)

        conn = get_raw_connection()
        try:
            rows = conn.execute(
                f"""
                SELECT ticker, date, close_price
                FROM stock_daily
                WHERE country = ?
                  AND date BETWEEN ? AND ?
                  AND ticker IN ({placeholders})
                ORDER BY date DESC
                """,
                [self.country_code, start_date, end_date, *tickers],
            ).fetchall()
        finally:
            conn.close()

        close_map: dict[str, float] = {}
        for row in rows:
            ticker = row["ticker"]
            if ticker not in close_map and row["close_price"] is not None:
                close_map[ticker] = float(row["close_price"])
        return close_map

    def _normalize_fdr_listing(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Normalize FinanceDataReader listing frames into common columns."""
        if frame is None or frame.empty:
            return pd.DataFrame()

        normalized = frame.copy()
        alias_map = {
            "ticker": ("Code", "Symbol", "code", "symbol", "종목코드", "ticker"),
            "name": ("Name", "name", "종목명"),
            "sector": ("Sector", "Industry", "industry", "업종", "sector"),
            "market": ("Market", "market", "시장"),
            "close_price": ("Close", "close", "종가"),
            "volume": ("Volume", "volume", "거래량"),
            "market_cap": ("Marcap", "MarketCap", "market_cap", "시가총액"),
            "daily_return": (
                "ChagesRatio",
                "ChangesRatio",
                "ChangeRatio",
                "daily_return",
                "등락률",
            ),
        }

        for target, aliases in alias_map.items():
            if target in normalized.columns:
                continue
            for column in aliases:
                if column in normalized.columns:
                    normalized[target] = normalized[column]
                    break

        if "ticker" in normalized.columns:
            normalized["ticker"] = (
                normalized["ticker"].astype(str).str.strip().str.zfill(6)
            )

        for numeric_column in ("close_price", "volume", "market_cap", "daily_return"):
            if numeric_column in normalized.columns:
                normalized[numeric_column] = pd.to_numeric(
                    normalized[numeric_column],
                    errors="coerce",
                )

        if "market_cap" in normalized.columns:
            market_cap = normalized["market_cap"].dropna()
            if not market_cap.empty and float(market_cap.max()) < 1_000_000_000_000:
                # FinanceDataReader's KRX-MARCAP snapshot uses 백만원 units.
                normalized["market_cap"] = normalized["market_cap"] * 1_000_000

        keep_columns = [
            column
            for column in (
                "ticker",
                "name",
                "sector",
                "market",
                "close_price",
                "volume",
                "market_cap",
                "daily_return",
            )
            if column in normalized.columns
        ]
        return normalized[keep_columns].dropna(subset=["ticker"]).drop_duplicates(
            subset=["ticker"]
        )

    def _merge_listing_metadata(
        self,
        snapshot: pd.DataFrame,
        listing: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge listing metadata into the fallback snapshot without losing rows."""
        if snapshot.empty or listing.empty:
            return snapshot

        merge_columns = ["ticker"]
        for field in ("name", "sector", "market"):
            if field in listing.columns:
                merge_columns.append(field)

        listing = listing[merge_columns].drop_duplicates(subset=["ticker"])
        merged = snapshot.merge(listing, on="ticker", how="left", suffixes=("", "_listing"))

        for field in ("name", "sector", "market"):
            fallback_col = f"{field}_listing"
            if fallback_col not in merged.columns:
                continue
            if field not in merged.columns:
                merged[field] = merged[fallback_col]
            else:
                merged[field] = merged[field].where(
                    merged[field].notna() & merged[field].astype(str).str.strip().ne(""),
                    merged[fallback_col],
                )
            merged = merged.drop(columns=[fallback_col])

        return merged

    def _is_generic_sector(self, sector: str | None) -> bool:
        if sector is None:
            return True
        return str(sector).strip() in ("", "기타")

    def _map_sector(self, raw_sector: str | None) -> str:
        if not raw_sector:
            return "기타"

        sector = str(raw_sector).strip()
        if not sector:
            return "기타"
        if sector in SECTORS:
            return sector
        if sector in KRX_INDEX_TO_GICS:
            return KRX_INDEX_TO_GICS[sector]
        if sector in KR_SECTOR_MAP:
            return KR_SECTOR_MAP[sector]

        normalized = sector.replace(" ", "").replace(",", "")
        for raw_key, mapped in {**KRX_INDEX_TO_GICS, **KR_SECTOR_MAP}.items():
            key = str(raw_key).replace(" ", "").replace(",", "")
            if not key:
                continue
            if key in normalized or normalized in key:
                return mapped

        return "기타"

    def _fetch_market_with_fdr(
        self,
        date_fmt: str,
        market: str,
        weekly_reference_date: str | None = None,
    ) -> pd.DataFrame | None:
        """Fallback current-day KR snapshot via FinanceDataReader."""
        try:
            import FinanceDataReader as fdr
        except ImportError as exc:
            raise RuntimeError(
                "FinanceDataReader 미설치: pip install finance-datareader"
            ) from exc

        snapshot = self._normalize_fdr_listing(fdr.StockListing("KRX-MARCAP"))
        if snapshot.empty:
            return None

        market_listing = pd.DataFrame()
        krx_listing = pd.DataFrame()
        try:
            krx_listing = self._normalize_fdr_listing(fdr.StockListing("KRX"))
        except Exception as exc:
            logger.warning(f"[KR] KRX FDR listing unavailable: {exc}")

        try:
            market_listing = self._normalize_fdr_listing(fdr.StockListing(market))
        except Exception as exc:
            logger.warning(f"[KR] {market} FDR listing unavailable: {exc}")

        if not krx_listing.empty:
            snapshot = self._merge_listing_metadata(snapshot, krx_listing)

        if not market_listing.empty:
            snapshot = self._merge_listing_metadata(snapshot, market_listing)
            tickers = set(market_listing["ticker"].tolist())
            snapshot = snapshot[snapshot["ticker"].isin(tickers)].copy()
        elif "market" in snapshot.columns:
            snapshot = snapshot[snapshot["market"].astype(str).str.upper() == market].copy()

        if snapshot.empty:
            logger.warning(f"[KR] {market} FDR fallback returned no rows")
            return None

        cached_universe = self._load_cached_universe_map()
        cached_metadata = self._get_cached_metadata(snapshot["ticker"].tolist())
        reference_close_map = self._load_cached_close_map(
            snapshot["ticker"].tolist(),
            self._iso_from_compact(weekly_reference_date),
            lookback_days=10,
        )

        rows = []
        for _, row in snapshot.iterrows():
            ticker = row["ticker"]
            close_price = row.get("close_price")
            if pd.isna(close_price):
                continue

            cached = cached_universe.get(ticker, {})
            metadata = cached_metadata.get(ticker, {})

            daily_return = row.get("daily_return")
            daily_return = None if pd.isna(daily_return) else float(daily_return)

            prev_close = reference_close_map.get(ticker)
            weekly_return = (
                compute_return_pct(float(close_price), prev_close)
                if prev_close not in (None, 0)
                else None
            )

            raw_sector = row.get("sector")
            mapped_sector = None
            if pd.notna(raw_sector) and raw_sector:
                mapped_sector = self._map_sector(str(raw_sector))

            sector = mapped_sector
            if self._is_generic_sector(sector):
                sector = metadata.get("sector") or cached.get("sector") or "기타"

            volume = row.get("volume")
            market_cap = row.get("market_cap")
            if pd.notna(market_cap):
                market_cap_value = float(market_cap)
            elif metadata.get("market_cap") is not None:
                market_cap_value = float(metadata["market_cap"])
            else:
                market_cap_value = cached.get("market_cap")

            rows.append(
                {
                    "ticker": ticker,
                    "name": (
                        row.get("name")
                        or metadata.get("name")
                        or cached.get("name")
                        or ticker
                    ),
                    "sector": sector,
                    "market_cap": market_cap_value,
                    "close_price": float(close_price),
                    "daily_return": daily_return,
                    "weekly_return": weekly_return,
                    "volume": (
                        float(volume)
                        if pd.notna(volume)
                        else cached.get("last_volume")
                    ),
                    "avg_volume_20d": (
                        cached.get("avg_volume_20d")
                        if cached.get("avg_volume_20d") is not None
                        else metadata.get("avg_volume_20d")
                    ),
                }
            )

        df = pd.DataFrame(rows)
        if not df.empty:
            generic_count = int((df["sector"] == "기타").sum())
            logger.info(
                f"[KR] {market} FDR sector coverage: "
                f"{df['sector'].nunique()} sectors, {generic_count}/{len(df)} generic"
            )
        logger.warning(f"[KR] {market} recovered via FinanceDataReader: {len(df)} rows")
        return df

    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """Fetch KOSPI + KOSDAQ daily snapshots."""
        self._pykrx_transport_failed = False
        for date_fmt in self._candidate_trading_dates(date):
            weekly_reference_date = self._resolve_weekly_reference_date(date_fmt)
            all_data = []
            for market in ("KOSPI", "KOSDAQ"):
                df = self._fetch_market(
                    date_fmt,
                    market,
                    weekly_reference_date=weekly_reference_date,
                )
                if df is not None and not df.empty:
                    all_data.append(df)

            if not all_data:
                if self._pykrx_transport_failed:
                    logger.warning(
                        "[KR] pykrx transport unhealthy, skipping older-date fallback"
                    )
                    break
                continue

            result = pd.concat(all_data, ignore_index=True)
            self.effective_date = self._iso_from_compact(date_fmt) or date
            logger.info(f"[KR] 전체: {len(result)}개 종목")
            return result

        logger.warning(f"[KR] 최근 7일 내 사용 가능한 데이터 없음 ({date})")
        return pd.DataFrame()

    def _candidate_trading_dates(self, date: str) -> list[str]:
        """Return recent calendar-date candidates without extra pykrx calls."""
        return self._recent_trading_dates(date, lookback_days=7)

    def _recent_trading_dates(self, date: str, lookback_days: int) -> list[str]:
        candidates: list[str] = []
        seen: set[str] = set()

        for raw_date in recent_dates(date, lookback_days=lookback_days):
            compact = raw_date.replace("-", "")
            if compact not in seen:
                seen.add(compact)
                candidates.append(compact)

        return candidates

    def _resolve_weekly_reference_date(self, date_fmt: str) -> str | None:
        """Pick an approximately five-trading-day-back reference date."""
        iso_date = self._iso_from_compact(date_fmt)
        if iso_date is None:
            return None

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
        """Fetch one Korea market snapshot."""
        try:
            ohlcv = self._call_pykrx(
                f"{market} OHLCV",
                krx.get_market_ohlcv,
                date_fmt,
                market=market,
                retries=2,
                retry_delay=0.5,
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
                    retries=2,
                    retry_delay=0.5,
                    validator=lambda frame: self._validate_ohlcv_frame(
                        frame,
                        required_columns=("종가",),
                    ),
                )
                time.sleep(0.5)

            sector_map = self._build_sector_map(date_fmt, market)
            time.sleep(0.5)

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

                    raw_sector = sector_map.get(ticker, "기타")
                    sector = self._map_sector(raw_sector)

                    rows.append(
                        {
                            "ticker": ticker,
                            "name": name,
                            "sector": sector,
                            "market_cap": market_cap,
                            "close_price": close_price,
                            "daily_return": daily_return,
                            "weekly_return": weekly_return,
                            "volume": volume,
                            "avg_volume_20d": None,
                        }
                    )
                except Exception as exc:
                    logger.debug(f"[KR] {ticker} skip: {exc}")
                    continue

            df = pd.DataFrame(rows)
            logger.info(f"[KR] {market}: {len(df)}개 종목")
            return df

        except Exception as exc:
            if self._is_transport_error(exc):
                self._pykrx_transport_failed = True

            try:
                fallback_df = self._fetch_market_with_fdr(
                    date_fmt,
                    market,
                    weekly_reference_date=weekly_reference_date,
                )
                if fallback_df is not None and not fallback_df.empty:
                    return fallback_df
            except Exception as fallback_exc:
                logger.error(
                    f"[KR] {market} FDR fallback failed: {fallback_exc}",
                    exc_info=True,
                )

            logger.error(f"[KR] {market} 수집 실패: {exc}", exc_info=True)
            return None

    def _build_sector_map(self, date_fmt: str, market: str) -> dict[str, str]:
        """Build ticker -> raw KRX sector name map from index constituents."""
        sector_map: dict[str, str] = {}
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
                if any(idx_name.startswith(prefix) for prefix in skip_prefixes):
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
                            sector_map.setdefault(stock_ticker, idx_name)
                    time.sleep(0.3)
                except Exception:
                    continue

        except Exception as exc:
            logger.warning(f"[KR] 업종 매핑 실패: {exc}")

        logger.info(f"[KR] {market} 업종 매핑: {len(sector_map)}개 종목")
        return sector_map
