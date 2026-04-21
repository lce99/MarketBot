"""베트남 시장 수집기 - vnstock 기반 HOSE/HNX 전종목 수집

vnstock은 무료, 무제한.
"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd

from src.collectors.base import BaseCollector
from src.collectors.date_utils import compute_period_return_from_closes
from src.config import (
    VN_INCREMENTAL_ABNORMAL_LOOKBACK_DAYS,
    VN_INCREMENTAL_FULL_REFRESH_WEEKDAY,
    VN_INCREMENTAL_LARGE_CAP_COUNT,
    VN_INCREMENTAL_MIN_CANDIDATES,
    VN_INCREMENTAL_STALE_AFTER_DAYS,
)
from src.database import (
    get_connection,
    get_instrument_universe,
    get_recent_abnormal_tickers,
)

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

    def _load_listing_from_cached_universe(self) -> pd.DataFrame:
        """Fall back to the last cached universe when vnstock listing APIs fail."""
        try:
            conn = get_connection()
            try:
                rows = get_instrument_universe(conn, self.country_code)
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(f"[VN] cached listing unavailable: {exc}")
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()

        listing = pd.DataFrame(
            [
                {
                    "ticker": row.get("ticker"),
                    "name": row.get("name") or row.get("ticker"),
                    "sector": row.get("sector"),
                    "market_cap": row.get("market_cap"),
                }
                for row in rows
                if row.get("ticker")
            ]
        )
        if listing.empty:
            return pd.DataFrame()

        listing = listing.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
        logger.warning(
            f"[VN] vnstock listing unavailable, using cached universe: {len(listing)} tickers"
        )
        return listing

    def _load_listing(self) -> pd.DataFrame:
        """Load the Vietnam listing table across supported vnstock versions."""
        errors: list[str] = []

        try:
            from vnstock import Listing

            for source in ("KBS", "VCI"):
                try:
                    listing_client = Listing(source=source)
                    base_listing = listing_client.all_symbols()
                    industries = listing_client.symbols_by_industries()
                    listing = self._merge_listing_frames(base_listing, industries)
                    if listing is not None and not listing.empty:
                        return listing
                except Exception as exc:
                    errors.append(f"Listing API ({source}): {exc}")
        except Exception as exc:
            errors.append(f"Listing import: {exc}")

        try:
            from vnstock import Quote

            for source in ("KBS", "VCI"):
                try:
                    listing_client = Quote(symbol="VCI", source=source)
                    base_listing = listing_client.listing.all_symbols()
                    industries = listing_client.listing.symbols_by_industries()
                    listing = self._merge_listing_frames(base_listing, industries)
                    if listing is not None and not listing.empty:
                        return listing
                except Exception as exc:
                    errors.append(f"Quote.listing API ({source}): {exc}")
        except Exception as exc:
            errors.append(f"Quote import: {exc}")

        try:
            from vnstock import Vnstock

            stock = Vnstock()
            legacy_listing = stock.stock().listing.all_symbols()
            listing = self._normalize_listing_frame(legacy_listing)
            if listing is not None and not listing.empty:
                return listing
        except Exception as exc:
            errors.append(f"Legacy stock.listing API: {exc}")

        cached_listing = self._load_listing_from_cached_universe()
        if not cached_listing.empty:
            return cached_listing

        joined_errors = "; ".join(errors) if errors else "unknown error"
        raise RuntimeError(f"베트남 종목 리스트 조회 실패: {joined_errors}")

    def _load_history(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Load quote history across supported vnstock interfaces and sources."""
        errors: list[str] = []

        try:
            from vnstock import Quote

            for source in ("KBS", "VCI"):
                try:
                    quote = Quote(symbol=ticker, source=source)
                    history = quote.history(
                        start=start_date,
                        end=end_date,
                        interval="1D",
                    )
                    if history is not None:
                        return history
                except Exception as exc:
                    errors.append(f"Quote API ({source}): {exc}")
        except Exception as exc:
            errors.append(f"Quote import: {exc}")

        try:
            from vnstock import Vnstock

            stock = Vnstock()
            for source in ("KBS", "VCI"):
                try:
                    history = stock.stock(symbol=ticker, source=source).quote.history(
                        start=start_date,
                        end=end_date,
                    )
                    if history is not None:
                        return history
                except Exception as exc:
                    errors.append(f"Legacy quote API ({source}): {exc}")
        except Exception as exc:
            errors.append(f"Legacy Vnstock import: {exc}")

        joined_errors = "; ".join(errors) if errors else "unknown error"
        raise RuntimeError(f"{ticker} 가격 이력 조회 실패: {joined_errors}")

    def _normalize_listing_frame(self, listing: pd.DataFrame) -> pd.DataFrame:
        """Normalize vnstock listing tables to the columns used by the collector."""
        if listing is None or listing.empty:
            return pd.DataFrame()

        listing = listing.copy()
        if "ticker" not in listing.columns and "symbol" in listing.columns:
            listing["ticker"] = listing["symbol"]
        if "name" not in listing.columns:
            for column in (
                "organ_name",
                "organ_short_name",
                "company_name",
                "short_name",
            ):
                if column in listing.columns:
                    listing["name"] = listing[column]
                    break
        if "industry" not in listing.columns:
            industry = None
            for column in ("industry_name", "icb_name3", "icb_name2", "sector"):
                if column in listing.columns:
                    industry = listing[column]
                    break
            if industry is not None:
                listing["industry"] = industry

        return listing

    def _merge_listing_frames(
        self,
        base_listing: pd.DataFrame,
        industries: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge vnstock listing/name data with industry classifications."""
        base_listing = self._normalize_listing_frame(base_listing)
        industries = self._normalize_listing_frame(industries)

        if base_listing.empty and industries.empty:
            return pd.DataFrame()
        if base_listing.empty:
            return industries
        if industries.empty:
            return base_listing

        merged = base_listing.merge(
            industries.drop_duplicates(subset=["ticker"]),
            on="ticker",
            how="left",
            suffixes=("", "_industry"),
        )

        if "name" not in merged.columns or merged["name"].isna().all():
            fallback_name = merged.get("name_industry")
            if fallback_name is not None:
                merged["name"] = fallback_name
        else:
            fallback_name = merged.get("name_industry")
            if fallback_name is not None:
                merged["name"] = merged["name"].fillna(fallback_name)

        if "industry" not in merged.columns or merged["industry"].isna().all():
            fallback_industry = merged.get("industry_industry")
            if fallback_industry is not None:
                merged["industry"] = fallback_industry
        else:
            fallback_industry = merged.get("industry_industry")
            if fallback_industry is not None:
                merged["industry"] = merged["industry"].fillna(fallback_industry)

        return merged

    def _select_listing_candidates(
        self,
        listing: pd.DataFrame,
        date: str,
    ) -> pd.DataFrame:
        """Use the latest active universe on weekdays and full rebuild weekly."""
        ticker_col = "ticker" if "ticker" in listing.columns else "symbol"
        if ticker_col not in listing.columns:
            return listing

        try:
            requested_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            return listing

        refresh_weekday = VN_INCREMENTAL_FULL_REFRESH_WEEKDAY.get(self.country_code)
        if (
            refresh_weekday is not None
            and requested_date.weekday() == refresh_weekday
        ):
            logger.info("[VN] weekly full rebuild day, skipping incremental filter")
            return listing

        try:
            conn = get_connection()
            try:
                universe_rows = get_instrument_universe(conn, self.country_code)
                abnormal_tickers = set(
                    get_recent_abnormal_tickers(
                        conn,
                        self.country_code,
                        date,
                        lookback_days=VN_INCREMENTAL_ABNORMAL_LOOKBACK_DAYS,
                    )
                )
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(f"[VN] incremental universe unavailable, use full rebuild: {exc}")
            return listing

        if not universe_rows:
            logger.info("[VN] cached universe missing, use full rebuild")
            return listing

        latest_seen_date = max(
            (row.get("last_seen_date") for row in universe_rows if row.get("last_seen_date")),
            default=None,
        )
        if latest_seen_date:
            try:
                stale_days = (
                    requested_date - datetime.strptime(latest_seen_date, "%Y-%m-%d")
                ).days
            except ValueError:
                stale_days = 0
            if stale_days > VN_INCREMENTAL_STALE_AFTER_DAYS:
                logger.info("[VN] cached universe is stale, use full rebuild")
                return listing

        active_tickers = {
            row["ticker"]
            for row in universe_rows
            if row.get("ticker")
            and int(row.get("last_is_filtered") or 0) == 0
            and int(row.get("last_is_abnormal") or 0) == 0
        }
        large_cap_tickers = {
            row["ticker"]
            for row in sorted(
                universe_rows,
                key=lambda row: float(row.get("market_cap") or 0.0),
                reverse=True,
            )[:VN_INCREMENTAL_LARGE_CAP_COUNT]
            if row.get("ticker")
        }
        candidate_tickers = active_tickers | abnormal_tickers | large_cap_tickers
        if not candidate_tickers:
            logger.info("[VN] incremental universe empty, use full rebuild")
            return listing

        filtered_listing = listing[listing[ticker_col].isin(candidate_tickers)].copy()
        min_candidates = min(len(listing), VN_INCREMENTAL_MIN_CANDIDATES)
        if len(filtered_listing) < min_candidates:
            logger.info(
                f"[VN] incremental universe too small ({len(filtered_listing)}), use full rebuild"
            )
            return listing

        logger.info(
            f"[VN] incremental universe {len(listing)} -> {len(filtered_listing)} "
            f"(active={len(active_tickers)}, abnormal={len(abnormal_tickers)}, "
            f"large_cap={len(large_cap_tickers)})"
        )
        return filtered_listing

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
            listing = self._load_listing()
            if listing is None or listing.empty:
                logger.warning("[VN] 종목 리스트 없음")
                return pd.DataFrame()

            logger.info(f"[VN] 전체 종목: {len(listing)}개")
            listing = self._select_listing_candidates(listing, date)
            logger.info(f"[VN] 수집 대상: {len(listing)}개")

            # 2) 각 종목의 일간 데이터 수집
            rows = []
            used_dates: list[str] = []
            for _, info in listing.iterrows():
                ticker = info.get("ticker") or info.get("symbol") or ""
                if not ticker:
                    continue

                try:
                    # vnstock으로 최근 며칠 데이터를 가져와 최신 거래일 봉을 선택
                    hist = self._load_history(ticker, start_date, end_date)
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
                    cached_sector = info.get("sector")
                    if cached_sector:
                        sector = cached_sector
                    else:
                        industry = info.get("industry", "") or ""
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
