"""Base collector shared by all market-specific collectors."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd

from src.collection_failures import CollectionFailure, summarize_raw_error
from src.config import (
    COUNTRIES,
    INSTRUMENT_METADATA_REFRESH_WEEKDAY,
    INSTRUMENT_METADATA_STALE_AFTER_DAYS,
)
from src.database import (
    get_connection,
    get_instrument_metadata,
    get_raw_connection,
    init_db,
    init_raw_db,
    log_collection,
    replace_abnormal_stocks,
    upsert_instrument_metadata,
    upsert_instrument_universe,
    upsert_sector_performance,
    upsert_stock_daily,
)
from src.filter import apply_filters

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base class for market data collectors."""

    country_code: str
    metadata_source: str = ""

    @abstractmethod
    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """Fetch all stocks for the market and return them as a DataFrame."""
        raise NotImplementedError

    def preflight(self, date: str) -> None:
        """Validate prerequisites before the collector hits external providers."""
        return None

    def get_run_mode(self) -> str:
        """Return the current collection mode for logging."""
        return getattr(self, "run_mode", "standard")

    def get_provider_name(self) -> str:
        """Return the provider label used in collection_log."""
        return COUNTRIES[self.country_code].get("collector", self.country_code.lower())

    def _to_collection_failure(
        self,
        exc: BaseException,
        *,
        default_stage: str,
    ) -> CollectionFailure:
        if isinstance(exc, CollectionFailure):
            return CollectionFailure(
                message=str(exc),
                failure_code=exc.failure_code,
                failure_stage=exc.failure_stage or default_stage,
                provider=exc.provider or self.get_provider_name(),
                raw_error_excerpt=exc.raw_error_excerpt or str(exc),
                run_mode=exc.run_mode or self.get_run_mode(),
            )

        message = str(exc).strip() or exc.__class__.__name__
        if isinstance(exc, SystemExit) and message in {"", "0", "1"}:
            message = "공급자 프로세스가 비정상 종료되었습니다."

        return CollectionFailure(
            message=message,
            failure_code="unexpected_exception",
            failure_stage=default_stage,
            provider=self.get_provider_name(),
            raw_error_excerpt=summarize_raw_error(str(exc)),
            run_mode=self.get_run_mode(),
        )

    def _log_failure(
        self,
        summary_conn,
        failure: CollectionFailure,
    ) -> None:
        log_collection(
            summary_conn,
            self.country_code,
            "failed",
            error=str(failure),
            failure_code=failure.failure_code,
            failure_stage=failure.failure_stage,
            run_mode=failure.run_mode or self.get_run_mode(),
            provider=failure.provider or self.get_provider_name(),
            raw_error_excerpt=failure.raw_error_excerpt,
        )

    def run_preflight(self, date: str | None = None) -> bool:
        """Run only the validation stage and persist structured failures."""
        if date is None:
            date = datetime.utcnow().strftime("%Y-%m-%d")
        self.effective_date = date

        country = self.country_code
        info = COUNTRIES[country]
        logger.info(f"[{info['flag']} {info['name_kr']}] preflight started: {date}")

        init_db()
        summary_conn = get_connection()
        try:
            self.preflight(date)
            logger.info(f"[{country}] preflight passed")
            return True
        except SystemExit as exc:
            failure = self._to_collection_failure(exc, default_stage="preflight")
            self._log_failure(summary_conn, failure)
            summary_conn.commit()
            logger.error(f"[{country}] preflight failed: {failure}", exc_info=True)
            raise failure
        except Exception as exc:
            failure = self._to_collection_failure(exc, default_stage="preflight")
            self._log_failure(summary_conn, failure)
            summary_conn.commit()
            logger.error(f"[{country}] preflight failed: {failure}", exc_info=True)
            raise failure
        finally:
            summary_conn.close()

    def run(self, date: str | None = None) -> bool:
        """Run the end-to-end collection pipeline for one market."""
        if date is None:
            date = datetime.utcnow().strftime("%Y-%m-%d")
        self.effective_date = date

        country = self.country_code
        info = COUNTRIES[country]
        logger.info(f"[{info['flag']} {info['name_kr']}] collection started: {date}")

        init_db()
        summary_conn = get_connection()
        raw_conn = None

        try:
            self.preflight(date)

            init_raw_db()
            raw_conn = get_raw_connection()
            df = self.fetch_all_stocks(date)
            if df.empty:
                failure = CollectionFailure(
                    message="데이터 없음",
                    failure_code="no_data",
                    failure_stage="fetch_all_stocks",
                    provider=self.get_provider_name(),
                    run_mode=self.get_run_mode(),
                    raw_error_excerpt="collector returned an empty dataframe",
                )
                self._log_failure(summary_conn, failure)
                summary_conn.commit()
                logger.warning(f"[{country}] no data returned")
                return False

            effective_date = getattr(self, "effective_date", date)
            if effective_date != date:
                logger.warning(
                    f"[{country}] requested {date}, using trading date {effective_date}"
                )

            total = len(df)
            logger.info(f"[{country}] fetched {total} stocks")

            df = apply_filters(df, country)
            filtered_count = int(df["is_filtered"].sum())
            abnormal_count = int(df["is_abnormal"].sum())
            logger.info(
                f"[{country}] filtered {filtered_count} stocks, "
                f"abnormal {abnormal_count} stocks"
            )

            stock_rows = []
            for _, row in df.iterrows():
                stock_rows.append(
                    {
                        "date": effective_date,
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
                    }
                )

            upsert_stock_daily(raw_conn, stock_rows)
            replace_abnormal_stocks(summary_conn, effective_date, country, stock_rows)
            upsert_instrument_universe(summary_conn, country, stock_rows)

            active = df[(df["is_filtered"] == 0) & (df["is_abnormal"] == 0)]
            sector_rows = self._aggregate_sectors(active, effective_date, country)
            upsert_sector_performance(summary_conn, sector_rows)

            log_collection(
                summary_conn,
                country,
                "success",
                total=total,
                filtered=filtered_count,
                abnormal=abnormal_count,
                run_mode=self.get_run_mode(),
                provider=self.get_provider_name(),
            )
            raw_conn.commit()
            summary_conn.commit()
            logger.info(
                f"[{country}] saved {len(sector_rows)} sectors and "
                f"{len(stock_rows)} stocks"
            )
            return True

        except SystemExit as exc:
            failure = self._to_collection_failure(exc, default_stage="run")
            self._log_failure(summary_conn, failure)
            summary_conn.commit()
            logger.error(f"[{country}] collection failed: {failure}", exc_info=True)
            raise failure
        except Exception as exc:
            failure = self._to_collection_failure(exc, default_stage="run")
            self._log_failure(summary_conn, failure)
            summary_conn.commit()
            logger.error(f"[{country}] collection failed: {failure}", exc_info=True)
            raise failure
        finally:
            summary_conn.close()
            if raw_conn is not None:
                raw_conn.close()

    def _is_metadata_refresh_due(self, date: str) -> bool:
        """Return True when the weekly metadata refresh should run."""
        weekday = INSTRUMENT_METADATA_REFRESH_WEEKDAY.get(self.country_code)
        if weekday is None:
            return False

        try:
            requested_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            return False

        return requested_date.weekday() == weekday

    def _metadata_row_is_fresh(self, row: dict, date: str) -> bool:
        """Check whether cached metadata is still fresh enough to reuse."""
        refreshed_at = row.get("last_refreshed_at")
        if not refreshed_at:
            return False

        try:
            requested_date = datetime.strptime(date, "%Y-%m-%d")
            refreshed = datetime.fromisoformat(refreshed_at)
        except ValueError:
            return False

        age_days = (requested_date.date() - refreshed.date()).days
        return age_days <= INSTRUMENT_METADATA_STALE_AFTER_DAYS

    def _get_cached_metadata(self, tickers: list[str]) -> dict[str, dict]:
        """Load cached instrument metadata for the current market."""
        if not tickers:
            return {}

        conn = get_connection()
        try:
            rows = get_instrument_metadata(conn, self.country_code, tickers=tickers)
        finally:
            conn.close()

        return {row["ticker"]: row for row in rows}

    def _upsert_metadata(self, rows: list[dict], source: str | None = None) -> None:
        """Persist refreshed metadata rows for later reuse."""
        if not rows:
            return

        conn = get_connection()
        try:
            upsert_instrument_metadata(
                conn,
                self.country_code,
                rows,
                source=source or self.metadata_source or self.country_code.lower(),
            )
            conn.commit()
        finally:
            conn.close()

    def _aggregate_sectors(
        self,
        df: pd.DataFrame,
        date: str,
        country: str,
    ) -> list[dict]:
        """Aggregate filtered stocks into sector-level rows."""
        now = datetime.utcnow().isoformat()
        results = []

        for sector, group in df.groupby("sector"):
            if len(group) == 0:
                continue

            daily_returns = group["daily_return"].dropna()
            avg_return = float(daily_returns.mean()) if len(daily_returns) > 0 else 0.0
            breadth = (
                float((daily_returns > 0).sum() / len(daily_returns))
                if len(daily_returns) > 0
                else 0.0
            )

            weekly_returns = (
                group["weekly_return"].dropna()
                if "weekly_return" in group.columns
                else pd.Series(dtype=float)
            )
            avg_weekly_return = (
                float(weekly_returns.mean()) if len(weekly_returns) > 0 else None
            )

            vol_change = 0.0
            if "volume" in group.columns and "avg_volume_20d" in group.columns:
                valid = group.dropna(subset=["volume", "avg_volume_20d"])
                valid = valid[valid["avg_volume_20d"] > 0]
                if len(valid) > 0:
                    ratio = valid["volume"] / valid["avg_volume_20d"]
                    vol_change = float((ratio.mean() - 1) * 100)

            sorted_group = group.dropna(subset=["daily_return"]).sort_values(
                "daily_return", ascending=False
            )
            top_gainers = [
                {"name": row["name"], "return": round(row["daily_return"], 2)}
                for _, row in sorted_group.head(3).iterrows()
            ]
            top_losers = [
                {"name": row["name"], "return": round(row["daily_return"], 2)}
                for _, row in sorted_group.tail(3).iterrows()
            ]

            results.append(
                {
                    "date": date,
                    "country": country,
                    "sector": sector,
                    "daily_return": round(avg_return, 4),
                    "weekly_return": (
                        round(avg_weekly_return, 4)
                        if avg_weekly_return is not None
                        else None
                    ),
                    "breadth": round(breadth, 4),
                    "volume_change": round(vol_change, 2),
                    "stock_count": len(group),
                    "top_gainers": top_gainers,
                    "top_losers": top_losers,
                    "collected_at": now,
                }
            )

        return results
