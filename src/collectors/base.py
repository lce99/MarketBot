"""Base collector shared by all market-specific collectors."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd

from src.config import COUNTRIES
from src.database import (
    get_connection,
    get_raw_connection,
    init_db,
    init_raw_db,
    log_collection,
    replace_abnormal_stocks,
    upsert_instrument_universe,
    upsert_sector_performance,
    upsert_stock_daily,
)
from src.filter import apply_filters

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base class for market data collectors."""

    country_code: str

    @abstractmethod
    def fetch_all_stocks(self, date: str) -> pd.DataFrame:
        """Fetch all stocks for the market and return them as a DataFrame."""
        raise NotImplementedError

    def run(self, date: str | None = None) -> bool:
        """Run the end-to-end collection pipeline for one market."""
        if date is None:
            date = datetime.utcnow().strftime("%Y-%m-%d")
        self.effective_date = date

        country = self.country_code
        info = COUNTRIES[country]
        logger.info(f"[{info['flag']} {info['name_kr']}] collection started: {date}")

        init_db()
        init_raw_db()
        summary_conn = get_connection()
        raw_conn = get_raw_connection()

        try:
            df = self.fetch_all_stocks(date)
            if df.empty:
                log_collection(summary_conn, country, "failed", error="데이터 없음")
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
            )
            raw_conn.commit()
            summary_conn.commit()
            logger.info(
                f"[{country}] saved {len(sector_rows)} sectors and "
                f"{len(stock_rows)} stocks"
            )
            return True

        except Exception as exc:
            log_collection(summary_conn, country, "failed", error=str(exc))
            summary_conn.commit()
            logger.error(f"[{country}] collection failed: {exc}", exc_info=True)
            raise
        finally:
            summary_conn.close()
            raw_conn.close()

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
