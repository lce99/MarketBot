"""SQLite database helpers for summary and raw market data."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Optional

from src.config import DATA_DIR, DB_PATH, RAW_DB_PATH


def _connect(path) -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_connection() -> sqlite3.Connection:
    """Return the summary DB connection."""
    return _connect(DB_PATH)


def get_raw_connection() -> sqlite3.Connection:
    """Return the raw DB connection."""
    return _connect(RAW_DB_PATH)


def init_db() -> None:
    """Create the summary schema if it does not exist yet."""
    conn = get_connection()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sector_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            country TEXT NOT NULL,
            sector TEXT NOT NULL,
            daily_return REAL,
            weekly_return REAL,
            breadth REAL,
            volume_change REAL,
            stock_count INTEGER,
            top_gainers TEXT,
            top_losers TEXT,
            collected_at TEXT NOT NULL,
            UNIQUE(date, country, sector)
        );

        CREATE TABLE IF NOT EXISTS abnormal_stock_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            name TEXT,
            country TEXT NOT NULL,
            sector TEXT NOT NULL,
            market_cap REAL,
            close_price REAL,
            daily_return REAL,
            volume REAL,
            avg_volume_20d REAL,
            UNIQUE(date, ticker)
        );

        CREATE TABLE IF NOT EXISTS benchmark_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            name TEXT,
            country TEXT NOT NULL,
            sector TEXT,
            close_price REAL,
            daily_return REAL,
            weekly_return REAL,
            UNIQUE(date, ticker)
        );

        CREATE TABLE IF NOT EXISTS trend_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            sector TEXT NOT NULL,
            trend_score REAL,
            countries_positive INTEGER,
            countries_negative INTEGER,
            global_avg_return REAL,
            global_breadth REAL,
            momentum_signal TEXT,
            UNIQUE(date, sector)
        );

        CREATE TABLE IF NOT EXISTS collection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            market TEXT NOT NULL,
            status TEXT NOT NULL,
            total_stocks INTEGER,
            filtered_stocks INTEGER,
            abnormal_stocks INTEGER,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS instrument_universe (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT NOT NULL,
            ticker TEXT NOT NULL,
            name TEXT,
            sector TEXT,
            market_cap REAL,
            last_close_price REAL,
            last_volume REAL,
            avg_volume_20d REAL,
            last_seen_date TEXT NOT NULL,
            last_is_filtered INTEGER DEFAULT 0,
            last_is_abnormal INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL,
            UNIQUE(country, ticker)
        );

        CREATE INDEX IF NOT EXISTS idx_sector_perf_date
            ON sector_performance(date);
        CREATE INDEX IF NOT EXISTS idx_sector_perf_country
            ON sector_performance(country);
        CREATE INDEX IF NOT EXISTS idx_abnormal_summary_date
            ON abnormal_stock_summary(date);
        CREATE INDEX IF NOT EXISTS idx_abnormal_summary_country
            ON abnormal_stock_summary(country, date);
        CREATE INDEX IF NOT EXISTS idx_benchmark_daily_date
            ON benchmark_daily(date);
        CREATE INDEX IF NOT EXISTS idx_benchmark_daily_country
            ON benchmark_daily(country, date);
        CREATE INDEX IF NOT EXISTS idx_trend_date
            ON trend_scores(date);
        CREATE INDEX IF NOT EXISTS idx_universe_country
            ON instrument_universe(country, last_seen_date);
        """
    )
    conn.commit()
    conn.close()


def init_raw_db() -> None:
    """Create the raw schema if it does not exist yet."""
    conn = get_raw_connection()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS stock_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            name TEXT,
            country TEXT NOT NULL,
            sector TEXT NOT NULL,
            market_cap REAL,
            close_price REAL,
            daily_return REAL,
            volume REAL,
            avg_volume_20d REAL,
            is_filtered INTEGER DEFAULT 0,
            is_abnormal INTEGER DEFAULT 0,
            UNIQUE(date, ticker)
        );

        CREATE INDEX IF NOT EXISTS idx_stock_daily_date
            ON stock_daily(date);
        CREATE INDEX IF NOT EXISTS idx_stock_daily_country
            ON stock_daily(country, date);
        """
    )
    conn.commit()
    conn.close()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _build_abnormal_rows(stock_rows: list[dict]) -> list[dict]:
    abnormal_rows = []
    for row in stock_rows:
        if int(row.get("is_abnormal") or 0) != 1:
            continue
        abnormal_rows.append(
            {
                "date": row["date"],
                "ticker": row["ticker"],
                "name": row.get("name", ""),
                "country": row["country"],
                "sector": row.get("sector", "Other"),
                "market_cap": row.get("market_cap"),
                "close_price": row.get("close_price"),
                "daily_return": row.get("daily_return"),
                "volume": row.get("volume"),
                "avg_volume_20d": row.get("avg_volume_20d"),
            }
        )
    return abnormal_rows


def _vacuum_summary_db() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()


def checkpoint_db() -> dict:
    """Checkpoint both DBs and migrate any legacy raw data out of the summary DB."""
    init_db()
    init_raw_db()

    migrated_rows = 0
    backfilled_abnormal_rows = 0
    vacuumed_summary = False

    summary_conn = get_connection()
    raw_conn = get_raw_connection()
    try:
        if _table_exists(summary_conn, "stock_daily"):
            legacy_rows = [
                dict(row)
                for row in summary_conn.execute(
                    """
                    SELECT date, ticker, name, country, sector, market_cap,
                           close_price, daily_return, volume, avg_volume_20d,
                           is_filtered, is_abnormal
                    FROM stock_daily
                    """
                ).fetchall()
            ]

            if legacy_rows:
                upsert_stock_daily(raw_conn, legacy_rows)
                migrated_rows = len(legacy_rows)

                abnormal_rows = _build_abnormal_rows(legacy_rows)
                if abnormal_rows:
                    upsert_abnormal_stocks(summary_conn, abnormal_rows)
                    backfilled_abnormal_rows = len(abnormal_rows)

            summary_conn.execute("DROP TABLE stock_daily")
            summary_conn.commit()
            raw_conn.commit()
            vacuumed_summary = True

        summary_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        raw_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        summary_conn.close()
        raw_conn.close()

    if vacuumed_summary:
        _vacuum_summary_db()

    return {
        "summary_db": str(DB_PATH),
        "raw_db": str(RAW_DB_PATH),
        "migrated_rows": migrated_rows,
        "backfilled_abnormal_rows": backfilled_abnormal_rows,
        "vacuumed_summary": vacuumed_summary,
    }


def upsert_stock_daily(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Bulk-upsert raw stock rows."""
    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO stock_daily (
            date, ticker, name, country, sector, market_cap,
            close_price, daily_return, volume, avg_volume_20d,
            is_filtered, is_abnormal
        )
        VALUES (
            :date, :ticker, :name, :country, :sector, :market_cap,
            :close_price, :daily_return, :volume, :avg_volume_20d,
            :is_filtered, :is_abnormal
        )
        ON CONFLICT(date, ticker) DO UPDATE SET
            name = excluded.name,
            sector = excluded.sector,
            market_cap = excluded.market_cap,
            close_price = excluded.close_price,
            daily_return = excluded.daily_return,
            volume = excluded.volume,
            avg_volume_20d = excluded.avg_volume_20d,
            is_filtered = excluded.is_filtered,
            is_abnormal = excluded.is_abnormal
        """,
        rows,
    )


def upsert_sector_performance(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Bulk-upsert sector aggregates into the summary DB."""
    for row in rows:
        if isinstance(row.get("top_gainers"), list):
            row["top_gainers"] = json.dumps(row["top_gainers"], ensure_ascii=False)
        if isinstance(row.get("top_losers"), list):
            row["top_losers"] = json.dumps(row["top_losers"], ensure_ascii=False)

    conn.executemany(
        """
        INSERT INTO sector_performance (
            date, country, sector, daily_return, weekly_return,
            breadth, volume_change, stock_count,
            top_gainers, top_losers, collected_at
        )
        VALUES (
            :date, :country, :sector, :daily_return, :weekly_return,
            :breadth, :volume_change, :stock_count,
            :top_gainers, :top_losers, :collected_at
        )
        ON CONFLICT(date, country, sector) DO UPDATE SET
            daily_return = excluded.daily_return,
            weekly_return = excluded.weekly_return,
            breadth = excluded.breadth,
            volume_change = excluded.volume_change,
            stock_count = excluded.stock_count,
            top_gainers = excluded.top_gainers,
            top_losers = excluded.top_losers,
            collected_at = excluded.collected_at
        """,
        rows,
    )


def upsert_abnormal_stocks(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Bulk-upsert abnormal stock summaries into the summary DB."""
    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO abnormal_stock_summary (
            date, ticker, name, country, sector,
            market_cap, close_price, daily_return, volume, avg_volume_20d
        )
        VALUES (
            :date, :ticker, :name, :country, :sector,
            :market_cap, :close_price, :daily_return, :volume, :avg_volume_20d
        )
        ON CONFLICT(date, ticker) DO UPDATE SET
            name = excluded.name,
            country = excluded.country,
            sector = excluded.sector,
            market_cap = excluded.market_cap,
            close_price = excluded.close_price,
            daily_return = excluded.daily_return,
            volume = excluded.volume,
            avg_volume_20d = excluded.avg_volume_20d
        """,
        rows,
    )


def upsert_instrument_universe(
    conn: sqlite3.Connection,
    country: str,
    rows: list[dict],
) -> None:
    """Persist the latest instrument snapshot for future prefiltering."""
    if not rows:
        return

    updated_at = datetime.utcnow().isoformat()
    universe_rows = [
        {
            "country": country,
            "ticker": row["ticker"],
            "name": row.get("name", ""),
            "sector": row.get("sector"),
            "market_cap": row.get("market_cap"),
            "last_close_price": row.get("close_price"),
            "last_volume": row.get("volume"),
            "avg_volume_20d": row.get("avg_volume_20d"),
            "last_seen_date": row["date"],
            "last_is_filtered": int(row.get("is_filtered", 0)),
            "last_is_abnormal": int(row.get("is_abnormal", 0)),
            "updated_at": updated_at,
        }
        for row in rows
    ]

    conn.executemany(
        """
        INSERT INTO instrument_universe (
            country, ticker, name, sector, market_cap,
            last_close_price, last_volume, avg_volume_20d,
            last_seen_date, last_is_filtered, last_is_abnormal, updated_at
        )
        VALUES (
            :country, :ticker, :name, :sector, :market_cap,
            :last_close_price, :last_volume, :avg_volume_20d,
            :last_seen_date, :last_is_filtered, :last_is_abnormal, :updated_at
        )
        ON CONFLICT(country, ticker) DO UPDATE SET
            name = excluded.name,
            sector = excluded.sector,
            market_cap = excluded.market_cap,
            last_close_price = excluded.last_close_price,
            last_volume = excluded.last_volume,
            avg_volume_20d = excluded.avg_volume_20d,
            last_seen_date = excluded.last_seen_date,
            last_is_filtered = excluded.last_is_filtered,
            last_is_abnormal = excluded.last_is_abnormal,
            updated_at = excluded.updated_at
        """,
        universe_rows,
    )


def replace_abnormal_stocks(
    conn: sqlite3.Connection,
    date: str,
    country: str,
    stock_rows: list[dict],
) -> None:
    """Replace one market's abnormal snapshot for the given date."""
    conn.execute(
        "DELETE FROM abnormal_stock_summary WHERE date = ? AND country = ?",
        (date, country),
    )
    upsert_abnormal_stocks(conn, _build_abnormal_rows(stock_rows))


def upsert_benchmark_daily(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Bulk-upsert benchmark rows into the summary DB."""
    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO benchmark_daily (
            date, ticker, name, country, sector,
            close_price, daily_return, weekly_return
        )
        VALUES (
            :date, :ticker, :name, :country, :sector,
            :close_price, :daily_return, :weekly_return
        )
        ON CONFLICT(date, ticker) DO UPDATE SET
            name = excluded.name,
            close_price = excluded.close_price,
            daily_return = excluded.daily_return,
            weekly_return = excluded.weekly_return
        """,
        rows,
    )


def upsert_trend_scores(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Bulk-upsert trend score rows into the summary DB."""
    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO trend_scores (
            date, sector, trend_score, countries_positive,
            countries_negative, global_avg_return,
            global_breadth, momentum_signal
        )
        VALUES (
            :date, :sector, :trend_score, :countries_positive,
            :countries_negative, :global_avg_return,
            :global_breadth, :momentum_signal
        )
        ON CONFLICT(date, sector) DO UPDATE SET
            trend_score = excluded.trend_score,
            countries_positive = excluded.countries_positive,
            countries_negative = excluded.countries_negative,
            global_avg_return = excluded.global_avg_return,
            global_breadth = excluded.global_breadth,
            momentum_signal = excluded.momentum_signal
        """,
        rows,
    )


def log_collection(
    conn: sqlite3.Connection,
    market: str,
    status: str,
    total: int = 0,
    filtered: int = 0,
    abnormal: int = 0,
    error: Optional[str] = None,
) -> None:
    """Append one collection log entry."""
    conn.execute(
        """
        INSERT INTO collection_log (
            timestamp, market, status, total_stocks,
            filtered_stocks, abnormal_stocks, error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (datetime.utcnow().isoformat(), market, status, total, filtered, abnormal, error),
    )


def get_latest_sector_performance(
    conn: sqlite3.Connection,
    date: Optional[str] = None,
    country: Optional[str] = None,
) -> list[dict]:
    """Read sector performance rows from the summary DB."""
    query = "SELECT * FROM sector_performance WHERE 1=1"
    params: list[object] = []

    if date:
        query += " AND date = ?"
        params.append(date)
    else:
        query += " AND date = (SELECT MAX(date) FROM sector_performance)"

    if country:
        query += " AND country = ?"
        params.append(country)

    query += " ORDER BY country, daily_return DESC"
    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def get_latest_benchmarks(
    conn: sqlite3.Connection,
    date: Optional[str] = None,
    country: Optional[str] = None,
) -> list[dict]:
    """Read the latest benchmark snapshot up to the requested date."""
    if date is None:
        row = conn.execute("SELECT MAX(date) FROM benchmark_daily").fetchone()
        date = row[0] if row and row[0] else None
        if date is None:
            return []

    subquery = """
        SELECT ticker, MAX(date) AS latest_date
        FROM benchmark_daily
        WHERE date <= ?
    """
    params: list[object] = [date]
    if country:
        subquery += " AND country = ?"
        params.append(country)
    subquery += " GROUP BY ticker"

    query = f"""
        SELECT b.*
        FROM benchmark_daily b
        JOIN (
            {subquery}
        ) latest
          ON latest.ticker = b.ticker
         AND latest.latest_date = b.date
        ORDER BY b.country,
                 CASE WHEN b.sector IS NULL THEN 0 ELSE 1 END,
                 b.sector,
                 b.name
    """
    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def _get_abnormal_from_summary(
    conn: sqlite3.Connection,
    date: Optional[str] = None,
) -> list[dict]:
    query = "SELECT * FROM abnormal_stock_summary WHERE 1=1"
    params: list[object] = []

    if date:
        query += " AND date = ?"
        params.append(date)
    else:
        query += " AND date = (SELECT MAX(date) FROM abnormal_stock_summary)"

    query += " ORDER BY ABS(daily_return) DESC"
    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def _get_abnormal_from_legacy_stock_daily(
    conn: sqlite3.Connection,
    date: Optional[str] = None,
) -> list[dict]:
    query = "SELECT * FROM stock_daily WHERE is_abnormal = 1"
    params: list[object] = []

    if date:
        query += " AND date = ?"
        params.append(date)
    else:
        query += " AND date = (SELECT MAX(date) FROM stock_daily WHERE is_abnormal = 1)"

    query += " ORDER BY ABS(daily_return) DESC"
    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def get_abnormal_stocks(
    conn: sqlite3.Connection,
    date: Optional[str] = None,
) -> list[dict]:
    """Read abnormal stock summaries, with fallback for legacy DBs."""
    if _table_exists(conn, "abnormal_stock_summary"):
        rows = _get_abnormal_from_summary(conn, date=date)
        if rows or not _table_exists(conn, "stock_daily"):
            return rows

    if _table_exists(conn, "stock_daily"):
        return _get_abnormal_from_legacy_stock_daily(conn, date=date)

    return []


def get_latest_collection_log(
    conn: sqlite3.Connection,
    market: str,
    status: Optional[str] = None,
) -> Optional[dict]:
    """Return the latest collection log for one market."""
    query = "SELECT * FROM collection_log WHERE market = ?"
    params: list[object] = [market]
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY timestamp DESC, id DESC LIMIT 1"
    row = conn.execute(query, params).fetchone()
    return dict(row) if row else None


def get_instrument_universe(
    conn: sqlite3.Connection,
    country: str,
) -> list[dict]:
    """Return the cached instrument universe snapshot for one market."""
    rows = conn.execute(
        """
        SELECT *
        FROM instrument_universe
        WHERE country = ?
        ORDER BY last_seen_date DESC, last_volume DESC
        """,
        (country,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_recent_collection_logs(
    conn: sqlite3.Connection,
    limit: int = 10,
    status: Optional[str] = None,
) -> list[dict]:
    """Return recent collection logs, optionally filtered by status."""
    query = "SELECT * FROM collection_log"
    params: list[object] = []
    if status:
        query += " WHERE status = ?"
        params.append(status)
    query += " ORDER BY timestamp DESC, id DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def get_latest_sector_dates_by_country(conn: sqlite3.Connection) -> dict[str, str]:
    """Return the latest sector date recorded for each market."""
    rows = conn.execute(
        """
        SELECT country, MAX(date) AS latest_date
        FROM sector_performance
        GROUP BY country
        """
    ).fetchall()
    return {
        row["country"]: row["latest_date"]
        for row in rows
        if row["latest_date"]
    }
