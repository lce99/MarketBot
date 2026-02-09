"""SQLite 데이터베이스 관리 - 스키마 생성, 데이터 저장/조회"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.config import DB_PATH, DATA_DIR


def get_connection() -> sqlite3.Connection:
    """DB 커넥션 반환. data 디렉토리 없으면 생성."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """스키마 생성 (테이블이 없을 때만)"""
    conn = get_connection()
    conn.executescript("""
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

        CREATE INDEX IF NOT EXISTS idx_sector_perf_date ON sector_performance(date);
        CREATE INDEX IF NOT EXISTS idx_sector_perf_country ON sector_performance(country);
        CREATE INDEX IF NOT EXISTS idx_stock_daily_date ON stock_daily(date);
        CREATE INDEX IF NOT EXISTS idx_stock_daily_country ON stock_daily(country, date);
        CREATE INDEX IF NOT EXISTS idx_trend_date ON trend_scores(date);
    """)
    conn.commit()
    conn.close()


def upsert_stock_daily(conn: sqlite3.Connection, rows: list[dict]):
    """개별 종목 일간 데이터 UPSERT (bulk)"""
    conn.executemany("""
        INSERT INTO stock_daily (date, ticker, name, country, sector, market_cap,
                                  close_price, daily_return, volume, avg_volume_20d,
                                  is_filtered, is_abnormal)
        VALUES (:date, :ticker, :name, :country, :sector, :market_cap,
                :close_price, :daily_return, :volume, :avg_volume_20d,
                :is_filtered, :is_abnormal)
        ON CONFLICT(date, ticker) DO UPDATE SET
            name=excluded.name, sector=excluded.sector, market_cap=excluded.market_cap,
            close_price=excluded.close_price, daily_return=excluded.daily_return,
            volume=excluded.volume, avg_volume_20d=excluded.avg_volume_20d,
            is_filtered=excluded.is_filtered, is_abnormal=excluded.is_abnormal
    """, rows)


def upsert_sector_performance(conn: sqlite3.Connection, rows: list[dict]):
    """섹터 성과 집계 UPSERT"""
    for row in rows:
        if isinstance(row.get("top_gainers"), list):
            row["top_gainers"] = json.dumps(row["top_gainers"], ensure_ascii=False)
        if isinstance(row.get("top_losers"), list):
            row["top_losers"] = json.dumps(row["top_losers"], ensure_ascii=False)
    conn.executemany("""
        INSERT INTO sector_performance (date, country, sector, daily_return, weekly_return,
                                         breadth, volume_change, stock_count,
                                         top_gainers, top_losers, collected_at)
        VALUES (:date, :country, :sector, :daily_return, :weekly_return,
                :breadth, :volume_change, :stock_count,
                :top_gainers, :top_losers, :collected_at)
        ON CONFLICT(date, country, sector) DO UPDATE SET
            daily_return=excluded.daily_return, weekly_return=excluded.weekly_return,
            breadth=excluded.breadth, volume_change=excluded.volume_change,
            stock_count=excluded.stock_count, top_gainers=excluded.top_gainers,
            top_losers=excluded.top_losers, collected_at=excluded.collected_at
    """, rows)


def upsert_benchmark_daily(conn: sqlite3.Connection, rows: list[dict]):
    """벤치마크 UPSERT"""
    conn.executemany("""
        INSERT INTO benchmark_daily (date, ticker, name, country, sector,
                                      close_price, daily_return, weekly_return)
        VALUES (:date, :ticker, :name, :country, :sector,
                :close_price, :daily_return, :weekly_return)
        ON CONFLICT(date, ticker) DO UPDATE SET
            name=excluded.name, close_price=excluded.close_price,
            daily_return=excluded.daily_return, weekly_return=excluded.weekly_return
    """, rows)


def upsert_trend_scores(conn: sqlite3.Connection, rows: list[dict]):
    """트렌드 스코어 UPSERT"""
    conn.executemany("""
        INSERT INTO trend_scores (date, sector, trend_score, countries_positive,
                                   countries_negative, global_avg_return,
                                   global_breadth, momentum_signal)
        VALUES (:date, :sector, :trend_score, :countries_positive,
                :countries_negative, :global_avg_return,
                :global_breadth, :momentum_signal)
        ON CONFLICT(date, sector) DO UPDATE SET
            trend_score=excluded.trend_score,
            countries_positive=excluded.countries_positive,
            countries_negative=excluded.countries_negative,
            global_avg_return=excluded.global_avg_return,
            global_breadth=excluded.global_breadth,
            momentum_signal=excluded.momentum_signal
    """, rows)


def log_collection(conn: sqlite3.Connection, market: str, status: str,
                   total: int = 0, filtered: int = 0, abnormal: int = 0,
                   error: Optional[str] = None):
    """수집 로그 기록"""
    conn.execute("""
        INSERT INTO collection_log (timestamp, market, status, total_stocks,
                                     filtered_stocks, abnormal_stocks, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (datetime.utcnow().isoformat(), market, status, total, filtered, abnormal, error))


def get_latest_sector_performance(conn: sqlite3.Connection,
                                   date: Optional[str] = None,
                                   country: Optional[str] = None) -> list[dict]:
    """최신 섹터 성과 조회"""
    query = "SELECT * FROM sector_performance WHERE 1=1"
    params = []
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
    return [dict(r) for r in rows]


def get_abnormal_stocks(conn: sqlite3.Connection,
                        date: Optional[str] = None) -> list[dict]:
    """비정상 급등/급락 종목 조회"""
    query = "SELECT * FROM stock_daily WHERE is_abnormal = 1"
    params = []
    if date:
        query += " AND date = ?"
        params.append(date)
    else:
        query += " AND date = (SELECT MAX(date) FROM stock_daily WHERE is_abnormal = 1)"
    query += " ORDER BY ABS(daily_return) DESC"
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]
