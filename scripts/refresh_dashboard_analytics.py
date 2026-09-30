"""Refresh local dashboard analytics without importing report or messaging code."""
from __future__ import annotations

import argparse
import json
from datetime import date as calendar_date, datetime, timezone
from pathlib import Path

from src import database
from src.analyzer import compute_trend_scores
from src.leadlag import update_lead_lag
from src.markets import ACTIVE_MARKETS


def correct_provenance(conn, corrections: list[dict]) -> int:
    """Correct only explicitly verified log rows, preserving unrelated history."""
    changed = 0
    for item in corrections:
        if item["market"] not in {"JP", "IN", "DE"} or item["provider"] != "yfinance":
            raise ValueError("Unsupported provenance correction")
        rows = conn.execute("SELECT id,provider FROM collection_log WHERE market=? AND timestamp=? AND status='success'",
                            (item["market"], item["timestamp"])).fetchall()
        if len(rows) != 1:
            raise ValueError(f"Verified log row not found uniquely for {item['market']}")
        row = rows[0]
        if row["provider"] == item["provider"]:
            continue
        if row["provider"] != item["expectedProvider"]:
            raise ValueError(f"Provider changed unexpectedly for {item['market']}")
        conn.execute("UPDATE collection_log SET provider=? WHERE id=?", (item["provider"], row["id"]))
        changed += 1
    return changed


def refresh(date: str | None = None, corrections: list[dict] | None = None) -> dict:
    database.init_db()
    conn = database.get_connection()
    try:
        placeholders = ','.join('?' for _ in ACTIVE_MARKETS)
        latest = conn.execute(f"SELECT MAX(date) FROM sector_performance WHERE country IN ({placeholders})",
                              tuple(ACTIVE_MARKETS)).fetchone()[0]
        if latest is None:
            raise ValueError("No active market observations available")
        as_of = date or latest
        calendar_date.fromisoformat(as_of)
        if as_of > latest:
            raise ValueError("Analysis date is newer than available observations")
        inputs = dict(conn.execute(f"SELECT country,MAX(date) FROM sector_performance WHERE country IN ({placeholders}) AND date<=? GROUP BY country",
                                  (*ACTIVE_MARKETS, as_of)).fetchall())
        trend_markets = [r[0] for r in conn.execute(f"SELECT DISTINCT country FROM sector_performance WHERE country IN ({placeholders}) AND date=? AND sector!='기타' AND daily_return IS NOT NULL ORDER BY country",
                                                   (*ACTIVE_MARKETS, as_of))]
        if not trend_markets:
            raise ValueError("No trend observations available for analysis date")
        corrected = correct_provenance(conn, corrections or [])
        conn.commit()
    finally:
        conn.close()

    trends = compute_trend_scores(date=as_of)
    leadlag = update_lead_lag(date=as_of)
    result = dict(timestamp=datetime.now(timezone.utc).isoformat(), asOfDate=as_of,
                  inputDates={code: inputs.get(code) for code in ACTIVE_MARKETS}, trendInputMarkets=trend_markets,
                  trendSectors=len(trends), pairsScored=leadlag["pairs_scored"], signalsCreated=leadlag["signals_created"],
                  outcomesVerified=leadlag["outcomes"]["verified"], outcomesExpired=leadlag["outcomes"]["expired"],
                  provenanceCorrections=corrected)
    conn = database.get_connection()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS dashboard_analytics_refresh (
            id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, as_of_date TEXT NOT NULL,
            input_dates_json TEXT NOT NULL, trend_input_markets_json TEXT NOT NULL,
            trend_sectors INTEGER NOT NULL, pairs_scored INTEGER NOT NULL, signals_created INTEGER NOT NULL,
            outcomes_verified INTEGER NOT NULL, outcomes_expired INTEGER NOT NULL)""")
        conn.execute("""INSERT INTO dashboard_analytics_refresh
            (timestamp,as_of_date,input_dates_json,trend_input_markets_json,trend_sectors,pairs_scored,
             signals_created,outcomes_verified,outcomes_expired) VALUES (?,?,?,?,?,?,?,?,?)""",
            (result["timestamp"],as_of,json.dumps(result["inputDates"]),json.dumps(trend_markets),len(trends),
             leadlag["pairs_scored"],leadlag["signals_created"],leadlag["outcomes"]["verified"],leadlag["outcomes"]["expired"]))
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', default=None)
    parser.add_argument('--db', type=Path, default=None, help='Existing summary database; useful for isolated verification')
    parser.add_argument('--provenance-corrections', type=Path, default=None)
    args = parser.parse_args()
    if args.db is not None:
        if not args.db.is_file():
            parser.error('Summary database must already exist')
        database.DB_PATH = args.db.resolve()
        database.DATA_DIR = args.db.resolve().parent
    corrections = json.loads(args.provenance_corrections.read_text(encoding='utf-8')) if args.provenance_corrections else []
    print(json.dumps(refresh(date=args.date, corrections=corrections), ensure_ascii=False))


if __name__ == '__main__':
    main()
