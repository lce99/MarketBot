"""Build a committed KR ticker-to-sector reference map from raw history."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_DB_PATH = ROOT / "data" / "marketbot_raw.db"
OUTPUT_PATH = ROOT / "src" / "collectors" / "data" / "kr_sector_reference.json"


def build_reference() -> dict:
    if not RAW_DB_PATH.exists():
        raise FileNotFoundError(f"Raw DB not found: {RAW_DB_PATH}")

    conn = sqlite3.connect(RAW_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    ticker,
                    sector,
                    date,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker
                        ORDER BY date DESC
                    ) AS rn
                FROM stock_daily
                WHERE country = 'KR'
                  AND sector IS NOT NULL
                  AND TRIM(sector) <> ''
                  AND sector <> '기타'
            )
            SELECT ticker, sector, date
            FROM ranked
            WHERE rn = 1
            ORDER BY ticker
            """
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        raise RuntimeError("No KR sector history found in raw DB")

    latest_date = max(row["date"] for row in rows)
    tickers = {row["ticker"]: row["sector"] for row in rows}
    return {
        "country": "KR",
        "source": str(RAW_DB_PATH.relative_to(ROOT)).replace("\\", "/"),
        "latest_date": latest_date,
        "ticker_count": len(tickers),
        "tickers": tickers,
    }


def main() -> None:
    payload = build_reference()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"Wrote {payload['ticker_count']} KR sectors to "
        f"{OUTPUT_PATH.relative_to(ROOT)}"
    )


if __name__ == "__main__":
    main()
