"""Export public summary observations; never read secrets, raw stocks or watchlists.

Only Python's standard library is required. The source database is opened read-only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.markets import ACTIVE_MARKETS as MARKETS, EXCLUDED_MARKETS


def export(db: Path, now: datetime | None = None, source_commit: str = "") -> dict:
    now = now or datetime.now(timezone.utc)
    conn = sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    def query(table: str, columns: dict, where: str = "", params: tuple = ()) -> list:
        if table not in tables:
            return []
        available = {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')}
        select = ",".join(f'"{column}" AS "{alias}"' if column in available else f'NULL AS "{alias}"' for column, alias in columns.items())
        return [dict(r) for r in conn.execute(f'SELECT {select} FROM "{table}" {where}', params)]

    sector_columns = dict(date="date", country="country", sector="sector", daily_return="dailyReturn", weekly_return="weeklyReturn", breadth="breadth", volume_change="volumeChange", stock_count="stockCount")
    sectors = query("sector_performance", sector_columns, "ORDER BY date,country,sector")
    sectors = [row for row in sectors if row['country'] in MARKETS]
    latest = {}
    for row in sectors:
        latest[row['country']] = max(latest.get(row['country'], ''), row['date'])
    latest_date = max(latest.values(), default=None)
    cutoff = (date.fromisoformat(latest_date) - timedelta(days=400)).isoformat() if latest_date else "0000-01-01"
    sectors = [r for r in sectors if r['date'] >= cutoff or r['date'] == latest.get(r['country'])]
    markets = []
    for code, name in MARKETS.items():
        logs = query("collection_log", dict(timestamp="timestamp", status="status", failure_code="failureCode", failure_stage="failureStage", provider="provider"), "WHERE market=? ORDER BY timestamp DESC,id DESC LIMIT 1", (code,))
        observed = latest.get(code)
        age = (now.date() - date.fromisoformat(observed)).days if observed else None
        markets.append(dict(code=code, name=name, latestDate=observed, calendarDaysOld=age, stale=age is None or age > 4, collection=logs[0] if logs else None))

    benchmarks = query("benchmark_daily", dict(date="date", country="country", ticker="ticker", name="name", sector="sector", close_price="close", daily_return="dailyReturn", weekly_return="weeklyReturn"), "WHERE (ticker,date) IN (SELECT ticker,MAX(date) FROM benchmark_daily GROUP BY ticker) ORDER BY country,ticker")
    benchmarks = [row for row in benchmarks if row['country'] in MARKETS]
    trends = query("trend_scores", dict(date="date", sector="sector", trend_score="score", countries_positive="positive", countries_negative="negative", global_avg_return="averageReturn", global_breadth="breadth", momentum_signal="signal"), "WHERE date=(SELECT MAX(date) FROM trend_scores) ORDER BY trend_score DESC")
    lead_lag = query("lead_lag_scores", dict(date="date", sector="sector", leader="leader", follower="follower", lag="lag", correlation="correlation", direction_agreement="agreement", n_obs="observations"), "WHERE date=(SELECT MAX(date) FROM lead_lag_scores) ORDER BY correlation DESC")
    signals = query("flow_signals", dict(created_date="date", sector="sector", leader="leader", follower="follower", lag="lag", leader_return="leaderReturn", predicted_direction="direction", correlation="correlation", status="status", target_date="targetDate", follower_return="followerReturn", hit="hit"), "ORDER BY created_date DESC,sector,leader,follower LIMIT 100")
    lead_lag = [row for row in lead_lag if row['leader'] in MARKETS and row['follower'] in MARKETS]
    signals = [row for row in signals if row['leader'] in MARKETS and row['follower'] in MARKETS]
    outcomes = query("flow_signals", dict(hit="hit", predicted_direction="direction", leader="leader", follower="follower"), "WHERE status='verified' AND hit IN (0,1)")
    outcomes = [row for row in outcomes if row['leader'] in MARKETS and row['follower'] in MARKETS]
    stats = dict(verified=len(outcomes), hits=sum(r['hit'] for r in outcomes), hitRate=sum(r['hit'] for r in outcomes)/len(outcomes) if outcomes else None)
    conn.close()
    result = dict(schemaVersion=1, generatedAt=now.isoformat(), source=dict(repository="lce99/MarketBot", commit=source_commit, databaseSHA256=hashlib.sha256(db.read_bytes()).hexdigest()), latestDate=latest_date, historyStart=min((r['date'] for r in sectors), default=None), markets=markets, sectorHistory=sectors, benchmarks=benchmarks, trends=trends, leadLag=lead_lag, flowSignals=signals, signalStats=stats)
    result['coverage'] = dict(targetMarkets=len(MARKETS), freshMarkets=sum(not m['stale'] for m in markets),
                              observedMarkets=sum(m['latestDate'] is not None for m in markets),
                              activeMarkets=list(MARKETS), excludedMarkets=list(EXCLUDED_MARKETS))

    def clean(value):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        if isinstance(value, dict):
            return {k: clean(v) for k, v in value.items()}
        if isinstance(value, list):
            return [clean(v) for v in value]
        return value
    return clean(result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', type=Path, default=Path('data/marketbot.db'))
    parser.add_argument('--output', type=Path, default=Path('docs/data/market-dashboard.json'))
    parser.add_argument('--source-commit', default='')
    args = parser.parse_args()
    data = export(args.db, source_commit=args.source_commit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n', encoding='utf-8')
    temporary.replace(args.output)
    stale = ",".join(market['code'] for market in data['markets'] if market['stale']) or "none"
    print(f"Exported {len(data['sectorHistory'])} sector observations; latest observation {data['latestDate']}; stale/missing markets: {stale}")


if __name__ == '__main__':
    main()
