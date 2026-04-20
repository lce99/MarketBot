"""Shared date helpers for market collectors."""

from datetime import datetime, timedelta


def recent_dates(target_date: str, lookback_days: int = 7) -> list[str]:
    """Return target_date and recent calendar dates in YYYY-MM-DD format."""
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    return [
        (dt - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(lookback_days)
    ]
