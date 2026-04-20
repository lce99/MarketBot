"""Shared date helpers for market collectors."""

from datetime import datetime, timedelta
from typing import Sequence


def recent_dates(target_date: str, lookback_days: int = 7) -> list[str]:
    """Return target_date and recent calendar dates in YYYY-MM-DD format."""
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    return [
        (dt - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(lookback_days)
    ]


def compute_return_pct(
    current_value: float | int | None,
    previous_value: float | int | None,
) -> float | None:
    """Return percentage change between two values."""
    if current_value is None or previous_value is None:
        return None

    previous = float(previous_value)
    current = float(current_value)
    if previous <= 0:
        return None

    return ((current - previous) / previous) * 100


def compute_period_return_from_closes(
    closes: Sequence[float],
    periods_back: int = 5,
) -> float | None:
    """Return percentage change using the close from N periods ago."""
    if len(closes) < periods_back + 1:
        return None

    return compute_return_pct(closes[-1], closes[-(periods_back + 1)])
