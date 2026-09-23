"""Out-of-sample evaluation helpers for stored lead-lag flow signals."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Iterable


GROUP_DIMENSIONS = (
    "country_pair",
    "leader",
    "follower",
    "sector",
    "lag",
    "direction",
)


def wilson_interval(hits: int, total: int, z: float = 1.96) -> tuple[float | None, float | None]:
    """Return a two-sided Wilson interval for a binomial hit rate."""
    if total <= 0:
        return None, None
    rate = hits / total
    denominator = 1 + (z * z / total)
    center = (rate + z * z / (2 * total)) / denominator
    margin = (
        z
        * math.sqrt(rate * (1 - rate) / total + z * z / (4 * total * total))
        / denominator
    )
    return center - margin, center + margin


def summarize_verified_signals(rows: Iterable[dict]) -> dict:
    """Summarize verified predictions against an ex-post majority baseline.

    The baseline always predicts the most frequent realised follower direction in
    the evaluated slice. It is deliberately hard to beat and exposes market-drift
    bias without fitting another forecasting model.
    """
    observations = list(rows)
    total = len(observations)
    hits = sum(int(row.get("hit") or 0) for row in observations)
    predicted_up = sum(int(row.get("predicted_direction") or 0) > 0 for row in observations)
    actual_up = sum(float(row.get("follower_return") or 0) > 0 for row in observations)
    actual_down = sum(float(row.get("follower_return") or 0) < 0 for row in observations)
    actual_flat = total - actual_up - actual_down
    naive_hits = max(actual_up, actual_down, actual_flat) if total else 0
    naive_direction = None
    if total:
        naive_direction = max(
            (("up", actual_up), ("down", actual_down), ("flat", actual_flat)),
            key=lambda item: item[1],
        )[0]

    hit_rate = hits / total if total else None
    naive_rate = naive_hits / total if total else None
    ci_low, ci_high = wilson_interval(hits, total)
    prediction_up_share = predicted_up / total if total else None
    outcome_up_share = actual_up / total if total else None

    return {
        "total": total,
        "hits": hits,
        "hit_rate": hit_rate,
        "ci95_low": ci_low,
        "ci95_high": ci_high,
        "naive_direction": naive_direction,
        "naive_hits": naive_hits,
        "naive_rate": naive_rate,
        "excess_vs_naive": (
            hit_rate - naive_rate if hit_rate is not None and naive_rate is not None else None
        ),
        "predicted_up": predicted_up,
        "predicted_down": total - predicted_up,
        "prediction_up_share": prediction_up_share,
        "actual_up": actual_up,
        "actual_down": actual_down,
        "actual_flat": actual_flat,
        "outcome_up_share": outcome_up_share,
        "direction_bias": (
            prediction_up_share - outcome_up_share
            if prediction_up_share is not None and outcome_up_share is not None
            else None
        ),
        "unique_created_dates": len(
            {row.get("created_date") for row in observations if row.get("created_date")}
        ),
        "unique_target_dates": len(
            {row.get("target_date") for row in observations if row.get("target_date")}
        ),
    }


def _group_value(row: dict, dimension: str):
    if dimension == "country_pair":
        return f"{row.get('leader')}→{row.get('follower')}"
    if dimension == "direction":
        return "up" if int(row.get("predicted_direction") or 0) > 0 else "down"
    return row.get(dimension)


def group_verified_signals(rows: Iterable[dict], dimension: str) -> list[dict]:
    """Group verified rows by one supported evaluation dimension."""
    if dimension not in GROUP_DIMENSIONS:
        raise ValueError(f"unsupported dimension: {dimension}")

    grouped: dict[object, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[_group_value(row, dimension)].append(row)

    results = []
    for value, observations in grouped.items():
        results.append({"group": value, **summarize_verified_signals(observations)})
    return sorted(results, key=lambda row: (-row["total"], str(row["group"])))


def evaluate_stored_predictions(conn) -> dict:
    """Evaluate all stored verified signals without changing database state."""
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM flow_signals
            WHERE status = 'verified'
              AND hit IS NOT NULL
              AND follower_return IS NOT NULL
            ORDER BY created_date, id
            """
        ).fetchall()
    ]
    status_counts = {
        row["status"]: row["count"]
        for row in conn.execute(
            "SELECT status, COUNT(*) AS count FROM flow_signals GROUP BY status"
        ).fetchall()
    }
    distinct_outcomes = len(
        {
            (row.get("target_date"), row.get("follower"), row.get("sector"))
            for row in rows
        }
    )
    return {
        "definitions": {
            "confidence_interval": "two-sided 95% Wilson binomial interval",
            "naive_baseline": "ex-post majority realised direction within each slice",
            "independence_warning": (
                "Signals sharing target_date/follower/sector are correlated; Wilson intervals "
                "are descriptive and may be too narrow."
            ),
        },
        "status_counts": status_counts,
        "distinct_target_outcomes": distinct_outcomes,
        "overall": summarize_verified_signals(rows),
        "groups": {
            dimension: group_verified_signals(rows, dimension)
            for dimension in GROUP_DIMENSIONS
        },
    }
