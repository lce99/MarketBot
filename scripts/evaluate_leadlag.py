"""Evaluate stored lead-lag predictions as JSON or Markdown."""

from __future__ import annotations

import argparse
import json

from src.database import get_connection
from src.leadlag_evaluation import evaluate_stored_predictions


def _pct(value) -> str:
    return "-" if value is None else f"{value * 100:.1f}%"


def _pp(value) -> str:
    return "-" if value is None else f"{value * 100:+.1f}pp"


def format_markdown(result: dict, min_sample: int = 1) -> str:
    overall = result["overall"]
    lines = [
        "# Lead-lag stored prediction evaluation",
        "",
        (
            f"Verified **{overall['hits']}/{overall['total']}**; accuracy "
            f"**{_pct(overall['hit_rate'])}** "
            f"(95% Wilson CI {_pct(overall['ci95_low'])}–{_pct(overall['ci95_high'])}); "
            f"naive **{_pct(overall['naive_rate'])}**; excess "
            f"**{_pp(overall['excess_vs_naive'])}**."
        ),
        "",
        f"> {result['definitions']['independence_warning']}",
    ]
    for dimension, rows in result["groups"].items():
        visible = [row for row in rows if row["total"] >= min_sample]
        if not visible:
            continue
        lines.extend(
            [
                "",
                f"## {dimension}",
                "",
                "| group | n | accuracy | 95% CI | naive | excess | predicted up | actual up |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in visible:
            lines.append(
                f"| {row['group']} | {row['total']} | {_pct(row['hit_rate'])} | "
                f"{_pct(row['ci95_low'])}–{_pct(row['ci95_high'])} | "
                f"{_pct(row['naive_rate'])} | {_pp(row['excess_vs_naive'])} | "
                f"{_pct(row['prediction_up_share'])} | {_pct(row['outcome_up_share'])} |"
            )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument("--min-sample", type=int, default=1)
    args = parser.parse_args()

    conn = get_connection()
    try:
        result = evaluate_stored_predictions(conn)
    finally:
        conn.close()

    if args.format == "markdown":
        print(format_markdown(result, min_sample=max(args.min_sample, 1)))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
