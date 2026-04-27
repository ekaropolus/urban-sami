#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Select a reproducible city sample from the official municipality population table")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/raw/inegi_cpv2020_city_population.csv"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw/city_samples/top20_population.csv"),
    )
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument(
        "--sort-by",
        choices=("population", "households"),
        default="population",
    )
    args = parser.parse_args()

    with args.input.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    metric = args.sort_by
    rows = [row for row in rows if float(row.get(metric) or 0.0) > 0.0]
    rows.sort(key=lambda row: float(row.get(metric) or 0.0), reverse=True)
    selected = rows[: max(1, args.top_n)]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["city_code", "city_name", "population", "households", "rank", "selection_metric"],
        )
        writer.writeheader()
        for idx, row in enumerate(selected, start=1):
            writer.writerow(
                {
                    "city_code": row.get("city_code", ""),
                    "city_name": row.get("city_name", ""),
                    "population": row.get("population", ""),
                    "households": row.get("households", ""),
                    "rank": idx,
                    "selection_metric": metric,
                }
            )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
