#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from urban_sami.io.inegi_cpv2020 import fetch_all_state_population_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch official INEGI CPV 2020 state population data")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw/inegi_cpv2020_state_population.csv"),
    )
    parser.add_argument("--include-national", action="store_true")
    args = parser.parse_args()

    rows = fetch_all_state_population_rows(include_national=args.include_national)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["source_file", "country_code", "level", "unit_code", "unit_label", "population", "population_male", "population_female"]
        )
        for row in rows:
            level = "country" if row.state_code == "00" else "state"
            writer.writerow(
                [
                    "inegi_cpv2020_widget",
                    "MX",
                    level,
                    row.state_code,
                    row.state_name,
                    row.population_total,
                    row.population_male,
                    row.population_female,
                ]
            )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
