#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from urban_sami.io.inegi_catalogounico import fetch_all_municipalities


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch official INEGI municipality population and household data")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw/inegi_cpv2020_city_population.csv"),
    )
    args = parser.parse_args()

    rows = fetch_all_municipalities()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "source_file",
                "country_code",
                "level",
                "unit_code",
                "unit_label",
                "city_code",
                "city_name",
                "population",
                "households",
                "population_male",
                "population_female",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    "inegi_wscatgeo_mgem",
                    "MX",
                    "city",
                    row.city_code,
                    row.city_name,
                    row.city_code,
                    row.city_name,
                    row.population_total,
                    row.households,
                    row.population_male,
                    row.population_female,
                ]
            )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
