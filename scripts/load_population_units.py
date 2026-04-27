#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import subprocess
from pathlib import Path


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"
NULL_MARKER = r"\N"


def _psql(sql: str, *, stdin_text: str | None = None) -> None:
    cmd = [
        DOCKER_EXE,
        "exec",
        "-i",
        DB_CONTAINER,
        "psql",
        "-U",
        POSTGRES_USER,
        "-d",
        DB_NAME,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    subprocess.run(cmd, input=stdin_text, text=True, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Load population unit CSV into urban_sami_exp.raw.population_units")
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--level", default="")
    args = parser.parse_args()

    rows = []
    with args.input_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if args.level and str(row.get("level", "")).strip() != args.level:
                continue
            rows.append(
                [
                    str(row.get("source_file", "")).strip(),
                    str(row.get("country_code", "MX")).strip() or "MX",
                    str(row.get("level", "")).strip(),
                    str(row.get("unit_code", "")).strip(),
                    str(row.get("unit_label", "")).strip(),
                    str(row.get("city_code", "")).strip(),
                    str(row.get("city_name", "")).strip(),
                    str(row.get("ageb_code", "")).strip(),
                    str(row.get("manzana_code", "")).strip(),
                    str(row.get("population", "")).strip() or NULL_MARKER,
                    str(row.get("households", "")).strip() or NULL_MARKER,
                    str(row.get("population_female", "")).strip() or NULL_MARKER,
                    str(row.get("population_male", "")).strip() or NULL_MARKER,
                    str(row.get("area_km2", "")).strip() or NULL_MARKER,
                ]
            )

    if args.truncate:
        _psql("TRUNCATE TABLE raw.population_units;")

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        writer.writerow(row)

    _psql(
        """
        COPY raw.population_units (
            source_file, country_code, level, unit_code, unit_label,
            city_code, city_name, ageb_code, manzana_code, population,
            households, population_female, population_male, area_km2
        )
        FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', HEADER false)
        """.strip(),
        stdin_text=buf.getvalue(),
    )
    print(f"loaded_rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
