#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import subprocess
from pathlib import Path

from urban_sami.io.denue_bulk import iter_denue_bulk_rows


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


def _copy_rows(tsv_text: str) -> None:
    _psql(
        """
        COPY raw.denue_establishments (
            source_file, denue_id, country_code, state_code, state_name,
            city_code, city_name, ageb_code, manzana_code, scian_code,
            per_ocu, latitude, longitude
        )
        FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', HEADER false)
        """.strip(),
        stdin_text=tsv_text,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Load DENUE bulk CSV files into urban_sami_exp.raw.denue_establishments")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("/home/hadox/cmd-center/data/polisplexity-core/data/denue_mx_bulk"),
    )
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--limit-files", type=int, default=0)
    parser.add_argument("--limit-rows", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--backfill-geom", action="store_true")
    args = parser.parse_args()

    files = sorted(path for path in args.input_dir.iterdir() if path.is_file())
    if args.limit_files > 0:
        files = files[: args.limit_files]

    if args.truncate:
        _psql("TRUNCATE TABLE raw.denue_establishments;")

    total_rows = 0
    pending = io.StringIO()
    writer = csv.writer(pending, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    for file_path in files:
        for row in iter_denue_bulk_rows(file_path):
            writer.writerow(
                [
                    row.source_file,
                    row.denue_id,
                    row.country_code,
                    row.state_code,
                    row.state_name,
                    row.city_code,
                    row.city_name,
                    row.ageb_code,
                    row.manzana_code,
                    row.scian_code,
                    row.per_ocu,
                    NULL_MARKER if row.latitude is None else row.latitude,
                    NULL_MARKER if row.longitude is None else row.longitude,
                ]
            )
            total_rows += 1
            if args.limit_rows > 0 and total_rows >= args.limit_rows:
                break
            if total_rows % args.batch_size == 0:
                _copy_rows(pending.getvalue())
                pending = io.StringIO()
                writer = csv.writer(pending, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        if args.limit_rows > 0 and total_rows >= args.limit_rows:
            break

    if pending.tell():
        _copy_rows(pending.getvalue())

    if args.backfill_geom:
        _psql(
            """
            UPDATE raw.denue_establishments
            SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            WHERE geom IS NULL AND longitude IS NOT NULL AND latitude IS NOT NULL
            """.strip()
        )
    print(f"loaded_rows={total_rows}")
    print(f"loaded_files={len(files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
