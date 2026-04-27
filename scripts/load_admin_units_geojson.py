#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"


def _psql(sql: str) -> None:
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
        "-f",
        "-",
    ]
    subprocess.run(cmd, check=True, text=True, input=sql)


def _sql_text(value: object) -> str:
    return str(value or "").replace("'", "''")


def main() -> int:
    parser = argparse.ArgumentParser(description="Load AGEB urban GeoJSON features into raw.admin_units")
    parser.add_argument("geojson", nargs="+", type=Path)
    parser.add_argument("--truncate-level", action="store_true")
    parser.add_argument("--level", default="ageb_u")
    parser.add_argument("--country-code", default="MX")
    parser.add_argument("--source-file", default="")
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    if args.truncate_level:
        _psql(f"DELETE FROM raw.admin_units WHERE level = '{_sql_text(args.level)}';")

    total = 0
    values: list[str] = []
    for path in args.geojson:
        doc = json.loads(path.read_text(encoding="utf-8"))
        for feature in doc.get("features", []):
            props = feature.get("properties", {}) or {}
            geom_json = json.dumps(feature.get("geometry", {})).replace("'", "''")
            unit_code = _sql_text(props.get("cvegeo"))
            city_code = f"{str(props.get('cve_ent', '')).strip()}{str(props.get('cve_mun', '')).strip()}"
            value = (
                f"('{_sql_text(args.source_file or path.name)}','{_sql_text(args.country_code)}','{_sql_text(args.level)}',"
                f"'{unit_code}','{_sql_text(props.get('cve_ageb'))}','{_sql_text(city_code)}','{_sql_text(props.get('nom_mun'))}',"
                f"'{_sql_text(props.get('cve_loc'))}',"
                f"{int(str(props.get('pob_total') or '0').replace(' ', '').replace(',', '') or '0')},"
                f"{int(str(props.get('total_viviendas_habitadas') or '0').replace(' ', '').replace(',', '') or '0')},"
                f"{int(str(props.get('pob_femenina') or '0').replace(' ', '').replace(',', '') or '0')},"
                f"{int(str(props.get('pob_masculina') or '0').replace(' ', '').replace(',', '') or '0')},"
                f"NULL,ST_SetSRID(ST_GeomFromGeoJSON('{geom_json}'),4326))"
            )
            values.append(value)
            total += 1
            if len(values) >= args.batch_size:
                _psql(
                    "INSERT INTO raw.admin_units "
                    "(source_file,country_code,level,unit_code,unit_label,city_code,city_name,parent_code,population,households,population_female,population_male,area_km2,geom) VALUES "
                    + ",".join(values)
                    + ";"
                )
                values = []
    if values:
        _psql(
            "INSERT INTO raw.admin_units "
            "(source_file,country_code,level,unit_code,unit_label,city_code,city_name,parent_code,population,households,population_female,population_male,area_km2,geom) VALUES "
            + ",".join(values)
            + ";"
        )
    print(f"loaded_rows={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
