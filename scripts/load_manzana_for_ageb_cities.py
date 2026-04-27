#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import time
from http.client import IncompleteRead
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from shapely.geometry import shape


WINDOWS_DOCKER = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DOCKER_EXE = os.environ.get("DOCKER_EXE") or shutil.which("docker") or WINDOWS_DOCKER
DB_CONTAINER = os.environ.get("DB_CONTAINER", "24-polisplexity-core-db-dev")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_NAME = os.environ.get("DB_NAME", "urban_sami_exp")
BASE_URL = os.environ.get("GEO_SOURCE_BASE_URL", "https://gaia.inegi.org.mx/wscatgeo/v2/geo").rstrip("/")
USER_AGENT = os.environ.get("GEO_SOURCE_USER_AGENT", "Polisplexity/1.0")
FETCH_TIMEOUT_SECONDS = float(os.environ.get("MANZANA_FETCH_TIMEOUT_SECONDS", "60"))
FETCH_MAX_ATTEMPTS = int(os.environ.get("MANZANA_FETCH_MAX_ATTEMPTS", "5"))
FETCH_BACKOFF_SECONDS = float(os.environ.get("MANZANA_FETCH_BACKOFF_SECONDS", "2.0"))


def _psql(sql: str, *, capture_output: bool = False) -> str:
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
    ]
    if capture_output:
        cmd.extend(["-AtF", "\t"])
    proc = subprocess.run(cmd, input=sql, text=True, capture_output=capture_output, check=True)
    return proc.stdout if capture_output else ""


def _sql_text(value: object) -> str:
    return str(value).replace("'", "''")


def _fetch_city_codes() -> list[str]:
    out = _psql(
        """
        SELECT DISTINCT city_code
        FROM raw.admin_units
        WHERE level = 'ageb_u'
        ORDER BY city_code;
        """,
        capture_output=True,
    )
    return [line.strip() for line in out.splitlines() if line.strip()]


def _prop(props: dict[str, object], *keys: str) -> str:
    lowered = {str(k).lower(): v for k, v in (props or {}).items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _to_float(text: str) -> str:
    try:
        return str(float(text))
    except Exception:
        return "\\N"


def _to_int_like(text: str) -> str:
    if not text:
        return "\\N"
    try:
        return str(int(float(text)))
    except Exception:
        return "\\N"


def _unit_label(city_code: str, ageb_code: str, manzana_code: str) -> str:
    return f"{city_code} AGEB {ageb_code} MZA {manzana_code}"


def _fetch_manzana_fc(city_code: str) -> dict:
    cve_ent = city_code[:2]
    cve_mun = city_code[2:5]
    url = f"{BASE_URL}/mza/{cve_ent}/{cve_mun}"
    last_error: Exception | None = None
    for attempt in range(1, FETCH_MAX_ATTEMPTS + 1):
        req = Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
        try:
            with urlopen(req, timeout=FETCH_TIMEOUT_SECONDS) as resp:  # noqa: S310
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            last_error = exc
            if exc.code not in {408, 425, 429, 500, 502, 503, 504} or attempt >= FETCH_MAX_ATTEMPTS:
                break
        except (TimeoutError, IncompleteRead, URLError) as exc:
            last_error = exc
            if attempt >= FETCH_MAX_ATTEMPTS:
                break
        except Exception as exc:
            last_error = exc
            if attempt >= FETCH_MAX_ATTEMPTS:
                break
        time.sleep(FETCH_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    message = f"failed to fetch manzana features for city {city_code} after {FETCH_MAX_ATTEMPTS} attempts"
    if last_error is not None:
        raise RuntimeError(f"{message}: {last_error}") from last_error
    raise RuntimeError(message)


def _tsv_escape(text: str) -> str:
    return text.replace("\t", " ").replace("\n", " ").replace("\r", " ")


def _bootstrap() -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_experiment_db.sh"
    subprocess.run(["bash", str(script)], check=True)


def _copy_city(city_code: str, *, refresh: bool) -> dict[str, int | str]:
    fc = _fetch_manzana_fc(city_code)
    features = fc.get("features") or []
    pop_rows: list[str] = []
    admin_rows: list[str] = []

    city_name = ""
    seen_agebs: set[str] = set()
    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry")
        if not geom:
            continue
        cvegeo = _prop(props, "cvegeo")
        cve_ent = _prop(props, "cve_ent")
        cve_mun = _prop(props, "cve_mun")
        cve_loc = _prop(props, "cve_loc")
        cve_ageb = _prop(props, "cve_ageb")
        cve_mza = _prop(props, "cve_mza")
        if not cvegeo or not cve_ageb or not cve_mza:
            continue
        unit_code = cvegeo
        parent_code = cvegeo[:13] if len(cvegeo) >= 13 else cve_ageb
        current_city_code = f"{cve_ent}{cve_mun}"
        name = _prop(props, "nom_mun") or city_code
        city_name = city_name or name
        label = _unit_label(current_city_code, cve_ageb, cve_mza)
        population = _to_int_like(_prop(props, "pob_total"))
        population_female = _to_int_like(_prop(props, "pob_femenina"))
        population_male = _to_int_like(_prop(props, "pob_masculina"))
        households = _to_int_like(_prop(props, "total_viviendas_habitadas"))
        geom_wkt = shape(geom).wkt

        pop_rows.append(
            "\t".join(
                [
                    _tsv_escape(f"inegi_wscatgeo_mza_{city_code}.json"),
                    "MX",
                    "manzana",
                    _tsv_escape(unit_code),
                    _tsv_escape(label),
                    _tsv_escape(current_city_code),
                    _tsv_escape(name),
                    _tsv_escape(cve_ageb),
                    _tsv_escape(cve_mza),
                    population,
                    households,
                    population_female,
                    population_male,
                    "\\N",
                    _tsv_escape(geom_wkt),
                ]
            )
        )
        admin_rows.append(
            "\t".join(
                [
                    _tsv_escape(f"inegi_wscatgeo_mza_{city_code}.json"),
                    "MX",
                    "manzana",
                    _tsv_escape(unit_code),
                    _tsv_escape(label),
                    _tsv_escape(current_city_code),
                    _tsv_escape(name),
                    _tsv_escape(parent_code),
                    population,
                    households,
                    population_female,
                    population_male,
                    "\\N",
                    _tsv_escape(geom_wkt),
                ]
            )
        )
        seen_agebs.add(parent_code)

    delete_sql = ""
    if refresh:
        delete_sql = f"""
DELETE FROM raw.population_units WHERE level = 'manzana' AND city_code = '{_sql_text(city_code)}';
DELETE FROM raw.admin_units WHERE level = 'manzana' AND city_code = '{_sql_text(city_code)}';
"""
    pop_block = "".join(line + "\n" for line in pop_rows)
    admin_block = "".join(line + "\n" for line in admin_rows)
    script = f"""
\\set ON_ERROR_STOP on
BEGIN;
{delete_sql}
CREATE TEMP TABLE tmp_manzana_pop (
    source_file TEXT,
    country_code TEXT,
    level TEXT,
    unit_code TEXT,
    unit_label TEXT,
    city_code TEXT,
    city_name TEXT,
    ageb_code TEXT,
    manzana_code TEXT,
    population DOUBLE PRECISION,
    households DOUBLE PRECISION,
    population_female DOUBLE PRECISION,
    population_male DOUBLE PRECISION,
    area_km2 DOUBLE PRECISION,
    geom_wkt TEXT
) ON COMMIT DROP;
COPY tmp_manzana_pop (
    source_file, country_code, level, unit_code, unit_label, city_code, city_name,
    ageb_code, manzana_code, population, households, population_female, population_male,
    area_km2, geom_wkt
) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', HEADER false);
{pop_block}\\.

CREATE TEMP TABLE tmp_manzana_admin (
    source_file TEXT,
    country_code TEXT,
    level TEXT,
    unit_code TEXT,
    unit_label TEXT,
    city_code TEXT,
    city_name TEXT,
    parent_code TEXT,
    population DOUBLE PRECISION,
    households DOUBLE PRECISION,
    population_female DOUBLE PRECISION,
    population_male DOUBLE PRECISION,
    area_km2 DOUBLE PRECISION,
    geom_wkt TEXT
) ON COMMIT DROP;
COPY tmp_manzana_admin (
    source_file, country_code, level, unit_code, unit_label, city_code, city_name,
    parent_code, population, households, population_female, population_male, area_km2, geom_wkt
) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', HEADER false);
{admin_block}\\.

INSERT INTO raw.population_units (
    source_file, country_code, level, unit_code, unit_label, city_code, city_name, ageb_code,
    manzana_code, population, households, population_female, population_male, area_km2
)
SELECT
    source_file,
    country_code,
    level,
    unit_code,
    unit_label,
    city_code,
    city_name,
    ageb_code,
    manzana_code,
    population,
    households,
    population_female,
    population_male,
    ST_Area(geography(ST_Multi(ST_SetSRID(ST_GeomFromText(geom_wkt), 4326)))) / 1000000.0
FROM tmp_manzana_pop;

INSERT INTO raw.admin_units (
    source_file, country_code, level, unit_code, unit_label, city_code, city_name, parent_code,
    population, households, population_female, population_male, area_km2, geom
)
SELECT
    source_file,
    country_code,
    level,
    unit_code,
    unit_label,
    city_code,
    city_name,
    parent_code,
    population,
    households,
    population_female,
    population_male,
    ST_Area(geography(ST_Multi(ST_SetSRID(ST_GeomFromText(geom_wkt), 4326)))) / 1000000.0,
    ST_Multi(ST_SetSRID(ST_GeomFromText(geom_wkt), 4326))
FROM tmp_manzana_admin;
COMMIT;
"""
    _psql(script)
    return {
        "city_code": city_code,
        "city_name": city_name,
        "features_seen": len(features),
        "manzana_loaded": len(pop_rows),
        "distinct_parent_agebs": len(seen_agebs),
    }


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Load INEGI manzana geometry and population for the cities currently covered by AGEB in urban_sami_exp.")
    parser.add_argument("--no-refresh", action="store_true", help="Do not delete existing manzana rows for the selected cities before insert.")
    args = parser.parse_args()

    refresh = not args.no_refresh
    _bootstrap()
    outdir = Path(__file__).resolve().parents[1] / "reports" / "manzana-load-ageb-cities-2026-04-24"
    outdir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, object]] = []
    for city_code in _fetch_city_codes():
        rows.append(_copy_city(city_code, refresh=refresh))
        _write_csv(outdir / "city_load_summary.csv", rows, list(rows[0].keys()))

    summary_sql = """
    SELECT 'manzana_population_rows', count(*)::text FROM raw.population_units WHERE level = 'manzana'
    UNION ALL
    SELECT 'manzana_admin_rows', count(*)::text FROM raw.admin_units WHERE level = 'manzana'
    UNION ALL
    SELECT 'manzana_cities', count(DISTINCT city_code)::text FROM raw.population_units WHERE level = 'manzana'
    UNION ALL
    SELECT 'manzana_distinct_agebs', count(DISTINCT city_code || ':' || ageb_code)::text FROM raw.population_units WHERE level = 'manzana';
    """
    summary_rows = []
    for line in _psql(summary_sql, capture_output=True).splitlines():
        if not line.strip():
            continue
        key, value = line.split("\t", 1)
        summary_rows.append({"metric": key, "value": value})
    _write_csv(outdir / "summary.csv", summary_rows, ["metric", "value"])
    report = [
        "# Manzana Load",
        "",
        "Source:",
        f"- `{BASE_URL}/mza/{{cve_ent}}/{{cve_mun}}`",
        "",
        "Files:",
        f"- [summary.csv]({(outdir / 'summary.csv').resolve()})",
        f"- [city_load_summary.csv]({(outdir / 'city_load_summary.csv').resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
