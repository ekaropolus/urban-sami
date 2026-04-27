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
FETCH_TIMEOUT_SECONDS = float(os.environ.get("AGEB_FETCH_TIMEOUT_SECONDS", "60"))
FETCH_MAX_ATTEMPTS = int(os.environ.get("AGEB_FETCH_MAX_ATTEMPTS", "5"))
FETCH_BACKOFF_SECONDS = float(os.environ.get("AGEB_FETCH_BACKOFF_SECONDS", "2.0"))
SOURCE_METHOD = "inegi_wscatgeo_agebu_full_v1"


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


def _bootstrap() -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_experiment_db.sh"
    subprocess.run(["bash", str(script)], check=True)


def _all_city_rows() -> list[dict[str, str]]:
    out = _psql(
        """
        SELECT city_code, COALESCE(MAX(city_name), '')
        FROM raw.population_units
        WHERE level = 'city'
          AND city_code <> ''
        GROUP BY city_code
        ORDER BY city_code;
        """,
        capture_output=True,
    )
    rows: list[dict[str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        city_code, city_name = (line.split("\t", 1) + [""])[:2]
        rows.append({"city_code": city_code.strip(), "city_name": city_name.strip()})
    return rows


def _error_city_rows() -> list[dict[str, str]]:
    out = _psql(
        f"""
        SELECT city_code, COALESCE(city_name, '')
        FROM experiments.ageb_load_status
        WHERE source_method = '{SOURCE_METHOD}'
          AND status = 'error'
        ORDER BY city_code;
        """,
        capture_output=True,
    )
    rows: list[dict[str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        city_code, city_name = (line.split("\t", 1) + [""])[:2]
        rows.append({"city_code": city_code.strip(), "city_name": city_name.strip()})
    return rows


def _marked_success(city_code: str) -> bool:
    out = _psql(
        f"""
        SELECT COUNT(*)
        FROM experiments.ageb_load_status
        WHERE source_method = '{SOURCE_METHOD}'
          AND city_code = '{_sql_text(city_code)}'
          AND status = 'success';
        """,
        capture_output=True,
    ).strip()
    return int(out or "0") > 0


def _city_has_complete_ageb(city_code: str) -> bool:
    out = _psql(
        f"""
        SELECT
          COALESCE((SELECT COUNT(*) FROM raw.population_units WHERE level = 'ageb_u' AND city_code = '{_sql_text(city_code)}'), 0),
          COALESCE((SELECT COUNT(*) FROM raw.admin_units WHERE level = 'ageb_u' AND city_code = '{_sql_text(city_code)}'), 0);
        """,
        capture_output=True,
    ).strip()
    if not out:
        return False
    pop_rows, admin_rows = [int(x or "0") for x in out.split("\t")]
    return pop_rows > 0 and admin_rows > 0 and pop_rows == admin_rows


def _set_status(
    *,
    city_code: str,
    city_name: str,
    status: str,
    features_seen: int | None = None,
    loaded_rows: int | None = None,
    error_message: str = "",
    notes: str = "",
    touch_finished: bool = False,
) -> None:
    finished_sql = "NOW()" if touch_finished else "NULL"
    _psql(
        f"""
        INSERT INTO experiments.ageb_load_status (
            source_method, city_code, city_name, status, started_at, finished_at,
            features_seen, loaded_rows, error_message, notes
        ) VALUES (
            '{SOURCE_METHOD}',
            '{_sql_text(city_code)}',
            '{_sql_text(city_name)}',
            '{_sql_text(status)}',
            NOW(),
            {finished_sql},
            {('NULL' if features_seen is None else str(int(features_seen)))},
            {('NULL' if loaded_rows is None else str(int(loaded_rows)))},
            '{_sql_text(error_message[:1000])}',
            '{_sql_text(notes[:1000])}'
        )
        ON CONFLICT (source_method, city_code) DO UPDATE SET
            city_name = EXCLUDED.city_name,
            status = EXCLUDED.status,
            features_seen = EXCLUDED.features_seen,
            loaded_rows = EXCLUDED.loaded_rows,
            error_message = EXCLUDED.error_message,
            notes = EXCLUDED.notes,
            finished_at = EXCLUDED.finished_at,
            started_at = CASE
                WHEN EXCLUDED.status = 'running' THEN NOW()
                ELSE experiments.ageb_load_status.started_at
            END;
        """
    )


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


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


def _to_int_like(text: str) -> str:
    if not text:
        return "\\N"
    try:
        return str(int(float(str(text).replace(",", "").replace(" ", ""))))
    except Exception:
        return "\\N"


def _tsv_escape(text: str) -> str:
    return text.replace("\t", " ").replace("\n", " ").replace("\r", " ")


def _fetch_ageb_fc(city_code: str) -> dict:
    cve_ent = city_code[:2]
    cve_mun = city_code[2:5]
    url = f"{BASE_URL}/agebu/{cve_ent}/{cve_mun}"
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
    message = f"failed to fetch ageb features for city {city_code} after {FETCH_MAX_ATTEMPTS} attempts"
    if last_error is not None:
        raise RuntimeError(f"{message}: {last_error}") from last_error
    raise RuntimeError(message)


def _copy_city(city_code: str, *, refresh: bool) -> dict[str, int | str]:
    fc = _fetch_ageb_fc(city_code)
    features = fc.get("features") or []
    pop_rows: list[str] = []
    admin_rows: list[str] = []
    city_name = ""

    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry")
        if not geom:
            continue
        unit_code = _prop(props, "cvegeo")
        cve_ent = _prop(props, "cve_ent")
        cve_mun = _prop(props, "cve_mun")
        cve_loc = _prop(props, "cve_loc")
        cve_ageb = _prop(props, "cve_ageb")
        if not unit_code or not cve_ageb:
            continue
        current_city_code = f"{cve_ent}{cve_mun}"
        name = _prop(props, "nom_mun") or city_code
        city_name = city_name or name
        unit_label = cve_ageb
        population = _to_int_like(_prop(props, "pob_total"))
        population_female = _to_int_like(_prop(props, "pob_femenina"))
        population_male = _to_int_like(_prop(props, "pob_masculina"))
        households = _to_int_like(_prop(props, "total_viviendas_habitadas"))
        geom_wkt = shape(geom).wkt

        pop_rows.append(
            "\t".join(
                [
                    _tsv_escape(f"inegi_wscatgeo_agebu_{city_code}.json"),
                    "MX",
                    "ageb_u",
                    _tsv_escape(unit_code),
                    _tsv_escape(unit_label),
                    _tsv_escape(current_city_code),
                    _tsv_escape(name),
                    _tsv_escape(cve_ageb),
                    "",
                    population,
                    households,
                    population_female,
                    population_male,
                    "\\N",
                ]
            )
        )
        admin_rows.append(
            "\t".join(
                [
                    _tsv_escape(f"inegi_wscatgeo_agebu_{city_code}.json"),
                    "MX",
                    "ageb_u",
                    _tsv_escape(unit_code),
                    _tsv_escape(unit_label),
                    _tsv_escape(current_city_code),
                    _tsv_escape(name),
                    _tsv_escape(cve_loc),
                    population,
                    households,
                    population_female,
                    population_male,
                    "\\N",
                    _tsv_escape(geom_wkt),
                ]
            )
        )

    delete_sql = ""
    if refresh:
        delete_sql = f"""
DELETE FROM raw.population_units WHERE level = 'ageb_u' AND city_code = '{_sql_text(city_code)}';
DELETE FROM raw.admin_units WHERE level = 'ageb_u' AND city_code = '{_sql_text(city_code)}';
"""
    pop_block = "".join(line + "\n" for line in pop_rows)
    admin_block = "".join(line + "\n" for line in admin_rows)
    script = f"""
\\set ON_ERROR_STOP on
BEGIN;
{delete_sql}
CREATE TEMP TABLE tmp_ageb_pop (
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
    area_km2 DOUBLE PRECISION
) ON COMMIT DROP;
COPY tmp_ageb_pop (
    source_file, country_code, level, unit_code, unit_label, city_code, city_name,
    ageb_code, manzana_code, population, households, population_female, population_male, area_km2
) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', HEADER false);
{pop_block}\\.

CREATE TEMP TABLE tmp_ageb_admin (
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
COPY tmp_ageb_admin (
    source_file, country_code, level, unit_code, unit_label, city_code, city_name,
    parent_code, population, households, population_female, population_male, area_km2, geom_wkt
) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', HEADER false);
{admin_block}\\.

INSERT INTO raw.population_units (
    source_file, country_code, level, unit_code, unit_label, city_code, city_name,
    ageb_code, manzana_code, population, households, population_female, population_male, area_km2
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
    COALESCE(area_km2, 0.0)
FROM tmp_ageb_pop;

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
FROM tmp_ageb_admin;
COMMIT;
"""
    _psql(script)
    return {
        "city_code": city_code,
        "city_name": city_name,
        "features_seen": len(features),
        "ageb_loaded": len(pop_rows),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="National resumable AGEB urban loader for all city-level units in urban_sami_exp.")
    parser.add_argument("--city-code", action="append", dest="city_codes", default=[], help="Optional specific city_code to process; repeatable.")
    parser.add_argument("--refresh-existing", action="store_true", help="Reload cities even if AGEB rows already exist.")
    parser.add_argument("--retry-errors-only", action="store_true", help="Process only cities currently marked as error for this source method.")
    parser.add_argument("--limit", type=int, default=0, help="Optional max number of cities to process in this run.")
    args = parser.parse_args()

    _bootstrap()
    outdir = Path(__file__).resolve().parents[1] / "reports" / "ageb-load-national-2026-04-24"
    outdir.mkdir(parents=True, exist_ok=True)

    if args.retry_errors_only:
        city_rows = _error_city_rows()
    else:
        city_rows = _all_city_rows()
        if args.city_codes:
            wanted = {str(code).strip() for code in args.city_codes if str(code).strip()}
            city_rows = [row for row in city_rows if row["city_code"] in wanted]

    rows: list[dict[str, object]] = []
    processed = 0
    for city in city_rows:
        city_code = city["city_code"]
        city_name = city["city_name"]
        if args.limit and processed >= args.limit:
            break
        processed += 1

        if not args.retry_errors_only and not args.refresh_existing:
            if _marked_success(city_code) or _city_has_complete_ageb(city_code):
                note = "skipped_existing" if _city_has_complete_ageb(city_code) else "skipped_status_success"
                _set_status(
                    city_code=city_code,
                    city_name=city_name,
                    status="success",
                    notes=note,
                    touch_finished=True,
                )
                rows.append(
                    {
                        "city_code": city_code,
                        "city_name": city_name,
                        "status": note,
                        "features_seen": "",
                        "ageb_loaded": "",
                    }
                )
                if rows:
                    _write_csv(outdir / "city_load_summary.csv", rows, list(rows[0].keys()))
                print(f"[skip] {city_code} {city_name}")
                continue

        _set_status(city_code=city_code, city_name=city_name, status="running", notes="")
        try:
            result = _copy_city(city_code, refresh=True)
            _set_status(
                city_code=city_code,
                city_name=str(result.get("city_name") or city_name),
                status="success",
                features_seen=int(result.get("features_seen") or 0),
                loaded_rows=int(result.get("ageb_loaded") or 0),
                notes="loaded",
                touch_finished=True,
            )
            rows.append(
                {
                    "city_code": city_code,
                    "city_name": result.get("city_name") or city_name,
                    "status": "loaded",
                    "features_seen": result.get("features_seen") or 0,
                    "ageb_loaded": result.get("ageb_loaded") or 0,
                }
            )
            _write_csv(outdir / "city_load_summary.csv", rows, list(rows[0].keys()))
            print(f"[ok] {city_code} {result.get('city_name') or city_name} rows={result.get('ageb_loaded')}")
        except Exception as exc:
            _set_status(
                city_code=city_code,
                city_name=city_name,
                status="error",
                error_message=str(exc),
                notes="load_failed",
                touch_finished=True,
            )
            rows.append(
                {
                    "city_code": city_code,
                    "city_name": city_name,
                    "status": "error",
                    "features_seen": "",
                    "ageb_loaded": "",
                }
            )
            _write_csv(outdir / "city_load_summary.csv", rows, list(rows[0].keys()))
            print(f"[error] {city_code} {city_name}: {exc}")

    summary_sql = f"""
    SELECT 'ageb_population_rows', count(*)::text FROM raw.population_units WHERE level = 'ageb_u'
    UNION ALL
    SELECT 'ageb_admin_rows', count(*)::text FROM raw.admin_units WHERE level = 'ageb_u'
    UNION ALL
    SELECT 'ageb_cities', count(DISTINCT city_code)::text FROM raw.population_units WHERE level = 'ageb_u'
    UNION ALL
    SELECT 'status_success', count(*)::text FROM experiments.ageb_load_status WHERE source_method = '{SOURCE_METHOD}' AND status = 'success'
    UNION ALL
    SELECT 'status_error', count(*)::text FROM experiments.ageb_load_status WHERE source_method = '{SOURCE_METHOD}' AND status = 'error'
    UNION ALL
    SELECT 'status_running', count(*)::text FROM experiments.ageb_load_status WHERE source_method = '{SOURCE_METHOD}' AND status = 'running';
    """
    summary_rows = []
    for line in _psql(summary_sql, capture_output=True).splitlines():
        if not line.strip():
            continue
        key, value = line.split("\t", 1)
        summary_rows.append({"metric": key, "value": value})
    _write_csv(outdir / "summary.csv", summary_rows, ["metric", "value"])
    report = [
        "# AGEB National Load",
        "",
        f"- source method: `{SOURCE_METHOD}`",
        "",
        "Files:",
        f"- [summary.csv]({(outdir / 'summary.csv').resolve()})",
        f"- [city_load_summary.csv]({(outdir / 'city_load_summary.csv').resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
