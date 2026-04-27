#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
from pathlib import Path


WINDOWS_DOCKER = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DOCKER_EXE = os.environ.get("DOCKER_EXE") or shutil.which("docker") or WINDOWS_DOCKER
DB_CONTAINER = os.environ.get("DB_CONTAINER", "24-polisplexity-core-db-dev")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_NAME = os.environ.get("DB_NAME", "urban_sami_exp")
ECONOMIC_SOURCE_METHOD = "denue_ageb_mix_v1"
NETWORK_SOURCE_METHOD = "osm_drive_ageb_overlay_v1"
CITY_NETWORK_SOURCE = "osm_drive_municipal_full_v1"


def _sql_text(value: object) -> str:
    return str(value).replace("'", "''")


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


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _size_case_sql(column: str = "per_ocu") -> str:
    return f"""
CASE
    WHEN lower(coalesce({column}, '')) = '' THEN 'unknown'
    WHEN lower(coalesce({column}, '')) ~ '(251 y mas|251\\+|mas de 250)' THEN 'large'
    WHEN lower(coalesce({column}, '')) ~ '(101 a 250|101-250|51 a 100|51-100)' THEN 'medium'
    WHEN lower(coalesce({column}, '')) ~ '(31 a 50|31-50|11 a 30|11-30)' THEN 'small'
    WHEN lower(coalesce({column}, '')) ~ '(0 a 5|1 a 5|0-5|1-5|6 a 10|6-10|hasta 10)' THEN 'micro'
    ELSE
        CASE
            WHEN regexp_replace(lower(coalesce({column}, '')), '[^0-9]+', ' ', 'g') ~ '^ *$' THEN 'unknown'
            ELSE
                CASE
                    WHEN (
                        SELECT max(x::int)
                        FROM unnest(regexp_split_to_array(trim(regexp_replace(lower(coalesce({column}, '')), '[^0-9]+', ' ', 'g')), ' +')) AS x
                    ) <= 10 THEN 'micro'
                    WHEN (
                        SELECT max(x::int)
                        FROM unnest(regexp_split_to_array(trim(regexp_replace(lower(coalesce({column}, '')), '[^0-9]+', ' ', 'g')), ' +')) AS x
                    ) <= 50 THEN 'small'
                    WHEN (
                        SELECT max(x::int)
                        FROM unnest(regexp_split_to_array(trim(regexp_replace(lower(coalesce({column}, '')), '[^0-9]+', ' ', 'g')), ' +')) AS x
                    ) <= 250 THEN 'medium'
                    ELSE 'large'
                END
        END
END
""".strip()


def _bootstrap() -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_experiment_db.sh"
    subprocess.run(["bash", str(script)], check=True)


def materialize_ageb_economic_mix(refresh: bool) -> None:
    delete_sql = ""
    if refresh:
        delete_sql = f"DELETE FROM derived.ageb_economic_mix WHERE source_method = '{ECONOMIC_SOURCE_METHOD}';"
    size_case = _size_case_sql("per_ocu")
    sql = f"""
\\set ON_ERROR_STOP on
BEGIN;
{delete_sql}
WITH ageb_base AS (
    SELECT
        unit_code AS ageb_code,
        unit_label AS ageb_label,
        RIGHT(unit_code, 4) AS local_ageb_code,
        city_code,
        city_name,
        LEFT(city_code, 2) AS state_code,
        COALESCE(population, 0.0) AS population,
        COALESCE(households, 0.0) AS occupied_dwellings,
        ST_Area(geography(geom)) / 1000000.0 AS area_km2
    FROM raw.admin_units
    WHERE level = 'ageb_u'
),
denue_clean AS (
    SELECT
        city_code,
        ageb_code AS local_ageb_code,
        LEFT(regexp_replace(COALESCE(scian_code, ''), '[^0-9]', '', 'g'), 2) AS scian2,
        COALESCE(per_ocu, '') AS per_ocu,
        {size_case} AS size_class
    FROM raw.denue_establishments
    WHERE city_code <> ''
      AND ageb_code <> ''
),
est_totals AS (
    SELECT city_code, local_ageb_code, COUNT(*)::double precision AS est_count
    FROM denue_clean
    GROUP BY city_code, local_ageb_code
),
category_rows AS (
    SELECT city_code, local_ageb_code, 'scian2'::text AS family, scian2 AS category, COUNT(*)::double precision AS est_count
    FROM denue_clean
    WHERE scian2 <> ''
    GROUP BY city_code, local_ageb_code, scian2
    UNION ALL
    SELECT city_code, local_ageb_code, 'per_ocu'::text AS family, per_ocu AS category, COUNT(*)::double precision AS est_count
    FROM denue_clean
    WHERE per_ocu <> ''
    GROUP BY city_code, local_ageb_code, per_ocu
    UNION ALL
    SELECT city_code, local_ageb_code, 'size_class'::text AS family, size_class AS category, COUNT(*)::double precision AS est_count
    FROM denue_clean
    WHERE size_class <> '' AND size_class <> 'unknown'
    GROUP BY city_code, local_ageb_code, size_class
    UNION ALL
    SELECT city_code, local_ageb_code, 'scian2_size_class'::text AS family, scian2 || '|' || size_class AS category, COUNT(*)::double precision AS est_count
    FROM denue_clean
    WHERE scian2 <> '' AND size_class <> '' AND size_class <> 'unknown'
    GROUP BY city_code, local_ageb_code, scian2, size_class
),
city_family_totals AS (
    SELECT city_code, family, SUM(est_count)::double precision AS total_est_city_family
    FROM (
        SELECT city_code, 'total'::text AS family, est_count FROM est_totals
        UNION ALL
        SELECT city_code, family, est_count FROM category_rows
    ) x
    GROUP BY city_code, family
),
total_rows AS (
    SELECT
        a.city_code,
        a.city_name,
        a.state_code,
        a.ageb_code,
        a.ageb_label,
        'total'::text AS family,
        'all'::text AS category,
        'all establishments'::text AS category_label,
        COALESCE(t.est_count, 0.0) AS est_count,
        CASE WHEN COALESCE(t.est_count, 0.0) > 0 THEN 1.0 ELSE 0.0 END AS est_share_within_ageb,
        CASE WHEN COALESCE(cf.total_est_city_family, 0.0) > 0 THEN COALESCE(t.est_count, 0.0) / cf.total_est_city_family ELSE 0.0 END AS est_share_within_city_family,
        COALESCE(t.est_count, 0.0) AS total_est_ageb,
        COALESCE(cf.total_est_city_family, 0.0) AS total_est_city_family,
        a.population,
        a.occupied_dwellings,
        a.area_km2,
        CASE WHEN a.area_km2 > 0 THEN a.population / a.area_km2 ELSE 0.0 END AS rho_pop,
        CASE WHEN a.area_km2 > 0 THEN a.occupied_dwellings / a.area_km2 ELSE 0.0 END AS rho_dwellings
    FROM ageb_base a
    LEFT JOIN est_totals t
      ON t.city_code = a.city_code
     AND t.local_ageb_code = a.local_ageb_code
    LEFT JOIN city_family_totals cf
      ON cf.city_code = a.city_code
     AND cf.family = 'total'
),
category_out AS (
    SELECT
        a.city_code,
        a.city_name,
        a.state_code,
        a.ageb_code,
        a.ageb_label,
        c.family,
        c.category,
        c.category AS category_label,
        c.est_count,
        CASE WHEN COALESCE(t.est_count, 0.0) > 0 THEN c.est_count / t.est_count ELSE 0.0 END AS est_share_within_ageb,
        CASE WHEN COALESCE(cf.total_est_city_family, 0.0) > 0 THEN c.est_count / cf.total_est_city_family ELSE 0.0 END AS est_share_within_city_family,
        COALESCE(t.est_count, 0.0) AS total_est_ageb,
        COALESCE(cf.total_est_city_family, 0.0) AS total_est_city_family,
        a.population,
        a.occupied_dwellings,
        a.area_km2,
        CASE WHEN a.area_km2 > 0 THEN a.population / a.area_km2 ELSE 0.0 END AS rho_pop,
        CASE WHEN a.area_km2 > 0 THEN a.occupied_dwellings / a.area_km2 ELSE 0.0 END AS rho_dwellings
    FROM category_rows c
    JOIN ageb_base a
      ON a.city_code = c.city_code
     AND a.local_ageb_code = c.local_ageb_code
    LEFT JOIN est_totals t
      ON t.city_code = c.city_code
     AND t.local_ageb_code = c.local_ageb_code
    LEFT JOIN city_family_totals cf
      ON cf.city_code = c.city_code
     AND cf.family = c.family
)
INSERT INTO derived.ageb_economic_mix (
    source_file, source_method, city_code, city_name, state_code, ageb_code, ageb_label,
    family, category, category_label, est_count, est_share_within_ageb, est_share_within_city_family,
    total_est_ageb, total_est_city_family, population, occupied_dwellings, area_km2,
    rho_pop, rho_dwellings, notes
)
SELECT
    'raw.denue_establishments+raw.admin_units',
    '{ECONOMIC_SOURCE_METHOD}',
    city_code,
    city_name,
    state_code,
    ageb_code,
    ageb_label,
    family,
    category,
    category_label,
    est_count,
    est_share_within_ageb,
    est_share_within_city_family,
    total_est_ageb,
    total_est_city_family,
    population,
    occupied_dwellings,
    area_km2,
    rho_pop,
    rho_dwellings,
    ''
FROM total_rows
UNION ALL
SELECT
    'raw.denue_establishments+raw.admin_units',
    '{ECONOMIC_SOURCE_METHOD}',
    city_code,
    city_name,
    state_code,
    ageb_code,
    ageb_label,
    family,
    category,
    category_label,
    est_count,
    est_share_within_ageb,
    est_share_within_city_family,
    total_est_ageb,
    total_est_city_family,
    population,
    occupied_dwellings,
    area_km2,
    rho_pop,
    rho_dwellings,
    ''
FROM category_out
ON CONFLICT (source_method, city_code, ageb_code, family, category) DO UPDATE SET
    source_file = EXCLUDED.source_file,
    city_name = EXCLUDED.city_name,
    state_code = EXCLUDED.state_code,
    ageb_label = EXCLUDED.ageb_label,
    category_label = EXCLUDED.category_label,
    est_count = EXCLUDED.est_count,
    est_share_within_ageb = EXCLUDED.est_share_within_ageb,
    est_share_within_city_family = EXCLUDED.est_share_within_city_family,
    total_est_ageb = EXCLUDED.total_est_ageb,
    total_est_city_family = EXCLUDED.total_est_city_family,
    population = EXCLUDED.population,
    occupied_dwellings = EXCLUDED.occupied_dwellings,
    area_km2 = EXCLUDED.area_km2,
    rho_pop = EXCLUDED.rho_pop,
    rho_dwellings = EXCLUDED.rho_dwellings,
    notes = EXCLUDED.notes;
COMMIT;
"""
    _psql(sql)


def _ageb_cities() -> list[str]:
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


def materialize_ageb_network_metrics(refresh: bool) -> None:
    if refresh:
        _psql(
            f"""
            \\set ON_ERROR_STOP on
            DELETE FROM derived.ageb_network_metrics
            WHERE source_method = '{NETWORK_SOURCE_METHOD}';
            """
        )
    for city_code in _ageb_cities():
        sql = f"""
\\set ON_ERROR_STOP on
BEGIN;
DELETE FROM derived.ageb_network_metrics
WHERE source_method = '{NETWORK_SOURCE_METHOD}'
  AND city_code = '{_sql_text(city_code)}';

CREATE TEMP TABLE tmp_ageb_base ON COMMIT DROP AS
SELECT
    unit_code AS ageb_code,
    unit_label AS ageb_label,
    RIGHT(unit_code, 4) AS local_ageb_code,
    city_code,
    city_name,
    LEFT(city_code, 2) AS state_code,
    COALESCE(population, 0.0) AS population,
    COALESCE(households, 0.0) AS occupied_dwellings,
    ST_Multi(geom) AS geom,
    ST_Area(geography(geom)) / 1000000.0 AS area_km2,
    ST_Perimeter(geography(geom)) / 1000.0 AS perimeter_km
FROM raw.admin_units
WHERE level = 'ageb_u'
  AND city_code = '{_sql_text(city_code)}';

CREATE INDEX tmp_ageb_base_geom_gix ON tmp_ageb_base USING GIST (geom);
CREATE INDEX tmp_ageb_base_ageb_idx ON tmp_ageb_base (ageb_code);

CREATE TEMP TABLE tmp_ageb_est ON COMMIT DROP AS
SELECT
    ageb_code AS local_ageb_code,
    COUNT(*)::double precision AS est_total
FROM raw.denue_establishments
WHERE city_code = '{_sql_text(city_code)}'
  AND ageb_code <> ''
GROUP BY ageb_code;

CREATE INDEX tmp_ageb_est_ageb_idx ON tmp_ageb_est (local_ageb_code);

CREATE TEMP TABLE tmp_node_metrics ON COMMIT DROP AS
SELECT
    a.ageb_code,
    COUNT(DISTINCT n.node_osmid)::double precision AS n_nodes,
    COUNT(DISTINCT n.node_osmid) FILTER (WHERE COALESCE(n.street_count, 0) > 1)::double precision AS intersection_count,
    AVG(COALESCE(n.street_count, 0.0))::double precision AS streets_per_node_avg,
    AVG(COALESCE(n.degree, 0.0))::double precision AS mean_degree,
    SUM(COALESCE(n.degree, 0.0))::double precision AS sum_degree
FROM tmp_ageb_base a
LEFT JOIN derived.city_network_nodes n
  ON n.source_method = '{CITY_NETWORK_SOURCE}'
 AND n.city_code = a.city_code
 AND ST_Intersects(a.geom, COALESCE(n.geom, ST_SetSRID(ST_MakePoint(n.x, n.y), 4326)))
GROUP BY a.ageb_code;

CREATE INDEX tmp_node_metrics_ageb_idx ON tmp_node_metrics (ageb_code);

CREATE TEMP TABLE tmp_edge_segments ON COMMIT DROP AS
SELECT
    a.ageb_code,
    e.row_id AS edge_row_id,
    ST_Length(geography(ST_CollectionExtract(ST_Intersection(e.geom, a.geom), 2)))::double precision AS clipped_length_m,
    COALESCE(e.length_m, ST_Length(geography(e.geom)))::double precision AS full_length_m,
    CASE WHEN ST_Crosses(e.geom, ST_Boundary(a.geom)) THEN 1 ELSE 0 END::double precision AS boundary_cross,
    CASE
        WHEN nu.geom IS NOT NULL
         AND nv.geom IS NOT NULL
         AND ST_Distance(geography(nu.geom), geography(nv.geom)) > 0
        THEN COALESCE(e.length_m, ST_Length(geography(e.geom))) / ST_Distance(geography(nu.geom), geography(nv.geom))
        WHEN nu.x IS NOT NULL
         AND nu.y IS NOT NULL
         AND nv.x IS NOT NULL
         AND nv.y IS NOT NULL
         AND ST_Distance(
                geography(ST_SetSRID(ST_MakePoint(nu.x, nu.y), 4326)),
                geography(ST_SetSRID(ST_MakePoint(nv.x, nv.y), 4326))
             ) > 0
        THEN COALESCE(e.length_m, ST_Length(geography(e.geom))) / ST_Distance(
                geography(ST_SetSRID(ST_MakePoint(nu.x, nu.y), 4326)),
                geography(ST_SetSRID(ST_MakePoint(nv.x, nv.y), 4326))
             )
        ELSE NULL
    END::double precision AS edge_circuity
FROM tmp_ageb_base a
JOIN derived.city_network_edges e
  ON e.source_method = '{CITY_NETWORK_SOURCE}'
 AND e.city_code = a.city_code
 AND ST_Intersects(a.geom, e.geom)
LEFT JOIN derived.city_network_nodes nu
  ON nu.source_method = e.source_method
 AND nu.city_code = e.city_code
 AND nu.node_osmid = e.u_osmid
LEFT JOIN derived.city_network_nodes nv
  ON nv.source_method = e.source_method
 AND nv.city_code = e.city_code
 AND nv.node_osmid = e.v_osmid;

CREATE INDEX tmp_edge_segments_ageb_idx ON tmp_edge_segments (ageb_code);

CREATE TEMP TABLE tmp_edge_metrics ON COMMIT DROP AS
SELECT
    ageb_code,
    COUNT(DISTINCT edge_row_id)::double precision AS n_edges,
    SUM(clipped_length_m)::double precision / 1000.0 AS street_length_total_km,
    AVG(clipped_length_m)::double precision AS edge_length_avg_m,
    AVG(edge_circuity)::double precision AS circuity_avg,
    SUM(boundary_cross)::double precision AS boundary_entry_edges
FROM tmp_edge_segments
WHERE clipped_length_m > 0
GROUP BY ageb_code;

CREATE INDEX tmp_edge_metrics_ageb_idx ON tmp_edge_metrics (ageb_code);

INSERT INTO derived.ageb_network_metrics (
    source_file, source_method, city_code, city_name, state_code, ageb_code, ageb_label,
    population, occupied_dwellings, est_total, ageb_area_km2, ageb_perimeter_km, rho_pop, rho_dwellings,
    n_nodes, n_edges, intersection_count, streets_per_node_avg, street_length_total_km, street_density_km_per_km2,
    intersection_density_km2, edge_length_avg_m, circuity_avg, mean_degree, sum_degree,
    boundary_entry_edges, boundary_entry_edges_per_km, notes
)
SELECT
    'raw.admin_units+derived.city_network_nodes+derived.city_network_edges',
    '{NETWORK_SOURCE_METHOD}',
    a.city_code,
    a.city_name,
    a.state_code,
    a.ageb_code,
    a.ageb_label,
    a.population,
    a.occupied_dwellings,
    COALESCE(est.est_total, 0.0) AS est_total,
    a.area_km2,
    a.perimeter_km,
    CASE WHEN a.area_km2 > 0 THEN a.population / a.area_km2 ELSE 0.0 END AS rho_pop,
    CASE WHEN a.area_km2 > 0 THEN a.occupied_dwellings / a.area_km2 ELSE 0.0 END AS rho_dwellings,
    COALESCE(nm.n_nodes, 0.0),
    COALESCE(em.n_edges, 0.0),
    COALESCE(nm.intersection_count, 0.0),
    COALESCE(nm.streets_per_node_avg, 0.0),
    COALESCE(em.street_length_total_km, 0.0),
    CASE WHEN a.area_km2 > 0 THEN COALESCE(em.street_length_total_km, 0.0) / a.area_km2 ELSE 0.0 END AS street_density_km_per_km2,
    CASE WHEN a.area_km2 > 0 THEN COALESCE(nm.intersection_count, 0.0) / a.area_km2 ELSE 0.0 END AS intersection_density_km2,
    COALESCE(em.edge_length_avg_m, 0.0),
    COALESCE(em.circuity_avg, 0.0),
    COALESCE(nm.mean_degree, 0.0),
    COALESCE(nm.sum_degree, 0.0),
    COALESCE(em.boundary_entry_edges, 0.0),
    CASE WHEN a.perimeter_km > 0 THEN COALESCE(em.boundary_entry_edges, 0.0) / a.perimeter_km ELSE 0.0 END AS boundary_entry_edges_per_km,
    ''
FROM tmp_ageb_base a
LEFT JOIN tmp_ageb_est est
  ON est.local_ageb_code = a.local_ageb_code
LEFT JOIN tmp_node_metrics nm
  ON nm.ageb_code = a.ageb_code
LEFT JOIN tmp_edge_metrics em
  ON em.ageb_code = a.ageb_code
ON CONFLICT (source_method, city_code, ageb_code) DO UPDATE SET
    source_file = EXCLUDED.source_file,
    city_name = EXCLUDED.city_name,
    state_code = EXCLUDED.state_code,
    ageb_label = EXCLUDED.ageb_label,
    population = EXCLUDED.population,
    occupied_dwellings = EXCLUDED.occupied_dwellings,
    est_total = EXCLUDED.est_total,
    ageb_area_km2 = EXCLUDED.ageb_area_km2,
    ageb_perimeter_km = EXCLUDED.ageb_perimeter_km,
    rho_pop = EXCLUDED.rho_pop,
    rho_dwellings = EXCLUDED.rho_dwellings,
    n_nodes = EXCLUDED.n_nodes,
    n_edges = EXCLUDED.n_edges,
    intersection_count = EXCLUDED.intersection_count,
    streets_per_node_avg = EXCLUDED.streets_per_node_avg,
    street_length_total_km = EXCLUDED.street_length_total_km,
    street_density_km_per_km2 = EXCLUDED.street_density_km_per_km2,
    intersection_density_km2 = EXCLUDED.intersection_density_km2,
    edge_length_avg_m = EXCLUDED.edge_length_avg_m,
    circuity_avg = EXCLUDED.circuity_avg,
    mean_degree = EXCLUDED.mean_degree,
    sum_degree = EXCLUDED.sum_degree,
    boundary_entry_edges = EXCLUDED.boundary_entry_edges,
    boundary_entry_edges_per_km = EXCLUDED.boundary_entry_edges_per_km,
    notes = EXCLUDED.notes;
COMMIT;
"""
        _psql(sql)


def write_summary_report(outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    summary_sql = f"""
    SELECT 'ageb_economic_mix_rows', count(*)::text
    FROM derived.ageb_economic_mix
    WHERE source_method = '{ECONOMIC_SOURCE_METHOD}'
    UNION ALL
    SELECT 'ageb_economic_mix_cities', count(DISTINCT city_code)::text
    FROM derived.ageb_economic_mix
    WHERE source_method = '{ECONOMIC_SOURCE_METHOD}'
    UNION ALL
    SELECT 'ageb_network_metrics_rows', count(*)::text
    FROM derived.ageb_network_metrics
    WHERE source_method = '{NETWORK_SOURCE_METHOD}'
    UNION ALL
    SELECT 'ageb_network_metrics_cities', count(DISTINCT city_code)::text
    FROM derived.ageb_network_metrics
    WHERE source_method = '{NETWORK_SOURCE_METHOD}';
    """
    rows = []
    for line in _psql(summary_sql, capture_output=True).splitlines():
        if not line.strip():
            continue
        key, value = line.split("\t", 1)
        rows.append({"metric": key, "value": value})
    _write_csv(outdir / "summary.csv", rows, ["metric", "value"])

    samples_sql = f"""
    SELECT city_code, ageb_code, family, category, est_count::text, est_share_within_ageb::text
    FROM derived.ageb_economic_mix
    WHERE source_method = '{ECONOMIC_SOURCE_METHOD}'
    ORDER BY city_code, ageb_code, family, category
    LIMIT 20;
    """
    sample_rows = []
    for line in _psql(samples_sql, capture_output=True).splitlines():
        if not line.strip():
            continue
        city_code, ageb_code, family, category, est_count, share = line.split("\t")
        sample_rows.append(
            {
                "city_code": city_code,
                "ageb_code": ageb_code,
                "family": family,
                "category": category,
                "est_count": est_count,
                "share_within_ageb": share,
            }
        )
    if sample_rows:
        _write_csv(outdir / "ageb_economic_mix_sample.csv", sample_rows, list(sample_rows[0].keys()))

    network_sql = f"""
    SELECT city_code, ageb_code, n_nodes::text, n_edges::text, street_density_km_per_km2::text, boundary_entry_edges_per_km::text
    FROM derived.ageb_network_metrics
    WHERE source_method = '{NETWORK_SOURCE_METHOD}'
    ORDER BY city_code, ageb_code
    LIMIT 20;
    """
    network_rows = []
    for line in _psql(network_sql, capture_output=True).splitlines():
        if not line.strip():
            continue
        city_code, ageb_code, n_nodes, n_edges, street_density, boundary_per_km = line.split("\t")
        network_rows.append(
            {
                "city_code": city_code,
                "ageb_code": ageb_code,
                "n_nodes": n_nodes,
                "n_edges": n_edges,
                "street_density_km_per_km2": street_density,
                "boundary_entry_edges_per_km": boundary_per_km,
            }
        )
    if network_rows:
        _write_csv(outdir / "ageb_network_metrics_sample.csv", network_rows, list(network_rows[0].keys()))

    report = [
        "# AGEB Derived Materialization",
        "",
        f"- `derived.ageb_economic_mix` source method: `{ECONOMIC_SOURCE_METHOD}`",
        f"- `derived.ageb_network_metrics` source method: `{NETWORK_SOURCE_METHOD}`",
        f"- city OSM base source: `{CITY_NETWORK_SOURCE}`",
        "",
        "Files:",
        f"- [summary.csv]({(outdir / 'summary.csv').resolve()})",
        f"- [ageb_economic_mix_sample.csv]({(outdir / 'ageb_economic_mix_sample.csv').resolve()})",
        f"- [ageb_network_metrics_sample.csv]({(outdir / 'ageb_network_metrics_sample.csv').resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize AGEB economic-mix and AGEB network-metrics tables.")
    parser.add_argument("--no-refresh", action="store_true", help="Append/update without truncating existing rows for these source methods.")
    parser.add_argument(
        "--only",
        choices=("all", "economic", "network"),
        default="all",
        help="Materialize all tables or only one branch.",
    )
    args = parser.parse_args()

    refresh = not args.no_refresh
    _bootstrap()
    if args.only in {"all", "economic"}:
        materialize_ageb_economic_mix(refresh=refresh)
    if args.only in {"all", "network"}:
        materialize_ageb_network_metrics(refresh=refresh)
    outdir = Path(__file__).resolve().parents[1] / "reports" / "ageb-derived-materialization-2026-04-24"
    write_summary_report(outdir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
