#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import subprocess
from pathlib import Path

from shapely.geometry import MultiPolygon, shape


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


def _sql_num(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "NULL"
    return raw


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_geom_map(geojson_dir: Path, city_codes: set[str]) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for path in sorted(geojson_dir.glob("*.geojson")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        for feat in doc.get("features", []):
            props = feat.get("properties", {}) or {}
            code = str(props.get("cvegeo", "")).strip()
            if code not in city_codes:
                continue
            geom = shape(feat.get("geometry"))
            if geom.geom_type == "Polygon":
                geom = MultiPolygon([geom])
            elif geom.geom_type != "MultiPolygon":
                geom = MultiPolygon([geom.buffer(0)])
            out[code] = {
                "source_file": path.name,
                "city_code": code,
                "city_name": str(props.get("nomgeo", "")).strip(),
                "state_code": str(props.get("cve_ent", "")).strip(),
                "geom_wkt": geom.wkt,
            }
        if city_codes.issubset(out.keys()):
            break
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Load city-scale OSM network metrics into urban_sami_exp.derived tables")
    parser.add_argument(
        "--metrics-csv",
        type=Path,
        default=Path("reports/city-connectivity-power-laws-2026-04-23/city_connectivity_rows.csv"),
    )
    parser.add_argument(
        "--municipal-geojson-dir",
        type=Path,
        default=Path("data/raw/inegi_municipal_geojson"),
    )
    parser.add_argument("--metrics-source-method", default="osm_drive_municipal_v1")
    parser.add_argument("--geom-source-method", default="inegi_municipal_geojson_v1")
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    rows = _read_csv(args.metrics_csv)
    city_codes = {str(r["city_code"]).zfill(5) for r in rows}
    geom_map = _load_geom_map(args.municipal_geojson_dir, city_codes)

    if args.truncate:
        _psql("TRUNCATE TABLE derived.city_network_metrics;")
        _psql("TRUNCATE TABLE derived.city_network_geoms;")

    metric_values: list[str] = []
    geom_values: list[str] = []
    metric_count = 0
    geom_count = 0

    for row in rows:
        city_code = str(row["city_code"]).zfill(5)
        metric_values.append(
            "("
            f"'{_sql_text(args.metrics_csv.name)}',"
            f"'{_sql_text(args.metrics_source_method)}',"
            f"'{_sql_text(city_code)}',"
            f"'{_sql_text(row.get('city_name', ''))}',"
            f"'{_sql_text(row.get('state_code', ''))}',"
            f"{_sql_num(row.get('population'))},"
            f"{_sql_num(row.get('occupied_dwellings'))},"
            f"{_sql_num(row.get('est_total'))},"
            f"{_sql_num(row.get('city_area_km2') or row.get('urban_area_km2'))},"
            f"{_sql_num(row.get('city_perimeter_km') or row.get('urban_perimeter_km'))},"
            f"{_sql_num(row.get('rho_pop'))},"
            f"{_sql_num(row.get('rho_dwellings'))},"
            f"{_sql_num(row.get('n_nodes'))},"
            f"{_sql_num(row.get('n_edges'))},"
            f"{_sql_num(row.get('intersection_count'))},"
            f"{_sql_num(row.get('streets_per_node_avg'))},"
            f"{_sql_num(row.get('street_length_total_km'))},"
            f"{_sql_num(row.get('street_density_km_per_km2'))},"
            f"{_sql_num(row.get('intersection_density_km2'))},"
            f"{_sql_num(row.get('edge_length_avg_m'))},"
            f"{_sql_num(row.get('circuity_avg'))},"
            f"{_sql_num(row.get('mean_degree'))},"
            f"{_sql_num(row.get('sum_degree'))},"
            f"{_sql_num(row.get('boundary_entry_edges'))},"
            f"{_sql_num(row.get('boundary_entry_edges_per_km'))},"
            f"''"
            ")"
        )
        metric_count += 1
        if len(metric_values) >= args.batch_size:
            _psql(
                "INSERT INTO derived.city_network_metrics "
                "(source_file,source_method,city_code,city_name,state_code,population,occupied_dwellings,est_total,city_area_km2,city_perimeter_km,rho_pop,rho_dwellings,n_nodes,n_edges,intersection_count,streets_per_node_avg,street_length_total_km,street_density_km_per_km2,intersection_density_km2,edge_length_avg_m,circuity_avg,mean_degree,sum_degree,boundary_entry_edges,boundary_entry_edges_per_km,notes) VALUES "
                + ",".join(metric_values)
                + " ON CONFLICT (source_method, city_code) DO UPDATE SET "
                "source_file=EXCLUDED.source_file, city_name=EXCLUDED.city_name, state_code=EXCLUDED.state_code, "
                "population=EXCLUDED.population, occupied_dwellings=EXCLUDED.occupied_dwellings, est_total=EXCLUDED.est_total, "
                "city_area_km2=EXCLUDED.city_area_km2, city_perimeter_km=EXCLUDED.city_perimeter_km, rho_pop=EXCLUDED.rho_pop, rho_dwellings=EXCLUDED.rho_dwellings, "
                "n_nodes=EXCLUDED.n_nodes, n_edges=EXCLUDED.n_edges, intersection_count=EXCLUDED.intersection_count, streets_per_node_avg=EXCLUDED.streets_per_node_avg, "
                "street_length_total_km=EXCLUDED.street_length_total_km, street_density_km_per_km2=EXCLUDED.street_density_km_per_km2, intersection_density_km2=EXCLUDED.intersection_density_km2, "
                "edge_length_avg_m=EXCLUDED.edge_length_avg_m, circuity_avg=EXCLUDED.circuity_avg, mean_degree=EXCLUDED.mean_degree, sum_degree=EXCLUDED.sum_degree, "
                "boundary_entry_edges=EXCLUDED.boundary_entry_edges, boundary_entry_edges_per_km=EXCLUDED.boundary_entry_edges_per_km, notes=EXCLUDED.notes;"
            )
            metric_values = []

    if metric_values:
        _psql(
            "INSERT INTO derived.city_network_metrics "
            "(source_file,source_method,city_code,city_name,state_code,population,occupied_dwellings,est_total,city_area_km2,city_perimeter_km,rho_pop,rho_dwellings,n_nodes,n_edges,intersection_count,streets_per_node_avg,street_length_total_km,street_density_km_per_km2,intersection_density_km2,edge_length_avg_m,circuity_avg,mean_degree,sum_degree,boundary_entry_edges,boundary_entry_edges_per_km,notes) VALUES "
            + ",".join(metric_values)
            + " ON CONFLICT (source_method, city_code) DO UPDATE SET "
            "source_file=EXCLUDED.source_file, city_name=EXCLUDED.city_name, state_code=EXCLUDED.state_code, "
            "population=EXCLUDED.population, occupied_dwellings=EXCLUDED.occupied_dwellings, est_total=EXCLUDED.est_total, "
            "city_area_km2=EXCLUDED.city_area_km2, city_perimeter_km=EXCLUDED.city_perimeter_km, rho_pop=EXCLUDED.rho_pop, rho_dwellings=EXCLUDED.rho_dwellings, "
            "n_nodes=EXCLUDED.n_nodes, n_edges=EXCLUDED.n_edges, intersection_count=EXCLUDED.intersection_count, streets_per_node_avg=EXCLUDED.streets_per_node_avg, "
            "street_length_total_km=EXCLUDED.street_length_total_km, street_density_km_per_km2=EXCLUDED.street_density_km_per_km2, intersection_density_km2=EXCLUDED.intersection_density_km2, "
            "edge_length_avg_m=EXCLUDED.edge_length_avg_m, circuity_avg=EXCLUDED.circuity_avg, mean_degree=EXCLUDED.mean_degree, sum_degree=EXCLUDED.sum_degree, "
            "boundary_entry_edges=EXCLUDED.boundary_entry_edges, boundary_entry_edges_per_km=EXCLUDED.boundary_entry_edges_per_km, notes=EXCLUDED.notes;"
        )

    for city_code in sorted(city_codes):
        geom = geom_map.get(city_code)
        if not geom:
            continue
        geom_values.append(
            "("
            f"'{_sql_text(geom['source_file'])}',"
            f"'{_sql_text(args.geom_source_method)}',"
            f"'{_sql_text(city_code)}',"
            f"'{_sql_text(geom['city_name'])}',"
            f"'{_sql_text(geom['state_code'])}',"
            "NULL,NULL,"
            f"ST_SetSRID(ST_GeomFromText('{_sql_text(geom['geom_wkt'])}'),4326)"
            ")"
        )
        geom_count += 1
        if len(geom_values) >= args.batch_size:
            _psql(
                "INSERT INTO derived.city_network_geoms "
                "(source_file,source_method,city_code,city_name,state_code,area_km2,perimeter_km,geom) VALUES "
                + ",".join(geom_values)
                + " ON CONFLICT (source_method, city_code) DO UPDATE SET "
                "source_file=EXCLUDED.source_file, city_name=EXCLUDED.city_name, state_code=EXCLUDED.state_code, geom=EXCLUDED.geom;"
            )
            geom_values = []

    if geom_values:
        _psql(
            "INSERT INTO derived.city_network_geoms "
            "(source_file,source_method,city_code,city_name,state_code,area_km2,perimeter_km,geom) VALUES "
            + ",".join(geom_values)
            + " ON CONFLICT (source_method, city_code) DO UPDATE SET "
            "source_file=EXCLUDED.source_file, city_name=EXCLUDED.city_name, state_code=EXCLUDED.state_code, geom=EXCLUDED.geom;"
        )

    print(f"loaded_metrics={metric_count}")
    print(f"loaded_geoms={geom_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
