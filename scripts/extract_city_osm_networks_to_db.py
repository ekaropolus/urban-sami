#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import time
from pathlib import Path

import networkx as nx
import osmnx as ox
from shapely.geometry import LineString, MultiPolygon, shape


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"
NULL_MARKER = r"\N"


def _psql(sql: str, *, stdin_text: str | None = None, at: bool = False) -> str:
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
    if at:
        cmd.append("-At")
    cmd.extend(["-f", "-"])
    proc = subprocess.run(cmd, input=stdin_text if stdin_text is not None else sql, text=True, check=True, capture_output=True)
    return proc.stdout


def _sql_text(value: object) -> str:
    return str(value or "").replace("\t", " ").replace("\r", " ").replace("\n", " ").replace("'", "''")


def _to_db_text(value: object) -> str:
    if value is None:
        return NULL_MARKER
    if isinstance(value, float) and math.isnan(value):
        return NULL_MARKER
    if isinstance(value, (list, tuple, dict, set)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    text = str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
    return text if text else NULL_MARKER


def _to_db_num(value: object) -> str:
    if value is None:
        return NULL_MARKER
    try:
        number = float(value)
    except Exception:
        return NULL_MARKER
    if math.isnan(number) or math.isinf(number):
        return NULL_MARKER
    return repr(number)


def _to_db_bool(value: object) -> str:
    if value is None:
        return NULL_MARKER
    if isinstance(value, float) and math.isnan(value):
        return NULL_MARKER
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).strip().lower()
    if text in {"true", "t", "1", "yes"}:
        return "true"
    if text in {"false", "f", "0", "no"}:
        return "false"
    return NULL_MARKER


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _city_geom_map(geojson_dir: Path) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for path in sorted(geojson_dir.glob("*.geojson")):
        obj = json.loads(path.read_text(encoding="utf-8"))
        for feat in obj.get("features", []):
            props = feat.get("properties", {}) or {}
            code = str(props.get("cvegeo", "")).strip().zfill(5)
            if not code:
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
                "state_code": str(props.get("cve_ent", "")).strip().zfill(2),
                "geom": geom.buffer(0).simplify(0.001, preserve_topology=True),
            }
    return out


def _boundary_entry_count(graph: nx.MultiDiGraph, poly) -> int:
    boundary = poly.boundary
    seen: set[tuple[object, object, object]] = set()
    count = 0
    for u, v, key, data in graph.edges(keys=True, data=True):
        geom = data.get("geometry")
        if geom is None:
            geom = LineString([(graph.nodes[u]["x"], graph.nodes[u]["y"]), (graph.nodes[v]["x"], graph.nodes[v]["y"])])
        if geom.crosses(boundary):
            identity = (u, v, key)
            if identity not in seen:
                seen.add(identity)
                count += 1
    return count


def _query_existing_success(source_method: str) -> set[str]:
    sql = (
        "SELECT city_code FROM experiments.city_network_extract_status "
        f"WHERE source_method = '{_sql_text(source_method)}' AND status = 'success' "
        "ORDER BY city_code;"
    )
    out = _psql(sql, at=True).strip()
    return {line.strip() for line in out.splitlines() if line.strip()}


def _set_status(source_method: str, city_row: dict[str, object], status: str, *, n_nodes: int | None = None, n_edges: int | None = None, error_message: str = "") -> None:
    finished = "NOW()" if status in {"success", "error"} else "NULL"
    sql = f"""
    INSERT INTO experiments.city_network_extract_status
    (source_method, city_code, city_name, state_code, status, started_at, finished_at, n_nodes, n_edges, error_message, notes)
    VALUES
    (
        '{_sql_text(source_method)}',
        '{_sql_text(city_row["city_code"])}',
        '{_sql_text(city_row["city_name"])}',
        '{_sql_text(city_row["state_code"])}',
        '{_sql_text(status)}',
        NOW(),
        {finished},
        {n_nodes if n_nodes is not None else 'NULL'},
        {n_edges if n_edges is not None else 'NULL'},
        '{_sql_text(error_message[:4000])}',
        ''
    )
    ON CONFLICT (source_method, city_code) DO UPDATE SET
        city_name = EXCLUDED.city_name,
        state_code = EXCLUDED.state_code,
        status = EXCLUDED.status,
        started_at = CASE WHEN EXCLUDED.status = 'running' THEN NOW() ELSE experiments.city_network_extract_status.started_at END,
        finished_at = CASE WHEN EXCLUDED.status IN ('success', 'error') THEN NOW() ELSE NULL END,
        n_nodes = EXCLUDED.n_nodes,
        n_edges = EXCLUDED.n_edges,
        error_message = EXCLUDED.error_message,
        notes = EXCLUDED.notes;
    """
    _psql(sql)


def _city_graph_stats(poly, cache_dir: Path) -> tuple[nx.MultiDiGraph, dict[str, float]]:
    ox.settings.use_cache = True
    ox.settings.cache_folder = str(cache_dir)
    ox.settings.log_console = False
    ox.settings.overpass_rate_limit = True
    ox.settings.requests_timeout = 180
    poly_buf = poly.buffer(0.003)
    poly_proj, _ = ox.projection.project_geometry(poly)
    area_m2 = poly_proj.area
    perimeter_km = poly_proj.length / 1000.0
    graph = ox.graph_from_polygon(poly_buf, network_type="drive", simplify=True, retain_all=True, truncate_by_edge=True)
    graph_proj = ox.project_graph(graph)
    stats = ox.stats.basic_stats(graph_proj, area=area_m2)
    degrees = [float(d) for _, d in graph_proj.degree()]
    n_nodes = float(stats.get("n", 0))
    n_edges = float(stats.get("m", 0))
    intersection_count = float(stats.get("intersection_count", 0))
    street_length_total_km = float(stats.get("street_length_total", 0.0)) / 1000.0
    street_density_km_per_km2 = float(stats.get("street_density_km", 0.0)) / 1000.0
    intersection_density_km2 = float(stats.get("intersection_density_km", 0.0))
    boundary_entry_edges = float(_boundary_entry_count(graph, poly))
    sum_degree = float(sum(degrees))
    mean_degree = sum_degree / len(degrees) if degrees else 0.0
    metrics = {
        "city_area_km2": area_m2 / 1_000_000.0,
        "city_perimeter_km": perimeter_km,
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "intersection_count": intersection_count,
        "streets_per_node_avg": float(stats.get("streets_per_node_avg", 0.0)),
        "street_length_total_km": street_length_total_km,
        "street_density_km_per_km2": street_density_km_per_km2,
        "intersection_density_km2": intersection_density_km2,
        "edge_length_avg_m": float(stats.get("edge_length_avg", 0.0)),
        "circuity_avg": float(stats.get("circuity_avg", 0.0)),
        "mean_degree": mean_degree,
        "sum_degree": sum_degree,
        "boundary_entry_edges": boundary_entry_edges,
        "boundary_entry_edges_per_km": boundary_entry_edges / perimeter_km if perimeter_km > 0 else 0.0,
    }
    return graph, metrics


def _make_node_rows(graph: nx.MultiDiGraph, city_row: dict[str, object], source_method: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for osmid, data in graph.nodes(data=True):
        geom = data.get("geometry")
        degree = graph.degree[osmid]
        rows.append(
            [
                "",
                source_method,
                str(city_row["city_code"]),
                str(city_row["city_name"]),
                str(city_row["state_code"]),
                str(osmid),
                _to_db_num(data.get("x")),
                _to_db_num(data.get("y")),
                _to_db_num(data.get("street_count")),
                _to_db_num(degree),
                _to_db_text(data.get("highway")),
                _to_db_text(data.get("ref")),
                _to_db_text(geom.wkt if geom is not None else None),
            ]
        )
    return rows


def _make_edge_rows(graph: nx.MultiDiGraph, city_row: dict[str, object], source_method: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for u, v, key, data in graph.edges(keys=True, data=True):
        geom = data.get("geometry")
        if geom is None:
            geom = LineString([(graph.nodes[u]["x"], graph.nodes[u]["y"]), (graph.nodes[v]["x"], graph.nodes[v]["y"])])
        rows.append(
            [
                "",
                source_method,
                str(city_row["city_code"]),
                str(city_row["city_name"]),
                str(city_row["state_code"]),
                str(u),
                str(v),
                str(int(key)),
                _to_db_text(data.get("osmid")),
                _to_db_text(data.get("highway")),
                _to_db_text(data.get("junction")),
                _to_db_text(data.get("lanes")),
                _to_db_bool(data.get("oneway")),
                _to_db_bool(data.get("reversed")),
                _to_db_num(data.get("length")),
                _to_db_text(data.get("name")),
                _to_db_text(data.get("maxspeed")),
                _to_db_text(data.get("tunnel")),
                _to_db_text(data.get("ref")),
                _to_db_text(data.get("bridge")),
                _to_db_text(data.get("access")),
                _to_db_text(data.get("width")),
                _to_db_text(geom.wkt),
            ]
        )
    return rows


def _tsv_block(rows: list[list[str]]) -> str:
    out_lines: list[str] = []
    for row in rows:
        out_lines.append("\t".join(str(col) for col in row))
    return "\n".join(out_lines) + ("\n" if out_lines else "")


def _persist_city(
    *,
    source_method: str,
    city_row: dict[str, object],
    geom_source_file: str,
    geom_wkt: str,
    metrics: dict[str, float],
    node_rows: list[list[str]],
    edge_rows: list[list[str]],
    refresh: bool,
) -> None:
    rho_pop = (float(city_row["population"]) / float(metrics["city_area_km2"])) if float(metrics["city_area_km2"]) > 0 else 0.0
    rho_dwellings = (float(city_row["occupied_dwellings"]) / float(metrics["city_area_km2"])) if float(metrics["city_area_km2"]) > 0 else 0.0
    delete_sql = ""
    if refresh:
        delete_sql = f"""
DELETE FROM derived.city_network_nodes WHERE source_method = '{_sql_text(source_method)}' AND city_code = '{_sql_text(city_row["city_code"])}';
DELETE FROM derived.city_network_edges WHERE source_method = '{_sql_text(source_method)}' AND city_code = '{_sql_text(city_row["city_code"])}';
DELETE FROM derived.city_network_metrics WHERE source_method = '{_sql_text(source_method)}' AND city_code = '{_sql_text(city_row["city_code"])}';
DELETE FROM derived.city_network_geoms WHERE source_method = '{_sql_text(source_method)}' AND city_code = '{_sql_text(city_row["city_code"])}';
"""
    script = f"""
\\set ON_ERROR_STOP on
BEGIN;
{delete_sql}
CREATE TEMP TABLE tmp_city_nodes (
    source_file TEXT,
    source_method TEXT,
    city_code TEXT,
    city_name TEXT,
    state_code TEXT,
    node_osmid TEXT,
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    street_count DOUBLE PRECISION,
    degree DOUBLE PRECISION,
    highway TEXT,
    ref TEXT,
    geom_wkt TEXT
) ON COMMIT DROP;
COPY tmp_city_nodes (
    source_file, source_method, city_code, city_name, state_code,
    node_osmid, x, y, street_count, degree, highway, ref, geom_wkt
) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', HEADER false);
{_tsv_block(node_rows)}\\.
CREATE TEMP TABLE tmp_city_edges (
    source_file TEXT,
    source_method TEXT,
    city_code TEXT,
    city_name TEXT,
    state_code TEXT,
    u_osmid TEXT,
    v_osmid TEXT,
    edge_key INTEGER,
    osmid TEXT,
    highway TEXT,
    junction TEXT,
    lanes TEXT,
    oneway BOOLEAN,
    reversed BOOLEAN,
    length_m DOUBLE PRECISION,
    name TEXT,
    maxspeed TEXT,
    tunnel TEXT,
    ref TEXT,
    bridge TEXT,
    access TEXT,
    width TEXT,
    geom_wkt TEXT
) ON COMMIT DROP;
COPY tmp_city_edges (
    source_file, source_method, city_code, city_name, state_code,
    u_osmid, v_osmid, edge_key, osmid, highway, junction, lanes,
    oneway, reversed, length_m, name, maxspeed, tunnel, ref, bridge,
    access, width, geom_wkt
) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N', HEADER false);
{_tsv_block(edge_rows)}\\.
INSERT INTO derived.city_network_nodes (
    source_file, source_method, city_code, city_name, state_code, node_osmid, x, y,
    street_count, degree, highway, ref, geom
)
SELECT
    COALESCE(source_file, ''),
    COALESCE(source_method, ''),
    COALESCE(city_code, ''),
    COALESCE(city_name, ''),
    COALESCE(state_code, ''),
    COALESCE(node_osmid, ''),
    x, y, street_count, degree,
    COALESCE(highway, ''),
    COALESCE(ref, ''),
    ST_SetSRID(ST_GeomFromText(geom_wkt), 4326)
FROM tmp_city_nodes
ON CONFLICT (source_method, city_code, node_osmid) DO UPDATE SET
    source_file = EXCLUDED.source_file,
    city_name = EXCLUDED.city_name,
    state_code = EXCLUDED.state_code,
    x = EXCLUDED.x,
    y = EXCLUDED.y,
    street_count = EXCLUDED.street_count,
    degree = EXCLUDED.degree,
    highway = EXCLUDED.highway,
    ref = EXCLUDED.ref,
    geom = EXCLUDED.geom;
INSERT INTO derived.city_network_edges (
    source_file, source_method, city_code, city_name, state_code, u_osmid, v_osmid, edge_key,
    osmid, highway, junction, lanes, oneway, reversed, length_m, name, maxspeed, tunnel,
    ref, bridge, access, width, geom
)
SELECT
    COALESCE(source_file, ''),
    COALESCE(source_method, ''),
    COALESCE(city_code, ''),
    COALESCE(city_name, ''),
    COALESCE(state_code, ''),
    COALESCE(u_osmid, ''),
    COALESCE(v_osmid, ''),
    COALESCE(edge_key, 0),
    COALESCE(osmid, ''),
    COALESCE(highway, ''),
    COALESCE(junction, ''),
    COALESCE(lanes, ''),
    oneway,
    reversed,
    length_m,
    COALESCE(name, ''),
    COALESCE(maxspeed, ''),
    COALESCE(tunnel, ''),
    COALESCE(ref, ''),
    COALESCE(bridge, ''),
    COALESCE(access, ''),
    COALESCE(width, ''),
    ST_SetSRID(ST_GeomFromText(geom_wkt), 4326)
FROM tmp_city_edges
ON CONFLICT (source_method, city_code, u_osmid, v_osmid, edge_key) DO UPDATE SET
    source_file = EXCLUDED.source_file,
    city_name = EXCLUDED.city_name,
    state_code = EXCLUDED.state_code,
    osmid = EXCLUDED.osmid,
    highway = EXCLUDED.highway,
    junction = EXCLUDED.junction,
    lanes = EXCLUDED.lanes,
    oneway = EXCLUDED.oneway,
    reversed = EXCLUDED.reversed,
    length_m = EXCLUDED.length_m,
    name = EXCLUDED.name,
    maxspeed = EXCLUDED.maxspeed,
    tunnel = EXCLUDED.tunnel,
    ref = EXCLUDED.ref,
    bridge = EXCLUDED.bridge,
    access = EXCLUDED.access,
    width = EXCLUDED.width,
    geom = EXCLUDED.geom;
INSERT INTO derived.city_network_metrics (
    source_file, source_method, city_code, city_name, state_code, population, occupied_dwellings,
    est_total, city_area_km2, city_perimeter_km, rho_pop, rho_dwellings, n_nodes, n_edges,
    intersection_count, streets_per_node_avg, street_length_total_km, street_density_km_per_km2,
    intersection_density_km2, edge_length_avg_m, circuity_avg, mean_degree, sum_degree,
    boundary_entry_edges, boundary_entry_edges_per_km, notes
) VALUES (
    'openstreetmap',
    '{_sql_text(source_method)}',
    '{_sql_text(city_row["city_code"])}',
    '{_sql_text(city_row["city_name"])}',
    '{_sql_text(city_row["state_code"])}',
    {float(city_row["population"])},
    {float(city_row["occupied_dwellings"])},
    {float(city_row["est_total"])},
    {metrics["city_area_km2"]},
    {metrics["city_perimeter_km"]},
    {rho_pop},
    {rho_dwellings},
    {metrics["n_nodes"]},
    {metrics["n_edges"]},
    {metrics["intersection_count"]},
    {metrics["streets_per_node_avg"]},
    {metrics["street_length_total_km"]},
    {metrics["street_density_km_per_km2"]},
    {metrics["intersection_density_km2"]},
    {metrics["edge_length_avg_m"]},
    {metrics["circuity_avg"]},
    {metrics["mean_degree"]},
    {metrics["sum_degree"]},
    {metrics["boundary_entry_edges"]},
    {metrics["boundary_entry_edges_per_km"]},
    ''
)
ON CONFLICT (source_method, city_code) DO UPDATE SET
    source_file = EXCLUDED.source_file,
    city_name = EXCLUDED.city_name,
    state_code = EXCLUDED.state_code,
    population = EXCLUDED.population,
    occupied_dwellings = EXCLUDED.occupied_dwellings,
    est_total = EXCLUDED.est_total,
    city_area_km2 = EXCLUDED.city_area_km2,
    city_perimeter_km = EXCLUDED.city_perimeter_km,
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
INSERT INTO derived.city_network_geoms (
    source_file, source_method, city_code, city_name, state_code, area_km2, perimeter_km, geom
) VALUES (
    '{_sql_text(geom_source_file)}',
    '{_sql_text(source_method)}',
    '{_sql_text(city_row["city_code"])}',
    '{_sql_text(city_row["city_name"])}',
    '{_sql_text(city_row["state_code"])}',
    {metrics["city_area_km2"]},
    {metrics["city_perimeter_km"]},
    ST_SetSRID(ST_GeomFromText('{_sql_text(geom_wkt)}'), 4326)
)
ON CONFLICT (source_method, city_code) DO UPDATE SET
    source_file = EXCLUDED.source_file,
    city_name = EXCLUDED.city_name,
    state_code = EXCLUDED.state_code,
    area_km2 = EXCLUDED.area_km2,
    perimeter_km = EXCLUDED.perimeter_km,
    geom = EXCLUDED.geom;
COMMIT;
"""
    _psql(script)


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract OSM drive networks for Mexican cities and persist nodes, edges, metrics, and support geometries into PostGIS.")
    parser.add_argument("--city-counts-csv", type=Path, default=Path("dist/independent_city_baseline/city_counts.csv"))
    parser.add_argument("--municipal-geojson-dir", type=Path, default=Path("data/raw/inegi_municipal_geojson"))
    parser.add_argument("--source-method", default="osm_drive_municipal_full_v1")
    parser.add_argument("--state-code", default="")
    parser.add_argument("--city-code", action="append", default=[])
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--cache-dir", type=Path, default=Path("data/cache/osmnx"))
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    counts_rows = _read_csv(root / args.city_counts_csv)
    geom_map = _city_geom_map(root / args.municipal_geojson_dir)
    selected_city_codes = {str(code).strip().zfill(5) for code in args.city_code if str(code).strip()}
    existing_success = set() if args.refresh else _query_existing_success(args.source_method)

    city_rows: list[dict[str, object]] = []
    for row in counts_rows:
        city_code = str(row.get("city_code", "")).strip().zfill(5)
        state_code = str(row.get("state_code", "")).strip().zfill(2)
        if args.state_code and state_code != str(args.state_code).strip().zfill(2):
            continue
        if selected_city_codes and city_code not in selected_city_codes:
            continue
        if city_code not in geom_map:
            continue
        if city_code in existing_success:
            continue
        city_rows.append(
            {
                "city_code": city_code,
                "city_name": str(row.get("city_name", "")).strip() or geom_map[city_code]["city_name"],
                "state_code": state_code or geom_map[city_code]["state_code"],
                "population": float(str(row.get("population", "0") or "0").strip()),
                "occupied_dwellings": float(str(row.get("households", "0") or "0").strip()),
                "est_total": float(str(row.get("est_count", "0") or "0").strip()),
            }
        )

    city_rows.sort(key=lambda r: (str(r["state_code"]), str(r["city_code"])))
    if args.limit > 0:
        city_rows = city_rows[: args.limit]

    args.cache_dir.mkdir(parents=True, exist_ok=True)

    processed = 0
    for city_row in city_rows:
        city_code = str(city_row["city_code"])
        city_geom = geom_map[city_code]
        print(f"extracting {city_code} {city_row['city_name']}", flush=True)
        _set_status(args.source_method, city_row, "running")
        try:
            graph, metrics = _city_graph_stats(city_geom["geom"], args.cache_dir)
            node_rows = _make_node_rows(graph, city_row, args.source_method)
            edge_rows = _make_edge_rows(graph, city_row, args.source_method)
            _persist_city(
                source_method=args.source_method,
                city_row=city_row,
                geom_source_file=str(city_geom["source_file"]),
                geom_wkt=city_geom["geom"].wkt,
                metrics=metrics,
                node_rows=node_rows,
                edge_rows=edge_rows,
                refresh=args.refresh,
            )
            _set_status(
                args.source_method,
                city_row,
                "success",
                n_nodes=len(node_rows),
                n_edges=len(edge_rows),
            )
            processed += 1
            print(f"done {city_code} nodes={len(node_rows)} edges={len(edge_rows)}", flush=True)
        except Exception as exc:
            _set_status(args.source_method, city_row, "error", error_message=str(exc))
            print(f"error {city_code} {city_row['city_name']}: {exc}", flush=True)
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    print(f"processed={processed}")
    print(f"attempted={len(city_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
