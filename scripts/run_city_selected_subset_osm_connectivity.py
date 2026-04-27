#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path

import networkx as nx
import osmnx as ox
from shapely.geometry import LineString, shape


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
AXIS = "#8b8478"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
BLUE = "#315c80"
RUST = "#b14d3b"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"


def _query_scalar(sql: str) -> str:
    cmd = [
        DOCKER_EXE, "exec", "-i", DB_CONTAINER, "psql",
        "-U", POSTGRES_USER, "-d", DB_NAME, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout.strip()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _mean(values):
    return sum(values) / float(len(values)) if values else 0.0


def _std(values):
    if len(values) < 2:
        return 0.0
    mu = _mean(values)
    return math.sqrt(sum((v - mu) ** 2 for v in values) / float(len(values) - 1))


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _fetch_union_geom(city_code: str, unit_codes: list[str]):
    in_list = ",".join("'" + c + "'" for c in unit_codes)
    sql = f"SELECT ST_AsGeoJSON(ST_UnaryUnion(ST_Collect(geom))) FROM raw.admin_units WHERE level='ageb_u' AND city_code='{city_code}' AND unit_code IN ({in_list});"
    gj = _query_scalar(sql)
    return shape(json.loads(gj))


def _boundary_entry_count(G, poly) -> int:
    boundary = poly.boundary
    count = 0
    seen = set()
    for u, v, k, data in G.edges(keys=True, data=True):
        geom = data.get("geometry")
        if geom is None:
            pu = (G.nodes[u]["x"], G.nodes[u]["y"])
            pv = (G.nodes[v]["x"], G.nodes[v]["y"])
            geom = LineString([pu, pv])
        if geom.crosses(boundary):
            key = tuple(sorted((u, v))) + (k,)
            if key not in seen:
                seen.add(key)
                count += 1
    return count


def _subset_stats(poly, cache_dir: Path) -> dict[str, float]:
    ox.settings.use_cache = True
    ox.settings.cache_folder = str(cache_dir)
    ox.settings.log_console = False
    poly_buf = poly.buffer(0.003)
    poly_proj, _ = ox.projection.project_geometry(poly)
    area_m2 = poly_proj.area
    perimeter_km = poly_proj.length / 1000.0
    G = ox.graph_from_polygon(poly_buf, network_type="drive", simplify=True, retain_all=True, truncate_by_edge=True)
    Gp = ox.project_graph(G)
    stats = ox.stats.basic_stats(Gp, area=area_m2)
    degrees = [d for _, d in Gp.degree()]
    boundary_entry_edges = float(_boundary_entry_count(G, poly))
    return {
        "subset_area_km2": area_m2 / 1_000_000.0,
        "subset_perimeter_km": perimeter_km,
        "n_nodes": float(stats.get("n", 0)),
        "n_edges": float(stats.get("m", 0)),
        "intersection_count": float(stats.get("intersection_count", 0)),
        "streets_per_node_avg": float(stats.get("streets_per_node_avg", 0.0)),
        "street_length_total_km": float(stats.get("street_length_total", 0.0)) / 1000.0,
        "street_density_km_per_km2": float(stats.get("street_density_km", 0.0)) / 1000.0,
        "intersection_density_km2": float(stats.get("intersection_density_km", 0.0)),
        "edge_length_avg_m": float(stats.get("edge_length_avg", 0.0)),
        "circuity_avg": float(stats.get("circuity_avg", 0.0)),
        "mean_degree": _mean(degrees),
        "std_degree": _std(degrees),
        "boundary_entry_edges": boundary_entry_edges,
        "boundary_entry_edges_per_km": boundary_entry_edges / perimeter_km if perimeter_km > 0 else 0.0,
    }


def _write_rank(path: Path, rows: list[dict[str, object]], metric: str, title: str) -> Path:
    width = 1140
    left = 260
    right = 70
    top = 96
    bottom = 78
    row_h = 28
    height = top + len(rows) * row_h + bottom
    vals = [float(r[metric]) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (v / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Street-network metrics on the mathematically selected AGEB subset only.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row[metric])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["city_name"]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.2f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-selected-subset-osm-connectivity-2026-04-22"
    figdir = outdir / "figures"
    cache_dir = root / "data" / "cache" / "osmnx"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    summary = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_best_subset_summary.csv")
    members = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_selected_ageb_members.csv")

    by_city = defaultdict(list)
    for r in members:
        by_city[r["city_code"]].append(r["unit_code"])

    rows = []
    for city in summary:
        city_code = city["city_code"]
        city_name = city["city_name"]
        poly = _fetch_union_geom(city_code, by_city[city_code])
        stats = _subset_stats(poly, cache_dir)
        rows.append(
            {
                "state_code": city["state_code"],
                "city_code": city_code,
                "city_name": city_name,
                "selected_ageb_n": city["selected_ageb_n"],
                "beta_local_subset": city["beta_local_subset"],
                "r2_local_subset": city["r2_local_subset"],
                **stats,
            }
        )
        _write_csv(outdir / "city_subset_osm_connectivity.csv", rows, list(rows[0].keys()))

    rows.sort(key=lambda r: (r["state_code"], r["city_code"]))
    _write_csv(outdir / "city_subset_osm_connectivity.csv", rows, list(rows[0].keys()))
    metric_summary = []
    metrics = [
        "intersection_density_km2",
        "streets_per_node_avg",
        "street_density_km_per_km2",
        "boundary_entry_edges",
        "boundary_entry_edges_per_km",
        "mean_degree",
        "circuity_avg",
    ]
    for metric in metrics:
        vals = [float(r[metric]) for r in rows]
        metric_summary.append(
            {
                "metric": metric,
                "mean": _mean(vals),
                "std": _std(vals),
                "min": min(vals),
                "max": max(vals),
                "cv": (_std(vals) / _mean(vals)) if _mean(vals) > 0 else 0.0,
            }
        )
    metric_summary.sort(key=lambda r: float(r["cv"]))
    _write_csv(outdir / "connectivity_metric_summary.csv", metric_summary, list(metric_summary[0].keys()))

    fig1 = _write_rank(
        figdir / "boundary_entry_edges.svg",
        sorted(rows, key=lambda r: float(r["boundary_entry_edges"]), reverse=True),
        "boundary_entry_edges",
        "Boundary-entry road count in selected subsets",
    )
    fig2 = _write_rank(
        figdir / "boundary_entry_edges_per_km.svg",
        sorted(rows, key=lambda r: float(r["boundary_entry_edges_per_km"]), reverse=True),
        "boundary_entry_edges_per_km",
        "Boundary-entry roads per km of subset perimeter",
    )
    fig3 = _write_rank(
        figdir / "intersection_density.svg",
        sorted(rows, key=lambda r: float(r["intersection_density_km2"]), reverse=True),
        "intersection_density_km2",
        "Intersection density in selected subsets",
    )
    fig4 = _write_rank(
        figdir / "streets_per_node.svg",
        sorted(rows, key=lambda r: float(r["streets_per_node_avg"]), reverse=True),
        "streets_per_node_avg",
        "Streets per node in selected subsets",
    )
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "boundary_entry_edges", "path": str(fig1.resolve()), "description": "Boundary-entry road count."},
            {"figure_id": "boundary_entry_edges_per_km", "path": str(fig2.resolve()), "description": "Boundary-entry roads per km of perimeter."},
            {"figure_id": "intersection_density", "path": str(fig3.resolve()), "description": "Intersection density."},
            {"figure_id": "streets_per_node", "path": str(fig4.resolve()), "description": "Streets per node."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# OSM Connectivity of Selected AGEB Subsets",
        "",
        "Street-network metrics were computed directly on the mathematically selected AGEB subsets for each city.",
        "",
        "## Files",
        f"- [city_subset_osm_connectivity.csv]({(outdir / 'city_subset_osm_connectivity.csv').resolve()})",
        f"- [connectivity_metric_summary.csv]({(outdir / 'connectivity_metric_summary.csv').resolve()})",
        "",
        "## Figures",
        f"- [boundary_entry_edges.svg]({fig1.resolve()})",
        f"- [boundary_entry_edges_per_km.svg]({fig2.resolve()})",
        f"- [intersection_density.svg]({fig3.resolve()})",
        f"- [streets_per_node.svg]({fig4.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
