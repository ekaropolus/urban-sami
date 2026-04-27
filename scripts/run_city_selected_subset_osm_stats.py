#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import subprocess
from collections import defaultdict
from pathlib import Path

import networkx as nx
import osmnx as ox
from shapely.geometry import shape


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


def _fetch_union_geom(city_code: str, unit_codes: list[str] | None = None):
    if unit_codes:
        in_list = ",".join("'" + c + "'" for c in unit_codes)
        where = f"level='ageb_u' AND city_code='{city_code}' AND unit_code IN ({in_list})"
    else:
        where = f"level='ageb_u' AND city_code='{city_code}'"
    sql = f"SELECT ST_AsGeoJSON(ST_UnaryUnion(ST_Collect(geom))) FROM raw.admin_units WHERE {where};"
    gj = _query_scalar(sql)
    return shape(json.loads(gj))


def _graph_stats(poly, *, cache_dir: Path) -> dict[str, float]:
    ox.settings.use_cache = True
    ox.settings.cache_folder = str(cache_dir)
    ox.settings.log_console = False
    G = ox.graph_from_polygon(poly, network_type="drive", simplify=True, retain_all=True, truncate_by_edge=True)
    Gp = ox.project_graph(G)
    area = poly.area if hasattr(poly, "area") else 0.0
    stats = ox.stats.basic_stats(Gp, area=area)
    degrees = [d for _, d in Gp.degree()]
    return {
        "n_nodes": float(stats.get("n", 0)),
        "n_edges": float(stats.get("m", 0)),
        "intersection_count": float(stats.get("intersection_count", 0)),
        "streets_per_node_avg": float(stats.get("streets_per_node_avg", 0.0)),
        "street_length_total_km": float(stats.get("street_length_total", 0.0)) / 1000.0,
        "street_density_km_per_km2": float(stats.get("street_density_km", 0.0)),
        "intersection_density_km2": float(stats.get("intersection_density_km", 0.0)),
        "edge_length_avg_m": float(stats.get("edge_length_avg", 0.0)),
        "circuity_avg": float(stats.get("circuity_avg", 0.0)),
        "self_loop_proportion": float(stats.get("self_loop_proportion", 0.0)),
        "mean_degree": _mean(degrees),
        "std_degree": _std(degrees),
    }


def _write_ratio_chart(path: Path, rows: list[dict[str, object]], metric: str, title: str) -> Path:
    width = 1140
    left = 250
    right = 70
    top = 96
    bottom = 78
    row_h = 28
    height = top + len(rows) * row_h + bottom
    vals = [abs(float(r[metric])) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (abs(v) / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Values are log2(subset/city). Positive means the selected AGEB subset is more connected than the full city fabric.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row[metric])
        color = TEAL if v >= 0 else RUST
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["city_name"]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{color}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:+.2f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-selected-subset-osm-stats-2026-04-22"
    figdir = outdir / "figures"
    cache_dir = root / "data" / "cache" / "osmnx"
    if outdir.exists():
        import shutil
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    summary = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_best_subset_summary.csv")
    members = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_selected_ageb_members.csv")

    by_city = defaultdict(list)
    for r in members:
        by_city[r["city_code"]].append(r["unit_code"])

    osm_rows = []
    for row in summary:
        city_code = row["city_code"]
        city_name = row["city_name"]
        subset_poly = _fetch_union_geom(city_code, by_city[city_code])
        city_poly = _fetch_union_geom(city_code, None)
        subset_stats = _graph_stats(subset_poly, cache_dir=cache_dir)
        city_stats = _graph_stats(city_poly, cache_dir=cache_dir)
        rec = {
            "state_code": row["state_code"],
            "city_code": city_code,
            "city_name": city_name,
            "selected_ageb_n": row["selected_ageb_n"],
            "subset_area_km2": subset_poly.area,
            "city_area_km2": city_poly.area,
        }
        for key, val in subset_stats.items():
            rec[f"subset_{key}"] = val
            rec[f"city_{key}"] = city_stats[key]
            base = city_stats[key]
            ratio = (val / base) if base not in (0, 0.0) else 0.0
            rec[f"log2_ratio_{key}"] = math.log(max(ratio, 1e-9), 2) if ratio > 0 else 0.0
        osm_rows.append(rec)

    osm_rows.sort(key=lambda r: (r["state_code"], r["city_code"]))
    _write_csv(outdir / "city_subset_osm_stats.csv", osm_rows, list(osm_rows[0].keys()))

    summary_rows = []
    ratio_fields = [k for k in osm_rows[0].keys() if k.startswith("log2_ratio_")]
    for field in ratio_fields:
        values = [float(r[field]) for r in osm_rows]
        summary_rows.append(
            {
                "metric": field.replace("log2_ratio_", ""),
                "mean_log2_ratio": _mean(values),
                "std_log2_ratio": _std(values),
                "positive_count": sum(1 for v in values if v > 0),
                "negative_count": sum(1 for v in values if v < 0),
            }
        )
    summary_rows.sort(key=lambda r: abs(float(r["mean_log2_ratio"])), reverse=True)
    _write_csv(outdir / "osm_ratio_summary.csv", summary_rows, list(summary_rows[0].keys()))

    fig1 = _write_ratio_chart(
        figdir / "intersection_density_ratio.svg",
        sorted(osm_rows, key=lambda r: float(r["log2_ratio_intersection_density_km2"]), reverse=True),
        "log2_ratio_intersection_density_km2",
        "Selected subset vs city: intersection density",
    )
    fig2 = _write_ratio_chart(
        figdir / "streets_per_node_ratio.svg",
        sorted(osm_rows, key=lambda r: float(r["log2_ratio_streets_per_node_avg"]), reverse=True),
        "log2_ratio_streets_per_node_avg",
        "Selected subset vs city: streets per node",
    )
    fig3 = _write_ratio_chart(
        figdir / "street_density_ratio.svg",
        sorted(osm_rows, key=lambda r: float(r["log2_ratio_street_density_km_per_km2"]), reverse=True),
        "log2_ratio_street_density_km_per_km2",
        "Selected subset vs city: street density",
    )
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "intersection_density_ratio", "path": str(fig1.resolve()), "description": "Subset/city intersection density ratio."},
            {"figure_id": "streets_per_node_ratio", "path": str(fig2.resolve()), "description": "Subset/city streets-per-node ratio."},
            {"figure_id": "street_density_ratio", "path": str(fig3.resolve()), "description": "Subset/city street density ratio."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# OSM Street-Network Stats for Selected Local AGEB Subsets",
        "",
        "For each city, street-network statistics were computed on the selected AGEB subset and on the full city AGEB union. Ratios are reported as `log2(subset/city)`.",
        "",
        "## Files",
        f"- [city_subset_osm_stats.csv]({(outdir / 'city_subset_osm_stats.csv').resolve()})",
        f"- [osm_ratio_summary.csv]({(outdir / 'osm_ratio_summary.csv').resolve()})",
        "",
        "## Figures",
        f"- [intersection_density_ratio.svg]({fig1.resolve()})",
        f"- [streets_per_node_ratio.svg]({fig2.resolve()})",
        f"- [street_density_ratio.svg]({fig3.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
