#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import random
import shutil
import subprocess
import argparse
from pathlib import Path

import osmnx as ox
from shapely.geometry import LineString, shape
from shapely.ops import unary_union


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
RUST = "#b14d3b"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"

N_RANDOM = 40
SEED = 20260423


def _query_lines(sql: str) -> list[str]:
    cmd = [
        DOCKER_EXE, "exec", "-i", DB_CONTAINER, "psql",
        "-U", POSTGRES_USER, "-d", DB_NAME, "-At", "-F", "|", "-v", "ON_ERROR_STOP=1", "-c", sql,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return [line for line in proc.stdout.splitlines() if line.strip()]


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


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _std(values: list[float]) -> float:
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


def _fetch_city_ageb_geoms(city_code: str) -> dict[str, object]:
    sql = (
        "SELECT unit_code, ST_AsGeoJSON(geom) "
        "FROM raw.admin_units "
        f"WHERE level='ageb_u' AND city_code='{city_code}' "
        "ORDER BY unit_code;"
    )
    rows = _query_lines(sql)
    out = {}
    for row in rows:
        unit_code, geojson = row.split("|", 1)
        out[unit_code] = shape(json.loads(geojson))
    return out


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


def _project_subset(poly, graph_crs):
    poly_proj, _ = ox.projection.project_geometry(poly, to_crs=graph_crs)
    return poly_proj


def _subset_metrics(Gp, graph_crs, unit_geoms: dict[str, object], selected_codes: list[str]) -> dict[str, float]:
    poly = unary_union([unit_geoms[code] for code in selected_codes])
    poly_proj = _project_subset(poly, graph_crs)
    area_m2 = poly_proj.area
    perim_km = poly_proj.length / 1000.0
    Gsub = ox.truncate.truncate_graph_polygon(Gp, poly_proj, truncate_by_edge=True)
    stats = ox.stats.basic_stats(Gsub, area=area_m2)
    degrees = [d for _, d in Gsub.degree()]
    boundary_entry_edges = float(_boundary_entry_count(Gp, poly_proj))
    return {
        "subset_area_km2": area_m2 / 1_000_000.0,
        "subset_perimeter_km": perim_km,
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
        "boundary_entry_edges_per_km": boundary_entry_edges / perim_km if perim_km > 0 else 0.0,
    }


def _empirical_summary(selected: float, random_values: list[float]) -> dict[str, float]:
    mu = _mean(random_values)
    sd = _std(random_values)
    rank = sum(v <= selected for v in random_values)
    percentile = rank / float(len(random_values)) if random_values else 0.0
    p_upper = sum(v >= selected for v in random_values) / float(len(random_values)) if random_values else 0.0
    p_lower = sum(v <= selected for v in random_values) / float(len(random_values)) if random_values else 0.0
    return {
        "selected_value": selected,
        "random_mean": mu,
        "random_std": sd,
        "z_vs_random": (selected - mu) / sd if sd > 0 else 0.0,
        "random_min": min(random_values) if random_values else 0.0,
        "random_max": max(random_values) if random_values else 0.0,
        "selected_percentile": percentile,
        "p_upper": p_upper,
        "p_lower": p_lower,
    }


def _write_metric_rank(path: Path, rows: list[dict[str, object]], metric: str, title: str) -> Path:
    width = 1180
    left = 270
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
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Selected subset compared against 100 random same-size AGEB subsets within the same city.</text>',
    ]
    x0 = left + (width - left - right) / 2.0
    body.append(f'<line x1="{x0:.2f}" y1="{top-16}" x2="{x0:.2f}" y2="{height-bottom+8}" stroke="{GRID}" stroke-width="1.5"/>')
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row[metric])
        color = TEAL if v >= 0 else RUST
        x1 = x0
        x2 = x0 + ((v / max(xmax, 1e-9)) * (width - left - right) / 2.0)
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["city_name"]}</text>')
        body.append(f'<line x1="{x1:.2f}" y1="{y:.2f}" x2="{x2:.2f}" y2="{y:.2f}" stroke="{color}" stroke-width="8"/>')
        body.append(f'<text x="{(x2 + (8 if v >= 0 else -8)):.2f}" y="{y+4:.2f}" text-anchor="{"start" if v >= 0 else "end"}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.2f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--city-codes", default="", help="Comma-separated city codes to process.")
    parser.add_argument("--outdir", default="", help="Optional custom output directory.")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    outdir = Path(args.outdir) if args.outdir else root / "reports" / "city-selected-subset-random-connectivity-baseline-2026-04-23"
    figdir = outdir / "figures"
    cache_dir = root / "data" / "cache" / "osmnx"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    ox.settings.use_cache = True
    ox.settings.cache_folder = str(cache_dir)
    ox.settings.log_console = False

    city_summary = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_best_subset_summary.csv")
    wanted = {c.strip() for c in args.city_codes.split(",") if c.strip()}
    if wanted:
        city_summary = [row for row in city_summary if row["city_code"] in wanted]
    selected_members = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_selected_ageb_members.csv")

    selected_by_city: dict[str, list[str]] = {}
    for row in selected_members:
        selected_by_city.setdefault(row["city_code"], []).append(row["unit_code"])

    metrics = [
        "boundary_entry_edges_per_km",
        "intersection_density_km2",
        "street_density_km_per_km2",
        "streets_per_node_avg",
        "mean_degree",
        "circuity_avg",
    ]

    long_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    random_rows: list[dict[str, object]] = []

    for idx, city in enumerate(city_summary, start=1):
        city_code = city["city_code"]
        city_name = city["city_name"]
        selected_codes = selected_by_city[city_code]
        unit_geoms = _fetch_city_ageb_geoms(city_code)
        all_codes = sorted(unit_geoms)
        city_union = unary_union(list(unit_geoms.values()))
        G = ox.graph_from_polygon(city_union.buffer(0.003), network_type="drive", simplify=True, retain_all=True, truncate_by_edge=True)
        Gp = ox.project_graph(G)
        graph_crs = Gp.graph["crs"]

        selected_stats = _subset_metrics(Gp, graph_crs, unit_geoms, selected_codes)

        rng = random.Random(SEED + int(city_code))
        city_random_rows = []
        for sample_id in range(1, N_RANDOM + 1):
            sample_codes = rng.sample(all_codes, len(selected_codes))
            stats = _subset_metrics(Gp, graph_crs, unit_geoms, sample_codes)
            row = {
                "state_code": city["state_code"],
                "city_code": city_code,
                "city_name": city_name,
                "sample_id": sample_id,
                **stats,
            }
            city_random_rows.append(row)
            random_rows.append(row)

        for metric in metrics:
            dist = [float(r[metric]) for r in city_random_rows]
            summary = _empirical_summary(float(selected_stats[metric]), dist)
            summary_rows.append(
                {
                    "state_code": city["state_code"],
                    "city_code": city_code,
                    "city_name": city_name,
                    "metric": metric,
                    **summary,
                }
            )

        long_rows.append(
            {
                "state_code": city["state_code"],
                "city_code": city_code,
                "city_name": city_name,
                "selected_ageb_n": len(selected_codes),
                **selected_stats,
            }
        )

        _write_csv(outdir / "city_selected_subset_metrics.csv", long_rows, list(long_rows[0].keys()))
        _write_csv(outdir / "city_random_subset_metrics.csv", random_rows, list(random_rows[0].keys()))
        _write_csv(outdir / "city_selected_vs_random_summary.csv", summary_rows, list(summary_rows[0].keys()))
        print(f"[{idx}/{len(city_summary)}] {city_name}: selected vs {N_RANDOM} random subsets complete", flush=True)

    strong_rows = []
    for metric in metrics:
        metric_rows = [r for r in summary_rows if r["metric"] == metric]
        strong_rows.append(
            {
                "metric": metric,
                "n_cities": len(metric_rows),
                "n_selected_above_95pct": sum(float(r["selected_percentile"]) >= 0.95 for r in metric_rows),
                "n_selected_above_90pct": sum(float(r["selected_percentile"]) >= 0.90 for r in metric_rows),
                "mean_z_vs_random": _mean([float(r["z_vs_random"]) for r in metric_rows]),
                "median_percentile": sorted(float(r["selected_percentile"]) for r in metric_rows)[len(metric_rows) // 2],
            }
        )
    _write_csv(outdir / "metric_signal_summary.csv", strong_rows, list(strong_rows[0].keys()))

    fig1 = _write_metric_rank(
        figdir / "boundary_entry_edges_per_km_z.svg",
        sorted((r for r in summary_rows if r["metric"] == "boundary_entry_edges_per_km"), key=lambda r: float(r["z_vs_random"]), reverse=True),
        "z_vs_random",
        "Boundary-entry roads per km: selected subset vs random subsets",
    )
    fig2 = _write_metric_rank(
        figdir / "intersection_density_z.svg",
        sorted((r for r in summary_rows if r["metric"] == "intersection_density_km2"), key=lambda r: float(r["z_vs_random"]), reverse=True),
        "z_vs_random",
        "Intersection density: selected subset vs random subsets",
    )
    fig3 = _write_metric_rank(
        figdir / "street_density_z.svg",
        sorted((r for r in summary_rows if r["metric"] == "street_density_km_per_km2"), key=lambda r: float(r["z_vs_random"]), reverse=True),
        "z_vs_random",
        "Street density: selected subset vs random subsets",
    )

    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "boundary_entry_edges_per_km_z", "path": str(fig1.resolve()), "description": "Z-score against random within-city subsets."},
            {"figure_id": "intersection_density_z", "path": str(fig2.resolve()), "description": "Z-score against random within-city subsets."},
            {"figure_id": "street_density_z", "path": str(fig3.resolve()), "description": "Z-score against random within-city subsets."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# Selected Subsets vs Random Same-Size AGEB Subsets",
        "",
        "This experiment compares each mathematically selected AGEB subset against 100 random same-size AGEB subsets drawn from the same city.",
        "",
        "## Files",
        f"- [city_selected_subset_metrics.csv]({(outdir / 'city_selected_subset_metrics.csv').resolve()})",
        f"- [city_random_subset_metrics.csv]({(outdir / 'city_random_subset_metrics.csv').resolve()})",
        f"- [city_selected_vs_random_summary.csv]({(outdir / 'city_selected_vs_random_summary.csv').resolve()})",
        f"- [metric_signal_summary.csv]({(outdir / 'metric_signal_summary.csv').resolve()})",
        "",
        "## Figures",
        f"- [boundary_entry_edges_per_km_z.svg]({fig1.resolve()})",
        f"- [intersection_density_z.svg]({fig2.resolve()})",
        f"- [street_density_z.svg]({fig3.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
