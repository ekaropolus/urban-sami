#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import shutil
import subprocess
from pathlib import Path

import networkx as nx
import osmnx as ox
from shapely.geometry import LineString, shape

from urban_sami.analysis.linear_models import ols_fit


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
BLUE = "#2563eb"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"


def _query_scalar(sql: str) -> str:
    cmd = [
        DOCKER_EXE, "exec", "-i", DB_CONTAINER, "psql",
        "-U", POSTGRES_USER, "-d", DB_NAME, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql,
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


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _corr(xs: list[float], ys: list[float]) -> float:
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = (sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys)) ** 0.5
    return num / den if den > 0 else 0.0


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _city_geom_map(root: Path, city_codes: set[str]) -> dict[str, object]:
    out: dict[str, object] = {}
    raw_dir = root / "data" / "raw" / "inegi_municipal_geojson"
    for path in sorted(raw_dir.glob("*.geojson")):
        obj = json.loads(path.read_text(encoding="utf-8"))
        for feat in obj.get("features", []):
            props = feat["properties"]
            code = str(props.get("cvegeo", "")).strip()
            if code in city_codes:
                out[code] = shape(feat["geometry"]).buffer(0).simplify(0.001, preserve_topology=True)
        if city_codes.issubset(out.keys()):
            break
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


def _city_graph_stats(poly, cache_dir: Path) -> dict[str, float]:
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
    degrees = [float(d) for _, d in Gp.degree()]
    n_nodes = float(stats.get("n", 0))
    n_edges = float(stats.get("m", 0))
    intersection_count = float(stats.get("intersection_count", 0))
    street_length_total_km = float(stats.get("street_length_total", 0.0)) / 1000.0
    street_density_km_per_km2 = float(stats.get("street_density_km", 0.0)) / 1000.0
    intersection_density_km2 = float(stats.get("intersection_density_km", 0.0))
    boundary_entry_edges = float(_boundary_entry_count(G, poly))
    sum_degree = float(sum(degrees))
    mean_degree = _mean(degrees)
    return {
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


def _build_rows(root: Path) -> list[dict[str, object]]:
    summary = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_best_subset_summary.csv")
    city_counts = {r["city_code"]: r for r in _read_csv(root / "dist" / "independent_city_baseline" / "city_counts.csv")}
    summary = sorted(summary, key=lambda r: _safe_float(city_counts[r["city_code"]]["population"]))
    geom_map = _city_geom_map(root, {r["city_code"] for r in summary})
    cache_dir = root / "data" / "cache" / "osmnx"
    cache_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    partial_path = root / "reports" / "city-connectivity-power-laws-2026-04-23" / "city_connectivity_rows_partial.csv"
    for city in summary:
        code = city["city_code"]
        counts = city_counts[code]
        pop = _safe_float(counts["population"])
        dwell = _safe_float(counts["households"])
        est = _safe_float(counts["est_count"])
        poly = geom_map[code]
        stats = _city_graph_stats(poly, cache_dir)
        area = float(stats["city_area_km2"])
        rows.append(
            {
                "state_code": city["state_code"],
                "city_code": code,
                "city_name": city["city_name"],
                "population": pop,
                "occupied_dwellings": dwell,
                "est_total": est,
                "rho_pop": pop / area if area > 0 else 0.0,
                "rho_dwellings": dwell / area if area > 0 else 0.0,
                **stats,
            }
        )
        _write_csv(partial_path, rows, list(rows[0].keys()))
        print(f"done {code} {city['city_name']}", flush=True)
    return sorted(rows, key=lambda r: (str(r["state_code"]), str(r["city_code"])))


def _fit_univariate(rows: list[dict[str, object]], x_key: str, y_key: str = "est_total") -> dict[str, object]:
    clean = [r for r in rows if _safe_float(r[x_key]) > 0 and _safe_float(r[y_key]) > 0]
    xs = [math.log(float(r[x_key])) for r in clean]
    ys = [math.log(float(r[y_key])) for r in clean]
    fit = ols_fit([[1.0, x] for x in xs], ys)
    return {
        "predictor_key": x_key,
        "n_obs": fit.n_obs,
        "alpha": fit.coefficients[0],
        "beta": fit.coefficients[1],
        "alpha_stderr": fit.stderr[0],
        "beta_stderr": fit.stderr[1],
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "corr_log": _corr(xs, ys),
    }


def _fit_bivariate(rows: list[dict[str, object]], x1_key: str, x2_key: str, y_key: str = "est_total") -> dict[str, object]:
    clean = [r for r in rows if _safe_float(r[x1_key]) > 0 and _safe_float(r[x2_key]) > 0 and _safe_float(r[y_key]) > 0]
    x1 = [math.log(float(r[x1_key])) for r in clean]
    x2 = [math.log(float(r[x2_key])) for r in clean]
    y = [math.log(float(r[y_key])) for r in clean]
    fit = ols_fit([[1.0, a, b] for a, b in zip(x1, x2)], y)
    return {
        "base_predictor": x1_key,
        "added_predictor": x2_key,
        "n_obs": fit.n_obs,
        "alpha": fit.coefficients[0],
        "beta_base": fit.coefficients[1],
        "beta_added": fit.coefficients[2],
        "alpha_stderr": fit.stderr[0],
        "beta_base_stderr": fit.stderr[1],
        "beta_added_stderr": fit.stderr[2],
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "corr_log_base_added": _corr(x1, x2),
    }


def _rank_chart(path: Path, title: str, subtitle: str, rows: list[dict[str, object]], metric: str, label_key: str = "predictor_key") -> Path:
    width = 1160
    left = 290
    right = 80
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
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row[metric])
        label = str(row[label_key])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{label}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-connectivity-power-laws-2026-04-23"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    rows = _build_rows(root)

    extensive_predictors = [
        "population",
        "occupied_dwellings",
        "city_area_km2",
        "boundary_entry_edges",
        "intersection_count",
        "street_length_total_km",
        "sum_degree",
    ]
    intensive_predictors = [
        "rho_pop",
        "rho_dwellings",
        "boundary_entry_edges_per_km",
        "intersection_density_km2",
        "street_density_km_per_km2",
        "mean_degree",
        "streets_per_node_avg",
    ]

    uni_rows = [_fit_univariate(rows, key) for key in extensive_predictors + intensive_predictors]
    uni_rows = sorted(uni_rows, key=lambda r: float(r["adj_r2"]), reverse=True)

    add_to_population_rows = []
    add_to_dwellings_rows = []
    base_pop = _fit_univariate(rows, "population")
    base_dwell = _fit_univariate(rows, "occupied_dwellings")
    connectivity_predictors = [
        "boundary_entry_edges",
        "intersection_count",
        "street_length_total_km",
        "sum_degree",
        "boundary_entry_edges_per_km",
        "intersection_density_km2",
        "street_density_km_per_km2",
        "mean_degree",
        "streets_per_node_avg",
    ]
    for key in connectivity_predictors:
        fit_pop = _fit_bivariate(rows, "population", key)
        fit_pop["delta_adj_r2"] = float(fit_pop["adj_r2"]) - float(base_pop["adj_r2"])
        add_to_population_rows.append(fit_pop)
        fit_dw = _fit_bivariate(rows, "occupied_dwellings", key)
        fit_dw["delta_adj_r2"] = float(fit_dw["adj_r2"]) - float(base_dwell["adj_r2"])
        add_to_dwellings_rows.append(fit_dw)

    add_to_population_rows = sorted(add_to_population_rows, key=lambda r: float(r["delta_adj_r2"]), reverse=True)
    add_to_dwellings_rows = sorted(add_to_dwellings_rows, key=lambda r: float(r["delta_adj_r2"]), reverse=True)

    _write_csv(outdir / "city_connectivity_rows.csv", rows, list(rows[0].keys()))
    _write_csv(outdir / "univariate_fits.csv", uni_rows, list(uni_rows[0].keys()))
    _write_csv(outdir / "population_plus_connectivity_fits.csv", add_to_population_rows, list(add_to_population_rows[0].keys()))
    _write_csv(outdir / "dwellings_plus_connectivity_fits.csv", add_to_dwellings_rows, list(add_to_dwellings_rows[0].keys()))

    fig1 = _rank_chart(
        figdir / "univariate_adj_r2.svg",
        "Univariate city-scale power laws",
        "Y = establishments. One predictor at a time.",
        uni_rows,
        "adj_r2",
    )
    fig2 = _rank_chart(
        figdir / "population_plus_connectivity_delta_adj_r2.svg",
        "Connectivity added to population law",
        "delta adjR2 relative to log(establishments) ~ log(population).",
        add_to_population_rows,
        "delta_adj_r2",
        "added_predictor",
    )
    fig3 = _rank_chart(
        figdir / "dwellings_plus_connectivity_delta_adj_r2.svg",
        "Connectivity added to occupied-dwellings law",
        "delta adjR2 relative to log(establishments) ~ log(occupied_dwellings).",
        add_to_dwellings_rows,
        "delta_adj_r2",
        "added_predictor",
    )
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "univariate_adj_r2", "path": str(fig1.resolve()), "description": "Univariate city-scale fits."},
            {"figure_id": "population_plus_connectivity_delta_adj_r2", "path": str(fig2.resolve()), "description": "Connectivity gains over population law."},
            {"figure_id": "dwellings_plus_connectivity_delta_adj_r2", "path": str(fig3.resolve()), "description": "Connectivity gains over occupied-dwellings law."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# City Connectivity Power Laws",
        "",
        "City-scale experiment using `Y = establishments` and city-wide street-network metrics.",
        "",
        "Support and scope:",
        "- city units are the loaded cities with municipal geometry and OSM street network available",
        f"- sample size: `{len(rows)}` cities",
        "",
        "Network variables:",
        "- extensive:",
        "  - `B_i = boundary_entry_edges`",
        "  - `J_i = intersection_count`",
        "  - `S_i = street_length_total_km`",
        "  - `K_i = sum_degree`",
        "- intensive:",
        "  - `b_i = boundary_entry_edges_per_km`",
        "  - `j_i = intersection_density_km2`",
        "  - `s_i = street_density_km_per_km2`",
        "  - `k_i = mean_degree`",
        "",
        "## Files",
        f"- [city_connectivity_rows.csv]({(outdir / 'city_connectivity_rows.csv').resolve()})",
        f"- [univariate_fits.csv]({(outdir / 'univariate_fits.csv').resolve()})",
        f"- [population_plus_connectivity_fits.csv]({(outdir / 'population_plus_connectivity_fits.csv').resolve()})",
        f"- [dwellings_plus_connectivity_fits.csv]({(outdir / 'dwellings_plus_connectivity_fits.csv').resolve()})",
        "",
        "## Figures",
        f"- [univariate_adj_r2.svg]({fig1.resolve()})",
        f"- [population_plus_connectivity_delta_adj_r2.svg]({fig2.resolve()})",
        f"- [dwellings_plus_connectivity_delta_adj_r2.svg]({fig3.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
