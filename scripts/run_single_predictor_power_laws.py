#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import shutil
import subprocess
from pathlib import Path

from pyproj import CRS, Transformer
from shapely.geometry import shape
from shapely.ops import transform

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
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"


def _query_tsv(sql: str, columns: list[str]) -> list[dict[str, str]]:
    cmd = [
        DOCKER_EXE, "exec", "-i", DB_CONTAINER, "psql",
        "-U", POSTGRES_USER, "-d", DB_NAME, "-AtF", "\t", "-v", "ON_ERROR_STOP=1", "-c", sql,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rows: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        rows.append({col: (parts[idx] if idx < len(parts) else "") for idx, col in enumerate(columns)})
    return rows


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


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _city_area_map(root: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    raw_dir = root / "data" / "raw" / "inegi_municipal_geojson"
    for path in sorted(raw_dir.glob("*.geojson")):
        obj = json.loads(path.read_text(encoding="utf-8"))
        for feat in obj.get("features", []):
            props = feat["properties"]
            code = str(props.get("cvegeo", "")).strip()
            if not code:
                continue
            g = shape(feat["geometry"])
            lon, lat = g.centroid.x, g.centroid.y
            zone = int((lon + 180.0) // 6.0) + 1
            epsg = 32600 + zone if lat >= 0 else 32700 + zone
            transformer = Transformer.from_crs(CRS.from_epsg(4326), CRS.from_epsg(epsg), always_xy=True)
            g_proj = transform(transformer.transform, g)
            out[code] = g_proj.area / 1_000_000.0
    return out


def _build_city_rows(root: Path) -> list[dict[str, float]]:
    city_counts = _read_csv(root / "dist" / "independent_city_baseline" / "city_counts.csv")
    area_map = _city_area_map(root)
    rows = []
    for row in city_counts:
        code = str(row["city_code"]).zfill(5)
        pop = _safe_float(row["population"])
        hh = _safe_float(row["households"])
        est = _safe_float(row["est_count"])
        area = area_map.get(code, 0.0)
        if pop <= 0 or est <= 0 or area <= 0:
            continue
        rows.append(
            {
                "city_code": code,
                "city_name": row["city_name"],
                "population": pop,
                "households": hh,
                "area_km2": area,
                "rho_pop": pop / area,
                "rho_hh": hh / area if hh > 0 else 0.0,
                "est_total": est,
            }
        )
    return rows


def _build_ageb_rows() -> list[dict[str, float]]:
    rows = _query_tsv(
        """
        WITH ageb_units AS (
            SELECT
                city_code,
                RIGHT(unit_code, 4) AS ageb_code,
                COALESCE(population,0)::text AS population,
                COALESCE(households,0)::text AS households,
                (ST_Area(geom::geography)/1000000.0)::text AS area_km2
            FROM raw.admin_units
            WHERE level='ageb_u'
              AND COALESCE(population,0) > 0
        ),
        est AS (
            SELECT city_code, ageb_code, COUNT(*)::text AS est_total
            FROM raw.denue_establishments
            WHERE city_code <> ''
              AND ageb_code <> ''
            GROUP BY city_code, ageb_code
        )
        SELECT
            a.city_code,
            a.ageb_code,
            a.population,
            a.households,
            a.area_km2,
            e.est_total
        FROM ageb_units a
        JOIN est e
          ON a.city_code = e.city_code
         AND a.ageb_code = e.ageb_code
        ORDER BY a.city_code, a.ageb_code
        """.strip(),
        ["city_code", "ageb_code", "population", "households", "area_km2", "est_total"],
    )
    out = []
    for row in rows:
        pop = _safe_float(row["population"])
        hh = _safe_float(row["households"])
        area = _safe_float(row["area_km2"])
        est = _safe_float(row["est_total"])
        if pop <= 0 or est <= 0 or area <= 0:
            continue
        out.append(
            {
                "city_code": str(row["city_code"]).zfill(5),
                "ageb_code": row["ageb_code"],
                "population": pop,
                "households": hh,
                "area_km2": area,
                "rho_pop": pop / area,
                "rho_hh": hh / area if hh > 0 else 0.0,
                "est_total": est,
            }
        )
    return out


def _build_subset_rows(root: Path) -> list[dict[str, float]]:
    path = root / "reports" / "city-subset-extended-power-law-2026-04-23" / "selected_only_feature_rows.csv"
    rows = _read_csv(path)
    out = []
    for row in rows:
        pop = _safe_float(row["subset_population"])
        area = _safe_float(row["subset_area_km2"])
        est = _safe_float(row["subset_est_total"])
        if pop <= 0 or area <= 0 or est <= 0:
            continue
        out.append(
            {
                "city_code": str(row["city_code"]).zfill(5),
                "city_name": row["city_name"],
                "population": pop,
                "area_km2": area,
                "rho_pop": _safe_float(row["subset_pop_density"]),
                "boundary_entry_edges_per_km": _safe_float(row["boundary_entry_edges_per_km"]),
                "mean_degree": _safe_float(row["mean_degree"]),
                "street_density_km_per_km2": _safe_float(row["street_density_km_per_km2"]),
                "est_total": est,
            }
        )
    return out


def _fit_simple(rows: list[dict[str, float]], x_key: str, *, city_fe: bool) -> dict[str, object]:
    clean = [r for r in rows if _safe_float(r.get(x_key, 0.0)) > 0 and _safe_float(r.get("est_total", 0.0)) > 0]
    y = [math.log(float(r["est_total"])) for r in clean]
    x = [math.log(float(r[x_key])) for r in clean]
    X = [[1.0, xi] for xi in x]
    city_fe_n = 0
    if city_fe:
        cities = sorted({str(r["city_code"]) for r in clean})
        city_fe_n = max(0, len(cities) - 1)
        X2 = []
        for vec, row in zip(X, clean):
            ext = vec[:]
            for c in cities[1:]:
                ext.append(1.0 if row["city_code"] == c else 0.0)
            X2.append(ext)
        X = X2
    fit = ols_fit(X, y)
    beta = fit.coefficients[1]
    beta_se = fit.stderr[1]
    alpha = fit.coefficients[0]
    alpha_se = fit.stderr[0]
    return {
        "predictor": x_key,
        "n_obs": fit.n_obs,
        "n_params": fit.n_params,
        "city_fe_n": city_fe_n,
        "alpha": alpha,
        "alpha_stderr": alpha_se,
        "beta": beta,
        "beta_stderr": beta_se,
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "rss": fit.rss,
    }


def _write_rank_chart(path: Path, title: str, rows: list[dict[str, object]], metric: str) -> Path:
    width = 1080
    left = 220
    right = 70
    top = 96
    bottom = 78
    row_h = 32
    height = top + len(rows) * row_h + bottom
    vals = [float(r[metric]) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (v / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">One predictor at a time.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row[metric])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["predictor"]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.3f}</text>')
    return _svg(path, width, height, ''.join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "single-predictor-power-laws-2026-04-23"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    city_rows = _build_city_rows(root)
    ageb_rows = _build_ageb_rows()
    subset_rows = _build_subset_rows(root)

    city_predictors = ["population", "households", "area_km2", "rho_pop", "rho_hh"]
    ageb_predictors = ["population", "households", "area_km2", "rho_pop", "rho_hh"]
    subset_predictors = ["population", "area_km2", "rho_pop", "boundary_entry_edges_per_km", "mean_degree", "street_density_km_per_km2"]

    city_results = [_fit_simple(city_rows, x, city_fe=False) for x in city_predictors]
    ageb_results = [_fit_simple(ageb_rows, x, city_fe=True) for x in ageb_predictors]
    subset_results = [_fit_simple(subset_rows, x, city_fe=False) for x in subset_predictors]

    _write_csv(outdir / "city_single_predictor_fits.csv", city_results, list(city_results[0].keys()))
    _write_csv(outdir / "ageb_single_predictor_fits.csv", ageb_results, list(ageb_results[0].keys()))
    _write_csv(outdir / "subset_single_predictor_fits.csv", subset_results, list(subset_results[0].keys()))
    _write_csv(outdir / "city_rows.csv", city_rows, list(city_rows[0].keys()))
    _write_csv(outdir / "ageb_rows.csv", ageb_rows, list(ageb_rows[0].keys()))
    _write_csv(outdir / "subset_rows.csv", subset_rows, list(subset_rows[0].keys()))

    fig1 = _write_rank_chart(figdir / "city_adj_r2.svg", "City single-predictor laws", sorted(city_results, key=lambda r: float(r["adj_r2"]), reverse=True), "adj_r2")
    fig2 = _write_rank_chart(figdir / "ageb_adj_r2.svg", "AGEB single-predictor laws", sorted(ageb_results, key=lambda r: float(r["adj_r2"]), reverse=True), "adj_r2")
    fig3 = _write_rank_chart(figdir / "subset_adj_r2.svg", "Selected-subset single-predictor laws", sorted(subset_results, key=lambda r: float(r["adj_r2"]), reverse=True), "adj_r2")
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "city_adj_r2", "path": str(fig1.resolve()), "description": "City single-predictor fits."},
            {"figure_id": "ageb_adj_r2", "path": str(fig2.resolve()), "description": "AGEB single-predictor fits."},
            {"figure_id": "subset_adj_r2", "path": str(fig3.resolve()), "description": "Selected subset single-predictor fits."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# Single-Predictor Power Laws",
        "",
        "Back-to-basics screen: one predictor at a time.",
        "",
        "Scales:",
        "- city: all available cities",
        "- ageb: all populated AGEB with city fixed effects",
        "- selected subsets: one mathematically selected subset per city",
        "",
        "## Files",
        f"- [city_single_predictor_fits.csv]({(outdir / 'city_single_predictor_fits.csv').resolve()})",
        f"- [ageb_single_predictor_fits.csv]({(outdir / 'ageb_single_predictor_fits.csv').resolve()})",
        f"- [subset_single_predictor_fits.csv]({(outdir / 'subset_single_predictor_fits.csv').resolve()})",
        "",
        "## Figures",
        f"- [city_adj_r2.svg]({fig1.resolve()})",
        f"- [ageb_adj_r2.svg]({fig2.resolve()})",
        f"- [subset_adj_r2.svg]({fig3.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
