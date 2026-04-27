#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import shutil
from pathlib import Path

from pyproj import CRS, Transformer
from shapely.geometry import shape
from shapely.ops import transform

from urban_sami.analysis.linear_models import ols_fit


BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"


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
            geom = shape(feat["geometry"])
            lon, lat = geom.centroid.x, geom.centroid.y
            zone = int((lon + 180.0) // 6.0) + 1
            epsg = 32600 + zone if lat >= 0 else 32700 + zone
            transformer = Transformer.from_crs(CRS.from_epsg(4326), CRS.from_epsg(epsg), always_xy=True)
            geom_proj = transform(transformer.transform, geom)
            out[code] = geom_proj.area / 1_000_000.0
    return out


def _build_city_rows(root: Path) -> list[dict[str, float | str]]:
    city_counts = _read_csv(root / "dist" / "independent_city_baseline" / "city_counts.csv")
    area_map = _city_area_map(root)
    rows: list[dict[str, float | str]] = []
    for row in city_counts:
        code = str(row["city_code"]).zfill(5)
        pop = _safe_float(row["population"])
        dwellings = _safe_float(row["households"])
        est = _safe_float(row["est_count"])
        area = area_map.get(code, 0.0)
        if pop <= 0 or est <= 0 or area <= 0:
            continue
        rows.append(
            {
                "city_code": code,
                "city_name": row["city_name"],
                "population": pop,
                "occupied_dwellings": dwellings,
                "area_km2": area,
                "rho_pop": pop / area,
                "rho_dwellings": dwellings / area if dwellings > 0 else 0.0,
                "est_total": est,
            }
        )
    return rows


PREDICTOR_LABELS = {
    "population": "population",
    "occupied_dwellings": "occupied_dwellings",
    "area_km2": "area_km2",
    "rho_pop": "rho_pop",
    "rho_dwellings": "rho_dwellings",
}


def _fit_simple(rows: list[dict[str, float | str]], x_key: str) -> dict[str, object]:
    clean = [r for r in rows if _safe_float(r.get(x_key, 0.0)) > 0 and _safe_float(r.get("est_total", 0.0)) > 0]
    y = [math.log(float(r["est_total"])) for r in clean]
    x = [math.log(float(r[x_key])) for r in clean]
    X = [[1.0, xi] for xi in x]
    fit = ols_fit(X, y)
    return {
        "predictor_key": x_key,
        "predictor": PREDICTOR_LABELS.get(x_key, x_key),
        "n_obs": fit.n_obs,
        "n_params": fit.n_params,
        "alpha": fit.coefficients[0],
        "alpha_stderr": fit.stderr[0],
        "beta": fit.coefficients[1],
        "beta_stderr": fit.stderr[1],
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
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Cities only. One predictor at a time.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        value = float(row[metric])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["predictor"]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(value):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(value)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{value:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-only-single-predictor-power-laws-2026-04-23"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    city_rows = _build_city_rows(root)
    predictors = ["population", "occupied_dwellings", "area_km2", "rho_pop", "rho_dwellings"]
    results = [_fit_simple(city_rows, x) for x in predictors]
    results = sorted(results, key=lambda row: float(row["adj_r2"]), reverse=True)

    _write_csv(outdir / "city_single_predictor_fits.csv", results, list(results[0].keys()))
    _write_csv(outdir / "city_rows.csv", city_rows, list(city_rows[0].keys()))

    fig = _write_rank_chart(figdir / "city_adj_r2.svg", "City Single-Predictor Laws", results, "adj_r2")
    _write_csv(
        figdir / "figures_manifest.csv",
        [{"figure_id": "city_adj_r2", "path": str(fig.resolve()), "description": "Cities only single-predictor fits."}],
        ["figure_id", "path", "description"],
    )

    top = results[0]
    lines = [
        "# City-Only Single-Predictor Power Laws",
        "",
        "This is the first-branch screen only: cities, no AGEB, no intra-urban layer, no selected subsets.",
        "",
        "Model form:",
        "- `log(Y_city) = alpha + beta log(X_city)`",
        "- `Y_city = total establishments`",
        "- one predictor at a time",
        "",
        f"Universe: `{len(city_rows)}` cities",
        "",
        "Variable note:",
        "- `occupied_dwellings` comes from the official INEGI municipal field `total_viviendas_habitadas`.",
        "- the raw pipeline stores that field under the older name `households`, but this experiment uses the stricter label `occupied_dwellings`.",
        "",
        "Predictors screened:",
        "- `population`",
        "- `occupied_dwellings`",
        "- `area_km2`",
        "- `rho_pop = population / area_km2`",
        "- `rho_dwellings = occupied_dwellings / area_km2`",
        "",
        "## Main Ranking",
        *(f"- `{row['predictor']}`: `adjR2 = {float(row['adj_r2']):.6f}`, `beta = {float(row['beta']):.6f}`" for row in results),
        "",
        f"Best predictor: `{top['predictor']}` with `adjR2 = {float(top['adj_r2']):.6f}`",
        "",
        "## Files",
        f"- [city_single_predictor_fits.csv]({(outdir / 'city_single_predictor_fits.csv').resolve()})",
        f"- [city_rows.csv]({(outdir / 'city_rows.csv').resolve()})",
        "",
        "## Figures",
        f"- [city_adj_r2.svg]({fig.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
