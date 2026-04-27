#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import json
import math
import shutil
import subprocess
from pathlib import Path

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit, pearson_corr


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
GOLD = "#b28a2e"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"


def _query_tsv(sql: str, columns: list[str]) -> list[dict[str, str]]:
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
        "-AtF",
        "\t",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rows: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        rows.append({col: (parts[idx] if idx < len(parts) else "") for idx, col in enumerate(columns)})
    return rows


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _fmt(v: float, digits: int = 3) -> str:
    return f"{v:.{digits}f}"


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _fetch_rows(city_code: str) -> list[dict[str, str]]:
    return _query_tsv(
        f"""
        WITH city_points AS (
            SELECT ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
            FROM raw.denue_establishments
            WHERE city_code = '{city_code}'
              AND longitude IS NOT NULL
              AND latitude IS NOT NULL
        ),
        city_center AS (
            SELECT ST_Centroid(ST_Collect(geom)) AS geom
            FROM city_points
        ),
        ageb AS (
            SELECT unit_code, unit_label, population, geom
            FROM raw.admin_units
            WHERE level = 'ageb_u' AND city_code = '{city_code}'
        ),
        counts AS (
            SELECT a.unit_code, COUNT(p.*)::int AS est_count
            FROM ageb a
            LEFT JOIN city_points p ON ST_Covers(a.geom, p.geom)
            GROUP BY a.unit_code
        )
        SELECT
            a.unit_code,
            a.unit_label,
            COALESCE(a.population, 0)::text AS population,
            c.est_count::text AS est_count,
            ST_X(ST_Centroid(a.geom))::text AS centroid_lon,
            ST_Y(ST_Centroid(a.geom))::text AS centroid_lat,
            ST_DistanceSphere(ST_Centroid(a.geom), cc.geom)::text AS dist_to_est_center_m
        FROM ageb a
        JOIN counts c ON c.unit_code = a.unit_code
        CROSS JOIN city_center cc
        ORDER BY a.unit_code
        """.strip(),
        ["unit_code", "unit_label", "population", "est_count", "centroid_lon", "centroid_lat", "dist_to_est_center_m"],
    )


def _model_metrics(design: list[list[float]], response: list[float], name: str) -> dict[str, object]:
    fit = ols_fit(design, response)
    coeffs = fit.coefficients
    return {
        "model": name,
        "n_obs": fit.n_obs,
        "n_params": fit.n_params,
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "rss": fit.rss,
        "coef_0": coeffs[0],
        "coef_1": coeffs[1] if len(coeffs) > 1 else "",
        "coef_2": coeffs[2] if len(coeffs) > 2 else "",
        "coef_3": coeffs[3] if len(coeffs) > 3 else "",
        "stderr_0": fit.stderr[0],
        "stderr_1": fit.stderr[1] if len(fit.stderr) > 1 else "",
        "stderr_2": fit.stderr[2] if len(fit.stderr) > 2 else "",
        "stderr_3": fit.stderr[3] if len(fit.stderr) > 3 else "",
        "fit": fit,
    }


def _write_model_comparison(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1040
    height = 560
    left = 180
    right = 60
    top = 100
    bottom = 90
    plot_w = width - left - right
    plot_h = height - top - bottom
    max_r2 = max(float(r["adj_r2"]) for r in rows) if rows else 1.0

    def px(v: float) -> float:
        return left + (v / max(max_r2 * 1.15, 1e-9)) * plot_w

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Centrality extension of the AGEB law</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Adjusted R² for nested models of log(Y) using population and centrality.</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}"/>',
    ]
    for tick in range(6):
        xv = max_r2 * 1.15 * tick / 5
        x = px(xv)
        body.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top+plot_h}" stroke="{GRID}"/>')
        body.append(f'<text x="{x:.2f}" y="{top+plot_h+26:.2f}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{xv:.2f}</text>')
    row_h = plot_h / len(rows)
    for i, row in enumerate(rows):
        y = top + i * row_h + row_h / 2
        adj = float(row["adj_r2"])
        body.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="13" font-family="{SANS}" fill="{TEXT}">{html.escape(str(row["model"]))}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(adj):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="10"/>')
        body.append(f'<text x="{px(adj)+8:.2f}" y="{y+4:.2f}" font-size="12" font-family="{SANS}" fill="{TEXT}">{adj:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def _write_residual_distance(path: Path, x: list[float], residual: list[float]) -> Path:
    width = 1020
    height = 620
    left = 88
    right = 40
    top = 96
    bottom = 86
    plot_w = width - left - right
    plot_h = height - top - bottom
    x_min, x_max = min(x), max(x)
    y_min, y_max = min(residual), max(residual)
    x_pad = (x_max - x_min) * 0.05
    y_pad = (y_max - y_min) * 0.08
    x_min -= x_pad
    x_max += x_pad
    y_min -= y_pad
    y_max += y_pad

    def px(v: float) -> float:
        return left + ((v - x_min) / max(x_max - x_min, 1e-9)) * plot_w

    def py(v: float) -> float:
        return top + plot_h - ((v - y_min) / max(y_max - y_min, 1e-9)) * plot_h

    corr = pearson_corr(x, residual)
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Residuals of log(Y) ~ log(N) versus centrality</text>',
        f'<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">Correlation between residual and log distance to establishment center: {corr:+.3f}</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}"/>',
    ]
    for tick in range(6):
        xv = x_min + (x_max - x_min) * tick / 5
        x0 = px(xv)
        body.append(f'<line x1="{x0:.2f}" y1="{top}" x2="{x0:.2f}" y2="{top+plot_h}" stroke="{GRID}"/>')
    for tick in range(6):
        yv = y_min + (y_max - y_min) * tick / 5
        y0 = py(yv)
        body.append(f'<line x1="{left}" y1="{y0:.2f}" x2="{left+plot_w}" y2="{y0:.2f}" stroke="{GRID}"/>')
    for xv, rv in zip(x, residual):
        body.append(f'<circle cx="{px(xv):.2f}" cy="{py(rv):.2f}" r="2.5" fill="{BLUE}" fill-opacity="0.55"/>')
    body.append(f'<line x1="{left}" y1="{py(0):.2f}" x2="{left+plot_w}" y2="{py(0):.2f}" stroke="{RUST}" stroke-dasharray="6,5"/>')
    return _svg(path, width, height, "".join(body))


def _write_band_distance(path: Path, rows: list[dict[str, float]]) -> Path:
    width = 1040
    height = 660
    left = 92
    right = 40
    top = 98
    bottom = 88
    plot_w = width - left - right
    plot_h = height - top - bottom
    x_min = min(r["log_dist"] for r in rows)
    x_max = max(r["log_dist"] for r in rows)
    y_min = min(r["log_y"] for r in rows)
    y_max = max(r["log_y"] for r in rows)
    x_pad = (x_max - x_min) * 0.05
    y_pad = (y_max - y_min) * 0.08
    x_min -= x_pad
    x_max += x_pad
    y_min -= y_pad
    y_max += y_pad

    def px(v: float) -> float:
        return left + ((v - x_min) / max(x_max - x_min, 1e-9)) * plot_w

    def py(v: float) -> float:
        return top + plot_h - ((v - y_min) / max(y_max - y_min, 1e-9)) * plot_h

    colors = {"low_pop": BLUE, "mid_pop": GOLD, "high_pop": RUST}
    labels = {"low_pop": "lower third of population", "mid_pop": "middle third", "high_pop": "upper third"}

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Distance effect within population bands</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Same-city AGEBs split by population thirds. If color clouds separate vertically, distance matters even after controlling rough size bands.</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}"/>',
    ]
    for tick in range(6):
        xv = x_min + (x_max - x_min) * tick / 5
        body.append(f'<line x1="{px(xv):.2f}" y1="{top}" x2="{px(xv):.2f}" y2="{top+plot_h}" stroke="{GRID}"/>')
    for tick in range(6):
        yv = y_min + (y_max - y_min) * tick / 5
        body.append(f'<line x1="{left}" y1="{py(yv):.2f}" x2="{left+plot_w}" y2="{py(yv):.2f}" stroke="{GRID}"/>')
    for row in rows:
        c = colors[row["band"]]
        body.append(f'<circle cx="{px(row["log_dist"]):.2f}" cy="{py(row["log_y"]):.2f}" r="2.5" fill="{c}" fill-opacity="0.58"/>')
    lx = width - 250
    ly = 124
    for i, key in enumerate(["low_pop", "mid_pop", "high_pop"]):
        y = ly + i * 22
        body.append(f'<circle cx="{lx}" cy="{y}" r="4.5" fill="{colors[key]}"/>')
        body.append(f'<text x="{lx+12}" y="{y+4}" font-size="12" font-family="{SANS}" fill="{TEXT}">{labels[key]}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    city_code = "14039"
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "ageb-centrality-extension-guadalajara-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    rows = _fetch_rows(city_code)
    rows = [r for r in rows if _to_float(r["population"]) > 0 and _to_float(r["est_count"]) > 0]
    if not rows:
        raise SystemExit("No AGEB rows found.")

    logy = [math.log(_to_float(r["est_count"])) for r in rows]
    logn = [math.log(_to_float(r["population"])) for r in rows]
    dist_km = [_to_float(r["dist_to_est_center_m"]) / 1000.0 for r in rows]
    logdist = [math.log(d + 0.25) for d in dist_km]
    dist2 = [d * d for d in dist_km]

    m0 = _model_metrics([[1.0, ln] for ln in logn], logy, "M0: logY ~ logN")
    m1 = _model_metrics([[1.0, ln, ld] for ln, ld in zip(logn, logdist)], logy, "M1: logY ~ logN + log(dist)")
    m2 = _model_metrics([[1.0, ln, d] for ln, d in zip(logn, dist_km)], logy, "M2: logY ~ logN + dist")
    m3 = _model_metrics([[1.0, ln, d, d2] for ln, d, d2 in zip(logn, dist_km, dist2)], logy, "M3: logY ~ logN + dist + dist²")
    models = [m0, m1, m2, m3]

    cmp1 = compare_nested_models(m0["fit"], m1["fit"])
    cmp2 = compare_nested_models(m0["fit"], m2["fit"])
    cmp3 = compare_nested_models(m0["fit"], m3["fit"])

    residual0 = m0["fit"].residuals
    residual_vs_dist = pearson_corr(logdist, residual0)

    pop_sorted = sorted(_to_float(r["population"]) for r in rows)
    q1 = pop_sorted[len(pop_sorted) // 3]
    q2 = pop_sorted[(2 * len(pop_sorted)) // 3]
    band_rows = []
    for r, ly, ld in zip(rows, logy, logdist):
        pop = _to_float(r["population"])
        if pop <= q1:
            band = "low_pop"
        elif pop <= q2:
            band = "mid_pop"
        else:
            band = "high_pop"
        band_rows.append({"band": band, "log_y": ly, "log_dist": ld})

    summary_rows = []
    for model in models:
        row = {k: v for k, v in model.items() if k != "fit"}
        summary_rows.append(row)
    _write_csv(outdir / "model_summary.csv", summary_rows, list(summary_rows[0].keys()))
    _write_csv(
        outdir / "nested_model_tests.csv",
        [
            {"comparison": "M0 vs M1", "f_stat": cmp1.f_stat, "df_num": cmp1.df_num, "df_den": cmp1.df_den, "p_value": cmp1.p_value},
            {"comparison": "M0 vs M2", "f_stat": cmp2.f_stat, "df_num": cmp2.df_num, "df_den": cmp2.df_den, "p_value": cmp2.p_value},
            {"comparison": "M0 vs M3", "f_stat": cmp3.f_stat, "df_num": cmp3.df_num, "df_den": cmp3.df_den, "p_value": cmp3.p_value},
        ],
        ["comparison", "f_stat", "df_num", "df_den", "p_value"],
    )
    _write_csv(
        outdir / "ageb_centrality_table.csv",
        [
            {
                "unit_code": r["unit_code"],
                "unit_label": r["unit_label"],
                "population": _to_float(r["population"]),
                "est_count": _to_float(r["est_count"]),
                "dist_to_est_center_km": d,
                "log_population": ln,
                "log_est_count": ly,
                "m0_residual": res,
            }
            for r, d, ln, ly, res in zip(rows, dist_km, logn, logy, residual0)
        ],
        ["unit_code", "unit_label", "population", "est_count", "dist_to_est_center_km", "log_population", "log_est_count", "m0_residual"],
    )

    fig1 = _write_model_comparison(figdir / "model_comparison.svg", models)
    fig2 = _write_residual_distance(figdir / "residual_vs_distance.svg", logdist, residual0)
    fig3 = _write_band_distance(figdir / "distance_within_population_bands.svg", band_rows)
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "model_comparison", "path": str(fig1.resolve()), "description": "Adjusted R2 across nested models."},
            {"figure_id": "residual_vs_distance", "path": str(fig2.resolve()), "description": "Residuals from M0 versus log distance."},
            {"figure_id": "distance_within_population_bands", "path": str(fig3.resolve()), "description": "Distance effect within population bands."},
        ],
        ["figure_id", "path", "description"],
    )

    report = [
        "# Guadalajara AGEB Centrality Extension",
        "",
        "This experiment keeps **all populated AGEBs** of Guadalajara. There is no manual AGEB selection.",
        "The only thing changing is the model:",
        "- `M0`: `log(Y) ~ log(N)`",
        "- `M1`: `log(Y) ~ log(N) + log(distance_to_establishment_center)`",
        "- `M2`: `log(Y) ~ log(N) + distance_to_establishment_center`",
        "- `M3`: `log(Y) ~ log(N) + distance_to_establishment_center + distance^2`",
        "",
        "## Main Result",
        "",
        f"- `M0` adjusted R²: `{float(m0['adj_r2']):.3f}`",
        f"- `M1` adjusted R²: `{float(m1['adj_r2']):.3f}`",
        f"- `M2` adjusted R²: `{float(m2['adj_r2']):.3f}`",
        f"- `M3` adjusted R²: `{float(m3['adj_r2']):.3f}`",
        "",
        f"- Correlation between `M0` residual and `log(distance)` = `{residual_vs_dist:+.3f}`",
        f"- `M0 vs M1` F-test p-value = `{cmp1.p_value:.6g}`",
        f"- `M0 vs M2` F-test p-value = `{cmp2.p_value:.6g}`",
        f"- `M0 vs M3` F-test p-value = `{cmp3.p_value:.6g}`",
        "",
        "## Interpretation",
        "",
        "If centrality lifts adjusted R² and the nested-model p-value is small, then the low AGEB fit was not mainly a data-size problem.",
        "It means the city interior needs at least one more structural axis besides population: centrality.",
        "",
        "## Files",
        "",
        f"- [model_summary.csv]({(outdir / 'model_summary.csv').resolve()})",
        f"- [nested_model_tests.csv]({(outdir / 'nested_model_tests.csv').resolve()})",
        f"- [ageb_centrality_table.csv]({(outdir / 'ageb_centrality_table.csv').resolve()})",
        "",
        "## Figures",
        "",
        f"- [model_comparison.svg]({fig1.resolve()})",
        f"- [residual_vs_distance.svg]({fig2.resolve()})",
        f"- [distance_within_population_bands.svg]({fig3.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(report), encoding="utf-8")
    print(json.dumps({"ok": True, "output_dir": str(outdir)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
