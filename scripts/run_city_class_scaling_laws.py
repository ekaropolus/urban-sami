#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import subprocess
from collections import Counter
from pathlib import Path

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

BG = "#f6f3ec"
PANEL = "#fffdf9"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#6b665d"
TEAL = "#0f766e"
RUST = "#b14d3b"
BLUE = "#2563eb"
GOLD = "#b45309"
ROSE = "#be185d"
SLATE = "#475569"
SERIF = "Georgia, 'Times New Roman', serif"
SANS = "Helvetica, Arial, sans-serif"
PALETTE = [TEAL, RUST, BLUE, GOLD, ROSE, SLATE]


def _psql(sql: str) -> str:
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
        "-At",
        "-F",
        "\t",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout


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


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _query_city_rows(source_method: str) -> list[dict[str, object]]:
    sql = f"""
    SELECT city_code, city_name, state_code,
           population::text, est_total::text,
           n_nodes::text, city_area_km2::text,
           street_density_km_per_km2::text, boundary_entry_edges_per_km::text
    FROM derived.city_network_metrics
    WHERE source_method = '{source_method}'
    ORDER BY city_code;
    """
    out = _psql(sql)
    rows: list[dict[str, object]] = []
    for line in out.splitlines():
        city_code, city_name, state_code, population, est_total, n_nodes, area, street_density, boundary_entry = line.split("\t")
        rows.append(
            {
                "city_code": city_code,
                "city_name": city_name,
                "state_code": state_code,
                "population": _safe_float(population),
                "est_total": _safe_float(est_total),
                "n_nodes": _safe_float(n_nodes),
                "city_area_km2": _safe_float(area),
                "street_density_km_per_km2": _safe_float(street_density),
                "boundary_entry_edges_per_km": _safe_float(boundary_entry),
            }
        )
    return rows


def _fit_simple(rows: list[dict[str, object]]) -> dict[str, object]:
    clean = [r for r in rows if float(r["population"]) > 0 and float(r["est_total"]) > 0]
    x = [math.log(float(r["population"])) for r in clean]
    y = [math.log(float(r["est_total"])) for r in clean]
    fit = ols_fit([[1.0, xi] for xi in x], y)
    return {
        "n_obs": fit.n_obs,
        "alpha": fit.coefficients[0],
        "alpha_stderr": fit.stderr[0],
        "beta": fit.coefficients[1],
        "beta_stderr": fit.stderr[1],
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "rss": fit.rss,
        "x": x,
        "y": y,
        "fit": fit,
    }


def _class_fits(rows: list[dict[str, object]], class_order: list[str]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for idx, class_label in enumerate(class_order):
        subset = [r for r in rows if r["class_label"] == class_label and float(r["population"]) > 0 and float(r["est_total"]) > 0]
        x = [math.log(float(r["population"])) for r in subset]
        y = [math.log(float(r["est_total"])) for r in subset]
        fit = ols_fit([[1.0, xi] for xi in x], y)
        rep = next((r for r in subset if str(r.get("is_representative", "0")) == "1"), None)
        out.append(
            {
                "class_id": str(idx),
                "class_label": class_label,
                "n_cities": fit.n_obs,
                "alpha": fit.coefficients[0],
                "alpha_stderr": fit.stderr[0],
                "beta": fit.coefficients[1],
                "beta_stderr": fit.stderr[1],
                "r2": fit.r2,
                "adj_r2": fit.adj_r2,
                "rss": fit.rss,
                "mean_population": sum(float(r["population"]) for r in subset) / len(subset),
                "mean_est_total": sum(float(r["est_total"]) for r in subset) / len(subset),
                "representative_city": rep["city_name"] if rep else "",
                "representative_city_code": rep["city_code"] if rep else "",
                "x": x,
                "y": y,
            }
        )
    return out


def _pooled_models(rows: list[dict[str, object]], class_order: list[str], base_class: str) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, tuple[float, float]]]:
    clean = [r for r in rows if float(r["population"]) > 0 and float(r["est_total"]) > 0]
    x = [math.log(float(r["population"])) for r in clean]
    y = [math.log(float(r["est_total"])) for r in clean]

    others = [c for c in class_order if c != base_class]

    X0 = [[1.0, xi] for xi in x]
    fit0 = ols_fit(X0, y)

    X1 = []
    for row, xi in zip(clean, x):
        vec = [1.0, xi]
        for c in others:
            vec.append(1.0 if row["class_label"] == c else 0.0)
        X1.append(vec)
    fit1 = ols_fit(X1, y)

    X2 = []
    for row, xi in zip(clean, x):
        vec = [1.0, xi]
        for c in others:
            vec.append(1.0 if row["class_label"] == c else 0.0)
        for c in others:
            vec.append(xi if row["class_label"] == c else 0.0)
        X2.append(vec)
    fit2 = ols_fit(X2, y)

    cmp01 = compare_nested_models(fit0, fit1)
    cmp12 = compare_nested_models(fit1, fit2)
    cmp02 = compare_nested_models(fit0, fit2)

    summary = [
        {
            "model_id": "M0_global",
            "description": "Global law: log(E) = alpha + beta log(N)",
            "n_obs": fit0.n_obs,
            "n_params": fit0.n_params,
            "adj_r2": fit0.adj_r2,
            "r2": fit0.r2,
            "rss": fit0.rss,
        },
        {
            "model_id": "M1_class_intercepts",
            "description": f"Common slope with class-specific intercepts, base class = {base_class}",
            "n_obs": fit1.n_obs,
            "n_params": fit1.n_params,
            "adj_r2": fit1.adj_r2,
            "r2": fit1.r2,
            "rss": fit1.rss,
        },
        {
            "model_id": "M2_class_intercepts_slopes",
            "description": f"Class-specific intercepts and slopes, base class = {base_class}",
            "n_obs": fit2.n_obs,
            "n_params": fit2.n_params,
            "adj_r2": fit2.adj_r2,
            "r2": fit2.r2,
            "rss": fit2.rss,
        },
    ]

    nested = [
        {
            "comparison": "M0_to_M1",
            "null": "classes do not change intercept",
            "f_stat": cmp01.f_stat,
            "df_num": cmp01.df_num,
            "df_den": cmp01.df_den,
            "p_value": cmp01.p_value,
        },
        {
            "comparison": "M1_to_M2",
            "null": "classes do not change slope once intercept shifts are allowed",
            "f_stat": cmp12.f_stat,
            "df_num": cmp12.df_num,
            "df_den": cmp12.df_den,
            "p_value": cmp12.p_value,
        },
        {
            "comparison": "M0_to_M2",
            "null": "classes do not matter at all",
            "f_stat": cmp02.f_stat,
            "df_num": cmp02.df_num,
            "df_den": cmp02.df_den,
            "p_value": cmp02.p_value,
        },
    ]

    class_params: dict[str, tuple[float, float]] = {}
    base_alpha = fit2.coefficients[0]
    base_beta = fit2.coefficients[1]
    class_params[base_class] = (base_alpha, base_beta)
    for idx, c in enumerate(others):
        gamma = fit2.coefficients[2 + idx]
        delta = fit2.coefficients[2 + len(others) + idx]
        class_params[c] = (base_alpha + gamma, base_beta + delta)
    return summary, nested, class_params


def _class_rank_svg(path: Path, rows: list[dict[str, object]], metric: str, title: str, subtitle: str) -> Path:
    width = 1120
    left = 320
    right = 90
    top = 96
    bottom = 76
    row_h = 34
    height = top + len(rows) * row_h + bottom
    vals = [float(r[metric]) for r in rows]
    xmin = min(vals)
    xmax = max(vals)
    if xmin >= 0:
        xmin = 0.0
    rng = max(xmax - xmin, 1e-9)

    def px(v: float) -> float:
        return left + ((v - xmin) / rng) * (width - left - right)

    zero_x = px(0.0)
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
        f'<line x1="{zero_x:.2f}" y1="{top-8}" x2="{zero_x:.2f}" y2="{height-bottom+10}" stroke="{GRID}" stroke-dasharray="4 4"/>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row[metric])
        color = PALETTE[i % len(PALETTE)]
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["class_label"]}</text>')
        body.append(f'<line x1="{px(min(v,0)):.2f}" y1="{y:.2f}" x2="{px(max(v,0)):.2f}" y2="{y:.2f}" stroke="{color}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def _scatter_panels_svg(path: Path, class_rows: list[dict[str, object]]) -> Path:
    cols = 2
    panel_w = 520
    panel_h = 360
    gap = 24
    left_pad = 56
    top_pad = 96
    rows_n = (len(class_rows) + cols - 1) // cols
    width = cols * panel_w + (cols - 1) * gap + 80
    height = rows_n * panel_h + (rows_n - 1) * gap + 120

    x_all = [x for row in class_rows for x in row["x"]]
    y_all = [y for row in class_rows for y in row["y"]]
    xmin = min(x_all)
    xmax = max(x_all)
    ymin = min(y_all)
    ymax = max(y_all)
    xr = max(xmax - xmin, 1e-9)
    yr = max(ymax - ymin, 1e-9)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="18" y="18" width="{width-36}" height="{height-36}" rx="20" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="42" y="50" font-size="28" font-family="{SERIF}" fill="{TEXT}">Class-Conditioned City Scaling</text>',
        f'<text x="42" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Each panel fits log(E) = alpha_c + beta_c log(N) within one network-topology class.</text>',
    ]

    for idx, row in enumerate(class_rows):
        color = PALETTE[idx % len(PALETTE)]
        pr = idx // cols
        pc = idx % cols
        px0 = 40 + pc * (panel_w + gap)
        py0 = 96 + pr * (panel_h + gap)
        body.append(f'<rect x="{px0}" y="{py0}" width="{panel_w}" height="{panel_h}" rx="18" fill="{PANEL}" stroke="{GRID}"/>')
        body.append(f'<text x="{px0+16}" y="{py0+28}" font-size="19" font-family="{SERIF}" fill="{TEXT}">{row["class_label"]}</text>')
        body.append(f'<text x="{px0+16}" y="{py0+48}" font-size="12" font-family="{SANS}" fill="{MUTED}">n={row["n_cities"]} | beta={float(row["beta"]):.3f} | adjR2={float(row["adj_r2"]):.3f}</text>')

        plot_x0 = px0 + left_pad
        plot_y0 = py0 + 56
        plot_w = panel_w - left_pad - 24
        plot_h = panel_h - 86

        def sx(v: float) -> float:
            return plot_x0 + ((v - xmin) / xr) * plot_w

        def sy(v: float) -> float:
            return plot_y0 + plot_h - ((v - ymin) / yr) * plot_h

        for g in range(4):
            gx = plot_x0 + g * plot_w / 3.0
            gy = plot_y0 + g * plot_h / 3.0
            body.append(f'<line x1="{gx:.2f}" y1="{plot_y0:.2f}" x2="{gx:.2f}" y2="{plot_y0+plot_h:.2f}" stroke="{GRID}" stroke-opacity="0.65"/>')
            body.append(f'<line x1="{plot_x0:.2f}" y1="{gy:.2f}" x2="{plot_x0+plot_w:.2f}" y2="{gy:.2f}" stroke="{GRID}" stroke-opacity="0.65"/>')

        x1, x2 = xmin, xmax
        y1 = float(row["alpha"]) + float(row["beta"]) * x1
        y2 = float(row["alpha"]) + float(row["beta"]) * x2
        body.append(f'<line x1="{sx(x1):.2f}" y1="{sy(y1):.2f}" x2="{sx(x2):.2f}" y2="{sy(y2):.2f}" stroke="{color}" stroke-width="2.3"/>')
        for xv, yv in zip(row["x"], row["y"]):
            body.append(f'<circle cx="{sx(xv):.2f}" cy="{sy(yv):.2f}" r="2.1" fill="{color}" fill-opacity="0.55"/>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fit city scaling laws separately within each persisted network-topology class.")
    parser.add_argument("--source-method", default="osm_drive_municipal_full_v1")
    parser.add_argument(
        "--cluster-members-csv",
        type=Path,
        default=Path("reports/city-network-typology-2026-04-24/cluster_members.csv"),
    )
    parser.add_argument(
        "--cluster-summary-csv",
        type=Path,
        default=Path("reports/city-network-typology-2026-04-24/cluster_summary.csv"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/city-class-scaling-laws-2026-04-24"),
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    outdir = root / args.output_dir
    figdir = outdir / "figures"
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    cluster_members = _read_csv(root / args.cluster_members_csv)
    cluster_summary = _read_csv(root / args.cluster_summary_csv)
    class_map = {row["city_code"]: row for row in cluster_members}
    rep_lookup = {row["class_label"]: row for row in cluster_summary}

    city_rows = _query_city_rows(args.source_method)
    usable_rows = []
    for row in city_rows:
        class_row = class_map.get(str(row["city_code"]))
        if not class_row:
            continue
        if float(row["population"]) <= 0 or float(row["est_total"]) <= 0:
            continue
        usable_rows.append(
            {
                **row,
                "cluster_id": class_row["cluster_id"],
                "class_label": class_row["class_label"],
                "is_representative": class_row["is_representative"],
            }
        )

    class_order = [row["class_label"] for row in cluster_summary]
    global_fit = _fit_simple(usable_rows)
    class_fit_rows = _class_fits(usable_rows, class_order)

    counts = Counter(row["class_label"] for row in usable_rows)
    base_class = max(class_order, key=lambda c: counts[c])
    pooled_summary, pooled_nested, pooled_class_params = _pooled_models(usable_rows, class_order, base_class)

    for row in class_fit_rows:
        rep = rep_lookup.get(row["class_label"], {})
        pooled_alpha, pooled_beta = pooled_class_params[row["class_label"]]
        row["pooled_alpha"] = pooled_alpha
        row["pooled_beta"] = pooled_beta
        row["representative_city"] = rep.get("representative_city", row["representative_city"])
        row["representative_city_code"] = rep.get("representative_city_code", row["representative_city_code"])
        row["rep_overlay_path"] = rep.get("rep_overlay_path", "")
        row["rep_denue_path"] = rep.get("rep_denue_path", "")

    class_fit_rows = sorted(class_fit_rows, key=lambda r: r["beta"], reverse=True)
    serializable_class_rows = []
    for row in class_fit_rows:
        serializable_class_rows.append(
            {
                key: value
                for key, value in row.items()
                if key not in {"x", "y"}
            }
        )

    _write_csv(outdir / "class_scaling_fits.csv", serializable_class_rows, list(serializable_class_rows[0].keys()))
    _write_csv(outdir / "pooled_model_summary.csv", pooled_summary, list(pooled_summary[0].keys()))
    _write_csv(outdir / "pooled_nested_tests.csv", pooled_nested, list(pooled_nested[0].keys()))

    global_row = [
        {
            "n_obs": global_fit["n_obs"],
            "alpha": global_fit["alpha"],
            "alpha_stderr": global_fit["alpha_stderr"],
            "beta": global_fit["beta"],
            "beta_stderr": global_fit["beta_stderr"],
            "r2": global_fit["r2"],
            "adj_r2": global_fit["adj_r2"],
            "rss": global_fit["rss"],
        }
    ]
    _write_csv(outdir / "global_scaling_fit.csv", global_row, list(global_row[0].keys()))

    beta_fig = _class_rank_svg(
        figdir / "class_beta_rank.svg",
        class_fit_rows,
        "beta",
        "Class-Specific Scaling Exponents",
        "Separate log(E) ~ log(N) fits within each network-topology class.",
    )
    r2_fig = _class_rank_svg(
        figdir / "class_adj_r2_rank.svg",
        class_fit_rows,
        "adj_r2",
        "Class-Specific Fit Strength",
        "Adjusted R² of the same law within each class.",
    )
    scatter_fig = _scatter_panels_svg(figdir / "class_scaling_scatter_panels.svg", class_fit_rows)
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "class_beta_rank", "path": str(beta_fig.resolve()), "description": "Class-specific scaling exponents."},
            {"figure_id": "class_adj_r2_rank", "path": str(r2_fig.resolve()), "description": "Class-specific adjusted R²."},
            {"figure_id": "class_scaling_scatter_panels", "path": str(scatter_fig.resolve()), "description": "Scatter panels by class."},
        ],
        ["figure_id", "path", "description"],
    )

    best_beta = max(class_fit_rows, key=lambda r: float(r["beta"]))
    worst_beta = min(class_fit_rows, key=lambda r: float(r["beta"]))
    best_r2 = max(class_fit_rows, key=lambda r: float(r["adj_r2"]))
    largest_delta = max(
        class_fit_rows,
        key=lambda r: abs(float(r["beta"]) - float(global_fit["beta"])),
    )

    def _p_text(v: object) -> str:
        p = _safe_float(v, float("nan"))
        if math.isnan(p):
            return "NA"
        return f"{p:.2e}" if p < 0.001 else f"{p:.4f}"

    lines = [
        "# City Class Scaling Laws",
        "",
        "This experiment tests whether the national city scaling law changes across the network-topology classes we derived from persisted OSM city networks.",
        "",
        "Core equation by class:",
        "- `log(E_i) = alpha_c + beta_c log(N_i) + epsilon_i`",
        "- `E_i = establishments`, `N_i = population`, `c = topology class`",
        "",
        f"Usable cities: `{len(usable_rows)}`",
        f"Base class for pooled interaction models: `{base_class}`",
        "",
        "## Global Law",
        f"- `log(E) = alpha + beta log(N)` with `beta = {float(global_fit['beta']):.3f}`, `adjR2 = {float(global_fit['adj_r2']):.3f}`, `n = {global_fit['n_obs']}`",
        "",
        "## By Class",
    ]
    for row in class_fit_rows:
        lines.append(
            f"- `{row['class_label']}`: `beta = {float(row['beta']):.3f}`, `adjR2 = {float(row['adj_r2']):.3f}`, `n = {row['n_cities']}`, representative city = `{row['representative_city']}`"
        )
    lines.extend(
        [
            "",
            "## Pooled Model Tests",
            f"- `M0 -> M1` (class intercepts only): `p = {_p_text(pooled_nested[0]['p_value'])}`",
            f"- `M1 -> M2` (class-specific slopes): `p = {_p_text(pooled_nested[1]['p_value'])}`",
            f"- `M0 -> M2` (full class effect): `p = {_p_text(pooled_nested[2]['p_value'])}`",
            "",
            "## Main Reading",
            f"- Highest class-specific exponent: `{best_beta['class_label']}` with `beta = {float(best_beta['beta']):.3f}`.",
            f"- Lowest class-specific exponent: `{worst_beta['class_label']}` with `beta = {float(worst_beta['beta']):.3f}`.",
            f"- Strongest within-class fit: `{best_r2['class_label']}` with `adjR2 = {float(best_r2['adj_r2']):.3f}`.",
            f"- Largest departure from the global exponent: `{largest_delta['class_label']}` with `|beta_c - beta_global| = {abs(float(largest_delta['beta']) - float(global_fit['beta'])):.3f}`.",
            "",
            "## Interpretation",
        ]
    )

    if _safe_float(pooled_nested[1]["p_value"], 1.0) < 0.05:
        lines.append(
            "- The slope test is significant, so the topology classes are not just shifting the prefactor. They are changing the scaling exponent itself."
        )
    else:
        lines.append(
            "- The slope test is not significant at conventional thresholds, so topology classes mainly shift the level of the law rather than the exponent."
        )

    if _safe_float(pooled_nested[0]["p_value"], 1.0) < 0.05:
        lines.append(
            "- The intercept test is significant, so the classes do alter the baseline level of establishments for a given population."
        )
    else:
        lines.append(
            "- The intercept test is weak, so the class effect is not mainly a baseline displacement."
        )

    lines.extend(
        [
            "",
            "## Files",
            f"- [global_scaling_fit.csv]({(outdir / 'global_scaling_fit.csv').resolve()})",
            f"- [class_scaling_fits.csv]({(outdir / 'class_scaling_fits.csv').resolve()})",
            f"- [pooled_model_summary.csv]({(outdir / 'pooled_model_summary.csv').resolve()})",
            f"- [pooled_nested_tests.csv]({(outdir / 'pooled_nested_tests.csv').resolve()})",
            "",
            "## Figures",
            f"- [class_beta_rank.svg]({beta_fig.resolve()})",
            f"- [class_adj_r2_rank.svg]({r2_fig.resolve()})",
            f"- [class_scaling_scatter_panels.svg]({scatter_fig.resolve()})",
        ]
    )
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
