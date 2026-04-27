#!/usr/bin/env python3
from __future__ import annotations

import csv
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


def _to_float(value: object, default: float = 0.0) -> float:
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


def _fetch_ageb_distance(city_code: str) -> dict[str, float]:
    rows = _query_tsv(
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
        )
        SELECT
            a.unit_code,
            ST_DistanceSphere(ST_Centroid(a.geom), cc.geom)::text AS dist_to_est_center_m
        FROM raw.admin_units a
        CROSS JOIN city_center cc
        WHERE a.level = 'ageb_u' AND a.city_code = '{city_code}'
        ORDER BY a.unit_code
        """.strip(),
        ["unit_code", "dist_to_est_center_m"],
    )
    return {row["unit_code"]: _to_float(row["dist_to_est_center_m"]) / 1000.0 for row in rows}


def _write_rank_chart(path: Path, title: str, subtitle: str, rows: list[dict[str, object]], metric_field: str, color: str, value_fmt) -> Path:
    width = 1200
    left = 430
    right = 120
    top = 100
    bottom = 84
    row_h = 28
    height = top + len(rows) * row_h + bottom
    values = [float(r[metric_field]) for r in rows]
    x_min = min(values + [0.0])
    x_max = max(values + [0.0])
    if math.isclose(x_min, x_max):
        x_min -= 0.1
        x_max += 0.1
    pad = max((x_max - x_min) * 0.08, 0.02)
    x_min -= pad
    x_max += pad
    plot_w = width - left - right

    def px(v: float) -> float:
        return left + ((v - x_min) / max(x_max - x_min, 1e-9)) * plot_w

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
    ]
    for tick in range(6):
        val = x_min + (x_max - x_min) * tick / 5
        x = px(val)
        body.append(f'<line x1="{x:.2f}" y1="{top-10}" x2="{x:.2f}" y2="{height-bottom+8}" stroke="{GRID}"/>')
        body.append(f'<text x="{x:.2f}" y="{height-bottom+34}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{value_fmt(val)}</text>')
    body.append(f'<line x1="{px(0):.2f}" y1="{top-10}" x2="{px(0):.2f}" y2="{height-bottom+8}" stroke="{RUST}" stroke-dasharray="6,5"/>')
    for idx, row in enumerate(rows):
        y = top + idx * row_h
        label = str(row["category_label"])
        metric = float(row[metric_field])
        body.append(f'<text x="{left-14}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{label}</text>')
        body.append(f'<circle cx="{px(metric):.2f}" cy="{y:.2f}" r="4.8" fill="{color}"/>')
        body.append(f'<text x="{width-right+10}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{value_fmt(metric)}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    city_code = "14039"
    root = Path(__file__).resolve().parents[1]
    fit_path = root / "reports" / "ageb-fitability-audit-guadalajara-2026-04-22" / "y_fitability_usable.csv"
    unit_path = root / "reports" / "ageb-city-native-experiments-guadalajara-2026-04-22" / "ageb_y_unit_counts.csv"
    outdir = root / "reports" / "ageb-centrality-extensions-usable-guadalajara-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    usable = _read_csv(fit_path)
    unit_rows = _read_csv(unit_path)
    dist_map = _fetch_ageb_distance(city_code)
    city_name = unit_rows[0]["city_name"]

    meta_by_y = {row["y_key"]: row for row in usable}
    unit_meta: dict[str, dict[str, float]] = {}
    values_by_y: dict[str, dict[str, float]] = {}
    for row in unit_rows:
        unit_code = row["unit_code"]
        unit_meta[unit_code] = {"population": _to_float(row["population"])}
        values_by_y.setdefault(row["y_key"], {})[unit_code] = _to_float(row["y_value"])

    summary_rows = []
    for y_key, meta in meta_by_y.items():
        values = values_by_y.get(y_key, {})
        filtered = []
        for unit_code, unit in unit_meta.items():
            pop = unit["population"]
            y = values.get(unit_code, 0.0)
            dist = dist_map.get(unit_code, 0.0)
            if pop > 0 and y > 0 and dist >= 0:
                filtered.append((unit_code, pop, y, dist))
        logy = [math.log(y) for _, _, y, _ in filtered]
        logn = [math.log(pop) for _, pop, _, _ in filtered]
        logdist = [math.log(dist + 0.25) for _, _, _, dist in filtered]

        m0 = ols_fit([[1.0, ln] for ln in logn], logy)
        m1 = ols_fit([[1.0, ln, ld] for ln, ld in zip(logn, logdist)], logy)
        cmp = compare_nested_models(m0, m1)
        delta_adj_r2 = m1.adj_r2 - m0.adj_r2
        residual_corr = pearson_corr(logdist, m0.residuals)
        summary_rows.append(
            {
                "family": meta["family"],
                "family_label": meta["family_label"],
                "category": meta["category"],
                "category_label": meta["category_label"],
                "y_key": y_key,
                "n_obs": len(filtered),
                "beta_m0": m0.coefficients[1],
                "r2_m0": m0.r2,
                "adj_r2_m0": m0.adj_r2,
                "beta_m1": m1.coefficients[1],
                "gamma_logdist_m1": m1.coefficients[2],
                "r2_m1": m1.r2,
                "adj_r2_m1": m1.adj_r2,
                "delta_adj_r2": delta_adj_r2,
                "residual_corr_logdist": residual_corr,
                "f_stat_m0_vs_m1": cmp.f_stat,
                "p_value_m0_vs_m1": cmp.p_value,
            }
        )

    summary_rows.sort(key=lambda r: float(r["delta_adj_r2"]), reverse=True)
    for idx, row in enumerate(summary_rows, start=1):
        row["delta_adj_r2_rank"] = idx

    _write_csv(outdir / "centrality_extension_summary.csv", summary_rows, list(summary_rows[0].keys()))

    fig1 = _write_rank_chart(
        figdir / "delta_adj_r2_rank.svg",
        "Gain from adding centrality",
        "Change in adjusted R² from M0: logY~logN to M1: logY~logN+log(dist). Higher means centrality explains more of what population misses.",
        summary_rows,
        "delta_adj_r2",
        TEAL,
        lambda v: f"{v:+.3f}",
    )
    fig2 = _write_rank_chart(
        figdir / "residual_corr_logdist_rank.svg",
        "Residual-distance correlation under the base model",
        "Correlation between M0 residuals and log distance. Strong negative values indicate central AGEBs sit above the population-only law.",
        summary_rows,
        "residual_corr_logdist",
        RUST,
        lambda v: f"{v:+.3f}",
    )
    fig3 = _write_rank_chart(
        figdir / "beta_shift_rank.svg",
        "Shift in beta after adding centrality",
        "Difference beta(M1)-beta(M0). Positive values mean the population effect re-emerges once centrality is controlled.",
        [{**r, "beta_shift": float(r["beta_m1"]) - float(r["beta_m0"])} for r in summary_rows],
        "beta_shift",
        BLUE,
        lambda v: f"{v:+.3f}",
    )
    _write_csv(figdir / "figures_manifest.csv", [
        {"figure_id": "delta_adj_r2_rank", "path": str(fig1.resolve()), "description": "Adjusted R² gain from centrality."},
        {"figure_id": "residual_corr_logdist_rank", "path": str(fig2.resolve()), "description": "Residual-distance correlation."},
        {"figure_id": "beta_shift_rank", "path": str(fig3.resolve()), "description": "Shift in beta after adding centrality."},
    ], ["figure_id", "path", "description"])

    top_gain = summary_rows[:5]
    report_lines = [
        "# Guadalajara AGEB Centrality Extension Across Usable Y",
        "",
        "All results use the same AGEB universe. No AGEB subset was selected.",
        "Only Y changed, and for each Y we compared:",
        "- `M0`: `log(Y) ~ log(N)`",
        "- `M1`: `log(Y) ~ log(N) + log(distance_to_establishment_center)`",
        "",
        "## Top centrality-sensitive Y",
        "",
    ]
    for row in top_gain:
        report_lines.append(
            f"- `{row['category_label']}`: `ΔadjR² = {float(row['delta_adj_r2']):+.3f}`, "
            f"`beta(M0) = {float(row['beta_m0']):+.3f}`, `beta(M1) = {float(row['beta_m1']):+.3f}`, "
            f"`corr(resid_M0, logdist) = {float(row['residual_corr_logdist']):+.3f}`, `p = {float(row['p_value_m0_vs_m1']):.3g}`"
        )
    report_lines.extend(
        [
            "",
            "## Files",
            "",
            f"- [centrality_extension_summary.csv]({(outdir / 'centrality_extension_summary.csv').resolve()})",
            "",
            "## Figures",
            "",
            f"- [delta_adj_r2_rank.svg]({fig1.resolve()})",
            f"- [residual_corr_logdist_rank.svg]({fig2.resolve()})",
            f"- [beta_shift_rank.svg]({fig3.resolve()})",
        ]
    )
    (outdir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    print(json.dumps({"ok": True, "output_dir": str(outdir), "n_y": len(summary_rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
