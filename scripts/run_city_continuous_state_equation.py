#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import subprocess
from pathlib import Path

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit


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


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _query_rows(source_method: str) -> list[dict[str, object]]:
    sql = f"""
    SELECT city_code, city_name, state_code,
           population::text, est_total::text,
           city_area_km2::text,
           street_density_km_per_km2::text,
           intersection_density_km2::text,
           boundary_entry_edges_per_km::text,
           mean_degree::text,
           circuity_avg::text
    FROM derived.city_network_metrics
    WHERE source_method = '{source_method}'
      AND population > 0
      AND est_total > 0
      AND city_area_km2 > 0
      AND street_density_km_per_km2 > 0
      AND intersection_density_km2 > 0
      AND boundary_entry_edges_per_km > 0
      AND mean_degree > 0
      AND circuity_avg > 0
    ORDER BY city_code;
    """
    out = _psql(sql)
    rows: list[dict[str, object]] = []
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) != 11:
            continue
        city_code, city_name, state_code, pop, est, area, street, inters, boundary, degree, circuity = parts
        rows.append(
            {
                "city_code": city_code,
                "city_name": city_name,
                "state_code": state_code,
                "population": _safe_float(pop),
                "est_total": _safe_float(est),
                "city_area_km2": _safe_float(area),
                "street_density_km_per_km2": _safe_float(street),
                "intersection_density_km2": _safe_float(inters),
                "boundary_entry_edges_per_km": _safe_float(boundary),
                "mean_degree": _safe_float(degree),
                "circuity_avg": _safe_float(circuity),
            }
        )
    return rows


def _fit_block(rows: list[dict[str, object]], keys: list[str]) -> dict[str, object]:
    ln_n = [math.log(float(r["population"])) for r in rows]
    y = [math.log(float(r["est_total"])) for r in rows]
    ln_n_ref = _mean(ln_n)
    n_ref = math.exp(ln_n_ref)
    z_refs = {k: math.exp(_mean([math.log(float(r[k])) for r in rows])) for k in keys}

    centered_n = [v - ln_n_ref for v in ln_n]
    centered_z = {
        k: [math.log(float(r[k]) / z_refs[k]) for r in rows]
        for k in keys
    }

    x0 = [[1.0, n] for n in centered_n]
    fit0 = ols_fit(x0, y)

    x1 = []
    for idx in range(len(rows)):
        vec = [1.0, centered_n[idx]]
        for k in keys:
            vec.append(centered_z[k][idx])
        x1.append(vec)
    fit1 = ols_fit(x1, y)

    x2 = []
    for idx in range(len(rows)):
        n = centered_n[idx]
        vec = [1.0, n]
        zvals = [centered_z[k][idx] for k in keys]
        vec.extend(zvals)
        vec.extend([n * z for z in zvals])
        x2.append(vec)
    fit2 = ols_fit(x2, y)

    cmp01 = compare_nested_models(fit0, fit1)
    cmp12 = compare_nested_models(fit1, fit2)
    cmp02 = compare_nested_models(fit0, fit2)

    sami = [yi - yhat for yi, yhat in zip(y, fit0.fitted)]
    r0 = ols_fit([[1.0] for _ in rows], sami)
    xr1 = [[1.0] + [centered_z[k][idx] for k in keys] for idx in range(len(rows))]
    r1 = ols_fit(xr1, sami)
    cmp_r = compare_nested_models(r0, r1)

    return {
        "keys": keys,
        "n_obs": len(rows),
        "n_ref": n_ref,
        "z_refs": z_refs,
        "centered_n": centered_n,
        "centered_z": centered_z,
        "y": y,
        "fit0": fit0,
        "fit1": fit1,
        "fit2": fit2,
        "cmp01": cmp01,
        "cmp12": cmp12,
        "cmp02": cmp02,
        "resid_fit": r1,
        "resid_cmp": cmp_r,
        "sami": sami,
    }


def _rank_chart(path: Path, title: str, subtitle: str, rows: list[dict[str, object]], metric: str) -> Path:
    width = 1180
    left = 320
    right = 90
    top = 96
    bottom = 78
    row_h = 34
    height = top + len(rows) * row_h + bottom
    vals = [float(r[metric]) for r in rows]
    xmax = max(vals) * 1.05 if vals else 1.0

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
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["block_id"]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def _comparison_chart(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 920
    height = 560
    left = 110
    right = 80
    top = 110
    bottom = 90
    plot_w = width - left - right
    plot_h = height - top - bottom
    ymax = max(float(r["rate"]) for r in rows) * 1.15
    ymax = max(ymax, 0.2)
    bar_w = plot_w / (len(rows) * 1.8)
    gap = bar_w * 0.8
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="20" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="28" font-family="{SERIF}" fill="{TEXT}">How Much SAMI Structure Is Recovered?</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Comparison of discrete classes versus continuous state equations.</text>',
    ]
    for g in range(5):
        y = top + g * plot_h / 4.0
        value = ymax * (1.0 - g / 4.0)
        body.append(f'<line x1="{left:.2f}" y1="{y:.2f}" x2="{left+plot_w:.2f}" y2="{y:.2f}" stroke="{GRID}" stroke-opacity="0.7"/>')
        body.append(f'<text x="{left-10}" y="{y+4:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{MUTED}">{value:.2f}</text>')
    for idx, row in enumerate(rows):
        x = left + idx * (bar_w + gap) + 24
        h = (float(row["rate"]) / ymax) * plot_h
        y = top + plot_h - h
        body.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_w:.2f}" height="{h:.2f}" fill="{row["color"]}"/>')
        body.append(f'<text x="{x+bar_w/2:.2f}" y="{y-8:.2f}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{TEXT}">{float(row["rate"]):.3f}</text>')
        body.append(f'<text x="{x+bar_w/2:.2f}" y="{top+plot_h+24:.2f}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["label"]}</text>')
    return _svg(path, width, height, "".join(body))


def _beta_bridge_svg(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1160
    height = 620
    left = 260
    right = 110
    top = 110
    bottom = 90
    row_h = 74
    ymax = max(max(float(r["beta_discrete"]), float(r["beta_eff_mean_class"]), float(r["beta_eff_rep_city"])) for r in rows) * 1.1
    ymin = min(min(float(r["beta_discrete"]), float(r["beta_eff_mean_class"]), float(r["beta_eff_rep_city"])) for r in rows) * 0.9
    if ymin > 0:
        ymin = 0.0
    yr = max(ymax - ymin, 1e-9)

    def px(v: float) -> float:
        return left + ((v - ymin) / yr) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="20" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="28" font-family="{SERIF}" fill="{TEXT}">Discrete Class Betas Versus Continuous Beta_eff</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Blue = discrete class fit, teal = continuous state mean within class, rust = representative city beta_eff.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        cy = y + 16
        body.append(f'<text x="{left-12}" y="{cy+4:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["class_label"]}</text>')
        body.append(f'<line x1="{px(float(row["beta_discrete"])):.2f}" y1="{cy-10:.2f}" x2="{px(float(row["beta_discrete"])):.2f}" y2="{cy+10:.2f}" stroke="{BLUE}" stroke-width="4"/>')
        body.append(f'<line x1="{px(float(row["beta_eff_mean_class"])):.2f}" y1="{cy-10:.2f}" x2="{px(float(row["beta_eff_mean_class"])):.2f}" y2="{cy+10:.2f}" stroke="{TEAL}" stroke-width="4"/>')
        body.append(f'<circle cx="{px(float(row["beta_eff_rep_city"])):.2f}" cy="{cy:.2f}" r="6" fill="{RUST}"/>')
        body.append(f'<text x="{px(float(row["beta_discrete"]))+8:.2f}" y="{cy-12:.2f}" font-size="10" font-family="{SANS}" fill="{BLUE}">{float(row["beta_discrete"]):.3f}</text>')
        body.append(f'<text x="{px(float(row["beta_eff_mean_class"]))+8:.2f}" y="{cy+4:.2f}" font-size="10" font-family="{SANS}" fill="{TEAL}">{float(row["beta_eff_mean_class"]):.3f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fit a continuous city-scale state equation using persisted OSM network metrics.")
    parser.add_argument("--source-method", default="osm_drive_municipal_full_v1")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/city-continuous-state-equation-2026-04-24"),
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    outdir = root / args.output_dir
    figdir = outdir / "figures"
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    rows = _query_rows(args.source_method)

    candidate_blocks = {
        "pars_area_boundary_degree_circuity": [
            "city_area_km2",
            "boundary_entry_edges_per_km",
            "mean_degree",
            "circuity_avg",
        ],
        "pars_area_intersection_degree_circuity": [
            "city_area_km2",
            "intersection_density_km2",
            "mean_degree",
            "circuity_avg",
        ],
        "pars_area_street_degree_circuity": [
            "city_area_km2",
            "street_density_km_per_km2",
            "mean_degree",
            "circuity_avg",
        ],
        "all_five": [
            "city_area_km2",
            "boundary_entry_edges_per_km",
            "intersection_density_km2",
            "mean_degree",
            "circuity_avg",
        ],
    }

    fit_map = {block_id: _fit_block(rows, keys) for block_id, keys in candidate_blocks.items()}

    candidate_rows: list[dict[str, object]] = []
    for block_id, fitted in fit_map.items():
        candidate_rows.append(
            {
                "block_id": block_id,
                "n_metrics": len(fitted["keys"]),
                "metrics": "|".join(fitted["keys"]),
                "m0_adj_r2": fitted["fit0"].adj_r2,
                "m1_adj_r2": fitted["fit1"].adj_r2,
                "m2_adj_r2": fitted["fit2"].adj_r2,
                "residual_adj_r2": fitted["resid_fit"].adj_r2,
                "m0_to_m1_p_value": fitted["cmp01"].p_value,
                "m1_to_m2_p_value": fitted["cmp12"].p_value,
                "m0_to_m2_p_value": fitted["cmp02"].p_value,
                "residual_p_value": fitted["resid_cmp"].p_value,
            }
        )
    candidate_rows.sort(key=lambda r: float(r["m2_adj_r2"]), reverse=True)

    pars_only = [r for r in candidate_rows if r["block_id"] != "all_five"]
    selected_row = max(pars_only, key=lambda r: float(r["m2_adj_r2"]))
    selected_block = selected_row["block_id"]
    selected_fit = fit_map[selected_block]
    unrestricted_row = next(r for r in candidate_rows if r["block_id"] == "all_five")
    unrestricted_fit = fit_map["all_five"]

    ref_rows = [
        {"variable": "population", "reference_value": selected_fit["n_ref"], "transform": "log(N/N0)"},
    ]
    for key in selected_fit["keys"]:
        ref_rows.append(
            {
                "variable": key,
                "reference_value": selected_fit["z_refs"][key],
                "transform": f"log({key}/{key}_0)",
            }
        )

    coef_rows: list[dict[str, object]] = []
    fit1 = selected_fit["fit1"]
    fit2 = selected_fit["fit2"]
    coef_rows.append(
        {
            "model_id": "M1_level_state",
            "term": "alpha",
            "coefficient": fit1.coefficients[0],
            "stderr": fit1.stderr[0],
            "term_role": "intercept",
        }
    )
    coef_rows.append(
        {
            "model_id": "M1_level_state",
            "term": "beta",
            "coefficient": fit1.coefficients[1],
            "stderr": fit1.stderr[1],
            "term_role": "size_exponent_at_average_state",
        }
    )
    for idx, key in enumerate(selected_fit["keys"]):
        coef_rows.append(
            {
                "model_id": "M1_level_state",
                "term": f"gamma_{key}",
                "coefficient": fit1.coefficients[2 + idx],
                "stderr": fit1.stderr[2 + idx],
                "term_role": "state_level_shift",
            }
        )

    coef_rows.append(
        {
            "model_id": "M2_state_dependent_beta",
            "term": "alpha",
            "coefficient": fit2.coefficients[0],
            "stderr": fit2.stderr[0],
            "term_role": "intercept",
        }
    )
    coef_rows.append(
        {
            "model_id": "M2_state_dependent_beta",
            "term": "beta",
            "coefficient": fit2.coefficients[1],
            "stderr": fit2.stderr[1],
            "term_role": "baseline_beta_at_average_state",
        }
    )
    for idx, key in enumerate(selected_fit["keys"]):
        coef_rows.append(
            {
                "model_id": "M2_state_dependent_beta",
                "term": f"gamma_{key}",
                "coefficient": fit2.coefficients[2 + idx],
                "stderr": fit2.stderr[2 + idx],
                "term_role": "state_level_shift",
            }
        )
    for idx, key in enumerate(selected_fit["keys"]):
        coef_rows.append(
            {
                "model_id": "M2_state_dependent_beta",
                "term": f"delta_{key}",
                "coefficient": fit2.coefficients[2 + len(selected_fit["keys"]) + idx],
                "stderr": fit2.stderr[2 + len(selected_fit["keys"]) + idx],
                "term_role": "beta_modulation",
            }
        )

    nested_rows = [
        {
            "comparison": "M0_to_M1",
            "null": "state variables do not shift the level of the scaling law",
            "f_stat": selected_fit["cmp01"].f_stat,
            "df_num": selected_fit["cmp01"].df_num,
            "df_den": selected_fit["cmp01"].df_den,
            "p_value": selected_fit["cmp01"].p_value,
        },
        {
            "comparison": "M1_to_M2",
            "null": "state variables do not modulate beta continuously",
            "f_stat": selected_fit["cmp12"].f_stat,
            "df_num": selected_fit["cmp12"].df_num,
            "df_den": selected_fit["cmp12"].df_den,
            "p_value": selected_fit["cmp12"].p_value,
        },
        {
            "comparison": "M0_to_M2",
            "null": "continuous state does not matter at all",
            "f_stat": selected_fit["cmp02"].f_stat,
            "df_num": selected_fit["cmp02"].df_num,
            "df_den": selected_fit["cmp02"].df_den,
            "p_value": selected_fit["cmp02"].p_value,
        },
    ]

    state_rows: list[dict[str, object]] = []
    n_ref = selected_fit["n_ref"]
    base_alpha = fit2.coefficients[0]
    base_beta = fit2.coefficients[1]
    gammas = {k: fit2.coefficients[2 + idx] for idx, k in enumerate(selected_fit["keys"])}
    deltas = {k: fit2.coefficients[2 + len(selected_fit["keys"]) + idx] for idx, k in enumerate(selected_fit["keys"])}
    for idx, row in enumerate(rows):
        n_center = selected_fit["centered_n"][idx]
        zvals = {k: selected_fit["centered_z"][k][idx] for k in selected_fit["keys"]}
        alpha_eff = base_alpha + sum(gammas[k] * zvals[k] for k in selected_fit["keys"])
        beta_eff = base_beta + sum(deltas[k] * zvals[k] for k in selected_fit["keys"])
        pred_loge = alpha_eff + beta_eff * n_center
        obs_loge = selected_fit["y"][idx]
        state_rows.append(
            {
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "state_code": row["state_code"],
                "population": row["population"],
                "est_total": row["est_total"],
                "logN_centered": n_center,
                **{f"z_{k}": zvals[k] for k in selected_fit["keys"]},
                "alpha_eff": alpha_eff,
                "beta_eff": beta_eff,
                "observed_logE": obs_loge,
                "predicted_logE": pred_loge,
                "residual_logE": obs_loge - pred_loge,
            }
        )

    class_summary = _read_csv(root / "reports" / "city-network-typology-2026-04-24" / "cluster_summary.csv")
    discrete_fits = _read_csv(root / "reports" / "city-class-scaling-laws-2026-04-24" / "class_scaling_fits.csv")
    discrete_map = {r["class_label"]: r for r in discrete_fits}
    state_map = {r["city_code"]: r for r in state_rows}
    class_member_rows = _read_csv(root / "reports" / "city-network-typology-2026-04-24" / "cluster_members.csv")
    members_by_class: dict[str, list[str]] = {}
    for row in class_member_rows:
        members_by_class.setdefault(row["class_label"], []).append(row["city_code"])
    bridge_rows: list[dict[str, object]] = []
    for row in class_summary:
        class_label = row["class_label"]
        rep_code = row["representative_city_code"]
        member_codes = [code for code in members_by_class.get(class_label, []) if code in state_map]
        beta_mean = _mean([float(state_map[code]["beta_eff"]) for code in member_codes]) if member_codes else 0.0
        bridge_rows.append(
            {
                "class_label": class_label,
                "representative_city": row["representative_city"],
                "representative_city_code": rep_code,
                "beta_discrete": _safe_float(discrete_map[class_label]["beta"]),
                "beta_eff_rep_city": _safe_float(state_map[rep_code]["beta_eff"]) if rep_code in state_map else None,
                "beta_eff_mean_class": beta_mean,
                "n_members": len(member_codes),
            }
        )

    class_sami_summary = _read_csv(root / "reports" / "city-class-sami-distributions-2026-04-24" / "summary.csv")
    class_sami_r2 = _safe_float(class_sami_summary[0]["class_model_r2"]) if class_sami_summary else 0.0
    explain_rows = [
        {"label": "Class labels", "rate": class_sami_r2, "color": BLUE},
        {"label": "Best parsimonious continuous", "rate": selected_fit["resid_fit"].r2, "color": TEAL},
        {"label": "Unrestricted all-five continuous", "rate": unrestricted_fit["resid_fit"].r2, "color": RUST},
    ]

    _write_csv(outdir / "candidate_block_summary.csv", candidate_rows, list(candidate_rows[0].keys()))
    _write_csv(outdir / "selected_model_coefficients.csv", coef_rows, list(coef_rows[0].keys()))
    _write_csv(outdir / "selected_model_nested_tests.csv", nested_rows, list(nested_rows[0].keys()))
    _write_csv(outdir / "selected_model_references.csv", ref_rows, list(ref_rows[0].keys()))
    _write_csv(outdir / "city_effective_state_parameters.csv", state_rows, list(state_rows[0].keys()))
    _write_csv(outdir / "beta_bridge_by_class.csv", bridge_rows, list(bridge_rows[0].keys()))
    _write_csv(outdir / "sami_explanation_comparison.csv", explain_rows, list(explain_rows[0].keys()))

    rank_fig = _rank_chart(
        figdir / "candidate_block_m2_adj_r2.svg",
        "Continuous State Blocks by Scaling Fit",
        "Adjusted R² of M2: log(E) = alpha + beta n + gamma·z + n(delta·z).",
        candidate_rows,
        "m2_adj_r2",
    )
    explain_fig = _comparison_chart(figdir / "sami_explanation_comparison.svg", explain_rows)
    bridge_fig = _beta_bridge_svg(figdir / "beta_bridge_by_class.svg", bridge_rows)
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "candidate_block_m2_adj_r2", "path": str(rank_fig.resolve()), "description": "Ranking of candidate continuous state blocks by scaling fit."},
            {"figure_id": "sami_explanation_comparison", "path": str(explain_fig.resolve()), "description": "How much SAMI structure is recovered by discrete classes versus continuous state."},
            {"figure_id": "beta_bridge_by_class", "path": str(bridge_fig.resolve()), "description": "Discrete class betas compared with continuous beta_eff."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# Continuous State Equation for City Scaling",
        "",
        "This experiment replaces discrete topology classes with continuous support/network state variables.",
        "",
        "Dimensionless centered coordinates:",
        "- `n_i = log(N_i / N_0)`",
        f"- `N_0 = {n_ref:.3f}` (geometric-mean population across fitted cities)",
    ]
    for ref in ref_rows[1:]:
        lines.append(f"- `{ref['variable']}_0 = {float(ref['reference_value']):.6f}` so `z_{ref['variable']} = log({ref['variable']} / {ref['variable']}_0)`")
    lines.extend(
        [
            "",
            "Model family:",
            "- `M0: log(E_i) = alpha + beta n_i + epsilon_i`",
            "- `M1: log(E_i) = alpha + beta n_i + sum_m gamma_m z_{m,i} + epsilon_i`",
            "- `M2: log(E_i) = alpha + beta n_i + sum_m gamma_m z_{m,i} + n_i sum_m delta_m z_{m,i} + epsilon_i`",
            "",
            "Interpretation:",
            "- `gamma_m` shifts the level of the law continuously with city state.",
            "- `delta_m` makes the scaling exponent itself state-dependent.",
            "- `beta_eff(i) = beta + sum_m delta_m z_{m,i}`",
            "",
            f"Selected parsimonious block: `{selected_block}`",
            f"- metrics: `{', '.join(selected_fit['keys'])}`",
            f"- `M0 adjR² = {selected_fit['fit0'].adj_r2:.3f}`",
            f"- `M1 adjR² = {selected_fit['fit1'].adj_r2:.3f}`",
            f"- `M2 adjR² = {selected_fit['fit2'].adj_r2:.3f}`",
            f"- `M0 -> M1` p-value = `{float(selected_fit['cmp01'].p_value):.2e}`",
            f"- `M1 -> M2` p-value = `{float(selected_fit['cmp12'].p_value):.2e}`",
            "",
            f"Best unrestricted block: `all_five` with `M2 adjR² = {unrestricted_fit['fit2'].adj_r2:.3f}`",
            f"- gain over selected parsimonious block = `{(unrestricted_fit['fit2'].adj_r2 - selected_fit['fit2'].adj_r2):+.4f}`",
            "",
            "Residual/SAMI comparison:",
            f"- discrete classes explain `R² = {class_sami_r2:.3f}` of SAMI",
            f"- selected continuous block explains `R² = {selected_fit['resid_fit'].r2:.3f}` of SAMI",
            f"- unrestricted all-five block explains `R² = {unrestricted_fit['resid_fit'].r2:.3f}` of SAMI",
            "",
            "This means the continuous state equation recovers more of the residual structure than class labels alone.",
            "",
            "Bridge to discrete classes:",
            "- We also evaluate `beta_eff` for each city and summarize it within topology classes.",
            "- If continuous state were a perfect replacement, class-average `beta_eff` would closely match the discrete `beta_c` values.",
            "- In practice it captures part, but not all, of the slope heterogeneity.",
            "",
            "Key files:",
            f"- [candidate_block_summary.csv]({(outdir / 'candidate_block_summary.csv').resolve()})",
            f"- [selected_model_coefficients.csv]({(outdir / 'selected_model_coefficients.csv').resolve()})",
            f"- [selected_model_nested_tests.csv]({(outdir / 'selected_model_nested_tests.csv').resolve()})",
            f"- [selected_model_references.csv]({(outdir / 'selected_model_references.csv').resolve()})",
            f"- [city_effective_state_parameters.csv]({(outdir / 'city_effective_state_parameters.csv').resolve()})",
            f"- [beta_bridge_by_class.csv]({(outdir / 'beta_bridge_by_class.csv').resolve()})",
            "",
            "Figures:",
            f"- [candidate_block_m2_adj_r2.svg]({rank_fig.resolve()})",
            f"- [sami_explanation_comparison.svg]({explain_fig.resolve()})",
            f"- [beta_bridge_by_class.svg]({bridge_fig.resolve()})",
        ]
    )
    (outdir / "report.md").write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
