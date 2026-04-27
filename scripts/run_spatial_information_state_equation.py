#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit, pearson_corr


WINDOWS_DOCKER = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DOCKER_EXE = os.environ.get("DOCKER_EXE") or shutil.which("docker") or WINDOWS_DOCKER
DB_CONTAINER = os.environ.get("DB_CONTAINER", "24-polisplexity-core-db-dev")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_NAME = os.environ.get("DB_NAME", "urban_sami_exp")

REGIME_SOURCE_METHOD = "bettencourt_spatial_information_regimes_v1"
FEATURES_SOURCE_METHOD = "bettencourt_spatial_information_state_features_v1"
MODEL_SOURCE_METHOD = "bettencourt_spatial_information_state_eq_v1"
NETWORK_SOURCE_METHOD = "osm_drive_municipal_full_v1"
OUTDIR = ROOT / "reports" / "spatial-information-state-equation-2026-04-25"
GAP_THRESHOLD = 0.10


def _bootstrap() -> None:
    script = ROOT / "scripts" / "bootstrap_experiment_db.sh"
    subprocess.run(["bash", str(script)], check=True)


def _psql_copy(sql: str) -> list[dict[str, str]]:
    script = f"""
\\set ON_ERROR_STOP on
COPY (
{sql}
) TO STDOUT WITH CSV HEADER;
"""
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
        "-v",
        "ON_ERROR_STOP=1",
    ]
    proc = subprocess.run(cmd, input=script, text=True, capture_output=True, check=True)
    lines = proc.stdout.splitlines()
    if not lines:
        return []
    return list(csv.DictReader(lines))


def _psql_exec(sql: str) -> None:
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
        "-v",
        "ON_ERROR_STOP=1",
    ]
    subprocess.run(cmd, input=sql, text=True, check=True)


def _copy_rows(table: str, columns: list[str], rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    lines: list[str] = []
    for row in rows:
        vals: list[str] = []
        for col in columns:
            value = row.get(col)
            if value is None:
                vals.append("\\N")
            else:
                text = str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")
                vals.append(text)
        lines.append("\t".join(vals) + "\n")
    sql = f"""
\\set ON_ERROR_STOP on
COPY {table} ({", ".join(columns)})
FROM STDIN WITH (FORMAT text);
"""
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
        "-v",
        "ON_ERROR_STOP=1",
    ]
    subprocess.run(cmd, input=sql + "".join(lines) + "\\.\n", text=True, check=True)


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _sql_text(value: object) -> str:
    return str(value).replace("'", "''")


def _query_rows() -> list[dict[str, object]]:
    rows = _psql_copy(
        f"""
        SELECT
            r.family,
            r.city_code,
            r.city_name,
            r.state_code,
            r.population::text,
            r.est_total::text,
            r.mi_manzana_nats::text,
            r.share_within_explained::text,
            r.share_between_explained::text,
            r.share_gap::text,
            n.city_area_km2::text,
            n.street_density_km_per_km2::text,
            n.mean_degree::text,
            n.circuity_avg::text
        FROM derived.city_spatial_information_regimes r
        JOIN derived.city_network_metrics n
          ON n.source_method = '{NETWORK_SOURCE_METHOD}'
         AND n.city_code = r.city_code
        WHERE r.source_method = '{REGIME_SOURCE_METHOD}'
        ORDER BY r.family, r.city_code
        """
    )
    out: list[dict[str, object]] = []
    for row in rows:
        out.append(
            {
                "family": row["family"],
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "state_code": row["state_code"],
                "population": _safe_float(row["population"]),
                "est_total": _safe_float(row["est_total"]),
                "mi_manzana_nats": _safe_float(row["mi_manzana_nats"]),
                "share_within_explained": _safe_float(row["share_within_explained"]),
                "share_between_explained": _safe_float(row["share_between_explained"]),
                "share_gap": _safe_float(row["share_gap"]),
                "city_area_km2": _safe_float(row["city_area_km2"]),
                "street_density_km_per_km2": _safe_float(row["street_density_km_per_km2"]),
                "mean_degree": _safe_float(row["mean_degree"]),
                "circuity_avg": _safe_float(row["circuity_avg"]),
            }
        )
    return out


def _geom_mean(values: list[float]) -> float:
    return math.exp(sum(math.log(v) for v in values) / float(len(values)))


def _fit_family(rows: list[dict[str, object]], family: str) -> dict[str, object]:
    clean = [
        r for r in rows
        if r["family"] == family
        and float(r["population"]) > 0
        and float(r["est_total"]) > 0
        and float(r["mi_manzana_nats"]) > 0
        and float(r["city_area_km2"]) > 0
        and float(r["street_density_km_per_km2"]) > 0
        and float(r["mean_degree"]) > 0
        and float(r["circuity_avg"]) > 0
        and 0.0 < float(r["share_within_explained"]) < 1.0
        and abs(float(r["share_gap"])) <= GAP_THRESHOLD
    ]
    pop_ref = _geom_mean([float(r["population"]) for r in clean])
    area_ref = _geom_mean([float(r["city_area_km2"]) for r in clean])
    street_ref = _geom_mean([float(r["street_density_km_per_km2"]) for r in clean])
    degree_ref = _geom_mean([float(r["mean_degree"]) for r in clean])
    circuity_ref = _geom_mean([float(r["circuity_avg"]) for r in clean])
    mi_ref = _geom_mean([float(r["mi_manzana_nats"]) for r in clean])
    within_mean = sum(float(r["share_within_explained"]) for r in clean) / float(len(clean))

    for r in clean:
        r["logE"] = math.log(float(r["est_total"]))
        r["n"] = math.log(float(r["population"]) / pop_ref)
        r["a"] = math.log(float(r["city_area_km2"]) / area_ref)
        r["s"] = math.log(float(r["street_density_km_per_km2"]) / street_ref)
        r["k"] = math.log(float(r["mean_degree"]) / degree_ref)
        r["c"] = math.log(float(r["circuity_avg"]) / circuity_ref)
        r["m"] = math.log(float(r["mi_manzana_nats"]) / mi_ref)
        r["q"] = float(r["share_within_explained"]) - within_mean

    y = [float(r["logE"]) for r in clean]
    n = [float(r["n"]) for r in clean]
    a = [float(r["a"]) for r in clean]
    s = [float(r["s"]) for r in clean]
    k = [float(r["k"]) for r in clean]
    c = [float(r["c"]) for r in clean]
    m = [float(r["m"]) for r in clean]
    q = [float(r["q"]) for r in clean]

    X0 = [[1.0, ni] for ni in n]
    fit0 = ols_fit(X0, y)

    X1 = [[1.0, ni, ai, si, ki, ci] for ni, ai, si, ki, ci in zip(n, a, s, k, c)]
    fit1 = ols_fit(X1, y)

    X2 = [[1.0, ni, ai, si, ki, ci, ni * ai, ni * si, ni * ki, ni * ci] for ni, ai, si, ki, ci in zip(n, a, s, k, c)]
    fit2 = ols_fit(X2, y)

    X3 = [[1.0, ni, ai, si, ki, ci, ni * ai, ni * si, ni * ki, ni * ci, mi, qi] for ni, ai, si, ki, ci, mi, qi in zip(n, a, s, k, c, m, q)]
    fit3 = ols_fit(X3, y)

    X4 = [[1.0, ni, ai, si, ki, ci, ni * ai, ni * si, ni * ki, ni * ci, mi, qi, ni * mi, ni * qi] for ni, ai, si, ki, ci, mi, qi in zip(n, a, s, k, c, m, q)]
    fit4 = ols_fit(X4, y)

    nested = {
        "M0_to_M1": compare_nested_models(fit0, fit1),
        "M1_to_M2": compare_nested_models(fit1, fit2),
        "M2_to_M3": compare_nested_models(fit2, fit3),
        "M3_to_M4": compare_nested_models(fit3, fit4),
        "M2_to_M4": compare_nested_models(fit2, fit4),
        "M0_to_M4": compare_nested_models(fit0, fit4),
    }

    correlations = []
    corr_targets = {
        "n_centered": n,
        "z_city_area_km2": a,
        "z_street_density_km_per_km2": s,
        "z_mean_degree": k,
        "z_circuity_avg": c,
        "z_log_mi_manzana": m,
    }
    share_within = [float(r["share_within_explained"]) for r in clean]
    share_between = [float(r["share_between_explained"]) for r in clean]
    for name, vec in corr_targets.items():
        correlations.append(
            {
                "family": family,
                "target": name,
                "corr_with_share_within_explained": pearson_corr(vec, share_within),
                "corr_with_share_between_explained": pearson_corr(vec, share_between),
            }
        )

    coef_rows: list[dict[str, object]] = []
    def add_coef(model_id: str, fit, terms: list[tuple[str, str]]) -> None:
        for idx, (term, role) in enumerate(terms):
            coef_rows.append(
                {
                    "family": family,
                    "model_id": model_id,
                    "term": term,
                    "coefficient": fit.coefficients[idx],
                    "stderr": fit.stderr[idx],
                    "term_role": role,
                }
            )

    add_coef("M0_size_only", fit0, [("alpha", "intercept"), ("beta", "size_exponent")])
    add_coef(
        "M1_network_level",
        fit1,
        [
            ("alpha", "intercept"),
            ("beta", "size_exponent"),
            ("gamma_area", "network_level_shift"),
            ("gamma_street", "network_level_shift"),
            ("gamma_degree", "network_level_shift"),
            ("gamma_circuity", "network_level_shift"),
        ],
    )
    add_coef(
        "M2_network_beta",
        fit2,
        [
            ("alpha", "intercept"),
            ("beta", "baseline_beta"),
            ("gamma_area", "network_level_shift"),
            ("gamma_street", "network_level_shift"),
            ("gamma_degree", "network_level_shift"),
            ("gamma_circuity", "network_level_shift"),
            ("delta_area", "beta_modulation"),
            ("delta_street", "beta_modulation"),
            ("delta_degree", "beta_modulation"),
            ("delta_circuity", "beta_modulation"),
        ],
    )
    add_coef(
        "M3_network_plus_info_level",
        fit3,
        [
            ("alpha", "intercept"),
            ("beta", "baseline_beta"),
            ("gamma_area", "network_level_shift"),
            ("gamma_street", "network_level_shift"),
            ("gamma_degree", "network_level_shift"),
            ("gamma_circuity", "network_level_shift"),
            ("delta_area", "beta_modulation"),
            ("delta_street", "beta_modulation"),
            ("delta_degree", "beta_modulation"),
            ("delta_circuity", "beta_modulation"),
            ("eta_log_mi", "information_level_shift"),
            ("eta_within_share", "information_level_shift"),
        ],
    )
    add_coef(
        "M4_full_information_state",
        fit4,
        [
            ("alpha", "intercept"),
            ("beta", "baseline_beta"),
            ("gamma_area", "network_level_shift"),
            ("gamma_street", "network_level_shift"),
            ("gamma_degree", "network_level_shift"),
            ("gamma_circuity", "network_level_shift"),
            ("delta_area", "beta_modulation"),
            ("delta_street", "beta_modulation"),
            ("delta_degree", "beta_modulation"),
            ("delta_circuity", "beta_modulation"),
            ("eta_log_mi", "information_level_shift"),
            ("eta_within_share", "information_level_shift"),
            ("psi_log_mi", "information_beta_modulation"),
            ("psi_within_share", "information_beta_modulation"),
        ],
    )

    gamma_terms = dict(zip(["area", "street", "degree", "circuity"], fit4.coefficients[2:6]))
    delta_terms = dict(zip(["area", "street", "degree", "circuity"], fit4.coefficients[6:10]))
    eta_log_mi = fit4.coefficients[10]
    eta_within = fit4.coefficients[11]
    psi_log_mi = fit4.coefficients[12]
    psi_within = fit4.coefficients[13]

    city_params: list[dict[str, object]] = []
    feature_rows: list[dict[str, object]] = []
    for r in clean:
        alpha_eff = (
            fit4.coefficients[0]
            + gamma_terms["area"] * float(r["a"])
            + gamma_terms["street"] * float(r["s"])
            + gamma_terms["degree"] * float(r["k"])
            + gamma_terms["circuity"] * float(r["c"])
            + eta_log_mi * float(r["m"])
            + eta_within * float(r["q"])
        )
        beta_eff = (
            fit4.coefficients[1]
            + delta_terms["area"] * float(r["a"])
            + delta_terms["street"] * float(r["s"])
            + delta_terms["degree"] * float(r["k"])
            + delta_terms["circuity"] * float(r["c"])
            + psi_log_mi * float(r["m"])
            + psi_within * float(r["q"])
        )
        pred = alpha_eff + beta_eff * float(r["n"])
        city_params.append(
            {
                "source_file": "derived.city_spatial_information_regimes|derived.city_network_metrics",
                "source_method": MODEL_SOURCE_METHOD,
                "family": family,
                "city_code": r["city_code"],
                "city_name": r["city_name"],
                "state_code": r["state_code"],
                "population": r["population"],
                "est_total": r["est_total"],
                "alpha_eff": alpha_eff,
                "beta_eff": beta_eff,
                "observed_logE": r["logE"],
                "predicted_logE": pred,
                "residual_logE": float(r["logE"]) - pred,
                "z_log_mi_manzana": r["m"],
                "z_share_within_explained": r["q"],
                "notes": "",
            }
        )
        feature_rows.append(
            {
                "source_file": "derived.city_spatial_information_regimes|derived.city_network_metrics",
                "source_method": FEATURES_SOURCE_METHOD,
                "regimes_source_method": REGIME_SOURCE_METHOD,
                "family": family,
                "city_code": r["city_code"],
                "city_name": r["city_name"],
                "state_code": r["state_code"],
                "population": r["population"],
                "est_total": r["est_total"],
                "log_est_total": r["logE"],
                "mi_manzana_nats": r["mi_manzana_nats"],
                "share_within_explained": r["share_within_explained"],
                "share_between_explained": r["share_between_explained"],
                "share_gap": r["share_gap"],
                "n_centered": r["n"],
                "z_city_area_km2": r["a"],
                "z_street_density_km_per_km2": r["s"],
                "z_mean_degree": r["k"],
                "z_circuity_avg": r["c"],
                "z_log_mi_manzana": r["m"],
                "z_share_within_explained": r["q"],
                "notes": "",
            }
        )

    model_summary = [
        {"family": family, "model_id": "M0_size_only", "description": "log(E)=alpha+beta n", "n_obs": fit0.n_obs, "n_params": fit0.n_params, "r2": fit0.r2, "adj_r2": fit0.adj_r2, "rss": fit0.rss},
        {"family": family, "model_id": "M1_network_level", "description": "add area/street/degree/circuity level block", "n_obs": fit1.n_obs, "n_params": fit1.n_params, "r2": fit1.r2, "adj_r2": fit1.adj_r2, "rss": fit1.rss},
        {"family": family, "model_id": "M2_network_beta", "description": "allow network block to modulate beta", "n_obs": fit2.n_obs, "n_params": fit2.n_params, "r2": fit2.r2, "adj_r2": fit2.adj_r2, "rss": fit2.rss},
        {"family": family, "model_id": "M3_network_plus_info_level", "description": "add information-level block", "n_obs": fit3.n_obs, "n_params": fit3.n_params, "r2": fit3.r2, "adj_r2": fit3.adj_r2, "rss": fit3.rss},
        {"family": family, "model_id": "M4_full_information_state", "description": "allow information block to modulate beta", "n_obs": fit4.n_obs, "n_params": fit4.n_params, "r2": fit4.r2, "adj_r2": fit4.adj_r2, "rss": fit4.rss},
    ]

    nested_rows = []
    for name, cmp in nested.items():
        nested_rows.append(
            {
                "family": family,
                "comparison": name,
                "f_stat": cmp.f_stat,
                "df_num": cmp.df_num,
                "df_den": cmp.df_den,
                "p_value": cmp.p_value,
                "rss_restricted": cmp.rss_restricted,
                "rss_full": cmp.rss_full,
            }
        )

    refs = {
        "N0": pop_ref,
        "A0": area_ref,
        "S0": street_ref,
        "K0": degree_ref,
        "C0": circuity_ref,
        "MI0": mi_ref,
        "within_mean": within_mean,
    }
    return {
        "family": family,
        "clean": clean,
        "refs": refs,
        "model_summary": model_summary,
        "coef_rows": coef_rows,
        "nested_rows": nested_rows,
        "city_params": city_params,
        "feature_rows": feature_rows,
        "correlations": correlations,
    }


def _persist(all_features, all_model_summary, all_coef_rows, all_city_params) -> None:
    _psql_exec(
        f"""
        DELETE FROM derived.city_spatial_information_state_features
        WHERE source_method = '{FEATURES_SOURCE_METHOD}';
        DELETE FROM derived.city_spatial_information_state_model_summary
        WHERE source_method = '{MODEL_SOURCE_METHOD}';
        DELETE FROM derived.city_spatial_information_state_model_coefficients
        WHERE source_method = '{MODEL_SOURCE_METHOD}';
        DELETE FROM derived.city_spatial_information_state_city_parameters
        WHERE source_method = '{MODEL_SOURCE_METHOD}';
        """
    )
    _copy_rows(
        "derived.city_spatial_information_state_features",
        [
            "source_file", "source_method", "regimes_source_method", "family", "city_code", "city_name",
            "state_code", "population", "est_total", "log_est_total", "mi_manzana_nats",
            "share_within_explained", "share_between_explained", "share_gap", "n_centered",
            "z_city_area_km2", "z_street_density_km_per_km2", "z_mean_degree", "z_circuity_avg",
            "z_log_mi_manzana", "z_share_within_explained", "notes",
        ],
        all_features,
    )
    _copy_rows(
        "derived.city_spatial_information_state_model_summary",
        ["source_file", "source_method", "family", "model_id", "description", "n_obs", "n_params", "r2", "adj_r2", "rss", "notes"],
        [
            {
                "source_file": "derived.city_spatial_information_state_features",
                "source_method": MODEL_SOURCE_METHOD,
                "notes": "",
                **row,
            }
            for row in all_model_summary
        ],
    )
    _copy_rows(
        "derived.city_spatial_information_state_model_coefficients",
        ["source_file", "source_method", "family", "model_id", "term", "coefficient", "stderr", "term_role", "notes"],
        [
            {
                "source_file": "derived.city_spatial_information_state_features",
                "source_method": MODEL_SOURCE_METHOD,
                "notes": "",
                **row,
            }
            for row in all_coef_rows
        ],
    )
    _copy_rows(
        "derived.city_spatial_information_state_city_parameters",
        [
            "source_file", "source_method", "family", "city_code", "city_name", "state_code", "population",
            "est_total", "alpha_eff", "beta_eff", "observed_logE", "predicted_logE", "residual_logE",
            "z_log_mi_manzana", "z_share_within_explained", "notes",
        ],
        all_city_params,
    )


def main() -> int:
    _bootstrap()
    OUTDIR.mkdir(parents=True, exist_ok=True)

    rows = _query_rows()
    family_results = [_fit_family(rows, family) for family in ("scian2", "size_class")]

    all_features = [row for fr in family_results for row in fr["feature_rows"]]
    all_model_summary = [row for fr in family_results for row in fr["model_summary"]]
    all_coef_rows = [row for fr in family_results for row in fr["coef_rows"]]
    all_nested_rows = [row for fr in family_results for row in fr["nested_rows"]]
    all_city_params = [row for fr in family_results for row in fr["city_params"]]
    all_corr_rows = [row for fr in family_results for row in fr["correlations"]]

    _persist(all_features, all_model_summary, all_coef_rows, all_city_params)

    _write_csv(OUTDIR / "feature_rows.csv", all_features, list(all_features[0].keys()))
    _write_csv(OUTDIR / "model_summary.csv", all_model_summary, list(all_model_summary[0].keys()))
    _write_csv(OUTDIR / "model_coefficients.csv", all_coef_rows, list(all_coef_rows[0].keys()))
    _write_csv(OUTDIR / "nested_tests.csv", all_nested_rows, list(all_nested_rows[0].keys()))
    _write_csv(OUTDIR / "city_state_parameters.csv", all_city_params, list(all_city_params[0].keys()))
    _write_csv(OUTDIR / "correlation_summary.csv", all_corr_rows, list(all_corr_rows[0].keys()))

    top_beta = sorted(all_city_params, key=lambda r: (str(r["family"]), -float(r["beta_eff"]), -float(r["population"])))
    bottom_beta = sorted(all_city_params, key=lambda r: (str(r["family"]), float(r["beta_eff"]), -float(r["population"])))
    _write_csv(OUTDIR / "top_beta_eff.csv", top_beta[:50], list(top_beta[0].keys()))
    _write_csv(OUTDIR / "bottom_beta_eff.csv", bottom_beta[:50], list(bottom_beta[0].keys()))

    lines = [
        "# Spatial Information State Equation",
        "",
        "## Purpose",
        "",
        "This phase extends the previous continuous city state equation by adding internal spatial-information variables derived from the `manzana -> AGEB -> city` decomposition.",
        "",
        "The goal is to test whether internal economic organization adds explanatory power beyond size and city network topology.",
        "",
        "## Variables",
        "",
        "For each city `i` and family `f`, we use:",
        "",
        "```math",
        "n_i = \\log(N_i / N_0)",
        "```",
        "",
        "```math",
        "a_i = \\log(A_i / A_0),\\quad s_i = \\log(S_i / S_0),\\quad k_i = \\log(K_i / K_0),\\quad c_i = \\log(C_i / C_0)",
        "```",
        "",
        "where `A = area`, `S = street density`, `K = mean degree`, `C = circuity`.",
        "",
        "We also add internal information coordinates:",
        "",
        "```math",
        "m_i = \\log(I_i(M;\\Lambda) / I_0)",
        "```",
        "",
        "```math",
        "q_i = \\text{share}_{within,i} - \\overline{\\text{share}_{within}}",
        "```",
        "",
        "where `share_within` is the fraction of explained spatial information that lives below the AGEB, at `manzana` scale.",
        "",
        f"Cities with `|share_gap| > {GAP_THRESHOLD:.2f}` were excluded from the main fits.",
        "",
        "## Model sequence",
        "",
        "```math",
        "M0: \\log E_i = \\alpha + \\beta n_i + \\varepsilon_i",
        "```",
        "",
        "```math",
        "M1: \\log E_i = \\alpha + \\beta n_i + \\gamma_a a_i + \\gamma_s s_i + \\gamma_k k_i + \\gamma_c c_i + \\varepsilon_i",
        "```",
        "",
        "```math",
        "M2: M1 + n_i(\\delta_a a_i + \\delta_s s_i + \\delta_k k_i + \\delta_c c_i)",
        "```",
        "",
        "```math",
        "M3: M2 + \\eta_m m_i + \\eta_q q_i",
        "```",
        "",
        "```math",
        "M4: M3 + n_i(\\psi_m m_i + \\psi_q q_i)",
        "```",
        "",
        "Interpretation:",
        "- `eta_*` shifts the level of the law with internal information.",
        "- `psi_*` lets internal information modulate the effective scaling exponent.",
        "",
    ]
    for fr in family_results:
        fam = fr["family"]
        refs = fr["refs"]
        ms = {row["model_id"]: row for row in fr["model_summary"]}
        nt = {row["comparison"]: row for row in fr["nested_rows"]}
        lines.extend(
            [
                f"## Family: {fam}",
                "",
                f"- sample size after filtering: `{int(ms['M4_full_information_state']['n_obs'])}` cities",
                f"- `N_0 = {refs['N0']:.3f}`",
                f"- `A_0 = {refs['A0']:.6f}`",
                f"- `S_0 = {refs['S0']:.6f}`",
                f"- `K_0 = {refs['K0']:.6f}`",
                f"- `C_0 = {refs['C0']:.6f}`",
                f"- `I_0 = {refs['MI0']:.6f}`",
                f"- mean `share_within_explained = {refs['within_mean']:.6f}`",
                "",
                f"- `M0 adjR² = {float(ms['M0_size_only']['adj_r2']):.4f}`",
                f"- `M1 adjR² = {float(ms['M1_network_level']['adj_r2']):.4f}`",
                f"- `M2 adjR² = {float(ms['M2_network_beta']['adj_r2']):.4f}`",
                f"- `M3 adjR² = {float(ms['M3_network_plus_info_level']['adj_r2']):.4f}`",
                f"- `M4 adjR² = {float(ms['M4_full_information_state']['adj_r2']):.4f}`",
                "",
                f"- `M0 -> M1` p = `{float(nt['M0_to_M1']['p_value']):.2e}`",
                f"- `M1 -> M2` p = `{float(nt['M1_to_M2']['p_value']):.2e}`",
                f"- `M2 -> M3` p = `{float(nt['M2_to_M3']['p_value']):.2e}`",
                f"- `M3 -> M4` p = `{float(nt['M3_to_M4']['p_value']):.2e}`",
                f"- `M2 -> M4` p = `{float(nt['M2_to_M4']['p_value']):.2e}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Files",
            f"- [feature_rows.csv]({(OUTDIR / 'feature_rows.csv').resolve()})",
            f"- [model_summary.csv]({(OUTDIR / 'model_summary.csv').resolve()})",
            f"- [model_coefficients.csv]({(OUTDIR / 'model_coefficients.csv').resolve()})",
            f"- [nested_tests.csv]({(OUTDIR / 'nested_tests.csv').resolve()})",
            f"- [city_state_parameters.csv]({(OUTDIR / 'city_state_parameters.csv').resolve()})",
            f"- [correlation_summary.csv]({(OUTDIR / 'correlation_summary.csv').resolve()})",
            f"- [top_beta_eff.csv]({(OUTDIR / 'top_beta_eff.csv').resolve()})",
            f"- [bottom_beta_eff.csv]({(OUTDIR / 'bottom_beta_eff.csv').resolve()})",
        ]
    )
    (OUTDIR / "monograph.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (OUTDIR / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
