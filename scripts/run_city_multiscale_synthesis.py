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

COARSE_SOURCE_METHOD = "bettencourt_coarse_graining_laws_v1"
REGIME_SOURCE_METHOD = "bettencourt_spatial_information_regimes_v1"
FEATURES_SOURCE_METHOD = "bettencourt_multiscale_synthesis_features_v1"
MODEL_SOURCE_METHOD = "bettencourt_multiscale_synthesis_models_v1"
THRESHOLD_UNITS = 10
GAP_THRESHOLD = 0.10
OUTDIR = ROOT / "reports" / "city-multiscale-synthesis-2026-04-25"


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
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _geom_mean(values: list[float]) -> float:
    return math.exp(sum(math.log(v) for v in values) / float(len(values)))


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2 == 1:
        return ordered[mid]
    return 0.5 * (ordered[mid - 1] + ordered[mid])


def _query_rows() -> list[dict[str, object]]:
    rows = _psql_copy(
        f"""
        SELECT
            r.family,
            r.city_code,
            r.city_name,
            r.state_code,
            p.population::text,
            p.est_total::text,
            p.n_manzana::text,
            p.n_ageb::text,
            p.beta_manzana::text,
            p.beta_ageb::text,
            p.delta_beta_ageb_minus_manzana::text AS delta_beta,
            p.r2_manzana::text,
            p.r2_ageb::text,
            p.delta_r2_ageb_minus_manzana::text AS delta_r2,
            p.resid_std_manzana::text,
            p.resid_std_ageb::text,
            p.delta_resid_std_ageb_minus_manzana::text AS delta_resid_std,
            r.mi_manzana_nats::text,
            r.mi_ageb_nats::text,
            r.mi_within_ageb_nats::text,
            r.share_between_explained::text,
            r.share_within_explained::text,
            r.share_gap::text
        FROM derived.city_coarse_graining_paths p
        JOIN derived.city_spatial_information_regimes r
          ON r.source_method = '{REGIME_SOURCE_METHOD}'
         AND r.city_code = p.city_code
        WHERE p.source_method = '{COARSE_SOURCE_METHOD}'
          AND p.n_manzana >= {THRESHOLD_UNITS}
          AND p.n_ageb >= {THRESHOLD_UNITS}
          AND abs(COALESCE(r.share_gap, 0.0)) <= {GAP_THRESHOLD}
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
                "n_manzana": _safe_float(row["n_manzana"]),
                "n_ageb": _safe_float(row["n_ageb"]),
                "beta_manzana": _safe_float(row["beta_manzana"]),
                "beta_ageb": _safe_float(row["beta_ageb"]),
                "delta_beta": _safe_float(row["delta_beta"]),
                "r2_manzana": _safe_float(row["r2_manzana"]),
                "r2_ageb": _safe_float(row["r2_ageb"]),
                "delta_r2": _safe_float(row["delta_r2"]),
                "resid_std_manzana": _safe_float(row["resid_std_manzana"]),
                "resid_std_ageb": _safe_float(row["resid_std_ageb"]),
                "delta_resid_std": _safe_float(row["delta_resid_std"]),
                "mi_manzana_nats": _safe_float(row["mi_manzana_nats"]),
                "mi_ageb_nats": _safe_float(row["mi_ageb_nats"]),
                "mi_within_ageb_nats": _safe_float(row["mi_within_ageb_nats"]),
                "share_between_explained": _safe_float(row["share_between_explained"]),
                "share_within_explained": _safe_float(row["share_within_explained"]),
                "share_gap": _safe_float(row["share_gap"]),
            }
        )
    return out


def _prepare_tables() -> None:
    _psql_exec(
        f"""
        DELETE FROM derived.city_multiscale_synthesis_features
        WHERE source_method = '{FEATURES_SOURCE_METHOD}';
        DELETE FROM derived.city_multiscale_synthesis_model_summary
        WHERE source_method = '{MODEL_SOURCE_METHOD}';
        DELETE FROM derived.city_multiscale_synthesis_model_coefficients
        WHERE source_method = '{MODEL_SOURCE_METHOD}';
        DELETE FROM derived.city_multiscale_synthesis_quadrants
        WHERE source_method = '{MODEL_SOURCE_METHOD}';
        """
    )


def _fit_family(rows: list[dict[str, object]], family: str) -> dict[str, object]:
    clean = [row for row in rows if row["family"] == family and row["population"] > 0 and row["mi_manzana_nats"] > 0]
    pop_ref = _geom_mean([row["population"] for row in clean])
    mi_ref = _geom_mean([row["mi_manzana_nats"] for row in clean])
    within_mean = _mean([row["share_within_explained"] for row in clean])
    delta_r2_mean = _median([row["delta_r2"] for row in clean])

    for row in clean:
        row["n_centered"] = math.log(row["population"] / pop_ref)
        row["z_log_mi_manzana"] = math.log(row["mi_manzana_nats"] / mi_ref)
        row["z_share_within_explained"] = row["share_within_explained"] - within_mean
        row["resid_improvement"] = -row["delta_resid_std"]
        row["delta_r2_centered"] = row["delta_r2"] - delta_r2_mean
        row["share_within_centered"] = row["share_within_explained"] - within_mean
        row["quadrant"] = (
            "high_within_strong_cg"
            if row["delta_r2_centered"] >= 0 and row["share_within_centered"] >= 0
            else "low_within_strong_cg"
            if row["delta_r2_centered"] >= 0 and row["share_within_centered"] < 0
            else "high_within_weak_cg"
            if row["delta_r2_centered"] < 0 and row["share_within_centered"] >= 0
            else "low_within_weak_cg"
        )

    feature_rows = [
        {
            "source_file": "derived.city_coarse_graining_paths+derived.city_spatial_information_regimes",
            "source_method": FEATURES_SOURCE_METHOD,
            "coarse_source_method": COARSE_SOURCE_METHOD,
            "regime_source_method": REGIME_SOURCE_METHOD,
            "family": family,
            "city_code": row["city_code"],
            "city_name": row["city_name"],
            "state_code": row["state_code"],
            "threshold_units": THRESHOLD_UNITS,
            "population": row["population"],
            "est_total": row["est_total"],
            "n_manzana": row["n_manzana"],
            "n_ageb": row["n_ageb"],
            "beta_manzana": row["beta_manzana"],
            "beta_ageb": row["beta_ageb"],
            "delta_beta": row["delta_beta"],
            "r2_manzana": row["r2_manzana"],
            "r2_ageb": row["r2_ageb"],
            "delta_r2": row["delta_r2"],
            "resid_std_manzana": row["resid_std_manzana"],
            "resid_std_ageb": row["resid_std_ageb"],
            "delta_resid_std": row["delta_resid_std"],
            "mi_manzana_nats": row["mi_manzana_nats"],
            "mi_ageb_nats": row["mi_ageb_nats"],
            "mi_within_ageb_nats": row["mi_within_ageb_nats"],
            "share_between_explained": row["share_between_explained"],
            "share_within_explained": row["share_within_explained"],
            "share_gap": row["share_gap"],
            "n_centered": row["n_centered"],
            "z_log_mi_manzana": row["z_log_mi_manzana"],
            "z_share_within_explained": row["z_share_within_explained"],
            "notes": "",
        }
        for row in clean
    ]

    quadrants = [
        {
            "source_file": "derived.city_multiscale_synthesis_features",
            "source_method": MODEL_SOURCE_METHOD,
            "family": family,
            "city_code": row["city_code"],
            "city_name": row["city_name"],
            "state_code": row["state_code"],
            "threshold_units": THRESHOLD_UNITS,
            "delta_r2": row["delta_r2"],
            "share_within_explained": row["share_within_explained"],
            "delta_r2_centered": row["delta_r2_centered"],
            "share_within_centered": row["share_within_centered"],
            "quadrant": row["quadrant"],
            "notes": "",
        }
        for row in clean
    ]

    outcomes = {
        "delta_r2": {
            "label": "AGEB-minus-manzana fit gain",
            "y": [row["delta_r2"] for row in clean],
        },
        "delta_beta": {
            "label": "AGEB-minus-manzana slope gain",
            "y": [row["delta_beta"] for row in clean],
        },
        "resid_improvement": {
            "label": "Residual-dispersion improvement",
            "y": [row["resid_improvement"] for row in clean],
        },
    }

    x_n = [row["n_centered"] for row in clean]
    x_mi = [row["z_log_mi_manzana"] for row in clean]
    x_within = [row["z_share_within_explained"] for row in clean]

    model_summaries: list[dict[str, object]] = []
    model_coefficients: list[dict[str, object]] = []
    nested_rows: list[dict[str, object]] = []
    correlation_rows: list[dict[str, object]] = []

    for outcome, meta in outcomes.items():
        y = meta["y"]
        designs = {
            "M0_intercept": [[1.0] for _ in clean],
            "M1_information": [[1.0, mi, within] for mi, within in zip(x_mi, x_within)],
            "M2_information_size": [[1.0, mi, within, n] for mi, within, n in zip(x_mi, x_within, x_n)],
            "M3_full_interaction": [
                [1.0, mi, within, n, mi * n, within * n]
                for mi, within, n in zip(x_mi, x_within, x_n)
            ],
        }
        descriptions = {
            "M0_intercept": "Outcome mean only.",
            "M1_information": "Internal information level block.",
            "M2_information_size": "Information plus city size.",
            "M3_full_interaction": "Information, size, and size-information interactions.",
        }
        terms = {
            "M0_intercept": ["intercept"],
            "M1_information": ["intercept", "eta_log_mi", "eta_within_share"],
            "M2_information_size": ["intercept", "eta_log_mi", "eta_within_share", "eta_size"],
            "M3_full_interaction": [
                "intercept",
                "eta_log_mi",
                "eta_within_share",
                "eta_size",
                "psi_log_mi_x_size",
                "psi_within_x_size",
            ],
        }

        fits: dict[str, object] = {}
        for model_id in ("M0_intercept", "M1_information", "M2_information_size", "M3_full_interaction"):
            fit = ols_fit(designs[model_id], y)
            fits[model_id] = fit
            model_summaries.append(
                {
                    "source_file": "derived.city_multiscale_synthesis_features",
                    "source_method": MODEL_SOURCE_METHOD,
                    "family": family,
                    "outcome": outcome,
                    "model_id": model_id,
                    "description": descriptions[model_id],
                    "n_obs": fit.n_obs,
                    "n_params": fit.n_params,
                    "r2": fit.r2,
                    "adj_r2": fit.adj_r2,
                    "rss": fit.rss,
                    "notes": "",
                }
            )
            for term, coef, stderr in zip(terms[model_id], fit.coefficients, fit.stderr):
                model_coefficients.append(
                    {
                        "source_file": "derived.city_multiscale_synthesis_features",
                        "source_method": MODEL_SOURCE_METHOD,
                        "family": family,
                        "outcome": outcome,
                        "model_id": model_id,
                        "term": term,
                        "coefficient": coef,
                        "stderr": stderr,
                        "term_role": "intercept" if term == "intercept" else "feature",
                        "notes": "",
                    }
                )

        for restricted, full in (
            ("M0_intercept", "M1_information"),
            ("M1_information", "M2_information_size"),
            ("M2_information_size", "M3_full_interaction"),
            ("M0_intercept", "M3_full_interaction"),
        ):
            cmp = compare_nested_models(fits[restricted], fits[full])
            nested_rows.append(
                {
                    "family": family,
                    "outcome": outcome,
                    "comparison": f"{restricted}_to_{full}",
                    "f_stat": cmp.f_stat,
                    "df_num": cmp.df_num,
                    "df_den": cmp.df_den,
                    "p_value": cmp.p_value if cmp.p_value is not None else "",
                }
            )

        for name, values in (
            ("corr_delta_r2_vs_within", x_within if outcome == "delta_r2" else None),
            ("corr_delta_r2_vs_log_mi", x_mi if outcome == "delta_r2" else None),
            ("corr_delta_r2_vs_size", x_n if outcome == "delta_r2" else None),
            ("corr_delta_beta_vs_within", x_within if outcome == "delta_beta" else None),
            ("corr_delta_beta_vs_log_mi", x_mi if outcome == "delta_beta" else None),
            ("corr_resid_gain_vs_within", x_within if outcome == "resid_improvement" else None),
        ):
            if values is None:
                continue
            correlation_rows.append(
                {
                    "family": family,
                    "name": name,
                    "value": pearson_corr(values, y),
                }
            )

    quadrant_counts: dict[str, int] = {}
    for row in quadrants:
        key = str(row["quadrant"])
        quadrant_counts[key] = quadrant_counts.get(key, 0) + 1

    return {
        "family": family,
        "rows": clean,
        "feature_rows": feature_rows,
        "quadrants": quadrants,
        "model_summaries": model_summaries,
        "model_coefficients": model_coefficients,
        "nested_rows": nested_rows,
        "correlation_rows": correlation_rows,
        "quadrant_counts": quadrant_counts,
        "within_mean": within_mean,
        "delta_r2_median": delta_r2_mean,
    }


def main() -> None:
    _bootstrap()
    _prepare_tables()
    OUTDIR.mkdir(parents=True, exist_ok=True)

    base_rows = _query_rows()
    families = sorted({str(row["family"]) for row in base_rows})

    all_feature_rows: list[dict[str, object]] = []
    all_quadrants: list[dict[str, object]] = []
    all_model_summaries: list[dict[str, object]] = []
    all_model_coefficients: list[dict[str, object]] = []
    all_nested_rows: list[dict[str, object]] = []
    all_correlation_rows: list[dict[str, object]] = []
    family_reports: list[dict[str, object]] = []
    top_strong_rows: list[dict[str, object]] = []
    top_within_rows: list[dict[str, object]] = []

    for family in families:
        out = _fit_family(base_rows, family)
        all_feature_rows.extend(out["feature_rows"])
        all_quadrants.extend(out["quadrants"])
        all_model_summaries.extend(out["model_summaries"])
        all_model_coefficients.extend(out["model_coefficients"])
        all_nested_rows.extend(out["nested_rows"])
        all_correlation_rows.extend(out["correlation_rows"])
        family_reports.append(
            {
                "family": family,
                "n_obs": len(out["rows"]),
                "mean_delta_r2": _mean([row["delta_r2"] for row in out["rows"]]),
                "mean_delta_beta": _mean([row["delta_beta"] for row in out["rows"]]),
                "mean_resid_improvement": _mean([row["resid_improvement"] for row in out["rows"]]),
                "mean_share_within_explained": _mean([row["share_within_explained"] for row in out["rows"]]),
                "mean_log_mi_manzana": _mean([row["z_log_mi_manzana"] for row in out["rows"]]),
                "within_mean": out["within_mean"],
                "delta_r2_median": out["delta_r2_median"],
                "quadrant_high_within_strong_cg": out["quadrant_counts"].get("high_within_strong_cg", 0),
                "quadrant_low_within_strong_cg": out["quadrant_counts"].get("low_within_strong_cg", 0),
                "quadrant_high_within_weak_cg": out["quadrant_counts"].get("high_within_weak_cg", 0),
                "quadrant_low_within_weak_cg": out["quadrant_counts"].get("low_within_weak_cg", 0),
            }
        )
        top_strong_rows.extend(
            sorted(out["rows"], key=lambda row: row["delta_r2"], reverse=True)[:20]
        )
        top_within_rows.extend(
            sorted(out["rows"], key=lambda row: row["share_within_explained"], reverse=True)[:20]
        )

    _copy_rows(
        "derived.city_multiscale_synthesis_features",
        [
            "source_file",
            "source_method",
            "coarse_source_method",
            "regime_source_method",
            "family",
            "city_code",
            "city_name",
            "state_code",
            "threshold_units",
            "population",
            "est_total",
            "n_manzana",
            "n_ageb",
            "beta_manzana",
            "beta_ageb",
            "delta_beta",
            "r2_manzana",
            "r2_ageb",
            "delta_r2",
            "resid_std_manzana",
            "resid_std_ageb",
            "delta_resid_std",
            "mi_manzana_nats",
            "mi_ageb_nats",
            "mi_within_ageb_nats",
            "share_between_explained",
            "share_within_explained",
            "share_gap",
            "n_centered",
            "z_log_mi_manzana",
            "z_share_within_explained",
            "notes",
        ],
        all_feature_rows,
    )
    _copy_rows(
        "derived.city_multiscale_synthesis_model_summary",
        [
            "source_file",
            "source_method",
            "family",
            "outcome",
            "model_id",
            "description",
            "n_obs",
            "n_params",
            "r2",
            "adj_r2",
            "rss",
            "notes",
        ],
        all_model_summaries,
    )
    _copy_rows(
        "derived.city_multiscale_synthesis_model_coefficients",
        [
            "source_file",
            "source_method",
            "family",
            "outcome",
            "model_id",
            "term",
            "coefficient",
            "stderr",
            "term_role",
            "notes",
        ],
        all_model_coefficients,
    )
    _copy_rows(
        "derived.city_multiscale_synthesis_quadrants",
        [
            "source_file",
            "source_method",
            "family",
            "city_code",
            "city_name",
            "state_code",
            "threshold_units",
            "delta_r2",
            "share_within_explained",
            "delta_r2_centered",
            "share_within_centered",
            "quadrant",
            "notes",
        ],
        all_quadrants,
    )

    _write_csv(
        OUTDIR / "feature_rows.csv",
        all_feature_rows,
        [
            "family",
            "city_code",
            "city_name",
            "state_code",
            "threshold_units",
            "population",
            "est_total",
            "n_manzana",
            "n_ageb",
            "beta_manzana",
            "beta_ageb",
            "delta_beta",
            "r2_manzana",
            "r2_ageb",
            "delta_r2",
            "resid_std_manzana",
            "resid_std_ageb",
            "delta_resid_std",
            "mi_manzana_nats",
            "mi_ageb_nats",
            "mi_within_ageb_nats",
            "share_between_explained",
            "share_within_explained",
            "share_gap",
            "n_centered",
            "z_log_mi_manzana",
            "z_share_within_explained",
        ],
    )
    _write_csv(
        OUTDIR / "family_summary.csv",
        family_reports,
        [
            "family",
            "n_obs",
            "mean_delta_r2",
            "mean_delta_beta",
            "mean_resid_improvement",
            "mean_share_within_explained",
            "mean_log_mi_manzana",
            "within_mean",
            "delta_r2_median",
            "quadrant_high_within_strong_cg",
            "quadrant_low_within_strong_cg",
            "quadrant_high_within_weak_cg",
            "quadrant_low_within_weak_cg",
        ],
    )
    _write_csv(
        OUTDIR / "model_summary.csv",
        all_model_summaries,
        ["family", "outcome", "model_id", "description", "n_obs", "n_params", "r2", "adj_r2", "rss"],
    )
    _write_csv(
        OUTDIR / "model_coefficients.csv",
        all_model_coefficients,
        ["family", "outcome", "model_id", "term", "coefficient", "stderr", "term_role"],
    )
    _write_csv(
        OUTDIR / "nested_tests.csv",
        all_nested_rows,
        ["family", "outcome", "comparison", "f_stat", "df_num", "df_den", "p_value"],
    )
    _write_csv(
        OUTDIR / "correlation_summary.csv",
        all_correlation_rows,
        ["family", "name", "value"],
    )
    _write_csv(
        OUTDIR / "quadrant_rows.csv",
        all_quadrants,
        ["family", "city_code", "city_name", "state_code", "threshold_units", "delta_r2", "share_within_explained", "delta_r2_centered", "share_within_centered", "quadrant"],
    )
    _write_csv(
        OUTDIR / "top_strong_cg.csv",
        sorted(top_strong_rows, key=lambda row: (row["family"], -row["delta_r2"])),
        [
            "family", "city_code", "city_name", "state_code", "population", "est_total",
            "n_manzana", "n_ageb", "delta_r2", "delta_beta", "share_within_explained",
            "mi_manzana_nats", "beta_manzana", "beta_ageb", "r2_manzana", "r2_ageb",
        ],
    )
    _write_csv(
        OUTDIR / "top_within_share.csv",
        sorted(top_within_rows, key=lambda row: (row["family"], -row["share_within_explained"])),
        [
            "family", "city_code", "city_name", "state_code", "population", "est_total",
            "n_manzana", "n_ageb", "share_within_explained", "delta_r2", "delta_beta",
            "mi_manzana_nats", "beta_manzana", "beta_ageb", "r2_manzana", "r2_ageb",
        ],
    )

    family_lookup = {row["family"]: row for row in family_reports}
    summary_lookup = {(row["family"], row["outcome"], row["model_id"]): row for row in all_model_summaries}
    nested_lookup = {(row["family"], row["outcome"], row["comparison"]): row for row in all_nested_rows}

    report_lines = ["# City Multiscale Synthesis", ""]
    for family in sorted(family_lookup):
        fam = family_lookup[family]
        m3_r2 = float(summary_lookup[(family, "delta_r2", "M3_full_interaction")]["adj_r2"])
        m1_r2 = float(summary_lookup[(family, "delta_r2", "M1_information")]["adj_r2"])
        p_m0_m1 = nested_lookup[(family, "delta_r2", "M0_intercept_to_M1_information")]["p_value"]
        p_m2_m3 = nested_lookup[(family, "delta_r2", "M2_information_size_to_M3_full_interaction")]["p_value"]
        report_lines.extend(
            [
                f"## Family: {family}",
                "",
                f"- sample: `{int(fam['n_obs'])}` cities",
                f"- mean `delta R²`: `{float(fam['mean_delta_r2']):.4f}`",
                f"- mean `delta beta`: `{float(fam['mean_delta_beta']):.4f}`",
                f"- mean `share_within_explained`: `{float(fam['mean_share_within_explained']):.4f}`",
                f"- `delta R²` model `M1 adjR² = {m1_r2:.4f}`",
                f"- `delta R²` model `M3 adjR² = {m3_r2:.4f}`",
                f"- `M0 -> M1` p = `{p_m0_m1}`",
                f"- `M2 -> M3` p = `{p_m2_m3}`",
                "",
            ]
        )
    (OUTDIR / "report.md").write_text("\n".join(report_lines), encoding="utf-8")

    monograph_lines = [
        "# Bettencourt Multiscale Synthesis",
        "",
        "## Question",
        "",
        "The previous two phases gave us two complementary results:",
        "",
        "1. coarse-graining from `manzana` to `AGEB` makes the population-establishment law much stronger;",
        "2. most of the internal economic information lives below the `AGEB`, at `manzana` scale.",
        "",
        "This synthesis asks whether these are the same phenomenon.",
        "",
        "In other words: does the law emerge under aggregation precisely because aggregation absorbs sub-AGEB economic heterogeneity?",
        "",
        "## Variables",
        "",
        "For each city `i`, define the coarse-graining gains:",
        "",
        "```math",
        "\\Delta R_i^2 = R^2_{i,AGEB} - R^2_{i,manzana}",
        "```",
        "",
        "```math",
        "\\Delta \\beta_i = \\beta_{i,AGEB} - \\beta_{i,manzana}",
        "```",
        "",
        "```math",
        "\\Delta \\sigma_i = \\sigma_{i,AGEB} - \\sigma_{i,manzana}.",
        "```",
        "",
        "We also define residual-dispersion improvement as:",
        "",
        "```math",
        "\\Delta \\sigma_i^{+} = -\\Delta \\sigma_i.",
        "```",
        "",
        "A positive value means coarse-graining reduces residual spread.",
        "",
        "From the information decomposition we use:",
        "",
        "```math",
        "m_i = \\log\\left(\\frac{I_i(M;\\Lambda)}{I_0}\\right)",
        "```",
        "",
        "```math",
        "q_i = \\text{share}_{within,i} - \\overline{\\text{share}_{within}}.",
        "```",
        "",
        "Interpretation:",
        "- `m_i` says how much spatial economic information the city contains at `manzana` scale",
        "- `q_i` says whether that information lives more or less below the `AGEB` than usual",
        "",
        "We also use centered city size:",
        "",
        "```math",
        "n_i = \\log(N_i/N_0).",
        "```",
        "",
        "## Models",
        "",
        "For each outcome `Y_i^* \\in \\{\\Delta R_i^2, \\Delta \\beta_i, \\Delta \\sigma_i^{+}\\}`, we fit:",
        "",
        "```math",
        "M0: Y_i^* = \\alpha + \\varepsilon_i",
        "```",
        "",
        "```math",
        "M1: Y_i^* = \\alpha + \\eta_m m_i + \\eta_q q_i + \\varepsilon_i",
        "```",
        "",
        "```math",
        "M2: Y_i^* = \\alpha + \\eta_m m_i + \\eta_q q_i + \\eta_n n_i + \\varepsilon_i",
        "```",
        "",
        "```math",
        "M3: Y_i^* = \\alpha + \\eta_m m_i + \\eta_q q_i + \\eta_n n_i + \\psi_m m_i n_i + \\psi_q q_i n_i + \\varepsilon_i",
        "```",
        "",
        "So we test, in order:",
        "- whether internal information matters at all,",
        "- whether city size adds explanatory power,",
        "- whether the effect of information changes with size.",
        "",
        "## Regime quadrants",
        "",
        "To make the result readable, we also classify cities by the signs of:",
        "",
        "```math",
        "\\Delta R_i^2 - \\operatorname{median}(\\Delta R^2)",
        "```",
        "",
        "and",
        "",
        "```math",
        "\\text{share}_{within,i} - \\overline{\\text{share}_{within}}.",
        "```",
        "",
        "This gives four descriptive quadrants:",
        "- `high_within_strong_cg`",
        "- `low_within_strong_cg`",
        "- `high_within_weak_cg`",
        "- `low_within_weak_cg`",
        "",
        "Interpretation:",
        "- `high_within_strong_cg`: cities where much structure lives below the AGEB and aggregation strongly strengthens the law",
        "- `high_within_weak_cg`: cities where sub-AGEB structure is strong but aggregation helps less",
        "",
        "## Files",
        f"- [report.md]({(OUTDIR / 'report.md').resolve()})",
        f"- [family_summary.csv]({(OUTDIR / 'family_summary.csv').resolve()})",
        f"- [model_summary.csv]({(OUTDIR / 'model_summary.csv').resolve()})",
        f"- [model_coefficients.csv]({(OUTDIR / 'model_coefficients.csv').resolve()})",
        f"- [nested_tests.csv]({(OUTDIR / 'nested_tests.csv').resolve()})",
        f"- [correlation_summary.csv]({(OUTDIR / 'correlation_summary.csv').resolve()})",
        f"- [quadrant_rows.csv]({(OUTDIR / 'quadrant_rows.csv').resolve()})",
        f"- [top_strong_cg.csv]({(OUTDIR / 'top_strong_cg.csv').resolve()})",
        f"- [top_within_share.csv]({(OUTDIR / 'top_within_share.csv').resolve()})",
        "",
        "## Persistence",
        "",
        "This synthesis is persisted in:",
        "- `derived.city_multiscale_synthesis_features`",
        "- `derived.city_multiscale_synthesis_model_summary`",
        "- `derived.city_multiscale_synthesis_model_coefficients`",
        "- `derived.city_multiscale_synthesis_quadrants`",
        "",
        "So the Bettencourt multiscale synthesis is now part of the persistent workflow, not an external notebook result.",
        "",
    ]
    (OUTDIR / "monograph.md").write_text("\n".join(monograph_lines), encoding="utf-8")

    print("done", flush=True)


if __name__ == "__main__":
    main()
