#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
from pathlib import Path

from urban_sami.artifacts.figures import (
    write_model_overview_figure,
    write_residual_histogram_figure,
    write_scaling_scatter_figure,
)
from urban_sami.modeling import compute_deviation_score, fit_by_name


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"


def _query(sql: str) -> list[dict[str, str]]:
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
        city_code, city_name, state_code, est_count, population, households = line.split("\t")
        rows.append(
            {
                "city_code": city_code,
                "city_name": city_name,
                "state_code": state_code,
                "est_count": est_count,
                "population": population,
                "households": households,
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Run an independent city-level DENUE vs population baseline from urban_sami_exp")
    parser.add_argument("--output-dir", type=Path, default=Path("dist/independent_city_baseline"))
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = _query(
        """
        WITH denue AS (
            SELECT city_code, MAX(city_name) AS city_name, MAX(state_code) AS state_code, COUNT(*)::double precision AS est_count
            FROM raw.denue_establishments
            WHERE country_code = 'MX' AND city_code <> ''
            GROUP BY city_code
        )
        SELECT p.unit_code AS city_code,
               p.unit_label AS city_name,
               SUBSTRING(p.unit_code FROM 1 FOR 2) AS state_code,
               COALESCE(denue.est_count, 0)::text AS est_count,
               COALESCE(p.population, 0)::text AS population,
               COALESCE(p.households, 0)::text AS households
        FROM raw.population_units AS p
        LEFT JOIN denue ON denue.city_code = p.unit_code
        WHERE p.level = 'city' AND p.country_code = 'MX'
        ORDER BY p.unit_code
        """.strip()
    )

    counts_path = args.output_dir / "city_counts.csv"
    with counts_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["city_code", "city_name", "state_code", "est_count", "population", "households"])
        writer.writeheader()
        writer.writerows(rows)

    fit_rows = [row for row in rows if float(row["population"]) > 0.0 and float(row["est_count"]) > 0.0]
    y = [float(row["est_count"]) for row in fit_rows]
    n = [float(row["population"]) for row in fit_rows]
    summary_rows = []
    for method in ("ols", "robust", "poisson", "negbin", "auto"):
        fit = fit_by_name(y, n, method)
        summary_rows.append(
            {
                "level": "city",
                "fit_method": method,
                "n_obs": len(fit_rows),
                "alpha": fit.alpha,
                "beta": fit.beta,
                "r2": fit.r2,
                "resid_std": fit.residual_std,
            }
        )

    summary_path = args.output_dir / "model_summary.csv"
    with summary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["level", "fit_method", "n_obs", "alpha", "beta", "r2", "resid_std"])
        writer.writeheader()
        writer.writerows(summary_rows)

    zero_est_count = sum(1 for row in rows if float(row["est_count"]) <= 0.0)
    overview_path = write_model_overview_figure(summary_rows, args.output_dir / "model_overview.svg", title="Independent City Baseline")
    best_row = max(summary_rows, key=lambda row: float(row["r2"]))
    residual_rows = []
    residual_values = []
    fit_alpha = float(best_row["alpha"])
    fit_beta = float(best_row["beta"])
    for row in fit_rows:
        population = float(row["population"])
        est_count = float(row["est_count"])
        score = compute_deviation_score(est_count, population, fit_alpha, fit_beta, float(best_row["resid_std"]))
        residual_values.append(score.epsilon_log)
        residual_rows.append(
            {
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "state_code": row["state_code"],
                "population": population,
                "est_count": est_count,
                "households": float(row["households"]),
                "y_expected": score.y_expected,
                "epsilon_log": score.epsilon_log,
                "sami": score.sami,
                "z_residual": score.z_residual,
            }
        )
    scatter_path = write_scaling_scatter_figure(
        fit_rows,
        args.output_dir / "scaling_scatter.svg",
        title="Mexico Municipalities: DENUE Establishments vs Population",
        x_key="population",
        y_key="est_count",
        fit_alpha=float(best_row["alpha"]),
        fit_beta=float(best_row["beta"]),
        annotation=f"{best_row['fit_method']}  β={float(best_row['beta']):.3f}  R²={float(best_row['r2']):.3f}",
    )
    residuals_path = args.output_dir / "residuals.csv"
    with residuals_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["city_code", "city_name", "state_code", "population", "est_count", "households", "y_expected", "epsilon_log", "sami", "z_residual"])
        writer.writeheader()
        writer.writerows(residual_rows)
    residual_hist_path = write_residual_histogram_figure(
        residual_values,
        args.output_dir / "residual_histogram.svg",
        title="City-level residual distribution",
        subtitle="Log residuals around the best-fitting municipal scaling model",
        bins=28,
    )
    report = {
        "workflow_id": "independent_city_baseline",
        "input_rows_total": len(rows),
        "input_rows_fit": len(fit_rows),
        "zero_establishment_rows": zero_est_count,
        "counts_csv": str(counts_path.resolve()),
        "summary_csv": str(summary_path.resolve()),
        "overview_svg": str(overview_path.resolve()),
        "scatter_svg": str(scatter_path.resolve()),
        "residuals_csv": str(residuals_path.resolve()),
        "residual_histogram_svg": str(residual_hist_path.resolve()),
        "sami_definition": "sami_raw_log_deviation",
        "standardized_score_field": "z_residual",
        "best_fit_by_r2": best_row,
    }
    report_path = args.output_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
